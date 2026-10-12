"""
Schema Validator for Ingestion Layer
Validates parquet files against database schemas and manages template-based table creation.
"""
import polars as pl
from pathlib import Path
from typing import Dict, List, Set, Optional, Any, Tuple
from dataclasses import dataclass
from shared.observability import get_logger
from shared.config import app_settings

logger = get_logger(__name__)


@dataclass
class ValidationResult:
    """Result of schema validation."""
    valid: bool
    missing_columns: List[str]
    extra_columns: List[str]
    type_mismatches: List[str]
    errors: List[str]


@dataclass
class SchemaDefinition:
    """Database schema definition."""
    name: str
    columns: Dict[str, str]  # column_name -> db_type
    primary_key: List[str]
    partition_key: Optional[str] = None
    order_by: Optional[List[str]] = None
    engine: Optional[str] = None
    is_fno: bool = False


class SchemaValidator:
    """Validates parquet files against expected database schemas."""
    
    # Polars to ClickHouse type mapping
    POLARS_TO_CH = {
        pl.Datetime: "DateTime64(3, 'Asia/Kolkata')",
        pl.Date: "Date",
        pl.String: "String",
        pl.Utf8: "String",
        pl.Float64: "Float64",
        pl.Float32: "Float32",
        pl.Int64: "Int64",
        pl.Int32: "Int32",
        pl.UInt64: "UInt64",
        pl.UInt32: "UInt32",
        pl.Boolean: "UInt8",
    }
    
    # Polars to PostgreSQL type mapping
    POLARS_TO_PG = {
        pl.Datetime: "TIMESTAMPTZ",
        pl.Date: "DATE",
        pl.String: "TEXT",
        pl.Utf8: "TEXT",
        pl.Float64: "DOUBLE PRECISION",
        pl.Float32: "REAL",
        pl.Int64: "BIGINT",
        pl.Int32: "INTEGER",
        pl.UInt64: "BIGINT",
        pl.UInt32: "INTEGER",
        pl.Boolean: "BOOLEAN",
    }

    def __init__(self, target_db: str = "clickhouse"):
        self.target_db = target_db.lower()
        self.type_map = self.POLARS_TO_CH if self.target_db == "clickhouse" else self.POLARS_TO_PG
        self.template_dir = Path(__file__).parent / "schemas"

    def get_template_path(self, is_fno: bool) -> Path:
        """Get the SQL template path for the given segment type."""
        segment_type = "fo" if is_fno else "equity"
        return self.template_dir / f"{segment_type}_{self.target_db}.sql"

    def load_template(self, is_fno: bool) -> str:
        """Load SQL template for table creation."""
        template_path = self.get_template_path(is_fno)
        if not template_path.exists():
            raise FileNotFoundError(f"Template not found: {template_path}")
        return template_path.read_text()

    def infer_schema_from_parquet(self, parquet_path: Path) -> Dict[str, pl.DataType]:
        """Infer schema from parquet file."""
        try:
            # Read only schema (no data)
            schema = pl.read_parquet_schema(parquet_path)
            return dict(schema)
        except Exception as e:
            logger.error(f"Failed to read parquet schema: {e}")
            raise

    def get_expected_schema(self, is_fno: bool) -> SchemaDefinition:
        """Get expected schema definition for F&O or Equity."""
        if is_fno:
            columns = {
                "timestamp": "DateTime64(3, 'Asia/Kolkata')",
                "symbol": "String",
                "underlying": "String",
                "expiry": "Date",
                "strike": "Float64",
                "instrument_type": "LowCardinality(String)",
                "open": "Float64",
                "high": "Float64",
                "low": "Float64",
                "close": "Float64",
                "volume": "UInt64",
                "open_interest": "Float64",
                "exchange": "LowCardinality(String)",
                "strike_price": "Float64",
                "lot_size": "UInt32",
                "tick_size": "Float64",
            }
        else:
            columns = {
                "timestamp": "DateTime64(3, 'Asia/Kolkata')",
                "symbol": "String",
                "open": "Float64",
                "high": "Float64",
                "low": "Float64",
                "close": "Float64",
                "volume": "UInt64",
                "open_interest": "Float64",
                "exchange": "LowCardinality(String)",
                "lot_size": "UInt32",
                "tick_size": "Float64",
            }
        
        return SchemaDefinition(
            name=f"market_data_{'fno' if is_fno else 'equity'}",
            columns=columns,
            primary_key=["timestamp", "symbol"] + (["underlying", "expiry", "strike", "instrument_type"] if is_fno else []),
            partition_key="toYYYYMM(timestamp)",
            order_by=["timestamp", "symbol"] + (["underlying", "expiry", "strike", "instrument_type"] if is_fno else []),
            is_fno=is_fno
        )

    def validate_parquet(self, parquet_path: Path, is_fno: Optional[bool] = None) -> ValidationResult:
        """
        Validate parquet file against expected schema.
        
        Args:
            parquet_path: Path to parquet file
            is_fno: Force F&O/Equity mode. If None, auto-detect from columns.
            
        Returns:
            ValidationResult with validation details
        """
        try:
            actual_schema = self.infer_schema_from_parquet(parquet_path)
            actual_columns = set(actual_schema.keys())
            
            # Auto-detect F&O if not specified
            if is_fno is None:
                fno_indicators = {"underlying", "expiry", "strike", "instrument_type"}
                is_fno = bool(fno_indicators & actual_columns)
            
            expected = self.get_expected_schema(is_fno)
            expected_columns = set(expected.columns.keys())
            
            # Check column presence
            missing = expected_columns - actual_columns
            extra = actual_columns - expected_columns
            
            # Check type compatibility
            type_mismatches = []
            for col, expected_type in expected.columns.items():
                if col in actual_schema:
                    actual_type = actual_schema[col]
                    expected_polars = self._db_type_to_polars(expected_type)
                    if not self._types_compatible(actual_type, expected_polars):
                        type_mismatches.append(f"{col}: expected {expected_type}, got {actual_type}")
            
            # Errors
            errors = []
            if missing:
                errors.append(f"Missing required columns: {sorted(missing)}")
            if type_mismatches:
                errors.append(f"Type mismatches: {type_mismatches}")
            
            valid = len(missing) == 0 and len(type_mismatches) == 0
            
            result = ValidationResult(
                valid=valid,
                missing_columns=sorted(missing),
                extra_columns=sorted(extra),
                type_mismatches=type_mismatches,
                errors=errors
            )
            
            if valid:
                logger.info(f"Parquet schema validation passed", extra={
                    "file": str(parquet_path),
                    "is_fno": is_fno,
                    "columns": len(actual_columns)
                })
            else:
                logger.warning(f"Parquet schema validation failed", extra={
                    "file": str(parquet_path),
                    "is_fno": is_fno,
                    "errors": errors,
                    "missing": missing,
                    "extra": extra,
                    "type_mismatches": type_mismatches
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Schema validation error: {e}")
            return ValidationResult(
                valid=False,
                missing_columns=[],
                extra_columns=[],
                type_mismatches=[],
                errors=[str(e)]
            )

    def _db_type_to_polars(self, db_type: str) -> pl.DataType:
        """Map database type to Polars type."""
        db_type = db_type.lower()
        if "datetime" in db_type or "timestamp" in db_type:
            return pl.Datetime
        elif "date" in db_type:
            return pl.Date
        elif "string" in db_type or "text" in db_type or "lowcardinality" in db_type:
            return pl.String
        elif "float64" in db_type or "double" in db_type:
            return pl.Float64
        elif "float32" in db_type or "real" in db_type:
            return pl.Float32
        elif "int64" in db_type or "bigint" in db_type:
            return pl.Int64
        elif "int32" in db_type or "integer" in db_type:
            return pl.Int32
        elif "uint64" in db_type:
            return pl.UInt64
        elif "uint32" in db_type:
            return pl.UInt32
        elif "bool" in db_type:
            return pl.Boolean
        return pl.String

    def _types_compatible(self, actual: pl.DataType, expected: pl.DataType) -> bool:
        """Check if actual Polars type is compatible with expected."""
        # Exact match
        if actual == expected:
            return True
        # Allow Int64/UInt64 interchange
        if actual in (pl.Int64, pl.UInt64) and expected in (pl.Int64, pl.UInt64):
            return True
        if actual in (pl.Int32, pl.UInt32) and expected in (pl.Int32, pl.UInt32):
            return True
        if actual in (pl.Float64, pl.Float32) and expected in (pl.Float64, pl.Float32):
            return True
        return False

    def render_create_table(self, is_fno: bool, database: str, table_name: str) -> str:
        """Render CREATE TABLE SQL from template."""
        template = self.load_template(is_fno)
        return template.format(database=database, table_name=table_name)

    def create_table_from_template(self, database: str, table_name: str, is_fno: bool) -> str:
        """Generate CREATE TABLE statement using template."""
        return self.render_create_table(is_fno, database, table_name)


def create_schema_validator(target_db: str = "clickhouse") -> SchemaValidator:
    """Factory function to create schema validator."""
    return SchemaValidator(target_db=target_db)
