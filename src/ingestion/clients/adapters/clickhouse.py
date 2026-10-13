from ingestion.clients import DataIngestor, ingestion_registry, IngestionResult
from shared.observability import get_logger
from shared.config import app_settings
from pathlib import Path
from typing import Any, Dict, List, Optional
import time
import polars as pl

logger = get_logger(__name__)


# Lazy import clickhouse-driver
try:
    from clickhouse_driver import Client
    from clickhouse_driver.errors import Error as ClickHouseError
    CLICKHOUSE_AVAILABLE = True
except ImportError:
    CLICKHOUSE_AVAILABLE = False
    Client = None
    ClickHouseError = Exception


@ingestion_registry.register("clickhouse")
class ClickHouseIngestor(DataIngestor):
    """ClickHouse data ingestor with connection pooling and bulk inserts."""
    
    def __init__(self, config: Optional[Any] = None):
        # Use config from settings if not provided
        if config is None:
            config = app_settings.ingestion_settings.clickhouse
        super().__init__(config)
        self._client: Optional[Client] = None
        self._schema_dir = Path("src/ingestion/schemas")

    @property
    def client_name(self) -> str:
        return "clickhouse"

    def connect(self) -> None:
        """Establish ClickHouse connection."""
        if not CLICKHOUSE_AVAILABLE:
            raise RuntimeError("clickhouse-driver not installed. Run: pip install clickhouse-driver")
        
        if self._client is not None:
            return
            
        try:
            # Build connection parameters from config
            connect_params = {
                "host": self.config.host,
                "port": self.config.port,
                "database": self.config.database,
                "user": self.config.username,
                "password": self.config.password,
                "secure": self.config.secure,
                "compression": self.config.compression,
                "connect_timeout": self.config.connect_timeout,
                "send_receive_timeout": 30,
                "sync_request_timeout": 30,
            }
            
            # Filter out empty values
            connect_params = {k: v for k, v in connect_params.items() if v not in ("", 0, None, False)}
            
            logger.info("Connecting to ClickHouse", extra={"host": self.config.host, "database": self.config.database})
            self._client = Client(**connect_params)
            
            # Test connection
            self._client.execute("SELECT 1")
            logger.info("ClickHouse connection established successfully")
            
        except ClickHouseError as e:
            logger.error("Failed to connect to ClickHouse", extra={"error": str(e)}, exc_info=True)
            raise
        except Exception as e:
            logger.error("Unexpected error connecting to ClickHouse", extra={"error": str(e)}, exc_info=True)
            raise

    def disconnect(self) -> None:
        """Close ClickHouse connection."""
        if self._client is not None:
            try:
                self._client.disconnect()
                logger.debug("ClickHouse connection closed")
            except Exception as e:
                logger.warning("Error closing ClickHouse connection", extra={"error": str(e)})
            finally:
                self._client = None

    def health_check(self) -> bool:
        """Check ClickHouse connectivity."""
        if self._client is None:
            try:
                self.connect()
            except Exception:
                return False
        
        try:
            result = self._client.execute("SELECT 1")
            return result == [(1,)]
        except Exception as e:
            logger.warning("ClickHouse health check failed", extra={"error": str(e)})
            return False

    def create_table(self, segment: str, database: str, table_name: str) -> bool:
        """Create ClickHouse table if not exists."""
        if self._client is None:
            self.connect()
        
        try:
            schema_file = (
            "fno_clickhouse.sql"
            if "fno" in segment.lower()
            else "equity_clickhouse.sql")

            schema_path = self._schema_dir / schema_file
            sql_template = schema_path.read_text(encoding="utf-8")

            # Substitute the template placeholders
            query = sql_template.format(
                database=database,
                table_name=table_name,
            )
            
            logger.info("Creating ClickHouse table", extra={"table": table_name, "database": database})
            self._client.execute(query)
            logger.info("ClickHouse table created/verified", extra={"table": table_name})
            return True
            
        except ClickHouseError as e:
            logger.error("Failed to create ClickHouse table", extra={"table": table_name, "error": str(e)}, exc_info=True)
            return False

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in ClickHouse."""
        if self._client is None:
            self.connect()
        
        try:
            query = f"EXISTS TABLE {self.config.database}.{table_name}"
            result = self._client.execute(query)
            return result[0][0] == 1
        except Exception as e:
            logger.warning("Error checking table existence", extra={"table": table_name, "error": str(e)})
            return False

    
    def check_date_exists(self, table_name: str, date: str) -> bool:
        """Check if data for a specific date exists in the table."""
        if self._client is None:
            self.connect()
        
        try:
            query = f"SELECT count() FROM {self.config.database}.{table_name} WHERE toDate(Timestamp) = '{date}' LIMIT 1"
            result = self._client.execute(query)
            count = result[0][0] if result else 0
            return count > 0
        except Exception as e:
            logger.warning(f"Error checking date existence for {table_name} on {date}: {e}")
            return False

    def delete_date_data(self, table_name: str, date: str) -> bool:
        """Delete data for a specific date from the table (for rollback)."""
        if self._client is None:
            self.connect()
        
        try:
            query = f"ALTER TABLE {self.config.database}.{table_name} DELETE WHERE toDate(Timestamp) = '{date}'"
            logger.info(f"Rolling back data for {table_name} on {date}")
            self._client.execute(query)
            logger.info(f"Successfully rolled back {table_name} for {date}")
            return True
        except Exception as e:
            logger.error(f"Rollback failed for {table_name} on {date}: {e}", exc_info=True)
            return False


    def ingest_batch(self, table_name: str, records: List[Dict[str, Any]]) -> IngestionResult:
        """Ingest a batch of records into ClickHouse."""
        if self._client is None:
            self.connect()
        
        if not records:
            return IngestionResult(success=True, records_ingested=0)
        
        start_time = time.time()
        
        try:
            # Prepare column names and data
            columns = list(records[0].keys())
            columns_str = ", ".join(columns)
            
            # Convert records to list of tuples (required by clickhouse-driver)
            data = [tuple(record[col] for col in columns) for record in records]
            
            query = f"INSERT INTO {self.config.database}.{table_name} ({columns_str}) VALUES"
            
            logger.debug("Ingesting batch to ClickHouse", extra={
                "table": table_name, 
                "records": len(records),
                "columns": len(columns)
            })
            
            self._client.execute(query, data, types_check=True)
            
            duration = time.time() - start_time
            logger.info("Batch ingested to ClickHouse", extra={
                "table": table_name,
                "records": len(records),
                "duration_seconds": round(duration, 3)
            })
            
            return IngestionResult(
                success=True,
                records_ingested=len(records),
                duration_seconds=duration
            )
            
        except ClickHouseError as e:
            duration = time.time() - start_time
            logger.error("ClickHouse batch ingest failed", extra={
                "table": table_name,
                "error": str(e),
                "records": len(records)
            }, exc_info=True)
            return IngestionResult(
                success=False,
                records_ingested=0,
                records_failed=len(records),
                error=str(e),
                duration_seconds=duration
            )

    def ingest_file(self, table_name: str, file_path: Path, format: str = "parquet") -> IngestionResult:
        """Ingest data from a Parquet file using ClickHouse file engine or direct insert."""
        if self._client is None:
            self.connect()
        
        start_time = time.time()
        
        try:
            if format.lower() == "parquet":
                # Read parquet with polars and batch insert
                df = pl.read_parquet(file_path)

                
                if df.height == 0:
                    logger.warning("Parquet file is empty", extra={"file": str(file_path)})
                    return IngestionResult(success=True, records_ingested=0)

                columns = df.columns

                # Convert to list of dicts
                data = [df.get_column(column).to_list() for column in columns]

                query = f"INSERT INTO {table_name} ({', '.join(f'`{c}`' for c in columns)}) VALUES"

                total_ingested = df.height
                total_failed = 0
                self._client.execute(
                    query,
                    data,
                    columnar=True,
                )
                duration = time.time() - start_time

                logger.info(f"File ingested to ClickHouse table: {table_name}, file: {file_path}, records: {total_ingested}, failed: {total_failed}, duration_seconds: {round(duration, 3)}")
                
                return IngestionResult(
                    success=total_failed == 0,
                    records_ingested=total_ingested,
                    records_failed=total_failed,
                    duration_seconds=duration
                )
            else:
                raise ValueError(f"Unsupported format: {format}")
                
        except Exception as e:
            duration = time.time() - start_time
            logger.error("File ingestion failed", extra={"file": str(file_path), "error": str(e)}, exc_info=True)
            return IngestionResult(
                success=False,
                records_ingested=0,
                records_failed=0,
                error=str(e),
                duration_seconds=duration
            )

    def get_table_schema(self, file_path: Path) -> dict:
        """Get parquet schema for segment."""            
        schema = pl.scan_parquet(file_path).collect_schema()
        return {column: str(dtype) for column, dtype in schema.items()}

    def get_db_table_schema(self, table_name: str) -> dict:
        """Get database table schema for segment."""            
        table_schema = self._client.execute(f"DESCRIBE TABLE {table_name}")
        table_schema_dict = {}
        for column in table_schema:
            table_schema_dict[column[0]] = column[1]

        return table_schema_dict

    def compare_schemas(self,
        parquet_schema: dict[str, str],
        db_schema: dict[str, str],
    ) -> dict:
        """Compare Parquet and ClickHouse schemas."""

        parquet_columns = set(parquet_schema)
        db_columns = set(db_schema)


        missing_in_parquet = db_columns - parquet_columns
        extra_in_parquet = parquet_columns - db_columns

        # Compare data types for columns present in both schemas.
        type_mismatches = {
            column: {
                "parquet": parquet_schema[column],
                "clickhouse": db_schema[column],
            }
            for column in parquet_columns & db_columns
            if not self.types_compatible(
                parquet_schema[column],
                db_schema[column],
            )
        }

        return {
            "is_compatible": (
                not missing_in_parquet
                and not extra_in_parquet
                and not type_mismatches
            ),
            "missing_in_parquet": sorted(missing_in_parquet),
            "extra_in_parquet": sorted(extra_in_parquet),
            "type_mismatches": type_mismatches,
        }


    def types_compatible(self,
        parquet_type: str,
        clickhouse_type: str,
    ) -> bool:
        """Basic type comparison; extend for your schema conventions."""

        parquet_type = parquet_type.lower().replace(" ", "")
        clickhouse_type = clickhouse_type.lower().replace(" ", "")

        # Normalize common equivalent types.
        aliases = {
            "lowcardinality(string)": "string",
            "datetime(time_unit='us',time_zone=none)": "datetime",
            "datetime('asia/kolkata')": "datetime",
            "string": "string",
            "large_string": "string",
            "utf8": "string",
            "int64": "int64",
            "uint64": "uint64",
            "int32": "int32",
            "uint32": "uint32",
            "float": "float32",
            "float32": "float32",
            "double": "float64",
            "float64": "float64",
            "date": "date",
        }

        parquet_normalized = aliases.get(parquet_type, parquet_type)
        clickhouse_normalized = aliases.get(clickhouse_type, clickhouse_type)

        # Explicit mappings for common Parquet -> ClickHouse types.
        compatible_types = {
            ("string", "string"),
            ("int32", "int32"),
            ("uint32", "uint32"),
            ("int64", "int64"),
            ("uint64", "uint64"),
            ("float32", "float32"),
            ("float64", "float64"),
            ("date", "date"),
        }

        return (
            parquet_normalized == clickhouse_normalized
            or (parquet_normalized, clickhouse_normalized) in compatible_types
        )
