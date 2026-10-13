from ingestion.clients.adapters.postgresql import PostgreSQLIngestor
from ingestion.clients import ingestion_registry, IngestionResult
from shared.observability import get_logger
from shared.config import app_settings
from pathlib import Path
from typing import Any, Dict, List, Optional
import time

logger = get_logger(__name__)


@ingestion_registry.register("timescaledb")
class TimescaleDBIngestor(PostgreSQLIngestor):
    """TimescaleDB data ingestor - inherits PostgreSQL flow, adds hypertable support."""
    
    def __init__(self, config: Optional[Any] = None):
        if config is None:
            config = app_settings.ingestion_settings.timescaledb
        super().__init__(config)
        self._schema_dir = Path("src/ingestion/schemas")

    @property
    def client_name(self) -> str:
        return "timescaledb"

    def create_table(self, segment: str, database: str, table_name: str) -> bool:
        """Create TimescaleDB hypertable from template + hypertable conversion."""
        if self._pool is None:
            self.connect()
        
        try:
            conn = self._pool.getconn()
            try:
                with conn.cursor() as cur:
                    # Use template for base table creation
                    schema_file = "fno_postgresql.sql" if "fno" in segment.lower() else "equity_postgresql.sql"
                    schema_path = self._schema_dir / schema_file
                    sql_template = schema_path.read_text(encoding="utf-8")
                    
                    query = sql_template.format(table_name=table_name)
                    
                    logger.info("Creating TimescaleDB base table from template", extra={"table": table_name})
                    cur.execute(query)
                    conn.commit()
                    
                    # Convert to hypertable
                    if "fno" in segment.lower():
                        time_col = "timestamp"
                        chunk_interval = getattr(self.config, "chunk_time_interval", "1 day")
                        
                        hypertable_query = f"""
                            SELECT create_hypertable(
                                '{table_name}', 
                                '{time_col}',
                                chunk_time_interval => INTERVAL '{chunk_interval}',
                                if_not_exists => TRUE,
                                migrate_data => TRUE
                            )
                        """
                        logger.info("Converting to hypertable", extra={"table": table_name})
                        cur.execute(hypertable_query)
                        conn.commit()
                        logger.info("Hypertable created/verified", extra={"table": table_name})
                    
                    # Create indexes for common queries
                    if "fno" in segment.lower():
                        indexes = [
                            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_underlying ON {table_name} (underlying)",
                            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_expiry ON {table_name} (expiry)",
                            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_instrument_type ON {table_name} (instrument_type)",
                        ]
                    else:
                        indexes = [
                            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol ON {table_name} (symbol)",
                        ]
                    
                    for idx_query in indexes:
                        cur.execute(idx_query)
                    conn.commit()
                    
                    logger.info("TimescaleDB hypertable created/verified", extra={"table": table_name})
                    return True
            finally:
                self._pool.putconn(conn)
                
        except Exception as e:
            logger.error("Failed to create TimescaleDB hypertable", extra={"table": table_name, "error": str(e)}, exc_info=True)
            return False

    # Inherit all other methods from PostgreSQLIngestor:
    # - connect, disconnect, health_check
    # - table_exists, check_date_exists, delete_date_data (same flow)
    # - ingest_batch, ingest_file
    # - get_table_schema, get_db_table_schema, compare_schemas
    
    # Override only what's needed
    def ingest_batch(self, table_name: str, records: List[Dict[str, Any]]) -> IngestionResult:
        """Ingest batch - inherits from PostgreSQL but with TimescaleDB optimizations."""
        return super().ingest_batch(table_name, records)

    def ingest_file(self, table_name: str, file_path: Path, format: str = "parquet") -> IngestionResult:
        """Ingest file - inherits from PostgreSQL."""
        return super().ingest_file(table_name, file_path, format)

    def get_table_schema(self, file_path: Path) -> dict:
        """Get parquet schema for segment."""
        return super().get_table_schema(file_path)

    def get_db_table_schema(self, table_name: str) -> dict:
        """Get database table schema."""
        return super().get_db_table_schema(table_name)

    def compare_schemas(self, parquet_schema: dict[str, str], db_schema: dict[str, str]) -> dict:
        """Compare Parquet and TimescaleDB schemas."""
        return super().compare_schemas(parquet_schema, db_schema)
