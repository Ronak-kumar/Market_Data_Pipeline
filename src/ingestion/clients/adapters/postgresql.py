from ingestion.clients import DataIngestor, ingestion_registry, IngestionResult
from shared.observability import get_logger
from shared.config import app_settings
from pathlib import Path
from typing import Any, Dict, List, Optional
import time

logger = get_logger(__name__)


# Lazy import psycopg2
try:
    import psycopg2
    from psycopg2 import pool
    from psycopg2.extras import execute_values
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    psycopg2 = None
    pool = None
    execute_values = None


@ingestion_registry.register("postgresql")
class PostgreSQLIngestor(DataIngestor):
    """PostgreSQL data ingestor with same flow as ClickHouse: date check + rollback."""
    
    def __init__(self, config: Optional[Any] = None):
        if config is None:
            config = app_settings.ingestion_settings.postgresql
        super().__init__(config)
        self._pool: Optional[pool.ThreadedConnectionPool] = None
        self._schema_dir = Path("src/ingestion/schemas")

    @property
    def client_name(self) -> str:
        return "postgresql"

    def connect(self) -> None:
        """Establish PostgreSQL connection pool."""
        if not PSYCOPG2_AVAILABLE:
            raise RuntimeError("psycopg2 not installed. Run: pip install psycopg2-binary")
        
        if self._pool is not None:
            return
            
        try:
            connect_params = {
                "host": self.config.host,
                "port": self.config.port,
                "database": self.config.database,
                "user": self.config.username,
                "password": self.config.password,
                "connect_timeout": self.config.connect_timeout,
                "application_name": getattr(self.config, "application_name", "market_data_pipeline"),
            }
            
            if self.config.ssl_mode != "disable":
                connect_params["sslmode"] = self.config.ssl_mode
                if self.config.ssl_cert:
                    connect_params["sslcert"] = self.config.ssl_cert
                if self.config.ssl_key:
                    connect_params["sslkey"] = self.config.ssl_key
                if self.config.ssl_ca:
                    connect_params["sslrootcert"] = self.config.ssl_ca
            
            connect_params = {k: v for k, v in connect_params.items() if v not in ("", 0, None)}
            
            logger.info("Creating PostgreSQL connection pool", extra={
                "host": self.config.host, "database": self.config.database,
                "pool_min": self.config.pool_min, "pool_max": self.config.pool_max
            })
            
            self._pool = pool.ThreadedConnectionPool(
                minconn=self.config.pool_min,
                maxconn=self.config.pool_max,
                **connect_params
            )
            
            conn = self._pool.getconn()
            conn.close()
            self._pool.putconn(conn)
            
            logger.info("PostgreSQL connection pool established successfully")
            
        except psycopg2.Error as e:
            logger.error("Failed to create PostgreSQL pool", extra={"error": str(e)}, exc_info=True)
            raise

    def disconnect(self) -> None:
        """Close PostgreSQL connection pool."""
        if self._pool is not None:
            try:
                self._pool.closeall()
                logger.debug("PostgreSQL connection pool closed")
            except Exception as e:
                logger.warning("Error closing PostgreSQL pool", extra={"error": str(e)})
            finally:
                self._pool = None

    def health_check(self) -> bool:
        """Check PostgreSQL connectivity."""
        if self._pool is None:
            try:
                self.connect()
            except Exception:
                return False
        
        try:
            conn = self._pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    return cur.fetchone() == (1,)
            finally:
                self._pool.putconn(conn)
        except Exception as e:
            logger.warning("PostgreSQL health check failed", extra={"error": str(e)})
            return False

    def _map_type(self, ch_type: str) -> str:
        """Map ClickHouse types to PostgreSQL types."""
        type_map = {
            "DateTime64(3, 'Asia/Kolkata')": "TIMESTAMPTZ",
            "Date": "DATE",
            "String": "TEXT",
            "LowCardinality(String)": "TEXT",
            "Float64": "DOUBLE PRECISION",
            "Float32": "REAL",
            "UInt64": "BIGINT",
            "UInt32": "INTEGER",
            "Int64": "BIGINT",
            "Int32": "INTEGER",
        }
        return type_map.get(ch_type, "TEXT")

    def create_table(self, segment: str, database: str, table_name: str) -> bool:
        """Create PostgreSQL table from template."""
        if self._pool is None:
            self.connect()
        
        try:
            schema_file = "fno_postgresql.sql" if "fno" in segment.lower() else "equity_postgresql.sql"
            schema_path = self._schema_dir / schema_file
            sql_template = schema_path.read_text(encoding="utf-8")

            query = sql_template.format(table_name=table_name)
            
            conn = self._pool.getconn()
            try:
                with conn.cursor() as cur:
                    logger.info("Creating PostgreSQL table", extra={"table": table_name, "database": database})
                    cur.execute(query)
                    conn.commit()
                    logger.info("PostgreSQL table created/verified", extra={"table": table_name})
                    return True
            finally:
                self._pool.putconn(conn)
                
        except psycopg2.Error as e:
            logger.error("Failed to create PostgreSQL table", extra={"table": table_name, "error": str(e)}, exc_info=True)
            return False

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in PostgreSQL."""
        if self._pool is None:
            self.connect()
        
        try:
            conn = self._pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)",
                        (table_name,)
                    )
                    return cur.fetchone()[0]
            finally:
                self._pool.putconn(conn)
        except Exception as e:
            logger.warning("Error checking table existence", extra={"table": table_name, "error": str(e)})
            return False

    def check_date_exists(self, table_name: str, date: str) -> bool:
        """Check if data for a specific date exists in the table."""
        if self._pool is None:
            self.connect()
        
        try:
            conn = self._pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT EXISTS (SELECT 1 FROM {table_name} WHERE DATE(timestamp) = %s LIMIT 1)",
                        (date,)
                    )
                    exists = cur.fetchone()[0]
                    logger.debug(f"Date existence check for {table_name} on {date}: {exists}")
                    return exists
            finally:
                self._pool.putconn(conn)
        except Exception as e:
            logger.warning(f"Error checking date existence for {table_name} on {date}: {e}")
            return False

    def delete_date_data(self, table_name: str, date: str) -> bool:
        """Delete data for a specific date from the table (for rollback)."""
        if self._pool is None:
            self.connect()
        
        try:
            conn = self._pool.getconn()
            try:
                with conn.cursor() as cur:
                    logger.info(f"Rolling back data for {table_name} on {date}")
                    cur.execute(
                        f"DELETE FROM {table_name} WHERE DATE(timestamp) = %s",
                        (date,)
                    )
                    deleted = cur.rowcount
                    conn.commit()
                    logger.info(f"Successfully rolled back {table_name} for {date} (deleted {deleted} rows)")
                    return True
            finally:
                self._pool.putconn(conn)
        except Exception as e:
            logger.error(f"Rollback failed for {table_name} on {date}: {e}", exc_info=True)
            return False

    def ingest_batch(self, table_name: str, records: List[Dict[str, Any]]) -> IngestionResult:
        """Ingest a batch of records into PostgreSQL using execute_values."""
        if self._pool is None:
            self.connect()
        
        if not records:
            return IngestionResult(success=True, records_ingested=0)
        
        start_time = time.time()
        
        try:
            conn = self._pool.getconn()
            try:
                columns = list(records[0].keys())
                columns_str = ", ".join(columns)
                data = [tuple(record[col] for col in columns) for record in records]
                query = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"
                
                logger.debug("Ingesting batch to PostgreSQL", extra={"table": table_name, "records": len(records)})
                
                with conn.cursor() as cur:
                    execute_values(cur, query, data, page_size=1000)
                conn.commit()
                
                duration = time.time() - start_time
                logger.info("Batch ingested to PostgreSQL", extra={"table": table_name, "records": len(records), "duration_seconds": round(duration, 3)})
                
                return IngestionResult(success=True, records_ingested=len(records), duration_seconds=duration)
            finally:
                self._pool.putconn(conn)
                
        except psycopg2.Error as e:
            duration = time.time() - start_time
            logger.error("PostgreSQL batch ingest failed", extra={"table": table_name, "error": str(e)}, exc_info=True)
            return IngestionResult(success=False, records_failed=len(records), error=str(e), duration_seconds=duration)

    def ingest_file(self, table_name: str, file_path: Path, format: str = "parquet") -> IngestionResult:
        """Ingest data from a Parquet file."""
        if self._pool is None:
            self.connect()
        
        start_time = time.time()
        
        try:
            if format.lower() == "parquet":
                import polars as pl
                
                logger.info("Reading Parquet file for ingestion", extra={"file": str(file_path)})
                df = pl.read_parquet(file_path)
                
                if df.height == 0:
                    return IngestionResult(success=True, records_ingested=0)
                
                records = df.to_dicts()
                
                batch_size = getattr(self.config, "batch_size", 10000)
                total_ingested = 0
                total_failed = 0
                
                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]
                    result = self.ingest_batch(table_name, batch)
                    if result.success:
                        total_ingested += result.records_ingested
                    else:
                        total_failed += result.records_failed
                        logger.error("Batch failed, continuing", extra={"batch_start": i, "error": result.error})
                
                duration = time.time() - start_time
                logger.info("File ingested to PostgreSQL", extra={
                    "table": table_name, "file": str(file_path),
                    "records": total_ingested, "failed": total_failed,
                    "duration_seconds": round(duration, 3)
                })
                
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
            return IngestionResult(success=False, error=str(e), duration_seconds=duration)

    def get_table_schema(self, file_path: Path) -> dict:
        """Get parquet schema for segment."""           
        import polars as pl
        schema = pl.scan_parquet(file_path).collect_schema()
        return {column: str(dtype) for column, dtype in schema.items()}

    def get_db_table_schema(self, table_name: str) -> dict:
        """Get database table schema."""
        if self._pool is None:
            self.connect()
        
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", (table_name,))
                columns = cur.fetchall()
                return {col[0]: col[1] for col in columns}
        finally:
            self._pool.putconn(conn)

    def compare_schemas(self, parquet_schema: dict[str, str], db_schema: dict[str, str]) -> dict:
        """Compare Parquet and PostgreSQL schemas."""
        parquet_columns = set(parquet_schema)
        db_columns = set(db_schema)

        missing_in_parquet = db_columns - parquet_columns
        extra_in_parquet = parquet_columns - db_columns

        type_mismatches = {}
        # Add type comparison logic here if needed
        
        return {
            "is_compatible": not missing_in_parquet and not extra_in_parquet and not type_mismatches,
            "missing_in_parquet": sorted(missing_in_parquet),
            "extra_in_parquet": sorted(extra_in_parquet),
            "type_mismatches": type_mismatches,
        }
