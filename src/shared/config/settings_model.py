from pydantic import BaseModel, Field
from typing import Optional
from pydantic_settings import SettingsConfigDict, BaseSettings

class Application(BaseModel):
    name: str 
    version: str 
    description: str
    environment: str
    debug: bool 


class ExtractorSettings(BaseModel):
    client: str = Field(..., description="The client to use for data extraction.")
    interval: int = Field(..., description="Candle Extraction interval.")
    max_retries: int = Field(..., description="Maximum number of retries for data extraction.")
    expiry_duration: int = Field(..., description="From the date of fetching how many forward months expiries is needed.")
    start_date: str = Field("", description="Starting date for data extraction.")
    end_date: str = Field("", description="End date for data extraction.")


class IngestionDatabaseSettings(BaseModel):
    """Base database connection settings - loaded from environment variables."""
    host: str = Field("", description="Database host")
    port: int = Field(0, description="Database port")
    database: str = Field("", description="Database name")
    username: str = Field("", description="Database username")
    password: str = Field("", description="Database password")
    # Optional connection pool settings
    pool_min: int = Field(1, description="Minimum pool connections")
    pool_max: int = Field(10, description="Maximum pool connections")
    connect_timeout: int = Field(10, description="Connection timeout in seconds")
    # SSL settings
    ssl_mode: str = Field("prefer", description="SSL mode: disable, allow, prefer, require, verify-ca, verify-full")
    ssl_cert: str = Field("", description="SSL certificate path")
    ssl_key: str = Field("", description="SSL key path")
    ssl_ca: str = Field("", description="SSL CA path")


class ClickHouseSettings(IngestionDatabaseSettings):
    """ClickHouse specific settings."""
    # ClickHouse uses different defaults
    port: int = Field(8123, description="ClickHouse HTTP port")
    secure: bool = Field(False, description="Use HTTPS")
    # ClickHouse specific
    compression: bool = Field(False, description="Enable compression")


class PostgreSQLSettings(IngestionDatabaseSettings):
    """PostgreSQL specific settings."""
    port: int = Field(5432, description="PostgreSQL port")
    # pgbouncer support
    application_name: str = Field("market_data_pipeline", description="Application name for pg_stat_activity")


class TimescaleDBSettings(PostgreSQLSettings):
    """TimescaleDB specific settings (inherits from PostgreSQL)."""
    # TimescaleDB specific
    chunk_time_interval: str = Field("1 day", description="Default chunk time interval for hypertables")


class IngestionSettings(BaseModel):
    """Ingestion layer configuration."""
    enabled: bool = Field(False, description="Enable ingestion layer")
    target: str = Field("clickhouse", description="Target database: clickhouse, postgresql, timescaledb")
    batch_size: int = Field(10000, description="Batch size for bulk inserts")
    max_workers: int = Field(4, description="Parallel workers for ingestion")
    create_tables: bool = Field(True, description="Auto-create tables if not exist")
    # Per-database settings (loaded from env)
    clickhouse: ClickHouseSettings = Field(default_factory=ClickHouseSettings)
    postgresql: PostgreSQLSettings = Field(default_factory=PostgreSQLSettings)
    timescaledb: TimescaleDBSettings = Field(default_factory=TimescaleDBSettings)


class DatalakeSettings(BaseModel):
    client: str = Field(..., description="Cloud delta lake configuration")
    push_to_cloud: bool = Field(..., description="Flag to be pushed to cloud or not")
    storage_name: str = Field(..., description="Storage object where the data will be stored")
    destination_folder: str = Field(..., description="Location where the data will be stored")


class BrokerConfiguration(BaseModel):
    processable_segments: list = Field(..., description="List of segments that can needs to be processed.")
    session_bounds: dict = Field(..., description="Contains the session bounds of each segment")
    symbol_name_mapping : dict = Field(..., description="Contains the mapping for all the Instrument name to the user selected name")


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    application: Application
    extractor_settings: ExtractorSettings
    broker_configuration: BrokerConfiguration
    cloud_settings: DatalakeSettings
    ingestion_settings: IngestionSettings = Field(default_factory=IngestionSettings)
