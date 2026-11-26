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




class ClickHouseSettings(BaseSettings):
    """ClickHouse connection configuration."""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )
    
    host: str = Field("localhost", description="ClickHouse host", alias="CLICKHOUSE_HOST")
    port: int = Field(9000, description="ClickHouse port", alias="CLICKHOUSE_PORT")
    database: str = Field("default", description="ClickHouse database", alias="CLICKHOUSE_DATABASE")
    username: str = Field("default", description="ClickHouse username", alias="CLICKHOUSE_USERNAME")
    password: str = Field("", description="ClickHouse password", alias="CLICKHOUSE_PASSWORD")
    secure: bool = Field(False, description="Use secure connection", alias="CLICKHOUSE_SECURE")
    compression: bool = Field(False, description="Enable compression", alias="CLICKHOUSE_COMPRESSION")
    connect_timeout: int = Field(10, description="Connection timeout in seconds", alias="CLICKHOUSE_CONNECT_TIMEOUT")


class PostgreSQLSettings(BaseSettings):
    """PostgreSQL connection configuration."""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )
    
    host: str = Field("localhost", description="PostgreSQL host", alias="POSTGRESQL_HOST")
    port: int = Field(5432, description="PostgreSQL port", alias="POSTGRESQL_PORT")
    database: str = Field("postgres", description="PostgreSQL database", alias="POSTGRESQL_DATABASE")
    username: str = Field("postgres", description="PostgreSQL username", alias="POSTGRESQL_USERNAME")
    password: str = Field("", description="PostgreSQL password", alias="POSTGRESQL_PASSWORD")
    ssl_mode: str = Field("prefer", description="SSL mode: disable, allow, prefer, require, verify-ca, verify-full", alias="POSTGRESQL_SSL_MODE")
    ssl_cert: str = Field("", description="SSL certificate path", alias="POSTGRESQL_SSL_CERT")
    ssl_key: str = Field("", description="SSL key path", alias="POSTGRESQL_SSL_KEY")
    ssl_ca: str = Field("", description="SSL CA certificate path", alias="POSTGRESQL_SSL_CA")
    pool_min: int = Field(1, description="Minimum pool connections", alias="POSTGRESQL_POOL_MIN")
    pool_max: int = Field(10, description="Maximum pool connections", alias="POSTGRESQL_POOL_MAX")
    connect_timeout: int = Field(10, description="Connection timeout in seconds", alias="POSTGRESQL_CONNECT_TIMEOUT")
    application_name: str = Field("market_data_pipeline", description="Application name for connection", alias="POSTGRESQL_APPLICATION_NAME")


class TimescaleDBSettings(BaseSettings):
    """TimescaleDB connection configuration (extends PostgreSQL)."""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )
    
    host: str = Field("localhost", description="TimescaleDB host", alias="TIMESCALEDB_HOST")
    port: int = Field(5432, description="TimescaleDB port", alias="TIMESCALEDB_PORT")
    database: str = Field("postgres", description="TimescaleDB database", alias="TIMESCALEDB_DATABASE")
    username: str = Field("postgres", description="TimescaleDB username", alias="TIMESCALEDB_USERNAME")
    password: str = Field("", description="TimescaleDB password", alias="TIMESCALEDB_PASSWORD")
    ssl_mode: str = Field("prefer", description="SSL mode", alias="TIMESCALEDB_SSL_MODE")
    ssl_cert: str = Field("", description="SSL certificate path", alias="TIMESCALEDB_SSL_CERT")
    ssl_key: str = Field("", description="SSL key path", alias="TIMESCALEDB_SSL_KEY")
    ssl_ca: str = Field("", description="SSL CA certificate path", alias="TIMESCALEDB_SSL_CA")
    pool_min: int = Field(1, description="Minimum pool connections", alias="TIMESCALEDB_POOL_MIN")
    pool_max: int = Field(10, description="Maximum pool connections", alias="TIMESCALEDB_POOL_MAX")
    connect_timeout: int = Field(10, description="Connection timeout in seconds", alias="TIMESCALEDB_CONNECT_TIMEOUT")
    application_name: str = Field("market_data_pipeline", description="Application name for connection", alias="TIMESCALEDB_APPLICATION_NAME")
    chunk_time_interval: str = Field("1 day", description="Chunk time interval for hypertables", alias="TIMESCALEDB_CHUNK_TIME_INTERVAL")


class IngestionSettings(BaseModel):
    """Ingestion layer configuration."""
    enabled: bool = Field(False, description="Enable ingestion layer")
    target: str = Field("clickhouse", description="Target database: clickhouse, postgresql, timescaledb")
    batch_size: int = Field(10000, description="Batch size for bulk inserts")
    max_workers: int = Field(4, description="Parallel workers for ingestion")
    create_tables: bool = Field(True, description="Auto-create tables if not exist")
    clickhouse: ClickHouseSettings = Field(default_factory=ClickHouseSettings)
    postgresql: PostgreSQLSettings = Field(default_factory=PostgreSQLSettings)
    timescaledb: TimescaleDBSettings = Field(default_factory=TimescaleDBSettings)
    # Segment to table mapping: segment_name -> target_table_name
    # Empty string for target_table means use segment_name.lower()
    segment_table_mapping: dict[str, str] = Field(default_factory=dict, description="Mapping from segment names to target table names")


class DatalakeSettings(BaseModel):
    client: str = Field(..., description="Cloud delta lake configuration")
    push_to_cloud: bool = Field(..., description="Flag to be pushed to cloud or not")
    storage_name: str = Field(..., description="Storage object where the data will be stored")
    destination_folder: str = Field(..., description="Location where the data will be stored")


class BrokerConfiguration(BaseModel):
    processable_segments: list = Field(default_factory=list, description="List of segments that can needs to be processed.")
    session_bounds: dict = Field(default_factory=dict, description="Contains the session bounds of each segment")
    symbol_name_mapping: dict = Field(default_factory=dict, description="Contains the mapping for all the Instrument name to the user selected name")


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
        populate_by_name=True,
    )

    application: Application
    extractor_settings: ExtractorSettings
    broker_configuration: dict[str, BrokerConfiguration] = Field(default_factory=dict)
    cloud_settings: DatalakeSettings
    ingestion_settings: IngestionSettings = Field(default_factory=IngestionSettings)
    
    # Database settings loaded directly from .env
    clickhouse: ClickHouseSettings = Field(default_factory=ClickHouseSettings)
    postgresql: PostgreSQLSettings = Field(default_factory=PostgreSQLSettings)
    timescaledb: TimescaleDBSettings = Field(default_factory=TimescaleDBSettings)

    def get_broker_config(self, broker_name: str) -> BrokerConfiguration:
        """Get broker configuration for a specific broker, returning empty config if not found."""
        return self.broker_configuration.get(broker_name.lower(), BrokerConfiguration())
    
    @property
    def active_broker_configuration(self) -> BrokerConfiguration:
        """Get the broker configuration for the currently active client."""
        client = self.extractor_settings.client.lower()
        return self.get_broker_config(client)
