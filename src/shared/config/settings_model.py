from pydantic import BaseModel, Field


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
    start_date: str = Field("Starting date for data extraction.")
    end_date: str = Field("End date for data extraction.")

class DatalakeSettings(BaseModel):
    client: str = Field(..., description="Cloud delta lake configuration")
    push_to_cloud: bool = Field(..., description="Flag to be pushed to cloud or not")
    storage_name: str = Field(..., description="Storage object where the data will be stored")
    destination_folder: str = Field(..., description="Location where the data will be stored")

class BrokerConfiguration(BaseModel):
    processable_segments: dict[str, list[str]] = Field(..., description="List of segments that can needs to be processed.")
    session_bounds: dict = Field(..., description="Contains the session bounds of each segment")
    symbol_name_mapping : dict = Field(..., description="Contains the mapping for all the Instrument name to the user selected name")

class AppSettings(BaseModel):
    application: Application
    extractor_settings: ExtractorSettings
    broker_configuration: BrokerConfiguration
    cloud_settings: DatalakeSettings


