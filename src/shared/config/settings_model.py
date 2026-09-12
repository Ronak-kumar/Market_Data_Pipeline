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
    processable_segments: list[str] = Field(..., description="List of segments that can needs to be processed.")
    start_date: str = Field("Starting date for data extraction.")
    end_date: str = Field("End date for data extraction.")

class AppSettings(BaseModel):
    application: Application
    extractor_settings: ExtractorSettings


