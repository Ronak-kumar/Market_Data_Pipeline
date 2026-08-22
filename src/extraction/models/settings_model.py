from pydantic import BaseModel, Field


class Application(BaseModel):
    name: str 
    version: str 
    description: str
    environment: str
    debug: bool 

class ExtractorSettings(BaseModel):
    client: str = Field(..., description="The client to use for data extraction.")
    max_retries: int = Field(..., description="Maximum number of retries for data extraction.")
    retry_delay: int = Field(..., description="Delay between retries for data extraction.")
    processable_segments: list[str] = Field(..., description="List of segments that can needs to be processed.")
    access_token_path: str = Field(..., description="Path to the access token file.")

class AppSettings(BaseModel):
    application: Application
    extractor_settings: ExtractorSettings


