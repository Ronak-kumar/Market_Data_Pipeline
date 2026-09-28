from dataclasses import dataclass
from polars import DataFrame

@dataclass
class CloudObject:
    client_name: str
    push_to_cloud_flag: bool
    cloud_instance: object
    storage_object: str
    destination_folder: str

@dataclass
class ClientObject:
    client_name: str
    provider: object
    parser: object
    transformer: object
    master_instrument: DataFrame

@dataclass
class ExtractionConfig:
    expiry_suffix_duration: int
    end_date: str
    start_date: str
    processable_segments: dict
    extraction_interval: int
    extraction_variation: str