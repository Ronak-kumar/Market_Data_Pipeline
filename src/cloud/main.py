from cloud import cloud_registry, cloud_discovery
from shared.config import get_application_settings
from glob import glob
from pathlib import Path

CACHE_PATH = Path(__file__).parent.parent / "cache"


def _get_settings():
    """Load settings fresh from disk so UI changes take effect immediately."""
    return get_application_settings()


class CloudOrchestrator:
    def __init__(self, settings=None):
        settings = settings or _get_settings()

        self.cloud_settings = settings.cloud_settings
        self.client = self.cloud_settings.client
        self.destination_prefix = self.cloud_settings.destination_folder
        self.bucket_name = self.cloud_settings.storage_name

        self._broker_client = settings.extractor_settings.client

        cloud_discovery()
        self.cloud_obj = self._initialize_cloud_client(self.client)

    def _initialize_cloud_client(self, cloud_provider: str):
        """Initialize the cloud client based on the provider specified in settings."""
        if cloud_provider not in cloud_registry._items:
            raise ValueError(f"Cloud provider '{cloud_provider}' is not registered.")

        cloud_client_class = cloud_registry.get(cloud_provider)
        return cloud_client_class()

    def push_to_cloud(self, date: str, data_layer: str) -> None:
        """Push files to the specified cloud provider."""
        if not self.cloud_obj:
            raise RuntimeError("Cloud client is not initialized.")

        files = glob(f"{CACHE_PATH}/{self._broker_client}/{data_layer}/{date}/*.parquet")

        for file_path in files:
            file_path = Path(file_path)
            self.cloud_obj.upload_files(file_path, self.bucket_name, self.destination_prefix)


if __name__ == "__main__":
    # Example usage
    a = CloudOrchestrator()
    a.push_to_cloud("2026-10-30", "Bronze")
