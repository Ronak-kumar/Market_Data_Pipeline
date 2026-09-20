from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class DefaultCloud(ABC):

    @abstractmethod
    def upload_files(self, filepath: Path, bucket_name: str, destination_prefix: str) -> None:
        pass

    def get_files(self, filepath: Path, bucket_name: str, destination_prefix: str) -> Any:
        pass