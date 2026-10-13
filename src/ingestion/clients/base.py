from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from shared.observability import get_logger
from polars import DataFrame
logger = get_logger(__name__)


@dataclass
class IngestionResult:
    """Result of an ingestion operation."""
    success: bool
    records_ingested: int = 0
    records_failed: int = 0
    error: Optional[str] = None
    duration_seconds: float = 0.0

class DataIngestor(ABC):
    """Abstract base class for data ingestors."""
    
    def __init__(self, config: Any):
        self.config = config
        self._connection = None
        self._pool = None
        logger.debug(f"Initializing {self.__class__.__name__}")

    @property
    @abstractmethod
    def client_name(self) -> str:
        """Return the client name for registry."""
        pass

    @abstractmethod
    def connect(self) -> None:
        """Establish database connection/pool."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection/pool."""
        pass

    @abstractmethod
    def health_check(self) -> bool:
        """Check database connectivity."""
        pass

    @abstractmethod
    def create_table(self, segment: str) -> bool:
        """Create table if not exists."""
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        pass

    @abstractmethod
    def ingest_batch(self, table_name: str, records: List[Dict[str, Any]]) -> IngestionResult:
        """Ingest a batch of records."""
        pass

    @abstractmethod
    def ingest_file(self, table_name: str, file_path: Path, format: str = "parquet") -> IngestionResult:
        """Ingest data from a file (Parquet, CSV, etc.)."""
        pass

    @abstractmethod
    def get_table_schema(self, file_path: Path) -> dict:
        """Get table schema for a segment. Override in subclasses."""
        pass

    @abstractmethod
    def get_db_table_schema(self, file_path: Path) -> dict:
        """Get db table schema for a segment. Override in subclasses."""
        pass

    @abstractmethod
    def compare_schemas(self, parquet_schema: dict[str, str], db_schema: dict[str, str]) -> dict:
        """Compare parqet schema and databse table schema."""
        pass

