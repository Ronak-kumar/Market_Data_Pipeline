from abc import ABC, abstractmethod
import polars as pl
from shared.observability import get_logger

logger = get_logger(__name__)

class InstrumentParser(ABC):
    @abstractmethod
    def parse(self):
        ...

    @abstractmethod
    def _normalize(self, df: pl.DataFrame) -> pl.DataFrame:
        ...
