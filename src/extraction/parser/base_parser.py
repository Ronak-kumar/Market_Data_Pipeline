from abc import ABC, abstractmethod
import polars as pl
from extraction.models.instrument_schema import MASTER_INSTRUMENT_SCHEMA
from shared.observability import get_logger

logger = get_logger(__name__)


class InstrumentParser(ABC):

    def _normalize(self, df: pl.DataFrame) -> pl.DataFrame:
        logger.debug("Normalizing instrument schema", extra={"input_columns": df.columns, "input_rows": df.height})

        # Add missing columns
        missing_columns = []
        for column, dtype in MASTER_INSTRUMENT_SCHEMA.items():
            if column not in df.columns:
                df = df.with_columns(
                    pl.lit(None)
                    .cast(dtype)
                    .alias(column)
                )
                missing_columns.append(column)

        if missing_columns:
            logger.debug("Added missing columns", extra={"missing_columns": missing_columns})

        # Select canonical columns
        result = df.select(list(MASTER_INSTRUMENT_SCHEMA.keys()))
        logger.debug("Schema normalization complete", extra={"output_columns": result.columns, "output_rows": result.height})
        return result


class GrowwInstrumentParser(InstrumentParser):

    def parse(self, data) -> pl.DataFrame:
        logger.info("Parsing Groww instrument data", extra={"input_type": type(data).__name__, "input_length": len(data) if hasattr(data, '__len__') else "unknown"})
        df = pl.DataFrame(data)
        result = self._normalize(df)
        logger.info("Groww instrument parsing complete", extra={"rows": result.height})
        return result


class UpstoxInstrumentParser(InstrumentParser):

    def parse(self, data) -> pl.DataFrame:
        logger.info("Parsing Upstox instrument data", extra={"input_type": type(data).__name__, "input_length": len(data) if hasattr(data, '__len__') else "unknown"})
        df = pl.DataFrame(data)
        result = self._normalize(df)
        logger.info("Upstox instrument parsing complete", extra={"rows": result.height})
        return result