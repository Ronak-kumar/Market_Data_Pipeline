from abc import ABC, abstractmethod
import polars as pl
from extraction.models.instrument_schema import MASTER_INSTRUMENT_SCHEMA


class InstrumentParser(ABC):

    def _normalize(self, df: pl.DataFrame) -> pl.DataFrame:

        # Add missing columns

        for column, dtype in MASTER_INSTRUMENT_SCHEMA.items():

            if column not in df.columns:
                df = df.with_columns(
                    pl.lit(None)
                    .cast(dtype)
                    .alias(column)
                )

        # Select canonical columns

        return df.select(list(MASTER_INSTRUMENT_SCHEMA.keys()))

class GrowwInstrumentParser(InstrumentParser):

    def parse(self, data) -> pl.DataFrame:

        df = pl.DataFrame(data)
        return self._normalize(df)

class UpstoxInstrumentParser(InstrumentParser):

    def parse(self, data) -> pl.DataFrame:

        df = pl.DataFrame(data)
        return self._normalize(df)