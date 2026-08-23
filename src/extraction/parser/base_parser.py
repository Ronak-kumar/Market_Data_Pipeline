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

        df = df.with_columns(
            pl.col("strike_price")
            .cast(pl.Float64, strict=False),

            pl.col("lot_size")
            .cast(pl.Int64, strict=False),

            pl.col("tick_size")
            .cast(pl.Float64, strict=False),
        )

        return self._normalize(df)

class UpstoxInstrumentParser(InstrumentParser):

    def parse(self, data) -> pl.DataFrame:

        df = pl.DataFrame(data)

        df = df.rename({
            "instrument_key": "instrument_key",
            "trading_symbol": "trading_symbol",
            # Upstox-specific mappings...
        })

        return self._normalize(df)