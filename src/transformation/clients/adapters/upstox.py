from transformation.clients import client_registry
from transformation.clients import DataTransformer
from transformation.core.cleaner import DataCleaner
from shared.config import app_settings
import polars as pl
from polars import DataFrame

@client_registry.register('upstox')
class UpstoxTransformationAdapter(DataTransformer):
    def __init__(self):
        pass
        
    def fno_transformation(self, df: DataFrame) -> DataFrame:
        ### Instrument type extraction 

        instrument_types = ["CE", "PE", "FUT"]
        instrument_pattern = "|".join(instrument_types)

        df = df.with_columns(
            pl.col("Ticker")
            .str.extract(
                rf"({instrument_pattern})",
                1,
            )
            .alias("Instrument_type")
        )

        df = df.with_columns(
            pl.col("Ticker")
            .str.replace(
                rf"({instrument_pattern}).*$",
                "",
            )
            .alias("_ticker_without_type")
        )

        ### Expiry extraction 
        expiry_pattern = r"(\d{1,2}[A-Z]{3}\d{2})"
        df = df.with_columns(
            pl.col("_ticker_without_type")
            .str.extract(expiry_pattern, 1)
            .alias("Expiry")
        )
        df = df.with_columns(
            pl.col("_ticker_without_type")
            .str.replace(expiry_pattern, "")
            .alias("_ticker_without_expiry")
        )

        ### Strike extraction
        strike_pattern = r"(\d+(?:\.\d+)?)$"
        df = df.with_columns(
            pl.when(pl.col("Instrument_type").is_in(["CE", "PE"]))
            .then(
                pl.col("_ticker_without_expiry")
                .str.extract(strike_pattern, 1)
                .cast(pl.Float64)
            )
            .otherwise(None)
            .alias("Strike")
        )

        ### Symbol extraction 
        df = df.with_columns(
            pl.col("_ticker_without_expiry")
            .str.replace(strike_pattern, "")
            .alias("Symbol")
        )

        expected_columns = [
                    "Timestamp", "Ticker", "Open", "High", "Low", "Close",
                    "Instrument_type", "Expiry", "Strike", "Volume",
                    "Open_Interest", "Symbol", "Exchange"
                ]

        return df.select(expected_columns)


    def equity_transformation(self, df: DataFrame) -> DataFrame:
        df = df.drop(["Open_Interest", "Volume"])
        spot_mapping = app_settings.transformation_settings.spot_name_mapping
        ##############################
        df = df.rename({"Ticker": "Symbol"})
        # Apply mapping to DataFrame
        df = df.with_columns([pl.col("Symbol").replace(spot_mapping)])
        expected_columns = ["Timestamp", "Symbol", "Open", "High", "Low", "Close", "Exchange"]
        return df.select(expected_columns)


    def mcx_transformation(self, df: DataFrame) -> DataFrame:

        ### Instrument type extraction 

        instrument_types = ["CE", "PE", "FUT"]
        instrument_pattern = "|".join(instrument_types)

        df = df.with_columns(
            pl.col("Ticker")
            .str.extract(
                rf"({instrument_pattern})",
                1,
            )
            .alias("Instrument_type")
        )

        df = df.with_columns(
            pl.col("Ticker")
            .str.replace(
                rf"({instrument_pattern}).*$",
                "",
            )
            .alias("_ticker_without_type")
        )

        ### Expiry extraction 
        expiry_pattern = r"(\d{1,2}[A-Z]{3}\d{2})"
        df = df.with_columns(
            pl.col("_ticker_without_type")
            .str.extract(expiry_pattern, 1)
            .alias("Expiry")
        )
        df = df.with_columns(
            pl.col("_ticker_without_type")
            .str.replace(expiry_pattern, "")
            .alias("_ticker_without_expiry")
        )

        ### Strike extraction
        strike_pattern = r"(\d+(?:\.\d+)?)$"
        df = df.with_columns(
            pl.when(pl.col("Instrument_type").is_in(["CE", "PE"]))
            .then(
                pl.col("_ticker_without_expiry")
                .str.extract(strike_pattern, 1)
                .cast(pl.Float64)
            )
            .otherwise(None)
            .alias("Strike")
        )

        ### Symbol extraction 
        df = df.with_columns(
            pl.col("_ticker_without_expiry")
            .str.replace(strike_pattern, "")
            .alias("Symbol")
        )

        expected_columns = [
                    "Timestamp", "Ticker", "Open", "High", "Low", "Close",
                    "Instrument_type", "Expiry", "Strike", "Volume",
                    "Open_Interest", "Symbol", "Exchange"
                ]

        return df.select(expected_columns)

