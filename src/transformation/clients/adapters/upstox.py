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
        df = df.with_columns(
            pl.col("Ticker").str.split("_").list.get(0).alias("Symbol"),
            pl.col("Ticker").str.split("_").list.get(1).alias("Expiry"),
            pl.col("Ticker").str.split("_").list.get(2).alias("Strike"),
            pl.col("Ticker").str.split("_").list.get(3).alias("Instrument_type"),
        )

        
        df = df.with_columns(
            [
                pl.col("Ticker").str.replace_all("_", "")
            ]
        )
        ### Asigning Fut strike to be 0 
        df = df.with_columns(
            pl.when(pl.col("Instrument_type") == "FUT")
            .then(pl.lit(0))
            .otherwise(pl.col("Strike"))
            .alias("Strike")
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
        df = df.with_columns(
            pl.col("Ticker").str.split("_").list.get(0).alias("Symbol"),
            pl.col("Ticker").str.split("_").list.get(1).alias("Expiry"),
            pl.col("Ticker").str.split("_").list.get(2).alias("Strike"),
            pl.col("Ticker").str.split("_").list.get(3).alias("Instrument_type"),
        )
        ####################################################################

        ### Asigning Fut strike to be 0 
        df = df.with_columns(
            pl.when(pl.col("Instrument_type") == "FUT")
            .then(pl.lit(0))
            .otherwise(pl.col("Strike"))
            .alias("Strike")
        )

        df = df.with_columns(
            [
                pl.col("Ticker").str.replace_all("_", "")
            ]
        )

        expected_columns = [
            "Timestamp", "Ticker", "Open", "High", "Low", "Close",
            "Instrument_type", "Expiry", "Strike", "Volume",
            "Open_Interest", "Symbol", "Exchange"
        ]

        return df.select(expected_columns)

