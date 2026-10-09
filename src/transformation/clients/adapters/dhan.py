from transformation.clients import transformer_registry
from transformation.clients import DataTransformer
from shared.config import app_settings
import polars as pl
from polars import DataFrame
from shared.observability import get_logger

logger = get_logger(__name__)

@transformer_registry.register('dhan')
class DhanTransformationAdapter(DataTransformer):
    def __init__(self):
        logger.debug("DhanTransformationAdapter initialized")

    def fno_transformation(self, df: DataFrame) -> DataFrame:
        logger.info("Starting FNO transformation", extra={"input_rows": df.height})
        ### Instrument type extraction 

        df = df.with_columns(
            pl.col("Ticker").str.split("-").list.get(0)
            .alias("Symbol"),

            pl.col("Ticker").str.split("-").list.get(1)
            .alias("Expiry"),

            pl.when(pl.col("Ticker").str.ends_with("-FUT"))
            .then(pl.lit(0))
            .otherwise(pl.col("Ticker").str.split("-").list.get(2))
            .alias("Strike"),

            pl.when(pl.col("Ticker").str.ends_with("-FUT"))
            .then(pl.lit("FUT")).otherwise(pl.col("Ticker").str.split("-").list.get(-1))
            .alias("Instrument_type"),
        )

        df = df.with_columns(
            [
                pl.col("Ticker").str.replace_all("-", "")
            ]
        )

        expected_columns = [
                    "Timestamp", "Ticker", "Open", "High", "Low", "Close",
                    "Instrument_type", "Expiry", "Strike", "Volume",
                    "Open_Interest", "Symbol", "Exchange"
                ]

        result = df.select(expected_columns)
        logger.info("FNO transformation completed", extra={"output_rows": result.height, "columns": result.columns})
        return result


    def equity_transformation(self, df: DataFrame) -> DataFrame:
        logger.info("Starting equity transformation", extra={"input_rows": df.height})
        df = df.drop(["Open_Interest", "Volume"])
        spot_mapping = app_settings.broker_configuration.symbol_name_mapping
        ##############################
        df = df.rename({"Ticker": "Symbol"})
        # Apply mapping to DataFrame
        df = df.with_columns([pl.col("Symbol").replace(spot_mapping)])
        expected_columns = ["Timestamp", "Symbol", "Open", "High", "Low", "Close", "Exchange"]
        result = df.select(expected_columns)
        logger.info("Equity transformation completed", extra={"output_rows": result.height, "columns": result.columns})
        return result

    def asset_segregation(self, df: DataFrame) -> dict[str, DataFrame]:
        logger.info("Starting asset segregation", extra={"input_rows": df.height})
        equity_df = df.filter(pl.col("Asset_Class") == "CASH")
        fno_df = df.filter(pl.col("Asset_Class") == "FO")
        logger.info("Asset segregation completed", extra={"equity_rows": equity_df.height, "fno_rows": fno_df.height})
        return {"equity": equity_df, "fno": fno_df}