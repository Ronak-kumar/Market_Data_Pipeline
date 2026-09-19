from pathlib import Path
from polars import DataFrame
import polars as pl
from transformation.core import DefaultValidator, DefaultConverter
from transformation.schemas import RAW_PARQUET_SCHEMA
from abc import abstractmethod
import re
from shared.observability import get_logger

logger = get_logger(__name__)

class DataTransformer:

    def base_transformation(self, filepath: Path, segment: str) -> DataFrame:
        logger.info("Starting base transformation", extra={"filepath": str(filepath), "segment": segment})
        validator = DefaultValidator()
        converter = DefaultConverter()

        df = pl.read_parquet(filepath)
        logger.debug("Read parquet file", extra={"filepath": str(filepath), "rows": df.height, "columns": df.columns})

        EXPECTED_SCHEMA = RAW_PARQUET_SCHEMA

        if "Volume" not in df.columns:
            df = df.with_columns([
                pl.lit(0, pl.UInt32).alias("Volume")
            ])
            logger.debug("Added missing Volume column with default 0")

        if "Open Interest" not in df.columns:
            df = df.with_columns([
                pl.lit(0, pl.UInt32).alias("Open Interest")
            ])
            logger.debug("Added missing Open Interest column with default 0")

        df = df.with_columns(
            pl.col(["Volume", "Open Interest"]).clip(lower_bound=0)
        )

        ### Schema Validation and Conversion
        try:
            validator.validate_schema(df, expected_schema=EXPECTED_SCHEMA)
            logger.debug("Schema validation passed")
        except ValueError as e:
            logger.warning("Schema validation failed, attempting conversion", extra={"error": str(e)})
            try:
                df = converter.schema_type_conversion(df, expected_schema=EXPECTED_SCHEMA)
                logger.info("Schema conversion successful")
            except ValueError as e:
                logger.error("Schema conversion failed", extra={"error": str(e)})
                raise

        ### Null Validation
        required_columns = ["Ticker", "Date", "Time", "Open", "High", "Low", "Close", "Volume", "Open Interest"]
        try:
            validator.null_validation(df, required_columns=required_columns)
            logger.debug("Null validation passed")
        except ValueError as e:
            logger.warning("Null validation failed, attempting handling", extra={"error": str(e)})
            try:
                df = converter.null_value_handling(df, required_columns=required_columns)
                logger.info("Null value handling successful")
            except ValueError as e:
                logger.error("Null value handling failed", extra={"error": str(e)})
                raise

        ### NaN Validation and Conversion
        numeric_columns = ["Open", "High", "Low", "Close", "Volume", "Open Interest"]
        try:
            validator.nan_validation(df, numeric_columns=numeric_columns)
            logger.debug("NaN validation passed")
        except ValueError as e:
            logger.warning("NaN validation failed, attempting handling", extra={"error": str(e)})
            try:
                df = converter.nan_value_handling(df, numeric_columns=numeric_columns)
                logger.info("NaN value handling successful")
            except ValueError as e:
                logger.error("NaN value handling failed", extra={"error": str(e)})
                raise

        ### Timestamp Creation
        try:
            df = converter.timestamp_creation(df, date_col="Date", time_col="Time")
            logger.debug("Timestamp creation successful")
        except ValueError as e:
            logger.error("Timestamp creation failed", extra={"error": str(e)})
            raise

        ### Duplicate Validation and Conversion
        required_columns = ["Open", "High", "Low", "Close", "Volume", "Open Interest"]
        try:
            validator.duplicate_validation(df)
            logger.debug("Duplicate validation passed")
        except ValueError as e:
            logger.warning("Duplicate validation failed, attempting handling", extra={"error": str(e)})
            try:
                df = converter.duplicate_value_handling(df)
                logger.info("Duplicate value handling successful")
            except ValueError as e:
                logger.error("Duplicate value handling failed", extra={"error": str(e)})
                raise

        df = df.rename({"Open Interest": "Open_Interest"})

        if "Exchange" not in df.columns:
            result = re.search(r"^([^_]+)", segment).group(1)

            df = df.with_columns([
                pl.lit(result).alias("Exchange")
            ])
            logger.debug("Added Exchange column", extra={"exchange": result})

        logger.info("Base transformation completed", extra={"rows": df.height, "columns": df.columns})
        return df

    @abstractmethod
    def fno_transformation(self, data: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def equity_transformation(self, data: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def mcx_transformation(self, data: DataFrame) -> DataFrame:
        pass

