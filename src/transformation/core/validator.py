import polars as pl
from shared.observability import get_logger

logger = get_logger(__name__)


class CandleValidationError(ValueError):
    """Raised when candle data fails validation."""


class DefaultValidator:

    def validate_schema(self, df: pl.DataFrame, expected_schema: dict[str, pl.DataType]) -> None:
        """
        Validate the schema of a DataFrame against an expected schema.

        Raises:
            CandleValidationError: If the schema does not match.
        """
        logger.debug("Validating schema", extra={"expected_columns": list(expected_schema.keys()), "actual_columns": df.columns})
        actual_schema = df.schema

        missing_columns = [
            column
            for column in expected_schema
            if column not in actual_schema
        ]

        if missing_columns:
            logger.error("Schema validation failed - missing columns", extra={"missing_columns": missing_columns})
            raise CandleValidationError(
                f"Missing required columns: {missing_columns}"
            )

        wrong_dtypes = {
            column: {
                "expected": expected_dtype,
                "actual": actual_schema[column],
            }
            for column, expected_dtype in expected_schema.items()
            if actual_schema[column] != expected_dtype
        }

        if wrong_dtypes:
            logger.error("Schema validation failed - dtype mismatch", extra={"wrong_dtypes": {k: {"expected": str(v["expected"]), "actual": str(v["actual"])} for k, v in wrong_dtypes.items()}})
            raise CandleValidationError(
                f"Schema dtype mismatch: {wrong_dtypes}"
            )

        logger.debug("Schema validation passed")

    def null_validation(self, df: pl.DataFrame, required_columns: list[str]) -> None:
        """
        Validate that required columns do not contain NULL values.

        Raises:
            CandleValidationError: If any required column contains NULL values.
        """
        logger.debug("Validating null values", extra={"required_columns": required_columns, "total_rows": df.height})
        required_nulls = (
            df
            .select(
                [
                    pl.col(column)
                    .is_null()
                    .sum()
                    .alias(column)
                    for column in required_columns
                ]
            )
            .row(0, named=True)
        )

        null_columns = {
            column: count
            for column, count in required_nulls.items()
            if count > 0
        }

        if null_columns:
            logger.error("Null validation failed", extra={"null_columns": null_columns, "total_rows": df.height})
            raise CandleValidationError(
                f"NULL values found: {null_columns}"
            )

        logger.debug("Null validation passed")

    def nan_validation(self, df: pl.DataFrame, numeric_columns: list[str]) -> None:
        """
        Validate that numeric columns do not contain NaN values.

        Raises:
            CandleValidationError: If any numeric column contains NaN values.
        """
        logger.debug("Validating NaN values", extra={"numeric_columns": numeric_columns, "total_rows": df.height})
        nan_columns = {}

        for column in numeric_columns:
            count = (
                df
                .select(
                    pl.col(column)
                    .is_nan()
                    .sum()
                    .alias("count")
                )
                .item()
            )

            if count > 0:
                nan_columns[column] = count

        if nan_columns:
            logger.error("NaN validation failed", extra={"nan_columns": nan_columns, "total_rows": df.height})
            raise CandleValidationError(
                f"NaN values found: {nan_columns}"
            )

        logger.debug("NaN validation passed")

    def candle_validation(self, df: pl.DataFrame) -> None:
        """
        Validate the integrity of candle data.

        Raises:
            CandleValidationError: If any candle data validation fails.
        """
        logger.debug("Validating candle data integrity", extra={"total_rows": df.height})
        invalid_prices = df.filter(
            (pl.col("Open") <= 0)
            | (pl.col("High") <= 0)
            | (pl.col("Low") <= 0)
            | (pl.col("Close") <= 0)
        )

        if not invalid_prices.is_empty():
            logger.error("Candle validation failed - non-positive prices", extra={"invalid_rows": invalid_prices.height, "total_rows": df.height})
            raise CandleValidationError(
                f"Found {invalid_prices.height} rows "
                "with non-positive prices"
            )

        # Validate OHLC relationships
        invalid_ohlc = df.filter(
            (pl.col("High") < pl.col("Open"))
            | (pl.col("High") < pl.col("Close"))
            | (pl.col("High") < pl.col("Low"))
            | (pl.col("Low") > pl.col("Open"))
            | (pl.col("Low") > pl.col("Close"))
            | (pl.col("Low") > pl.col("High"))
        )

        if not invalid_ohlc.is_empty():
            logger.error("Candle validation failed - invalid OHLC relationships", extra={"invalid_rows": invalid_ohlc.height, "total_rows": df.height})
            raise CandleValidationError(
                f"Found {invalid_ohlc.height} rows "
                "with invalid OHLC relationships"
            )

        logger.debug("Candle validation passed")

    def duplicate_validation(self, df: pl.DataFrame) -> None:
        """
        Validate that there are no duplicate candles based on Ticker and Timestamp.

        Raises:
            CandleValidationError: If duplicate candles are found.
        """
        logger.debug("Validating duplicates", extra={"total_rows": df.height})
        duplicates = (
            df
            .group_by(["Ticker", "Timestamp"])
            .agg(pl.count().alias("count"))
            .filter(pl.col("count") > 1)
        )

        if not duplicates.is_empty():
            logger.error("Duplicate validation failed", extra={"duplicate_groups": duplicates.height, "total_rows": df.height})
            raise CandleValidationError(
                f"Found {duplicates.height} duplicate candle groups"
            )

        logger.debug("Duplicate validation passed")