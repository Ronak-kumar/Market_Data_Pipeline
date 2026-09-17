import polars as pl
class CandleValidationError(ValueError): """Raised when candle data fails validation."""


class DefaultValidator:

    def validate_schema(self, df: pl.DataFrame, expected_schema: dict[str, pl.DataType]) -> None:
        """
        Validate the schema of a DataFrame against an expected schema.

        Raises:
            CandleValidationError: If the schema does not match.
        """
        actual_schema = df.schema

        missing_columns = [
            column
            for column in expected_schema
            if column not in actual_schema
        ]

        if missing_columns:
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
            raise CandleValidationError(
                f"Schema dtype mismatch: {wrong_dtypes}"
            )

    def null_validation(self, df: pl.DataFrame, required_columns: list[str]) -> None:
        """
        Validate that required columns do not contain NULL values.

        Raises:
            CandleValidationError: If any required column contains NULL values.
        """
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
            raise CandleValidationError(
                f"NULL values found: {null_columns}"
            )

    def nan_validation(self, df: pl.DataFrame, numeric_columns: list[str]) -> None:
        """
        Validate that numeric columns do not contain NaN values.

        Raises:
            CandleValidationError: If any numeric column contains NaN values.
        """
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
            raise CandleValidationError(
                f"NaN values found: {nan_columns}"
            )

    def candle_validation(self, df: pl.DataFrame) -> None:
        """
        Validate the integrity of candle data.

        Raises:
            CandleValidationError: If any candle data validation fails.
        """
        invalid_prices = df.filter(
            (pl.col("Open") <= 0)
            | (pl.col("High") <= 0)
            | (pl.col("Low") <= 0)
            | (pl.col("Close") <= 0)
        )

        if not invalid_prices.is_empty():
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
            raise CandleValidationError(
                f"Found {invalid_ohlc.height} rows "
                "with invalid OHLC relationships"
            )

    def duplicate_validation(self, df: pl.DataFrame) -> None:
        """
        Validate that there are no duplicate candles based on Ticker and Timestamp.

        Raises:
            CandleValidationError: If duplicate candles are found.
        """
        duplicates = (
            df
            .group_by(["Ticker", "Timestamp"])
            .agg(pl.count().alias("count"))
            .filter(pl.col("count") > 1)
        )

        if not duplicates.is_empty():
            raise CandleValidationError(
                f"Found {duplicates.height} duplicate candle groups"
            )