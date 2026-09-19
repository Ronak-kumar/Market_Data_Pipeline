import polars as pl
from shared.observability import get_logger

logger = get_logger(__name__)


class DefaultConverter:

    def schema_type_conversion(self, df: pl.DataFrame, expected_schema: dict[str, pl.DataType]) -> pl.DataFrame:
        """
        Convert the schema of a DataFrame to the expected types.

        Args:
            df (pl.DataFrame): The input DataFrame.

        Returns:
            pl.DataFrame: The DataFrame with converted schema.
        """
        logger.debug("Converting schema types", extra={"expected_types": {k: str(v) for k, v in expected_schema.items()}, "input_rows": df.height})
        result = df.with_columns(
            pl.col(column).cast(dtype)
            for column, dtype in expected_schema.items()
        )
        logger.debug("Schema type conversion completed", extra={"output_rows": result.height})
        return result

    def timestamp_creation(self, df: pl.DataFrame, date_col: str, time_col: str) -> pl.DataFrame:
        """
        Create a Timestamp column by combining Date and Time columns.

        Args:
            df (pl.DataFrame): The input DataFrame.
            date_col (str): The name of the Date column.
            time_col (str): The name of the Time column.

        Returns:
            pl.DataFrame: The DataFrame with the Timestamp column.
        """
        logger.debug("Creating timestamp column", extra={"date_col": date_col, "time_col": time_col, "input_rows": df.height})
        df = df.with_columns([(pl.col(date_col) + " " + pl.col(time_col)).alias("Timestamp")])
        df = df.with_columns([
            pl.col(date_col).str.strptime(pl.Date, format="%d-%m-%Y"),
            pl.col(time_col).str.strptime(pl.Time, format="%H:%M:%S"),
            pl.col("Timestamp").str.strptime(pl.Datetime, format="%d-%m-%Y %H:%M:%S")
        ])
        logger.debug("Timestamp creation completed", extra={"output_rows": df.height})
        return df

    def null_value_handling(self, df: pl.DataFrame, required_columns: list[str]) -> pl.DataFrame:
        """
        Handle NULL values in required columns by dropping rows with NULLs.

        Args:
            df (pl.DataFrame): The input DataFrame.
            required_columns (list[str]): List of required column names.

        Returns:
            pl.DataFrame: The DataFrame with NULL values handled.
        """
        before_rows = df.height
        logger.debug("Handling NULL values", extra={"required_columns": required_columns, "input_rows": before_rows})
        result = df.drop_nulls(subset=required_columns)
        after_rows = result.height
        dropped = before_rows - after_rows
        if dropped > 0:
            logger.warning("Dropped rows with NULL values", extra={"dropped_rows": dropped, "remaining_rows": after_rows})
        logger.debug("NULL value handling completed", extra={"output_rows": after_rows})
        return result

    def nan_value_handling(self, df: pl.DataFrame, numeric_columns: list[str]) -> pl.DataFrame:
        """
        Handle NaN values in numeric columns by dropping rows with NaNs.

        Args:
            df (pl.DataFrame): The input DataFrame.
            numeric_columns (list[str]): List of numeric column names.

        Returns:
            pl.DataFrame: The DataFrame with NaN values handled.
        """
        before_rows = df.height
        logger.debug("Handling NaN values", extra={"numeric_columns": numeric_columns, "input_rows": before_rows})
        result = df.filter(
            ~pl.any_horizontal(
                [
                    pl.col(column).is_nan()
                    for column in numeric_columns
                ]
            )
        )
        after_rows = result.height
        dropped = before_rows - after_rows
        if dropped > 0:
            logger.warning("Dropped rows with NaN values", extra={"dropped_rows": dropped, "remaining_rows": after_rows})
        logger.debug("NaN value handling completed", extra={"output_rows": after_rows})
        return result

    def duplicate_value_handling(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Handle duplicate rows in the DataFrame by dropping duplicates.

        Args:
            df (pl.DataFrame): The input DataFrame.

        Returns:
            pl.DataFrame: The DataFrame with duplicates removed.
        """
        before_rows = df.height
        logger.debug("Handling duplicate values", extra={"input_rows": before_rows})
        result = df.unique(
            subset=["Ticker", "Timestamp"],
            keep="first",
            maintain_order=True
        )
        after_rows = result.height
        dropped = before_rows - after_rows
        if dropped > 0:
            logger.warning("Dropped duplicate rows", extra={"dropped_rows": dropped, "remaining_rows": after_rows})
        logger.debug("Duplicate value handling completed", extra={"output_rows": after_rows})
        return result

    def candle_value_handling(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Remove the negative values from the candles

        Args:
            df (pl.DataFrame): The input DataFrame.

        Returns:
            pl.DataFrame: The DataFrame with validated candle values.
        """
        before_rows = df.height
        logger.debug("Handling invalid candle values", extra={"input_rows": before_rows})
        result = df.filter(
            (pl.col("Open") > 0)
            & (pl.col("High") > 0)
            & (pl.col("Low") > 0)
            & (pl.col("Close") > 0)
        )
        after_rows = result.height
        dropped = before_rows - after_rows
        if dropped > 0:
            logger.warning("Dropped rows with non-positive prices", extra={"dropped_rows": dropped, "remaining_rows": after_rows})
        logger.debug("Candle value handling completed", extra={"output_rows": after_rows})
        return result