import polars as pl


class DefaultConverter:

    def schema_type_conversion(self, df: pl.DataFrame, expected_schema: dict[str, pl.DataType]) -> pl.DataFrame:
        """
        Convert the schema of a DataFrame to the expected types.

        Args:
            df (pl.DataFrame): The input DataFrame.

        Returns:
            pl.DataFrame: The DataFrame with converted schema.
        """
        return df.with_columns(
        pl.col(column).cast(dtype)
        for column, dtype in expected_schema.items())

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
        df = df.with_columns([(pl.col(date_col) + " " + pl.col(time_col)).alias("Timestamp")])
        df = df.with_columns([
            pl.col(date_col).str.strptime(pl.Date, format="%d-%m-%Y"),
            pl.col(time_col).str.strptime(pl.Time, format="%H:%M:%S"),
            pl.col("Timestamp").str.strptime(pl.Datetime, format="%d-%m-%Y %H:%M:%S")
        ])

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
        return df.drop_nulls(subset=required_columns)

    def nan_value_handling(self, df: pl.DataFrame, numeric_columns: list[str]) -> pl.DataFrame:
        """
        Handle NaN values in numeric columns by dropping rows with NaNs.

        Args:
            df (pl.DataFrame): The input DataFrame.
            numeric_columns (list[str]): List of numeric column names.

        Returns:
            pl.DataFrame: The DataFrame with NaN values handled.
        """
        return df.filter(
                ~pl.any_horizontal(
                    [
                        pl.col(column).is_nan()
                        for column in numeric_columns
                    ]
                )
            )

    def duplicate_value_handling(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Handle duplicate rows in the DataFrame by dropping duplicates.

        Args:
            df (pl.DataFrame): The input DataFrame.
        Returns:
            pl.DataFrame: The DataFrame with duplicates removed.
        """
        return df.unique(
        subset=["Ticker", "Timestamp"],
        keep="first",
        maintain_order=True)
