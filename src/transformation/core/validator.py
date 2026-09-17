import polars as pl
class CandleValidationError(ValueError): """Raised when candle data fails validation."""

def validate_parquet_schema(filepath: str) -> None:
        # File read and schema loading
        df = pl.read_parquet(filepath)
        actual_schema = df.schema

        # Schema validation
        PARQUET_EXPECTED_SCHEMA = {
            "Ticker": pl.String,
            "Date": pl.String,
            "Time": pl.String,
            "Open": pl.Float64,
            "High": pl.Float64,
            "Low": pl.Float64,
            "Close": pl.Float64,
            "Volume": pl.Float64,
            "Open Interest": pl.Float64,
        }
        
        for column, expected_dtype in PARQUET_EXPECTED_SCHEMA.items():

            if column not in actual_schema:
                raise ValueError(
                    f"Missing column: {column}"
                )

            if actual_schema[column] != expected_dtype:
                raise TypeError(
                    f"Invalid dtype for {column}: "
                    f"expected {expected_dtype}, "
                    f"got {actual_schema[column]}"
                )

        return df

def validate_candles(
    df: pl.DataFrame,
    expected_schema: dict[str, pl.DataType],
    *,
    check_duplicates: bool = True,
    check_ohlc: bool = True,
    check_positive_prices: bool = True,
    check_volume: bool = True,
    check_nan: bool = True,
) -> None:
    """
    Validate normalized market candle data.

    Raises:
        CandleValidationError: If any validation fails.
    """

    # ---------------------------------------------------------
    # 1. Schema validation
    # ---------------------------------------------------------

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

    # ---------------------------------------------------------
    # 2. Required columns
    # ---------------------------------------------------------

    required_columns = [
        "Timestamp",
        "Ticker",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Open Interest",
    ]

    missing_required = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_required:
        raise CandleValidationError(
            f"Missing candle columns: {missing_required}"
        )

    # ---------------------------------------------------------
    # 3. NULL validation
    # ---------------------------------------------------------

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

    # ---------------------------------------------------------
    # 4. NaN validation
    # ---------------------------------------------------------

    if check_nan:
        numeric_columns = [
            column
            for column in [
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "OI",
            ]
            if column in df.columns
        ]

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

    # ---------------------------------------------------------
    # 5. Positive price validation
    # ---------------------------------------------------------

    if check_positive_prices:

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

    # ---------------------------------------------------------
    # 6. OHLC relationship validation
    # ---------------------------------------------------------

    if check_ohlc:

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

    # ---------------------------------------------------------
    # 7. Volume validation
    # ---------------------------------------------------------

    if check_volume and "Volume" in df.columns:

        invalid_volume = df.filter(
            pl.col("Volume") < 0
        )

        if not invalid_volume.is_empty():
            raise CandleValidationError(
                f"Found {invalid_volume.height} rows "
                "with negative volume"
            )

    # ---------------------------------------------------------
    # 8. Timestamp validation
    # ---------------------------------------------------------

    if not df.schema["Timestamp"].is_temporal():
        raise CandleValidationError(
            "Timestamp column must be a temporal datatype"
        )

    # ---------------------------------------------------------
    # 9. Duplicate candle validation
    # ---------------------------------------------------------

    if check_duplicates:

        duplicate_keys = (
            df
            .group_by(["Ticker", "Timestamp"])
            .len()
            .filter(pl.col("len") > 1)
        )

        if not duplicate_keys.is_empty():
            raise CandleValidationError(
                f"Found {duplicate_keys.height} "
                "duplicate Ticker + Timestamp combinations"
            )

    # ---------------------------------------------------------
    # Everything passed
    # ---------------------------------------------------------

    return None
