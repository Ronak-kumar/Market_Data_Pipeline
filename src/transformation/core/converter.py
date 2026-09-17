import polars as pl

def base_transformation(df: pl.DataFrame) -> pl.DataFrame:
    # Data Type Conversion
    df = df.with_columns([
        pl.col("Open").cast(pl.Float32),
        pl.col("High").cast(pl.Float32),
        pl.col("Low").cast(pl.Float32),
        pl.col("Close").cast(pl.Float32),
        pl.col("Volume").cast(pl.UInt32),
        pl.col("Open Interest").cast(pl.UInt32)
    ])

    df = df.with_columns([
        (pl.col("Date") + " " + pl.col("Time")).alias("Timestamp")
    ])
    df = df.with_columns([
        pl.col("Date").str.strptime(pl.Date, format="%d-%m-%Y"),
        pl.col("Time").str.strptime(pl.Time, format="%H:%M:%S"),
        pl.col("Timestamp").str.strptime(pl.Datetime, format="%d-%m-%Y%H:%M:%S")
    ])

    return df