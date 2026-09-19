import polars as pl

RAW_PARQUET_SCHEMA = {
    "Ticker": pl.String, "Date": pl.String, "Time": pl.String,
    "Open": pl.Float32, "High": pl.Float32, "Low": pl.Float32,
    "Close": pl.Float32, "Volume": pl.UInt32, "Open Interest": pl.UInt32,
}

FNO_CANONICAL_SCHEMA = {}

INDICES_CANONICAL_SCHEMA = {}

MCX_CANONICAL_SCHEMA = {}