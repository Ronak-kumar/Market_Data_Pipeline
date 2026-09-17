from pathlib import WindowsPath
from polars import DataFrame
from typing import Dict
import polars as pl
from transformation.core.validator import validate_candles, validate_parquet_schema
from transformation.core.converter import base_transformation


class DataTransformer:

    def base_transformation(self, filepath: WindowsPath) -> DataFrame:

        df = validate_parquet_schema(filepath)
        df = base_transformation(df)
        EXPECTED_SCHEMA = { "Timestamp": pl.Datetime("us"), "Ticker": pl.String, "Open": pl.Float32, "High": pl.Float32, "Low": pl.Float32, "Close": pl.Float32, "Volume": pl.UInt32, "Open Interest":  pl.UInt32}
        validate_candles(df, expected_schema=EXPECTED_SCHEMA)

        return df

    
    def fno_transformation(self, data: DataFrame) -> DataFrame:
        # base clean
        # data trsansformation
        # data validation
        # range filler 
        pass
        
    def equity_transformation(self, data: DataFrame) -> DataFrame:
        pass
        
    def mcx_transformation(self, data: DataFrame) -> DataFrame:
        pass

