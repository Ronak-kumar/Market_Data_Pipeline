from pathlib import WindowsPath
from polars import DataFrame
from typing import Dict
import polars as pl
from transformation.core import DefaultValidator, DefaultConverter
from transformation.schemas import RAW_PARQUET_SCHEMA

class DataTransformer:

    def base_transformation(self, filepath: WindowsPath) -> DataFrame:
        validator = DefaultValidator()
        converter = DefaultConverter()

        df = pl.read_parquet(filepath)

        EXPECTED_SCHEMA = RAW_PARQUET_SCHEMA

        ### Schema Validation and Conversion
        try:
            validator.validate_schema(df, expected_schema=EXPECTED_SCHEMA)
        except ValueError as e:
            try:
                df = converter.schema_type_conversion(df, expected_schema=EXPECTED_SCHEMA)
            except ValueError as e:
                print(f"Schema conversion failed: {e}")
                raise


        ### Null Validation
        required_columns = ["Ticker", "Date", "Time", "Open", "High", "Low", "Close", "Volume", "Open Interest"]
        try:
            validator.null_validation(df, required_columns=required_columns)
        except ValueError as e:
            try:
                df = converter.null_value_handling(df, required_columns=required_columns)
            except ValueError as e:
                print(f"Null value handling failed: {e}")
                raise


        ### NaN Validation and Conversion
        numeric_columns = ["Open", "High", "Low", "Close", "Volume", "Open Interest"]
        try:
            validator.nan_validation(df, numeric_columns=numeric_columns)
        except ValueError as e:
            try:
                df = converter.nan_value_handling(df, numeric_columns=numeric_columns)
            except ValueError as e:
                print(f"NaN value handling failed: {e}")
                raise

        ### Timestamp Creation
        try:
            df = converter.timestamp_creation(df, date_col="Date", time_col="Time")
        except ValueError as e:
            print(f"Timestamp creation failed: {e}")
            raise

        ### Duplicate Validation and Conversion
        required_columns = ["Open", "High", "Low", "Close", "Volume", "Open Interest"]
        try:
            validator.duplicate_validation(df)
        except ValueError as e:
            try:
                df = converter.duplicate_value_handling(df)
            except ValueError as e:
                print(f"Duplicate value handling failed: {e}")
                raise

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

