from pathlib import WindowsPath
from polars import DataFrame
from typing import Dict

class DataTransformer:

    def base_transformation(self, filepath: WindowsPath) -> DataFrame: 
        pass

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

    
