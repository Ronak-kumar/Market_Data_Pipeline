import datetime
from pathlib import WindowsPath
from transformation.clients.adapters.upstox import UpstoxTransformationAdapter

if __name__ == "__main__":
    transformeer = UpstoxTransformationAdapter()
    filemap = {datetime.date(2026, 9, 11): 
            {'NSE_INDEX': WindowsPath('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/2026-09-11/NSE_INDEX.parquet'),
            'NSE_FO': WindowsPath('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/2026-09-11/NSE_FO.parquet'),
            'BSE_INDEX': WindowsPath('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/2026-09-11/BSE_INDEX.parquet'),
            'BSE_FO': WindowsPath('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/2026-09-11/BSE_FO.parquet'),
            'MCX_FO': WindowsPath('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/2026-09-11/MCX_FO.parquet')}}

    for date, filepaths in filemap.items():
        print(f"Date: {date}")
        for segment, filepath in filepaths.items():
            print(f"Segment: {segment}, Filepath: {filepath}")

            normalized_df = transformeer.base_transformation(filepath=filepath, segment=segment)

            if "MCX" in segment:
                df = transformeer.mcx_transformation(normalized_df)
            elif "INDEX" in segment:
                df = transformeer.equity_transformation(normalized_df)
            elif "FO" in segment:
                df = transformeer.fno_transformation(normalized_df)

            print(df)
