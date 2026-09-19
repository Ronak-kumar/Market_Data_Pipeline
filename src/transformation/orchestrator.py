import datetime
from pathlib import Path
from transformation.clients.adapters.upstox import UpstoxTransformationAdapter

def transform_data(filemap: dict[str: Path]) -> None:
    transformeer = UpstoxTransformationAdapter()
    for date, filepaths in filemap.items():
        print(f"Date: {date}")
        for segment, filepath in filepaths.items():
            print(f"Segment: {segment}, Filepath: {filepath}")

            normalized_df = transformeer.base_transformation(filepath=filepath, segment=segment)

            if "INDEX" in segment:
                segment_frame = transformeer.equity_transformation(normalized_df)
            elif "FO" in segment or "MCX" in segment:
                segment_frame = transformeer.fno_transformation(normalized_df)

            parts = list(filepath.parts)
            parts[parts.index("Bronze")] = "Silver"

            saving_path = Path(*parts[:-1])
            saving_path.mkdir(parents=True, exist_ok=True)
            segment_frame.write_parquet(saving_path / f"{segment}.parquet")


### Debugging purpose only ####
if __name__ == "__main__":
    transformeer = UpstoxTransformationAdapter()
    filemap = {datetime.date(2026, 9, 18): 
            {'NSE_INDEX': Path('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/2026-09-18/NSE_INDEX.parquet'),
            'NSE_FO': Path('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/2026-09-18/NSE_FO.parquet'),
            'BSE_INDEX': Path('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/2026-09-18/BSE_INDEX.parquet'),
            'BSE_FO': Path('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/2026-09-18/BSE_FO.parquet'),
            'MCX_FO': Path('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/2026-09-18/MCX_FO.parquet')}}

    for date, filepaths in filemap.items():
        print(f"Date: {date}")
        for segment, filepath in filepaths.items():
            print(f"Segment: {segment}, Filepath: {filepath}")

            normalized_df = transformeer.base_transformation(filepath=filepath, segment=segment)

            if "MCX" in segment:
                segment_frame = transformeer.mcx_transformation(normalized_df)
            elif "INDEX" in segment:
                segment_frame = transformeer.equity_transformation(normalized_df)
            elif "FO" in segment:
                segment_frame = transformeer.fno_transformation(normalized_df)

            saving_path = filepath.replace("Bronze", "Silver")
            saving_path.mkdir(parents=True, exist_ok=True)
            segment_frame.write_parquet(saving_path / f"{segment}.parquet")