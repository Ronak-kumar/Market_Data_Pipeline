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
