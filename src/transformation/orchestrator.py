import datetime
from pathlib import Path
from transformation.clients import DataTransformer
# from transformation.clients.adapters.upstox import UpstoxTransformationAdapter
from shared.observability import get_logger

logger = get_logger(__name__)

def transform_data(filemap: dict[str, Path], transformer: DataTransformer) -> None:
    logger.info("Starting transformation pipeline", extra={"dates_count": len(filemap)})
    for date, filepaths in filemap.items():
        logger.info("Processing date", extra={"date": str(date), "segments_count": len(filepaths)})
        for segment, filepath in filepaths.items():
            logger.info("Processing segment", extra={"segment": segment, "filepath": str(filepath)})

            normalized_df = transformer.base_transformation(filepath=filepath, segment=segment)
            logger.debug("Base transformation completed", extra={"segment": segment, "rows": normalized_df.height})

            if "INDEX" in segment:
                segment_frame = transformer.equity_transformation(normalized_df)
                logger.debug("Equity transformation completed", extra={"segment": segment, "rows": segment_frame.height})
            elif "FO" in segment or "MCX" in segment:
                segment_frame = transformer.fno_transformation(normalized_df)
                logger.debug("FNO transformation completed", extra={"segment": segment, "rows": segment_frame.height})
            else:
                logger.warning("Unknown segment type, skipping", extra={"segment": segment})
                continue

            parts = list(filepath.parts)
            parts[parts.index("Bronze")] = "Silver"

            saving_path = Path(*parts[:-1])
            saving_path.mkdir(parents=True, exist_ok=True)
            output_path = saving_path / f"{segment}.parquet"
            segment_frame.write_parquet(output_path)
            logger.info("Segment written to Silver layer", extra={"segment": segment, "output_path": str(output_path), "rows": segment_frame.height})


### Debugging purpose only ####
if __name__ == "__main__":
    transformer = UpstoxTransformationAdapter()
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

            normalized_df = transformer.base_transformation(filepath=filepath, segment=segment)

            if "MCX" in segment:
                segment_frame = transformer.mcx_transformation(normalized_df)
            elif "INDEX" in segment:
                segment_frame = transformer.equity_transformation(normalized_df)
            elif "FO" in segment:
                segment_frame = transformer.fno_transformation(normalized_df)

            saving_path = filepath.replace("Bronze", "Silver")
            saving_path.mkdir(parents=True, exist_ok=True)
            segment_frame.write_parquet(saving_path / f"{segment}.parquet")