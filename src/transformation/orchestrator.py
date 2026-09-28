import datetime
from pathlib import Path
from transformation.clients import DataTransformer
from shared.observability import get_logger
from transformation.core import IntradayRangeFiller
from shared.config import app_settings
from transformation.clients.adapters.upstox import UpstoxTransformationAdapter

logger = get_logger(__name__)

def transform_data(filemap: dict[str, Path], transformer: DataTransformer, segment_type_map: dict) -> None:
    logger.info("Starting transformation pipeline", extra={"dates_count": len(filemap)})

    result_filemap = {}

    for date, filepaths in filemap.items():
        logger.info("Processing date", extra={"date": str(date), "segments_count": len(filepaths)})
        date_map = {}
        for segment, filepath in filepaths.items():
            logger.info("Processing segment", extra={"segment": segment, "filepath": str(filepath)})

            segment_parts = segment.split("_")

            segment = "_".join(segment_parts[:2])
            segment_asset_class = segment_parts[-1]

            normalized_df = transformer.base_transformation(filepath=filepath, segment=segment)
            logger.debug("Base transformation completed", extra={"segment": segment, "rows": normalized_df.height})

            range = app_settings.broker_configuration.session_bounds.get(segment, {})
            if len(range) == 0:
                range["start"], range["end"] = normalized_df["Time"].min()[:5], normalized_df["Time"].max()[:5]

            range_filled_segment_frame = IntradayRangeFiller.range_filler(normalized_df, date=date, start_time=range["start"], end_time=range["end"],
                                                                            interval=app_settings.extractor_settings.interval)
            logger.debug("Range Filling Completed", extra={"segment": segment, "rows": range_filled_segment_frame.height})

            segment_type = segment_asset_class.lower()

            if "cash" == segment_type:
                segment_frame = transformer.equity_transformation(range_filled_segment_frame)
                logger.debug("Equity transformation completed", extra={"segment": segment, "rows": segment_frame.height})
            elif "fo" == segment_type:
                segment_frame = transformer.fno_transformation(range_filled_segment_frame)
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
            date_map[segment] = output_path

        result_filemap[date] = date_map

    return result_filemap

if __name__ == "__main__":
    transformeer = UpstoxTransformationAdapter()
    filemap = {datetime.date(2026, 9, 24): 
            {'BSE_EQ_CASH': Path('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/Bronze/2026-09-24/BSE_EQ_CASH.parquet'),
            'BSE_FO': Path('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/Bronze/2026-09-24/BSE_FO.parquet'),
            'BSE_INDEX_CASH': Path('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/Bronze/2026-09-24/BSE_INDEX_CASH.parquet'),
            'GLOBAL_INDEX_CASH': Path('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/Bronze/2026-09-24/GLOBAL_INDEX_CASH.parquet'),
            'GLOBAL_INDICATOR_CASH': Path('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/Bronze/2026-09-24/GLOBAL_INDICATOR_CASH.parquet'),
            'MCX_FO': Path('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/Bronze/2026-09-24/MCX_FO.parquet'),
            'NCD_FO': Path('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/Bronze/2026-09-24/NCD_FO.parquet'),
            'NSE_FO': Path('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/Bronze/2026-09-24/NSE_FO.parquet'),
            'NSE_INDEX_CASH': Path('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/Bronze/2026-09-24/NSE_INDEX_CASH.parquet'),
            'NSE_EQ_CASH': Path('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/Bronze/2026-09-24/NSE_EQ_CASH.parquet'),
            'NSE_COM_CASH': Path('D:/Development/Coding_Projects/Main_projects/Market_Data_Pipeline/src/extraction/cache/upstox/Bronze/2026-09-24/NSE_COM_CASH.parquet')
            }
            }
    transform_data(filemap, transformeer, {})
