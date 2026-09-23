import datetime
from pathlib import Path
from transformation.clients import DataTransformer
from shared.observability import get_logger
from transformation.core import IntradayRangeFiller
from shared.config import app_settings

logger = get_logger(__name__)

def transform_data(filemap: dict[str, Path], transformer: DataTransformer, segment_type_map: dict) -> None:
    logger.info("Starting transformation pipeline", extra={"dates_count": len(filemap)})
    for date, filepaths in filemap.items():
        logger.info("Processing date", extra={"date": str(date), "segments_count": len(filepaths)})
        for segment, filepath in filepaths.items():
            logger.info("Processing segment", extra={"segment": segment, "filepath": str(filepath)})

            normalized_df = transformer.base_transformation(filepath=filepath, segment=segment)
            logger.debug("Base transformation completed", extra={"segment": segment, "rows": normalized_df.height})

            range = app_settings.broker_configuration.session_bounds[segment]
            range_filled_segment_frame = IntradayRangeFiller.range_filler(normalized_df, date=date, start_time=range["start"], end_time=range["end"],
                                                                            interval=app_settings.extractor_settings.interval)
            logger.debug("Range Filling Completed", extra={"segment": segment, "rows": range_filled_segment_frame.height})

            segment_type = (segment_type_map[segment]).lower()

            if "cash" in segment_type:
                segment_frame = transformer.equity_transformation(range_filled_segment_frame)
                logger.debug("Equity transformation completed", extra={"segment": segment, "rows": segment_frame.height})
            elif "fo" in segment_type:
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


