import datetime
from pathlib import Path
from transformation.clients import DataTransformer
from shared.observability import get_logger
from transformation.core import IntradayRangeFiller
from shared.config import app_settings
from datetime import date

logger = get_logger(__name__)

def transform_data(filepaths: dict[str, Path], transformer: DataTransformer, date: date) -> None:

    for segment, filepath in filepaths.items():
        logger.info("Processing segment", extra={"segment": segment, "filepath": str(filepath)})

        segment_parts = segment.split("_")

        segment = "_".join(segment_parts[:2])

        normalized_df = transformer.base_transformation(filepath=filepath, segment=segment)
        logger.debug("Base transformation completed", extra={"segment": segment, "rows": normalized_df.height})

        range = app_settings.broker_configuration.session_bounds.get(segment, {})

        if len(range) == 0:
            range["start"], range["end"] = normalized_df["Time"].min()[:5], normalized_df["Time"].max()[:5]

        range_filled_segment_frame = IntradayRangeFiller.range_filler(normalized_df, date=date, start_time=range["start"], end_time=range["end"],
                                                                        interval=app_settings.extractor_settings.interval)
        logger.debug("Range Filling Completed", extra={"segment": segment, "rows": range_filled_segment_frame.height})

        segments_asset_class_map = transformer.asset_segregation(range_filled_segment_frame)

        for segment_type, segment_frame in segments_asset_class_map.items():

            if segment_frame.height == 0:
                continue

            segment_frame = segment_frame.drop(["Asset_Class"])

            if "equity" == segment_type:
                segment_frame = transformer.equity_transformation(segment_frame)
                logger.debug("Equity transformation completed", extra={"segment": segment, "rows": segment_frame.height})
            elif "fno" == segment_type:
                segment_frame = transformer.fno_transformation(segment_frame)
                logger.debug("FNO transformation completed", extra={"segment": segment, "rows": segment_frame.height})
            else:
                logger.warning("Unknown segment type, skipping", extra={"segment": segment})
                continue

            parts = list(filepath.parts)
            parts[parts.index("Bronze")] = "Silver"
            saving_path = Path(*parts[:-1])
            saving_path.mkdir(parents=True, exist_ok=True)
            file_name = f"{segment}_{segment_type.upper()}"
            output_path = saving_path / f"{file_name}.parquet"
            segment_frame.write_parquet(output_path)
            logger.info("Segment written to Silver layer", extra={"segment": segment, "output_path": str(output_path), "rows": segment_frame.height})


