import datetime
from pathlib import Path
from transformation.clients import DataTransformer
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
