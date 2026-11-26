import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from shared.config.settings import get_application_settings
from shared.observability import get_logger

logger = get_logger(__name__)

CACHE_PATH = Path(__file__).parent.parent / "cache"


def _get_settings():
    """Load settings fresh from disk so UI changes take effect immediately."""
    return get_application_settings()


class PipelineOrchestrator:
    """Unified orchestrator for the complete market data pipeline."""

    def __init__(self, config_override: Optional[Dict] = None):
        self.config_override = config_override or {}
        self._apply_config_overrides()
        self.parquet_to_table_map = {
        }

    def _apply_config_overrides(self):
        """Apply runtime config overrides."""
        settings = _get_settings()
        for key, value in self.config_override.items():
            if hasattr(settings, key):
                setattr(settings, key, value)
                logger.info(f"Config override: {key} = {value}")

    def run_extraction(self) -> Dict:
        """Run extraction phase - produces Bronze layer. Reads dates from settings.yaml."""
        from extraction.main import DailyDataExtractor

        settings = _get_settings()
        logger.info("=" * 60)
        logger.info("PHASE 1: EXTRACTION (Bronze Layer)")
        logger.info("=" * 60)

        # Read dates from settings.yaml (source of truth)
        start_date = settings.extractor_settings.start_date
        end_date = settings.extractor_settings.end_date

        extractor = DailyDataExtractor()

        if start_date == "" or end_date == "":
            logger.info("No date range specified in settings.yaml, processing current day (intraday)")
            process_able_date = datetime.now()
            dates = [process_able_date]
            variation = "intraday"
        else:
            start, end = datetime.strptime(start_date, "%Y-%m-%d").date(), datetime.strptime(end_date, "%Y-%m-%d").date()
            dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
            logger.info(f"Historical date range: start={start_date}, end={end_date}, days={len(dates)}")
            variation = "historical"
        for date in dates:
            processed_data = extractor._day_process(date=date, variation=variation)

    def run_cloud_push(self, data_layer: str) -> None:
        """Push data to cloud (S3). Reads date from settings.yaml if not provided."""
        from cloud import CloudOrchestrator

        settings = _get_settings()

        if not settings.cloud_settings.push_to_cloud:
            logger.info("Cloud push disabled in settings, skipping")
            return

        logger.info("=" * 60)
        logger.info("PHASE 3: CLOUD PUSH (S3)")
        logger.info("=" * 60)

        # Read dates from settings.yaml (source of truth)
        start_date = settings.extractor_settings.start_date
        end_date = settings.extractor_settings.end_date
        cloud_orchestrator = CloudOrchestrator(settings)

        if start_date == "" or end_date == "":
            logger.info("No date range specified in settings.yaml, processing current day (intraday)")
            process_able_date = datetime.now()
            dates = [process_able_date]
        else:
            start, end = datetime.strptime(start_date, "%Y-%m-%d").date(), datetime.strptime(end_date, "%Y-%m-%d").date()
            dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
            logger.info(f"Historical date range: start={start_date}, end={end_date}, days={len(dates)}")

        for date in dates:
            process_able_date = date.strftime("%Y-%m-%d")

            try:
                cloud_orchestrator.push_to_cloud(process_able_date, data_layer)
                logger.info(f"Cloud push executed successfully for {data_layer} on {process_able_date}")
            except Exception as e:
                logger.error(f"Cloud push failed for date {process_able_date} Exception: {e}")

    def run_transformation(self, data_layer: str) -> None:
        """Run transformation phase - Bronze to Silver. Reads date from settings.yaml if not provided."""
        from transformation import transform_data
        from transformation import transformer_discovery
        from transformation import transformer_registry

        settings = _get_settings()

        logger.info("=" * 60)
        logger.info("PHASE 2: TRANSFORMATION (Silver Layer)")
        logger.info("=" * 60)

        # Use date from YAML if not provided
        start_date = settings.extractor_settings.start_date
        end_date = settings.extractor_settings.end_date

        # Initialize transformer (uses same client as extraction)
        transformer_discovery()
        client_name = settings.extractor_settings.client.lower()
        transformer = transformer_registry.get(client_name)()

        if start_date == "" or end_date == "":
            logger.info("No date range specified in settings.yaml, processing current day (intraday)")
            current_date = datetime.now()
            dates = [current_date]
        else:
            start, end = datetime.strptime(start_date, "%Y-%m-%d").date(), datetime.strptime(end_date, "%Y-%m-%d").date()
            dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
            logger.info(f"Historical date range: start={start_date}, end={end_date}, days={len(dates)}")

        for date in dates:
            process_able_date = date.strftime("%Y-%m-%d")

            cache_dir = CACHE_PATH / client_name / data_layer / process_able_date

            files = {
                path.name.split(".")[0]: path
                for path in cache_dir.iterdir()
                if path.is_file()
            }

            # Transform the bronze data
            silver_result = transform_data(files, transformer, date)

            logger.info(f"Transformation completed for {process_able_date} date")

    def run_ingestion(self, data_layer: str, target_db: Optional[str] = None) -> None:
        """Run DB ingestion phase - Silver to Database. Reads date from settings.yaml if not provided."""
        from ingestion.core.orchestrator import IngestionOrchestrator

        settings = _get_settings()

        if not settings.ingestion_settings.enabled:
            logger.info("DB ingestion disabled in settings, skipping")
            return {}

        logger.info("=" * 60)
        logger.info("PHASE 4: DB INGESTION (ClickHouse/PostgreSQL/TimescaleDB)")
        logger.info("=" * 60)

        client_name = settings.extractor_settings.client.lower()
        start_date = settings.extractor_settings.start_date
        end_date = settings.extractor_settings.end_date

        if start_date == "" or end_date == "":
            logger.info("No date range specified in settings.yaml, processing current day (intraday)")
            current_date = datetime.now()
            dates = [current_date]
        else:
            start, end = datetime.strptime(start_date, "%Y-%m-%d").date(), datetime.strptime(end_date, "%Y-%m-%d").date()
            dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
            logger.info(f"Historical date range: start={start_date}, end={end_date}, days={len(dates)}")

        for date in dates:
            process_able_date = date.strftime("%Y-%m-%d")

            cache_dir = CACHE_PATH / client_name / data_layer / process_able_date
            files = {
                path.name.split(".")[0]: path
                for path in cache_dir.iterdir()
                if path.is_file()
            }

            # Override target if specified
            original_target = settings.ingestion_settings.target
            if target_db:
                settings.ingestion_settings.target = target_db

            try:
                orchestrator = IngestionOrchestrator()
                all_results = {}

                date_str = process_able_date

                if len(self.parquet_to_table_map) == 0:
                    segments_table_map = {segment: "" for segment in files.keys()}
                else:
                    segments_table_map = self.parquet_to_table_map
                # Find silver base path
                silver_base = CACHE_PATH / client_name / "Silver"

                logger.info(f"Ingesting date {date_str} with segments: {segments_table_map}")
                results = orchestrator.run_ingestion(silver_base, date_str, segments_table_map)
                all_results[date_str] = results

            finally:
                if target_db:
                    settings.ingestion_settings.target = original_target


def main():
    pipeline = PipelineOrchestrator({})
    pipeline.run_extraction()
    pipeline.run_cloud_push(data_layer="Bronze")
    pipeline.run_transformation(data_layer="Bronze")
    pipeline.run_cloud_push(data_layer="Silver")
    pipeline.run_ingestion(data_layer="Silver")


if __name__ == "__main__":
    main()
