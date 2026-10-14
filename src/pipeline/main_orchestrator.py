import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from shared.config import app_settings
from shared.observability import get_logger

logger = get_logger(__name__)

class PipelineOrchestrator:
    """Unified orchestrator for the complete market data pipeline."""
    
    def __init__(self, config_override: Optional[Dict] = None):
        self.config_override = config_override or {}
        self._apply_config_overrides()
        self.parquet_to_table_map = {"BSE_INDEX_EQUITY": "spot", "BSE_FO_FNO": "options", "MCX_FO_FNO": "", "NSE_COM_FNO": "", "NSE_EQ_EQUITY": "", "NSE_FO_FNO": "options", "NSE_INDEX_EQUITY": "spot"}

    def _apply_config_overrides(self):
        """Apply runtime config overrides."""
        for key, value in self.config_override.items():
            if hasattr(app_settings, key):
                setattr(app_settings, key, value)
                logger.info(f"Config override: {key} = {value}")

    def run_extraction(self, start_date: str = "", end_date: str = "") -> Dict:
        """Run extraction phase - produces Bronze layer."""
        from extraction.core.daily_data_extractor import DailyDataExtractor
        
        logger.info("=" * 60)
        logger.info("PHASE 1: EXTRACTION (Bronze Layer)")
        logger.info("=" * 60)
        
        # Override dates
        original_start = app_settings.extractor_settings.start_date
        original_end = app_settings.extractor_settings.end_date
        
        try:
            app_settings.extractor_settings.start_date = start_date
            app_settings.extractor_settings.end_date = end_date
            
            extractor = DailyDataExtractor()
            master_instrument = extractor.client_object.master_instrument
            variation = extractor.extraction_config.extraction_variation

            if start_date == "" or end_date == "":
                logger.info("No date range specified, processing current day (intraday)")
                process_able_date = datetime.now()
                dates = [process_able_date]
            else:
                start, end = datetime.strptime(start_date, "%Y-%m-%d").date(), datetime.strptime(end_date, "%Y-%m-%d").date()
                dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
                logger.info("Historical date range", extra={"start_date": start_date, "end_date": end_date, "days": len(dates)})

            for date in dates:
                processed_data = extractor._day_process(date=date, master_instrument=master_instrument, variation=variation)

        finally:
            # Restore original dates
            app_settings.extractor_settings.start_date = original_start
            app_settings.extractor_settings.end_date = original_end

    def run_cloud_push(self, data_layer: str, date: str = "",) -> None:
        """Push data to cloud (S3)."""
        from extraction.core.daily_data_extractor import DailyDataExtractor

        if not app_settings.cloud_settings.push_to_cloud:
            logger.info("Cloud push disabled in settings, skipping")
            return

        logger.info("=" * 60)
        logger.info("PHASE 3: CLOUD PUSH (S3)")
        logger.info("=" * 60)

        client = app_settings.extractor_settings.client
        if date == "":
            process_able_date = datetime.strftime(datetime.now(), "%Y-%m-%d") 
        else:
            process_able_date = date

        cache_dir = Path(__file__).parent.parent / "cache" / client / data_layer / process_able_date
        files = list(cache_dir.iterdir())

        try:
            extractor = DailyDataExtractor()
            extractor._push_to_cloud(files)
            logger.info("Cloud push executed succesfully")
        except Exception as e:
            logger.info(f"Cloud push failed between excuption: {e}")

    def run_transformation(self, data_layer: str, date: str = "") -> Dict:
        """Run transformation phase - Bronze → Silver."""
        from transformation.orchestrator import transform_data
        from extraction.clients import client_discovery
        from transformation.clients import transformer_discovery
        from transformation.clients import transformer_registry
        
        logger.info("=" * 60)
        logger.info("PHASE 2: TRANSFORMATION (Silver Layer)")
        logger.info("=" * 60)

        client = app_settings.extractor_settings.client
        if date == "":
            process_able_date = datetime.strftime(datetime.now(), "%Y-%m-%d") 
        else:
            process_able_date = date

        date = datetime.strptime(process_able_date, "%Y-%m-%d").date()

        cache_dir = Path(__file__).parent.parent / "cache" / client / data_layer / process_able_date
        files = {
                path.name.split(".")[0]: path
                for path in cache_dir.iterdir()
                if path.is_file()
            }
        
        # Initialize transformer (uses same client as extraction)
        client_discovery()
        transformer_discovery()
        
        client_name = app_settings.extractor_settings.client.lower()
        transformer = transformer_registry.get(client_name)()
        
        # Transform the bronze data
        silver_result = transform_data(files, transformer, date)
        
        logger.info(f"Transformation completed for {date} date")
        return silver_result

    def run_ingestion(self, data_layer: str, date: str = "", target_db: Optional[str] = None) -> Dict:
        """Run DB ingestion phase - Silver → Database."""
        from ingestion.core.orchestrator import IngestionOrchestrator
        
        if not app_settings.ingestion_settings.enabled:
            logger.info("DB ingestion disabled in settings, skipping")
            return {}
        
        logger.info("=" * 60)
        logger.info("PHASE 4: DB INGESTION (ClickHouse/PostgreSQL/TimescaleDB)")
        logger.info("=" * 60)

        client = app_settings.extractor_settings.client
        if date == "":
            process_able_date = datetime.strftime(datetime.now(), "%Y-%m-%d") 
        else:
            process_able_date = date

        cache_dir = Path(__file__).parent.parent / "cache" / client / data_layer / process_able_date
        files = {
                path.name.split(".")[0]: path
                for path in cache_dir.iterdir()
                if path.is_file()
            }
        
        # Override target if specified
        original_target = app_settings.ingestion_settings.target
        if target_db:
            app_settings.ingestion_settings.target = target_db
        
        try:
            orchestrator = IngestionOrchestrator()
            all_results = {}
            
            date_str = process_able_date

            if len(self.parquet_to_table_map) == 0:
                segments_table_map = {segment: "" for segment in files.keys()}
            else:
                segments_table_map = self.parquet_to_table_map
            # Find silver base path
            silver_base = Path(__file__).parent.parent / "cache" / app_settings.extractor_settings.client / "Silver"
            
            logger.info(f"Ingesting date {date_str} with segments: {segments_table_map}")
            results = orchestrator.run_ingestion(silver_base, date_str, segments_table_map)
            all_results[date_str] = results
            
            return all_results
        finally:
            if target_db:
                app_settings.ingestion_settings.target = original_target

def main():
    pipeline = PipelineOrchestrator({})
    pipeline.run_extraction()
    pipeline.run_cloud_push(data_layer="Bronze")
    pipeline.run_transformation(data_layer="Bronze")
    pipeline.run_cloud_push(data_layer="Silver")
    pipeline.run_cloud_push(data_layer="Silver")
    pipeline.run_ingestion(data_layer="Silver")


if __name__ == "__main__":
    main()
