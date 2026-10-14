#!/usr/bin/env python
"""
Unified Market Data Pipeline Orchestrator
Runs the complete pipeline: Extraction → Transformation → Cloud Push → DB Ingestion
"""
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
    
    def run_extraction(self, start_date: str, end_date: str) -> Dict:
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
            bronze_result = extractor.process()
            
            logger.info(f"Extraction completed for {len(bronze_result)} dates")
            return bronze_result
            
        finally:
            # Restore original dates
            app_settings.extractor_settings.start_date = original_start
            app_settings.extractor_settings.end_date = original_end
    
    def run_transformation(self, bronze_result: Dict) -> Dict:
        """Run transformation phase - Bronze → Silver."""
        from transformation.orchestrator import transform_data
        from extraction.clients import client_discovery
        from transformation.clients import transformer_discovery
        from transformation.clients import transformer_registry
        
        logger.info("=" * 60)
        logger.info("PHASE 2: TRANSFORMATION (Silver Layer)")
        logger.info("=" * 60)
        
        # Initialize transformer (uses same client as extraction)
        client_discovery()
        transformer_discovery()
        
        client_name = app_settings.extractor_settings.client.lower()
        transformer = transformer_registry.get(client_name)()
        
        # Transform the bronze data
        silver_result = transform_data(bronze_result, transformer)
        
        logger.info(f"Transformation completed for {len(silver_result)} dates")
        return silver_result
    
    def run_cloud_push(self, bronze_result: Dict, silver_result: Dict) -> None:
        """Push data to cloud (S3) - handled automatically by extractor/transformer."""
        if not app_settings.cloud_settings.push_to_cloud:
            logger.info("Cloud push disabled in settings, skipping")
            return
        
        logger.info("=" * 60)
        logger.info("PHASE 3: CLOUD PUSH (S3)")
        logger.info("=" * 60)
        
        # The extractor/transformer already handle cloud push internally
        # via _push_to_cloud() calls during processing
        logger.info("Cloud push handled automatically during extraction/transformation")
    
    def run_ingestion(self, silver_result: Dict, target_db: Optional[str] = None) -> Dict:
        """Run DB ingestion phase - Silver → Database."""
        from ingestion.core.orchestrator import IngestionOrchestrator
        
        if not app_settings.ingestion_settings.enabled:
            logger.info("DB ingestion disabled in settings, skipping")
            return {}
        
        logger.info("=" * 60)
        logger.info("PHASE 4: DB INGESTION (ClickHouse/PostgreSQL/TimescaleDB)")
        logger.info("=" * 60)
        
        # Override target if specified
        original_target = app_settings.ingestion_settings.target
        if target_db:
            app_settings.ingestion_settings.target = target_db
        
        try:
            orchestrator = IngestionOrchestrator()
            all_results = {}
            
            for date, date_map in silver_result.items():
                date_str = date.strftime("%Y-%m-%d")

                if len(self.parquet_to_table_map) == 0:
                    segments_table_map = {segment: "" for segment in date_map.keys()}
                else:
                    segments_table_map = self.parquet_to_table_map
                # Find silver base path
                silver_base = Path(__file__).parent.parent / "extraction" / "cache" / app_settings.extractor_settings.client / "Silver"
                
                logger.info(f"Ingesting date {date_str} with segments: {segments_table_map}")
                results = orchestrator.run_ingestion(silver_base, date_str, segments_table_map)
                all_results[date_str] = results
            
            return all_results
        finally:
            if target_db:
                app_settings.ingestion_settings.target = original_target
    
    def run_full_pipeline(
        self,
        start_date: str,
        end_date: str,
        skip_extraction: bool = False,
        skip_transformation: bool = False,
        skip_ingestion: bool = False,
        target_db: Optional[str] = None,
    ) -> Dict:
        """Run the complete pipeline end-to-end."""
        
        logger.info(f"Starting full pipeline: {start_date} to {end_date}")
        
        results = {
            "extraction": {},
            "transformation": {},
            "ingestion": {},
        }
        
        # Phase 1: Extraction
        if not skip_extraction:
            results["extraction"] = self.run_extraction(start_date, end_date)
        else:
            logger.info("Skipping extraction (using existing Bronze data)")
            results["extraction"] = self.run_extraction(start_date, end_date)
        
        # Phase 2: Transformation
        if not skip_transformation:
            results["transformation"] = self.run_transformation(results["extraction"])
        else:
            logger.info("Skipping transformation (using existing Silver data)")
            results["transformation"] = results["extraction"]
        
        # Phase 3: Cloud Push (automatic)
        self.run_cloud_push(results["extraction"], results["transformation"])
        
        # Phase 4: DB Ingestion
        if not skip_ingestion:
            results["ingestion"] = self.run_ingestion(
                results["transformation"], 
                target_db=target_db
            )
        else:
            logger.info("Skipping DB ingestion")
        
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETED")
        logger.info("=" * 60)
        
        return results


def parse_date(date_str: str) -> str:
    """Parse and validate date string."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")


def main():
    parser = argparse.ArgumentParser(
        description="Unified Market Data Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline for date range
  python -m pipeline.orchestrator --start-date 2026-10-01 --end-date 2026-10-05

  # Run with specific target database
  python -m pipeline.orchestrator --start-date 2026-10-01 --end-date 2026-10-05 --target-db postgresql

  # Skip extraction (use existing Bronze data)
  python -m pipeline.orchestrator --start-date 2026-10-01 --end-date 2026-10-05 --skip-extraction

  # Skip ingestion
  python -m pipeline.orchestrator --start-date 2026-10-01 --end-date 2026-10-05 --skip-ingestion

  # Override client at runtime
  python -m pipeline.orchestrator --start-date 2026-10-01 --end-date 2026-10-05 --client zerodha
        """
    )
    
    parser.add_argument(
        "--start-date",
        type=parse_date,
        required=True,
        help="Start date (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--end-date",
        type=parse_date,
        required=True,
        help="End date (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--client",
        choices=["upstox", "groww", "dhan", "zerodha"],
        help="Override broker client (default from settings.yaml)"
    )
    
    parser.add_argument(
        "--target-db",
        choices=["clickhouse", "postgresql", "timescaledb"],
        help="Override target database for ingestion"
    )
    
    parser.add_argument(
        "--skip-extraction",
        action="store_true",
        help="Skip extraction phase (use existing Bronze data)"
    )
    
    parser.add_argument(
        "--skip-transformation",
        action="store_true",
        help="Skip transformation phase (use existing Silver data)"
    )
    
    parser.add_argument(
        "--skip-ingestion",
        action="store_true",
        help="Skip DB ingestion phase"
    )
    
    parser.add_argument(
        "--segments",
        nargs="+",
        help="Specific segments to process (default: all from settings)"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level"
    )
    
    args = parser.parse_args()
    
    # Build config overrides
    config_override = {}
    if args.client:
        config_override["extractor_settings.client"] = args.client
    if args.segments:
        config_override["broker_configuration.processable_segments"] = args.segments
    
    # Run pipeline
    orchestrator = PipelineOrchestrator(config_override=config_override)
    
    try:
        results = orchestrator.run_full_pipeline(
            start_date=args.start_date,
            end_date=args.end_date,
            skip_extraction=args.skip_extraction,
            skip_transformation=args.skip_transformation,
            skip_ingestion=args.skip_ingestion,
            target_db=args.target_db,
        )
        
        # Print summary
        print("\n" + "=" * 60)
        print("PIPELINE SUMMARY")
        print("=" * 60)
        print(f"Date Range: {args.start_date} to {args.end_date}")
        print(f"Client: {args.client or app_settings.extractor_settings.client}")
        print(f"Target DB: {args.target_db or app_settings.ingestion_settings.target}")
        print(f"Extraction: {'Skipped' if args.skip_extraction else 'Completed'}")
        print(f"Transformation: {'Skipped' if args.skip_transformation else 'Completed'}")
        print(f"Ingestion: {'Skipped' if args.skip_ingestion else 'Completed'}")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    # Run pipeline
    config_override = {}
    orchestrator = PipelineOrchestrator(config_override=config_override)
    
    results = orchestrator.run_full_pipeline(
        start_date="2026-10-09",
        end_date="2026-10-09",
        skip_extraction=False,
        skip_transformation=False,
        skip_ingestion=False,
        target_db="clickhouse",
    )


    # main()
