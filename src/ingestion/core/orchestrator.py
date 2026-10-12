"""
Ingestion Orchestrator
Coordinates data ingestion from Silver layer to target databases.
"""
from ingestion.clients import DataIngestor, IngestionResult, ingestion_registry, ingestion_discovery
from shared.config import app_settings
from shared.observability import get_logger
from pathlib import Path
from typing import Dict, List, Optional, Any
import time

logger = get_logger(__name__)


class IngestionOrchestrator:
    """Orchestrates ingestion from Silver layer to target database."""
    
    def __init__(self):
        self.settings = app_settings.ingestion_settings
        self.ingestor: Optional[DataIngestor] = None
        self._initialize_ingestor()

    def _initialize_ingestor(self) -> None:
        """Initialize the target ingestor based on settings."""
        ingestion_discovery()
        
        target = self.settings.target.lower()
        try:
            if target == "clickhouse":
                self.ingestor = ingestion_registry.get("clickhouse")()
            elif target == "postgresql":
                self.ingestor = ingestion_registry.get("postgresql")()
            elif target == "timescaledb":
                self.ingestor = ingestion_registry.get("timescaledb")()
            else:
                raise ValueError(f"Unknown ingestion target: {target}")
            
            logger.info(f"Ingestion orchestrator initialized with target: {target}")
            
        except KeyError as e:
            logger.error(f"Ingestor not registered for target: {target}", extra={"available": list(ingestion_registry._items.keys())})
            raise
        except Exception as e:
            logger.error(f"Failed to initialize ingestor: {e}", exc_info=True)
            raise

    def run_ingestion(self, silver_base_path: Path, date: str, segments: List[str]) -> Dict[str, IngestionResult]:
        """
        Run ingestion for a specific date and segments.
        
        Args:
            silver_base_path: Base path to silver layer (e.g., src/transformation/cache/{client}/Silver)
            date: Date string (YYYY-MM-DD)
            segments: List of segments to ingest
            
        Returns:
            Dict mapping segment to IngestionResult
        """
        if not self.settings.enabled:
            logger.info("Ingestion layer is disabled in settings")
            return {}
        
        if self.ingestor is None:
            logger.error("No ingestor initialized")
            return {}
        
        # Connect to database
        try:
            self.ingestor.connect()
        except Exception as e:
            logger.error("Failed to connect to database", extra={"error": str(e)})
            return {seg: IngestionResult(success=False, error=str(e)) for seg in segments}
        
        results = {}
        silver_date_path = silver_base_path / date
        
        if not silver_date_path.exists():
            logger.warning(f"Silver date path does not exist: {silver_date_path}")
            return {seg: IngestionResult(success=False, error="Path not found") for seg in segments}
        
        for segment in segments:
            logger.info(f"Starting ingestion for {segment} on {date}")
            
            # Find parquet file for segment
            parquet_file = silver_date_path / f"{segment}.parquet"
            
            if not parquet_file.exists():
                logger.warning(f"Parquet file not found: {parquet_file}")
                results[segment] = IngestionResult(success=False, error="Parquet file not found")
                continue
            
            # Get table schema
            schema = self.ingestor.get_table_schema(parquet_file)
            
            # Create table if needed
            if self.settings.create_tables:
                self.ingestor.create_table(segment.lower(), self.settings.clickhouse.database, segment.lower())
            
            # Ingest file
            result = self.ingestor.ingest_file(segment.lower(), parquet_file, format="parquet")
            results[segment] = result
            
            logger.info(f"Ingestion complete for {segment}", extra={
                "success": result.success,
                "records": result.records_ingested,
                "failed": result.records_failed,
                "duration": result.duration_seconds
            })
        
        # Disconnect
        self.ingestor.disconnect()
        
        return results

    def run_full_backfill(self, silver_base_path: Path, start_date: str, end_date: str, segments: List[str]) -> Dict[str, Dict[str, IngestionResult]]:
        """
        Run ingestion for a date range (backfill).
        
        Returns:
            Dict mapping date -> segment -> IngestionResult
        """
        from datetime import datetime, timedelta
        
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
        
        all_results = {}
        
        for date in dates:
            date_str = date.strftime("%Y-%m-%d")
            logger.info(f"Running backfill ingestion for {date_str}")
            all_results[date_str] = self.run_ingestion(silver_base_path, date_str, segments)
        
        return all_results

    def health_check(self) -> bool:
        """Check database connectivity."""
        if self.ingestor is None:
            return False
        return self.ingestor.health_check()


def run_ingestion(
    silver_base_path: str,
    date: str,
    segments: List[str],
    target: Optional[str] = None
) -> Dict[str, IngestionResult]:
    """
    Convenience function to run ingestion.
    
    Args:
        silver_base_path: Path to silver layer base directory
        date: Date string (YYYY-MM-DD)
        segments: List of segments to ingest
        target: Override target database (clickhouse, postgresql, timescaledb)
        
    Returns:
        Dict mapping segment to IngestionResult
    """
    # Temporarily override target if provided
    original_target = app_settings.ingestion_settings.target
    if target:
        app_settings.ingestion_settings.target = target
    
    try:
        orchestrator = IngestionOrchestrator()
        return orchestrator.run_ingestion(Path(silver_base_path), date, segments)
    finally:
        if target:
            app_settings.ingestion_settings.target = original_target


if __name__ == "__main__":
    # CLI entry point for standalone ingestion
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Ingest Parquet files into ClickHouse/PostgreSQL/TimescaleDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest single parquet file with auto table name
  python -m ingestion.core.orchestrator /data/parquet/NSE_FNO.parquet
  
  # Ingest with explicit table name
  python -m ingestion.core.orchestrator /data/parquet/NSE_FNO.parquet --table market_data_nse_fno
  
  # Ingest multiple files
  python -m ingestion.core.orchestrator /data/parquet/NSE_FNO.parquet /data/parquet/NSE_EQ.parquet
  
  # Ingest with explicit table names for multiple files
  python -m ingestion.core.orchestrator /data/parquet/NSE_FNO.parquet --table market_data_nse_fno /data/parquet/NSE_EQ.parquet --table market_data_nse_eq
  
  # Override target database
  python -m ingestion.core.orchestrator /data/parquet/NSE_FNO.parquet --target postgresql
        """
    )
    
    parser.add_argument(
        "parquet_files",
        nargs="+",
        help="Path(s) to parquet file(s) to ingest"
    )
    
    parser.add_argument(
        "--table", "-t",
        action="append",
        help="Table name for corresponding parquet file. If not provided, derived from filename. Use multiple --table for multiple files."
    )
    
    parser.add_argument(
        "--target",
        choices=["clickhouse", "postgresql", "timescaledb"],
        help="Override target database (default from settings.yaml)"
    )
    
    parser.add_argument(
        "--is-fno",
        action="append",
        type=lambda x: x.lower() == "true",
        help="Force F&O mode for corresponding parquet file (true/false). Auto-detected if not provided."
    )
    
    parser.add_argument(
        "--create-table",
        action="store_true",
        default=True,
        help="Create table if not exists (default: True)"
    )
    
    parser.add_argument(
        "--no-create-table",
        action="store_false",
        dest="create_table",
        help="Disable auto table creation"
    )
    
    args = parser.parse_args()
    
    # Validate table count matches file count if provided
    if args.table and len(args.table) != len(args.parquet_files):
        print(f"Error: Number of --table arguments ({len(args.table)}) must match number of parquet files ({len(args.parquet_files)})")
        sys.exit(1)
    
    if args.is_fno and len(args.is_fno) != len(args.parquet_files):
        print(f"Error: Number of --is-fno arguments ({len(args.is_fno)}) must match number of parquet files ({len(args.parquet_files)})")
        sys.exit(1)
    
    # Enable ingestion
    app_settings.ingestion_settings.enabled = True
    
    # Override target if provided
    original_target = app_settings.ingestion_settings.target
    if args.target:
        app_settings.ingestion_settings.target = args.target
    
    app_settings.ingestion_settings.create_tables = args.create_table
    
    try:
        orchestrator = IngestionOrchestrator()
        
        results = {}
        
        for i, parquet_file in enumerate(args.parquet_files):
            file_path = Path(parquet_file)
            
            if not file_path.exists():
                print(f"Error: Parquet file not found: {file_path}")
                results[str(file_path)] = IngestionResult(success=False, error="File not found")
                continue
            
            # Determine table name
            if args.table:
                table_name = args.table[i]
            else:
                # Derive from filename
                table_name = file_path.stem
                if not table_name.startswith("market_data_"):
                    table_name = f"market_data_{table_name}"
            
            # Determine is_fno
            is_fno = None
            if args.is_fno:
                is_fno = args.is_fno[i]
            
            print(f"Processing: {file_path} -> {table_name}" + (f" (F&O: {is_fno})" if is_fno is not None else " (auto-detect)"))
            
            # Use validate_and_ingest_file if available
            if hasattr(orchestrator.ingestor, 'validate_and_ingest_file'):
                result = orchestrator.ingestor.validate_and_ingest_file(
                    file_path=file_path,
                    table_name=table_name,
                    is_fno=is_fno,
                    format="parquet"
                )
            else:
                # Fallback
                schema = orchestrator.ingestor.get_table_schema(table_name.replace("market_data_", "").upper())
                if app_settings.ingestion_settings.create_tables:
                    orchestrator.ingestor.create_table(schema)
                result = orchestrator.ingestor.ingest_file(table_name, file_path, format="parquet")
            
            results[str(file_path)] = result
        
        # Print results
        print("\n=== Ingestion Results ===")
        all_success = True
        for file_path, result in results.items():
            status = "SUCCESS" if result.success else "FAILED"
            print(f"  {file_path} -> {status}")
            print(f"    Records: {result.records_ingested}, Failed: {result.records_failed}, Duration: {result.duration_seconds:.2f}s")
            if result.error:
                print(f"    Error: {result.error}")
            if not result.success:
                all_success = False
        
        if not all_success:
            sys.exit(1)
            
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if args.target:
            app_settings.ingestion_settings.target = original_target