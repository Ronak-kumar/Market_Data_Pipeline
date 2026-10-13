"""
Ingestion Orchestrator
Coordinates data ingestion from Silver layer to target databases.
"""
from ingestion.clients import DataIngestor, IngestionResult, ingestion_registry, ingestion_discovery
from shared.config import app_settings
from shared.observability import get_logger
from pathlib import Path
from typing import Dict, List, Optional, Any

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

    def run_ingestion(self, silver_base_path: Path, date: str, segments: Dict[str, str]) -> Dict[str, IngestionResult]:
        """
        Run ingestion for a specific date and segments.
        
        Args:
            silver_base_path: Base path to silver layer (e.g., src/transformation/cache/{client}/Silver)
            date: Date string (YYYY-MM-DD)
            segments: Dictionary mapping segment names to target table names
            
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
        
        for segment, target_table in segments.items():
            logger.info(f"Starting ingestion for {segment} on {date}")
            
            # Find parquet file for segment
            parquet_file = silver_date_path / f"{segment}.parquet"
            
            if not parquet_file.exists():
                logger.warning(f"Parquet file not found: {parquet_file}")
                results[segment] = IngestionResult(success=False, error="Parquet file not found")
                continue
            
            # Get table schema
            parquet_schema = self.ingestor.get_table_schema(parquet_file)

            target_table = segment.lower() if target_table == "" else target_table 

            # Create table if needed    
            if self.settings.create_tables:
                self.ingestor.create_table(segment.lower(), self.settings.clickhouse.database, target_table)

            # match the schema of provided table and parquet schema
            db_table_schema = self.ingestor.get_db_table_schema("tetst")
            compatible_evaluation = self.ingestor.compare_schemas(parquet_schema, db_table_schema)
            
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
