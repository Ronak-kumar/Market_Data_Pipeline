"""
Ingestion Orchestrator
Coordinates data ingestion from Silver layer to target databases.
"""
from ingestion.clients import DataIngestor, IngestionResult, ingestion_registry, ingestion_discovery
from shared.config import get_application_settings
from shared.observability import get_logger
from pathlib import Path
from typing import Dict, List, Optional, Any

logger = get_logger(__name__)


def _get_settings():
    """Load settings fresh from disk so UI changes take effect immediately."""
    return get_application_settings()


class IngestionOrchestrator:
    """Orchestrates ingestion from Silver layer to target database."""

    def __init__(self):
        self.settings = _get_settings().ingestion_settings
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

    def run_ingestion(self, silver_base_path: Path, date: str, segments: Optional[Dict[str, str]] = None) -> Dict[str, IngestionResult]:
        """
        Run ingestion for a specific date and segments.

        Args:
            silver_base_path: Base path to silver layer (e.g., src/transformation/cache/{client}/Silver)
            date: Date string (YYYY-MM-DD)
            segments: Optional dictionary mapping segment names to target table names.
                      If not provided, uses segment_table_mapping from settings.

        Returns:
            Dict mapping segment to IngestionResult
        """
        # Use segments from parameter, or fall back to settings mapping
        if segments is None:
            segments = self.settings.segment_table_mapping
            if not segments:
                logger.warning("No segment_table_mapping configured in settings and none provided as parameter")
                return {}

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
            self.ingestor.disconnect()
            return {seg: IngestionResult(success=False, error="Path not found") for seg in segments}

        # Check if date already exists in database
        existing_segments = []
        for segment, target_table in segments.items():
            target_table = segment.lower() if target_table == "" else target_table
            if self.ingestor.table_exists(target_table):
                if self.ingestor.check_date_exists(target_table, date):
                    existing_segments.append(segment)
                    logger.info(f"Date {date} already exists for segment {segment}, skipping")

        if len(existing_segments) == len(segments):
            logger.info(f"All segments for date {date} already exist in database, skipping ingestion")
            self.ingestor.disconnect()
            return {seg: IngestionResult(success=True, records_ingested=0, error="Already exists") for seg in segments}

        # Track successfully ingested segments for potential rollback
        successful_tables = []
        results = {}

        try:
            for segment, target_table in segments.items():
                # Skip already existing segments
                if segment in existing_segments:
                    results[segment] = IngestionResult(success=True, records_ingested=0, error="Already exists")
                    continue

                logger.info(f"Starting ingestion for {segment} on {date}")

                # Find parquet file for segment
                parquet_file = silver_date_path / f"{segment}.parquet"

                if not parquet_file.exists():
                    logger.warning(f"Parquet file not found: {parquet_file}")
                    results[segment] = IngestionResult(success=False, error="Parquet file not found")
                    # Rollback all successful segments
                    self._rollback_date(date, successful_tables)
                    self.ingestor.disconnect()
                    return {seg: IngestionResult(success=False, error="Parquet file not found") for seg in segments}

                # Get table schema
                parquet_schema = self.ingestor.get_table_schema(parquet_file)

                target_table = segment.lower() if target_table == "" else target_table

                # Create table if needed
                if self.settings.create_tables:
                    # Get database name from the appropriate ingestor config
                    if hasattr(self.ingestor.config, 'database'):
                        db_name = self.ingestor.config.database
                    else:
                        settings = _get_settings()
                        if self.settings.target == "clickhouse":
                            db_name = settings.clickhouse.database
                        elif self.settings.target == "postgresql":
                            db_name = settings.postgresql.database
                        elif self.settings.target == "timescaledb":
                            db_name = settings.timescaledb.database
                        else:
                            db_name = "default"
                    self.ingestor.create_table(segment.lower(), db_name, target_table)

                # match the schema of provided table and parquet schema
                db_table_schema = self.ingestor.get_db_table_schema(target_table)
                compatible_evaluation = self.ingestor.compare_schemas(parquet_schema, db_table_schema)

                # Ingest file
                result = self.ingestor.ingest_file(target_table, parquet_file, format="parquet")
                results[segment] = result

                if not result.success:
                    logger.error(f"Ingestion failed for {segment}: {result.error}")
                    # Rollback all successful segments for this date
                    self._rollback_date(date, successful_tables)
                    raise Exception(f"Ingestion failed for {segment}: {result.error}")

                successful_tables.append(target_table)
                logger.info(f"Ingestion complete for {segment}", extra={
                    "success": result.success,
                    "records": result.records_ingested,
                    "failed": result.records_failed,
                    "duration": result.duration_seconds
                })

            logger.info(f"All segments ingested successfully for date {date}")

        except Exception as e:
            logger.error(f"Ingestion failed, rolled back: {e}")
            # Rollback any successful segments
            if successful_tables:
                self._rollback_date(date, successful_tables)
            # Mark all as failed
            for seg in segments:
                if seg not in results or not results[seg].success:
                    results[seg] = IngestionResult(success=False, error=f"Rolled back due to failure: {str(e)}")
        finally:
            # Disconnect
            self.ingestor.disconnect()

        return results

    def _rollback_date(self, date: str, ingested_tables: List[str]) -> bool:
        """Rollback ingestion for a specific date by deleting the data."""
        success = True
        for table in ingested_tables:
            table_name = table
            try:
                if self.ingestor.delete_date_data(table_name, date):
                    logger.info(f"Rolled back table {table_name} for date {date}")
                else:
                    logger.warning(f"No data to rollback for table {table_name} on date {date}")
            except Exception as e:
                logger.error(f"Rollback failed for table {table_name} on date {date}: {e}")
                success = False
        return success


    def run_full_backfill(self, silver_base_path: Path, start_date: str, end_date: str, segments: Optional[List[str]] = None) -> Dict[str, Dict[str, IngestionResult]]:
        """
        Run ingestion for a date range (backfill).

        Args:
            silver_base_path: Base path to silver layer
            start_date: Start date string (YYYY-MM-DD)
            end_date: End date string (YYYY-MM-DD)
            segments: Optional list of segments. If not provided, uses keys from segment_table_mapping in settings.

        Returns:
            Dict mapping date -> segment -> IngestionResult
        """
        from datetime import datetime, timedelta

        if segments is None:
            segments = list(self.settings.segment_table_mapping.keys())
            if not segments:
                logger.warning("No segments configured in segment_table_mapping and none provided")
                return {}

        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]

        all_results = {}

        for date in dates:
            date_str = date.strftime("%Y-%m-%d")
            logger.info(f"Running backfill ingestion for {date_str}")
            all_results[date_str] = self.run_ingestion(silver_base_path, date_str, self.settings.segment_table_mapping)

        return all_results

    def health_check(self) -> bool:
        """Check database connectivity."""
        if self.ingestor is None:
            return False
        return self.ingestor.health_check()


def run_ingestion(
    silver_base_path: str,
    date: str,
    segments: Optional[List[str]] = None,
    target: Optional[str] = None
) -> Dict[str, IngestionResult]:
    """
    Convenience function to run ingestion.

    Args:
        silver_base_path: Path to silver layer base directory
        date: Date string (YYYY-MM-DD)
        segments: Optional list of segments to ingest. If not provided, uses segment_table_mapping from settings.
        target: Override target database (clickhouse, postgresql, timescaledb)

    Returns:
        Dict mapping segment to IngestionResult
    """
    settings = _get_settings()

    # Temporarily override target if provided
    original_target = settings.ingestion_settings.target
    if target:
        settings.ingestion_settings.target = target

    try:
        orchestrator = IngestionOrchestrator()

        # If segments list provided, build dict from settings mapping
        if segments is not None:
            segment_dict = {}
            for seg in segments:
                segment_dict[seg] = settings.ingestion_settings.segment_table_mapping.get(seg, "")
        else:
            segment_dict = settings.ingestion_settings.segment_table_mapping

        return orchestrator.run_ingestion(Path(silver_base_path), date, segment_dict)
    finally:
        if target:
            settings.ingestion_settings.target = original_target


if __name__ == "__main__":
    run_ingestion("src/extraction/cache/upstox/Silver", "2026-10-30")