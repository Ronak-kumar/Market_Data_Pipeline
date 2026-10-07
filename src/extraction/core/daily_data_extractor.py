from shared.config import app_settings
import polars as pl
from extraction.clients import client_discovery
from extraction.clients import client_registry
from extraction.parser import parser_registry, parser_discovery
from shared.observability import get_logger
from tqdm import tqdm
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
from cloud import cloud_discovery, cloud_registry
from transformation.orchestrator import transform_data
from transformation.clients import transformer_discovery, transformer_registry
from extraction.models.data_models import ClientObject, CloudObject, ExtractionConfig
from dateutil.relativedelta import relativedelta

logger = get_logger(__name__)

# Schema constant for candle data
CANDLE_SCHEMA = [
    "Asset_Class", "Ticker", "Date", "Time", 
    "Open", "High", "Low", "Close", "Volume", "Open Interest"
]


class DailyDataExtractor:
    def __init__(self):
        self._settings_config = app_settings
        self._initialize_client()
        self._initialize_cloud_clients()
        self._config()
        self.segment_type_map = self._settings_config.broker_configuration.processable_segments

    def _config(self):
        expiry_suffix_duration = self._settings_config.extractor_settings.expiry_duration
        end_date = self._settings_config.extractor_settings.end_date
        start_date = self._settings_config.extractor_settings.start_date
        processable_segments = self._settings_config.broker_configuration.processable_segments
        extraction_interval = self._settings_config.extractor_settings.interval
        extraction_variation = "intraday" if end_date == "" and start_date == "" else "historical"

        self.extraction_config = ExtractionConfig(
            expiry_suffix_duration, end_date, start_date, 
            processable_segments, extraction_interval, extraction_variation
        )

    def _initialize_client(self):
        client_discovery()
        parser_discovery()
        transformer_discovery()

        client = self._settings_config.extractor_settings.client.lower()
        try:
            provider = client_registry.get(client)()
            logger.info("Provider instantiated", extra={"client": client})
        except Exception as e:
            logger.error("Failed to instantiate provider", extra={"client": client, "error": str(e)}, exc_info=True)
            raise

        try:
            parser = parser_registry.get(client)()
            logger.info("Parser instantiated", extra={"client": client})
        except Exception as e:
            logger.error("Failed to instantiate Parser", extra={"client": client, "error": str(e)}, exc_info=True)
            raise

        try:
            transformer = transformer_registry.get(client)()
            logger.info("Transformer Object instantiated", extra={"client": client})
        except Exception as e:
            logger.error("Failed to instantiate Transformer", extra={"client": client, "error": str(e)}, exc_info=True)
            raise

        master_instrument = self._initialize_master_instrument(provider=provider, parser=parser, client=client)

        self.client_object = ClientObject(
            client_name=client, provider=provider, parser=parser,
            transformer=transformer, master_instrument=master_instrument
        )

        logger.info("DailyDataExtractor initialized", extra={"client": client})

    def _initialize_cloud_clients(self):
        cloud_discovery()

        cloud_client = self._settings_config.cloud_settings.client.lower()
        cloud_push_flag = self._settings_config.cloud_settings.push_to_cloud
        storage_object = self._settings_config.cloud_settings.storage_name
        destination_folder = self._settings_config.cloud_settings.destination_folder

        try:
            cloud_object = cloud_registry.get(cloud_client)()
            logger.info("Cloud Object instantiated", extra={"client": cloud_client})
        except Exception as e:
            logger.error("Failed to instantiate Cloud", extra={"client": cloud_client, "error": str(e)}, exc_info=True)
            raise

        self.cloud_object = CloudObject(
            client_name=cloud_client, 
            push_to_cloud_flag=cloud_push_flag, 
            cloud_instance=cloud_object,
            storage_object=storage_object, 
            destination_folder=destination_folder
        )

    def _push_to_cloud(self, processed_data: dict) -> None:
        if not self.cloud_object.push_to_cloud_flag:
            return

        for _, filepath in processed_data.items():
            try:
                storage_object = self.cloud_object.storage_object
                destination_folder = self.cloud_object.destination_folder
                self.cloud_object.cloud_instance.upload_files(
                    filepath=filepath, 
                    bucket_name=storage_object, 
                    destination_prefix=destination_folder
                )
                logger.info("Cloud upload success", extra={"filepath": str(filepath)})
            except Exception as e:
                logger.warning("Cloud upload failed", extra={"filepath": str(filepath), "error": str(e)}, exc_info=True)

    def _initialize_master_instrument(self, provider, parser, client):
        try:
            master_instrument = parser.parse(provider.master_instrument_data)
            logger.info("Master instrument parsed", extra={"rows": master_instrument.height})
        except Exception as e:
            logger.error("Failed to parse master instrument", extra={"client": client, "error": str(e)}, exc_info=True)
            raise

        return master_instrument

    def process(self) -> Dict:
        logger.info("Starting extraction process")
        master_instrument = self.client_object.master_instrument
        start_date = self.extraction_config.start_date
        end_date = self.extraction_config.end_date
        variation = self.extraction_config.extraction_variation

        if start_date == "" or end_date == "":
            logger.info("No date range specified, processing current day (intraday)")
            process_able_date = datetime.now()
            dates = [process_able_date]
        else:
            start, end = datetime.strptime(start_date, "%Y-%m-%d").date(), datetime.strptime(end_date, "%Y-%m-%d").date()
            dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
            logger.info("Historical date range", extra={"start_date": start_date, "end_date": end_date, "days": len(dates)})

        date_map = {}
        for date in dates:
            processed_data = self._day_process(date=date, master_instrument=master_instrument, variation=variation)
            date_map[date] = processed_data

        logger.info("Extraction process completed", extra={"dates_processed": len(date_map)})
        return date_map

    def _day_process(self, date, master_instrument, variation) -> dict:
        """Per day processing with staged parquet resilience."""
        date_str = datetime.strftime(date, "%Y-%m-%d")

        accepted_expiry_bar = date + relativedelta(months=3)
        master_instrument = master_instrument.filter(
            ~((pl.col("symbol_asset_class") == "FO")
                & (pl.col("symbol_expiry").dt.date() > accepted_expiry_bar))
        )

        master_instrument = master_instrument.sort("symbol_name")

        logger.debug("Expiry suffixes loaded", extra={"date": date_str, "suffixes": self.extraction_config.expiry_suffix_duration})

        logger.info("Processing date...", extra={"date": date_str, "variation": variation})
        processed_data = self._processing_day(master_instrument, provider=self.client_object.provider, date=date_str, variation=variation)
        
        # Push processed data to datalake
        self._push_to_cloud(processed_data)
        
        # Create silver data
        silver_processed_data = transform_data({date: processed_data}, self.client_object.transformer)
        for date, silver_data in silver_processed_data.items():
            logger.info("Silver data created", extra={"date": date})
            self._push_to_cloud(silver_data)

        return processed_data

    def _processing_day(self, master_instrument, provider, date, variation) -> Dict[str, Any]:
        """Process all segments for a given date with staged writes and resume capability."""
        logger.info("Starting daily processing", extra={"date": date, "variation": variation})

        segment_list = self.segment_type_map
        segment_map = {}
        
        if not segment_list:
            logger.warning("No segment selected for processing", extra={"processable_segments": self.extraction_config.processable_segments})
            return segment_map

        timeframe = str(self.extraction_config.extraction_interval)
        logger.debug("Processing configuration", extra={"interval": timeframe, "segments": segment_list})

        base_cache_path = Path(__file__).parent.parent / "cache" / self.client_object.client_name / "Bronze" / date
        base_cache_path.mkdir(parents=True, exist_ok=True)

        for segment in segment_list:
            logger.info("Processing segment", extra={"segment": segment})
            
            # Check if segment is already complete (final parquet exists)
            final_parquet = base_cache_path / f"{segment}.parquet"
            if final_parquet.exists():
                logger.info(f"Segment: {segment} already complete, skipping")
                segment_map[segment] = final_parquet
                continue

            segment_df = master_instrument.filter(pl.col("segment") == segment)
            logger.debug("Segment filtered", extra={"segment": segment, "instrument_count": segment_df.height})

            # Process segment with staged writes
            staging_dir = base_cache_path / f"{segment}_staging"
            segment_rows_count, fetched_count, skipped_count, error_count = self._fetch_segment_with_staging(
                segment_df, segment, provider, date, variation, timeframe, staging_dir
            )

            logger.info("Segment fetch complete", extra={
                "segment": segment,
                "fetched": fetched_count,
                "skipped": skipped_count,
                "errors": error_count,
                "total_rows": segment_rows_count
            })

            if segment_rows_count == 0:
                logger.warning("No data for segment", extra={"segment": segment})
                continue

            # Consolidate staging files into final parquet
            self._consolidate_staging_to_final(staging_dir, final_parquet, segment)
            
            logger.info("Segment written to Bronze layer", extra={"segment": segment, "output_path": str(final_parquet)})
            segment_map[segment] = final_parquet

        return segment_map

    def _fetch_segment_with_staging(
        self,
        segment_df: pl.DataFrame,
        segment: str,
        provider,
        date: str,
        variation: str,
        timeframe: str,
        staging_dir: Path
    ) -> tuple[list[list], int, int, int]:
        """
        Fetch segment data with per-symbol staged parquet writes.
        Resumes from existing staged files on restart.
        """
        rows_count = 0
        fetched_count = skipped_count = error_count = 0

        # Ensure staging directory exists
        staging_dir.mkdir(parents=True, exist_ok=True)

        # Find already staged symbols (resume capability)
        staged_symbols = self._get_staged_symbols(staging_dir)
        if staged_symbols:
            logger.info(f"Resume: {len(staged_symbols)} symbols already staged for {segment} on {date}")

        pbar = tqdm(segment_df.iter_rows(named=True), total=segment_df.height,
            desc=segment, unit="key", leave=False)

        for row in pbar:
            trading_symbol = row.get("trading_symbol", "")
            instrument_key = row.get("instrument_key", trading_symbol)
            safe_key = row.get("symbol_name", "")

            # Skip if already staged
            if safe_key in staged_symbols:
                skipped_count += 1
                pbar.set_postfix({"staged_skip": skipped_count})
                continue

            try:
                candles = provider.fetch_instrument(
                    row=row,
                    variation=variation,
                    date=date,
                    interval=self.extraction_config.extraction_interval
                )

            except Exception as e:
                error_count += 1
                logger.error("Failed to fetch candles",
                    extra={
                        "trading_symbol": trading_symbol,
                        "instrument_key": instrument_key,
                        "error": str(e)
                    })
                continue

            if not candles:
                skipped_count += 1
                logger.debug(f'No candles returned {trading_symbol}')
                
                # Write empty marker file to track completion
                self._write_symbol_staging(safe_key + "_empty", staging_dir, [])
                continue

            # Convert candles to rows
            symbol_rows = candles
            rows_count += len(symbol_rows)

            # Write THIS symbol's data to staging file IMMEDIATELY
            self._write_symbol_staging(safe_key, staging_dir, symbol_rows)
            logger.debug(f"Staged {len(symbol_rows)} candles for {instrument_key}")

            fetched_count += 1
            staged_symbols.add(safe_key)  # Update local set for progress tracking
            pbar.set_postfix({"fetched": fetched_count, "errors": error_count, "skipped": skipped_count})

        return rows_count, fetched_count, skipped_count, error_count

    def _get_staged_symbols(self, staging_dir: Path) -> set:
        """Get set of instrument keys that already have staging files."""
        if not staging_dir.exists():
            return set()
        
        staged = set()
        for f in staging_dir.glob("*.parquet"):
            # Remove .parquet extension to get instrument key
            staged.add(f.stem)
        return staged

    def _write_symbol_staging(self, safe_key: str, staging_dir: Path, symbol_rows: list[list]) -> None:
        """Write a single symbol's data to staging parquet file."""
        staging_file = staging_dir / f"{safe_key}.parquet"
        
        if symbol_rows:
            symbol_df = pl.DataFrame(symbol_rows, orient="row", schema=CANDLE_SCHEMA)
        else:
            # Empty marker file for symbols with no data
            symbol_df = pl.DataFrame(schema=CANDLE_SCHEMA)
        
        # Atomic write: write to temp, then rename
        temp_file = staging_file.with_suffix(".parquet.tmp")
        symbol_df.write_parquet(temp_file)
        temp_file.rename(staging_file)

    def _consolidate_staging_to_final(self, staging_dir: Path, final_parquet: Path, segment: str) -> None:
        """Consolidate all staging files into final segment parquet."""
        parquet_files = list(staging_dir.glob("*.parquet"))
        
        if not parquet_files:
            logger.warning("No staging files to consolidate", extra={"segment": segment})
            # Create empty final file
            empty_df = pl.DataFrame(schema=CANDLE_SCHEMA)
            empty_df.write_parquet(final_parquet)
            return

        logger.info(f"Consolidating {len(parquet_files)} staging files for {segment}")
        
        # Read and concatenate all non-empty staging files
        dfs = []
        for f in parquet_files:
            try:
                df = pl.read_parquet(f)
                if df.height > 0:
                    dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to read staging file {f}: {e}")

        if dfs:
            consolidated_df = pl.concat(dfs, how="vertical")
        else:
            consolidated_df = pl.DataFrame(schema=CANDLE_SCHEMA)

        # Atomic write final parquet
        temp_final = final_parquet.with_suffix(".parquet.tmp")
        consolidated_df.write_parquet(temp_final)
        temp_final.rename(final_parquet)
        
        logger.info(f"Consolidated {consolidated_df.height} rows for {segment}")

        # Optionally clean up staging directory (comment out to keep for debugging)
        import shutil
        shutil.rmtree(staging_dir)


if __name__ == "__main__":
    main_runner = DailyDataExtractor()
    date_map = main_runner.process()
