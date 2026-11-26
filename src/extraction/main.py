from shared.config import get_application_settings
import polars as pl
from extraction.clients import client_discovery
from extraction.clients import client_registry
from extraction.parser import parser_registry, parser_discovery
from shared.observability import get_logger
from tqdm import tqdm
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
from dateutil.relativedelta import relativedelta

logger = get_logger(__name__)


def _get_settings():
    """Load settings fresh from disk so UI changes take effect immediately."""
    return get_application_settings()


"""
extraction flow:
_day_process(): core function from where the extraction flow starts for a given date and variation. It handles the Bronze layer processing for all segments.
                                        |
                                        v
_processing_day(): processes all segments for a given date. It iterates over the segments, fetches data for each segment, and writes the results to the Bronze layer.
                                        |
                                        v
_fetch_segment_with_staging(): fetches data for a specific segment. It handles the per-symbol fetching and writes the results to staging parquet files. It also supports resuming from existing staged files.
                                        |
                                        v
_get_staged_symbols(): retrieves the set of instrument keys that already have staging files. This is used to determine which symbols have already been processed and can be skipped.
                                        |
                                        v
_consolidate_staging_to_final(): consolidates all staging files for a segment into a final parquet file. It reads all non-empty staging files, concatenates them, and writes the result to the final output file.
It also cleans up the staging directory after consolidation.
"""

# Schema constant for candle data
CANDLE_SCHEMA = [
    "Asset_Class", "Ticker", "Date", "Time",
    "Open", "High", "Low", "Close", "Volume", "Open Interest"
]

# Manifest schema for tracking staged symbols (typed for Polars)
MANIFEST_SCHEMA = {
    "symbol_name": pl.Utf8,
    "status": pl.Utf8,          # "success" | "empty" | "error"
    "row_count": pl.Int64,
    "timestamp": pl.Datetime,
    "file_name": pl.Utf8        # filename or "" for empty/error
}

CACHE_PATH = Path(__file__).parent.parent / "cache"


class DailyDataExtractor:
    def __init__(self):
        self._settings_config = _get_settings()
        self._extractor_settings = self._settings_config.extractor_settings

        self._initialize_client()
        # self._config()
        self.segment_type_map = self._settings_config.active_broker_configuration.processable_segments
        self._progress_callback = None

    def _initialize_client(self):
        client_discovery()
        parser_discovery()

        client = self._extractor_settings.client.lower()
        self._client = client
        try:
            self._provider = client_registry.get(client)()
            logger.info(f"Provider instantiated: {client}")
        except Exception as e:
            logger.error(f"Failed to instantiate provider for {client}: {e}")
            raise

        try:
            self._parser = parser_registry.get(client)()
            logger.info(f"Parser instantiated: {client}")
        except Exception as e:
            logger.error(f"Failed to instantiate Parser for {client}: {e}")
            raise

        self._master_instrument = self._initialize_master_instrument(provider=self._provider, parser=self._parser, client=self._client)

        logger.info(f"DailyDataExtractor initialized: {self._client}")

    def _master_instrument_cache(self):
        cached_master_instrument = CACHE_PATH / "master_instrument.csv"
        if cached_master_instrument.exists():
            cached_master_instrument_df = pl.read_csv(cached_master_instrument, schema_overrides=[
                pl.Boolean, pl.String, pl.String, pl.String,
                pl.Int64, pl.String, pl.String, pl.String,
                pl.String, pl.Int64, pl.Float64, pl.String,
                pl.Int64, pl.String, pl.String, pl.Float64,
                pl.String, pl.String, pl.String, pl.Float64,
                pl.String, pl.Float64, pl.String, pl.String,
                pl.Boolean, pl.Float64, pl.Boolean, pl.Int64,
                pl.String, pl.String, pl.String, pl.Date,
                ])
            master_instrument = pl.concat([cached_master_instrument_df, master_instrument])
            master_instrument = master_instrument.unique(keep="first")   # keep first occurrence
            fallback_days_for_cache = (datetime.now() - relativedelta(3)).date()
            master_instrument = master_instrument.remove(
                (pl.col("symbol_asset_class") == "FO") &
                (pl.col("symbol_expiry") < fallback_days_for_cache)
            )
            master_instrument.write_csv(cached_master_instrument)

        master_instrument.write_csv(cached_master_instrument)

        return master_instrument

    def _initialize_master_instrument(self, provider, parser, client):
        try:
            master_instrument = parser.parse(provider.master_instrument_data)
            logger.info(f"Master instrument parsed: {master_instrument.height} rows")
        except Exception as e:
            logger.error(f"Failed to parse master instrument for {client}: {e}")
            raise

        master_instrument = self._master_instrument_cache()
        return master_instrument

    def _day_process(self, date, variation) -> dict:
        """Per day processing with staged parquet resilience."""
        date_str = datetime.strftime(date, "%Y-%m-%d")

        accepted_expiry_bar = date + relativedelta(months=self._extractor_settings.expiry_duration)
        master_instrument = self._master_instrument.filter(
            ~((pl.col("symbol_asset_class") == "FO")
                & (pl.col("symbol_expiry").dt.date() > accepted_expiry_bar))
        )

        master_instrument = master_instrument.sort("symbol_name")

        logger.debug(f"Expiry suffixes loaded: date={date_str}, months={self._extractor_settings.expiry_duration}")

        logger.info(f"Processing date: {date_str}, variation: {variation}")
        processed_data = self._processing_day(master_instrument, date=date_str, variation=variation)

        return processed_data

    def _processing_day(self, master_instrument, date, variation) -> Dict[str, Any]:
        """Process all segments for a given date with staged writes and resume capability."""
        logger.info(f"Starting daily processing: date={date}, variation={variation}")

        segment_list = [segment for segment in self.segment_type_map if "#" not in segment]

        segment_map = {}

        if not segment_list:
            logger.warning(f"No segment selected for processing: {self.segment_type_map}")
            return segment_map

        timeframe = str(getattr(self._extractor_settings, "interval", 1))

        logger.debug(f"Processing configuration: interval={timeframe}, segments={segment_list}")

        base_cache_path = CACHE_PATH / self._client / "Bronze" / date
        base_cache_path.mkdir(parents=True, exist_ok=True)

        for seg_idx, segment in enumerate(segment_list):
            logger.info(f"Processing segment: {segment}")

            # Check if segment is already complete (final parquet exists)
            final_parquet = base_cache_path / f"{segment}.parquet"
            if final_parquet.exists():
                logger.info(f"Segment {segment} already complete, skipping")
                segment_map[segment] = final_parquet
                # Update progress
                pct = ((seg_idx + 1) / len(segment_list)) * 100
                continue

            segment_df = master_instrument.filter(pl.col("segment") == segment)
            logger.debug(f"Segment filtered: segment={segment}, instrument_count={segment_df.height}")

            # Process segment with staged writes
            staging_dir = base_cache_path / f"{segment}_staging"
            segment_rows_count, fetched_count, skipped_count, error_count = self._fetch_segment_with_staging(
                segment_df, segment, self._provider, date, variation, timeframe, staging_dir
            )

            logger.info(f"Segment fetch complete: segment={segment}, fetched={fetched_count}, skipped={skipped_count}, errors={error_count}, total_rows={segment_rows_count}")

            if segment_rows_count == 0:
                logger.warning(f"No data for segment: {segment}")
                # continue

            # Consolidate staging files into final parquet
            self._consolidate_staging_to_final(staging_dir, final_parquet, segment)

            logger.info(f"Segment written to Bronze layer: segment={segment}, output_path={final_parquet}")
            segment_map[segment] = final_parquet

        return segment_map


    def _fetch_segment_with_staging(self, segment_df: pl.DataFrame, segment: str, provider, date: str, variation: str, timeframe: str, staging_dir: Path) -> tuple[list[list], int, int, int]:
        """
        Fetch segment data with per-symbol staged parquet writes.
        Resumes from existing staged files on restart.
        Uses a manifest file to track processed symbols (including empty ones) without writing empty parquet files.
        """
        rows_count = 0
        fetched_count = skipped_count = error_count = 0

        # Ensure staging directory exists
        staging_dir.mkdir(parents=True, exist_ok=True)

        # Load or create manifest
        manifest_path = staging_dir / "_manifest.parquet"
        if manifest_path.exists():
            manifest_df = pl.read_parquet(manifest_path)
            staged_symbols = set(manifest_df.filter(pl.col("status") != "pending")["symbol_name"].to_list())
            manifest_rows = manifest_df.to_dicts()
            logger.info(f"Resume: {len(staged_symbols)} symbols already staged for {segment} on {date}")
        else:
            staged_symbols = set()
            manifest_rows = []

        pbar = tqdm(segment_df.iter_rows(named=True), total=segment_df.height, desc=segment, unit="key", leave=False)

        for row in pbar:
            trading_symbol = row.get("trading_symbol", "")
            instrument_key = row.get("instrument_key", trading_symbol)
            safe_key = row.get("symbol_name", "")

            # Skip if already staged (success, empty, or error)
            if safe_key in staged_symbols:
                skipped_count += 1
                pbar.set_postfix({"fetched": fetched_count, "errors": error_count, "skipped": skipped_count})
                continue

            try:
                candles = provider.fetch_instrument(
                    row=row,
                    variation=variation,
                    date=date,
                    interval=timeframe
                )

            except Exception as e:
                error_count += 1
                # Record error in manifest
                manifest_rows.append({
                    "symbol_name": safe_key,
                    "status": "error",
                    "row_count": 0,
                    "timestamp": datetime.now(),
                    "file_name": ""  # empty string, not None
                })
                logger.error(f"Failed to fetch candles: trading_symbol={trading_symbol}, instrument_key={instrument_key}, error={e}")
                continue

            if not candles:
                skipped_count += 1
                pbar.set_postfix({"fetched": fetched_count, "errors": error_count, "skipped": skipped_count})
                logger.debug(f"No candles returned: {trading_symbol}")

                # Write manifest entry ONLY - no parquet file for empty data
                manifest_rows.append({
                    "symbol_name": safe_key,
                    "status": "empty",
                    "row_count": 0,
                    "timestamp": datetime.now(),
                    "file_name": ""  # empty string, not None
                })
                continue

            # Convert candles to rows
            symbol_rows = candles
            rows_count += len(symbol_rows)

            # Write THIS symbol's data to staging file IMMEDIATELY
            staging_file = staging_dir / f"{safe_key}.parquet"
            symbol_df = pl.DataFrame(symbol_rows, orient="row", schema=CANDLE_SCHEMA)
            symbol_df.write_parquet(staging_file)

            # Record success in manifest
            manifest_rows.append({
                "symbol_name": safe_key,
                "status": "success",
                "row_count": len(symbol_rows),
                "timestamp": datetime.now(),
                "file_name": staging_file.name
            })

            logger.debug(f"Staged {len(symbol_rows)} candles for {instrument_key}")

            fetched_count += 1
            staged_symbols.add(safe_key)  # Update local set for progress tracking
            pbar.set_postfix({"fetched": fetched_count, "errors": error_count, "skipped": skipped_count})

            # Periodically flush manifest to disk (every 50 symbols)
            if fetched_count % 50 == 0:
                manifest_df = pl.DataFrame(manifest_rows, schema=MANIFEST_SCHEMA)
                manifest_df.write_parquet(manifest_path)

        # Final manifest flush
        if manifest_rows:
            manifest_df = pl.DataFrame(manifest_rows, schema=MANIFEST_SCHEMA)
            manifest_df.write_parquet(manifest_path)

        return rows_count, fetched_count, skipped_count, error_count

    def _get_staged_symbols(self, staging_dir: Path) -> set:
        """Get set of instrument keys that already have staging files (from manifest)."""
        manifest_path = staging_dir / "_manifest.parquet"
        if not manifest_path.exists():
            return set()

        manifest_df = pl.read_parquet(manifest_path)
        # Return symbols with status success or empty (already processed)
        return set(manifest_df.filter(pl.col("status").is_in(["success", "empty"]))["symbol_name"].to_list())

    def _consolidate_staging_to_final(self, staging_dir: Path, final_parquet: Path, segment: str) -> None:
        """Consolidate all staging files into final segment parquet using manifest."""
        manifest_path = staging_dir / "_manifest.parquet"

        if not manifest_path.exists():
            logger.warning(f"No manifest found, nothing to consolidate: segment={segment}")
            empty_df = pl.DataFrame(schema=CANDLE_SCHEMA)
            empty_df.write_parquet(final_parquet)
            return

        manifest_df = pl.read_parquet(manifest_path)

        # Only consolidate symbols with status "success" (have data files)
        success_manifest = manifest_df.filter(pl.col("status") == "success")

        if success_manifest.height == 0:
            logger.warning(f"No successful symbols to consolidate: segment={segment}")
            empty_df = pl.DataFrame(schema=CANDLE_SCHEMA)
            empty_df.write_parquet(final_parquet)
            return

        logger.info(f"Consolidating {success_manifest.height} successful symbols for {segment}")

        # Read only the files listed in manifest for successful symbols
        dfs = []
        for row in success_manifest.iter_rows(named=True):
            file_name = row["file_name"]
            if file_name:
                file_path = staging_dir / file_name
                try:
                    df = pl.read_parquet(file_path)
                    if df.height > 0:
                        dfs.append(df)
                except Exception as e:
                    logger.warning(f"Failed to read staging file {file_path}: {e}")

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
    date_map = main_runner._day_process(datetime.now(), "intraday")