from shared.config import app_settings
import polars as pl
from extraction.clients import client_discovery
from extraction.clients import client_registry
from extraction.parser import parser_registry, parser_discovery
from shared.observability import get_logger
from tqdm import tqdm
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict
from zoneinfo import ZoneInfo
from shared.utils.aws_s3_manager import S3BucketManager
from transformation.orchestrator import transform_data

logger = get_logger(__name__)

class DailyDataExtractor:
    def __init__(self, app_settings):
        self._settings_config = app_settings
        client_discovery()
        parser_discovery()
        self._s3_object = S3BucketManager()
        logger.info("DailyDataExtractor initialized", extra={"client": app_settings.extractor_settings.client})

    def _processing_day(self, master_instrument, provider, date, variation) -> Dict[str, pl.DataFrame]:
        logger.info("Starting daily processing", extra={"date": date, "variation": variation})
        segment_map = {segment: pl.DataFrame() for segment in self._settings_config.extractor_settings.processable_segments}
        if len(segment_map) == 0:
            logger.warning("No segment selected for processing", extra={"processable_segments": self._settings_config.extractor_settings.processable_segments})
            return segment_map

        data_fetching_interval = self._settings_config.extractor_settings.interval
        logger.debug("Processing configuration", extra={"interval": data_fetching_interval, "segments": list(segment_map.keys())})

        for segment, _ in segment_map.items():
            logger.info("Processing segment", extra={"segment": segment})
            segment_df = master_instrument.filter(pl.col("segment") == segment)
            logger.debug("Segment filtered", extra={"segment": segment, "instrument_count": segment_df.height})

            pbar = tqdm(
                segment_df.iter_rows(named=True),
                total=segment_df.height,
                desc=f"{segment}",
                unit="key",
                leave=False,
            )

            segment_rows = []
            fetched_count = 0
            skipped_count = 0
            error_count = 0

            for row in pbar:
                pbar.set_postfix(key=row["instrument_key"], status="fetching")

                if "fo" in segment.lower():
                    if not any(exp in row["trading_symbol"] for exp in provider._expiry_suffixes):
                        skipped_count += 1
                        continue

                ### Implementation Variation (Intraday, Historical, Expired Historical)
                try:
                    if row["expiry"] is not None and datetime.now().date() > datetime.fromtimestamp(row["expiry"] / 1000, tz=ZoneInfo("Asia/Kolkata")).date():
                        candles, name = provider.fetch_expired_historical_instrument(context=row, interval=data_fetching_interval, start_date=date, end_date=date)
                        logger.debug("Fetching expired historical", extra={"instrument_key": row["instrument_key"], "name": name})
                    elif variation == "intraday":
                        candles, name = provider.fetch_instrument(context=row, interval=data_fetching_interval)
                        logger.debug("Fetching intraday", extra={"instrument_key": row["instrument_key"], "name": name})
                    elif variation == "historical":
                        candles, name = provider.fetch_historical_instrument(context=row, interval=data_fetching_interval, start_date=date, end_date=date)
                        logger.debug("Fetching historical", extra={"instrument_key": row["instrument_key"], "name": name})
                    else:
                        logger.warning("Unknown variation", extra={"variation": variation})
                        continue
                except Exception as e:
                    error_count += 1
                    trading_symbol = row.get("trading_symbol", "")
                    logger.error("Failed to fetch candles", extra={"instrument_key": row["instrument_key"], "Trading_Symbol": trading_symbol, "error": str(e)}, exc_info=True)
                    continue

                if candles is None or name is None:
                    skipped_count += 1
                    trading_symbol = row.get("trading_symbol", "")
                    logger.debug("No candles returned", extra={"instrument_key": row["instrument_key"], "Trading_Symbol": trading_symbol})
                    continue

                for candle_row in candles:
                    dt = datetime.fromisoformat(candle_row[0])
                    segment_rows.append([
                        name,
                        dt.strftime("%d-%m-%Y"),
                        dt.strftime("%H:%M:%S"),
                        *candle_row[1:7],
                    ])
                fetched_count += 1

            logger.info("Segment fetch complete", extra={
                "segment": segment,
                "fetched": fetched_count,
                "skipped": skipped_count,
                "errors": error_count,
                "total_rows": len(segment_rows)
            })

            if not segment_rows:
                logger.warning("No data for segment", extra={"segment": segment})
                segment_map[segment] = pl.DataFrame()
                continue

            segment_frame = pl.DataFrame(segment_rows, orient="row", schema=["Ticker", "Date", "Time", "Open", "High", "Low", "Close", "Volume", "Open Interest"], )
            logger.debug("Segment DataFrame created", extra={"segment": segment, "rows": segment_frame.height, "columns": segment_frame.columns})

            saving_path = Path(__file__).parent.parent / "cache" / self._settings_config.extractor_settings.client.lower() / "Bronze" / date
            saving_path.mkdir(parents=True, exist_ok=True)
            output_path = saving_path / f"{segment}.parquet"
            segment_frame.write_parquet(output_path)
            logger.info("Segment written to Bronze layer", extra={"segment": segment, "output_path": str(output_path), "rows": segment_frame.height})
            segment_map[segment] = output_path

        return segment_map

    def process(self) -> Dict:
        logger.info("Starting extraction process")
        client = self._settings_config.extractor_settings.client.lower()
        try:
            provider = client_registry.get(client)()
            logger.info("Provider instantiated", extra={"client": client})
        except Exception as e:
            logger.error("Failed to instantiate provider", extra={"client": client, "error": str(e)}, exc_info=True)
            return {}

        try:
            parser = parser_registry.get(client)()
            logger.info("Parser instantiated", extra={"client": client})
        except Exception as e:
            logger.error("Failed to instantiate Parser", extra={"client": client, "error": str(e)}, exc_info=True)
            return {}

        
        try:
            master_instrument = parser.parse(provider.master_instrument_data)
            logger.info("Master instrument parsed", extra={"rows": master_instrument.height})
        except Exception as e:
            logger.error("Failed to parse master instrument", extra={"client": client, "error": str(e)}, exc_info=True)
            return {}

        date_map = {}

        if self._settings_config.extractor_settings.start_date == "" or self._settings_config.extractor_settings.end_date == "":
            logger.info("No date range specified, processing current day (intraday)")
            process_able_date = datetime.now()
            date_str = process_able_date.strftime("%Y-%m-%d")
            provider._expiry_suffixes = provider.get_expiry_suffixes(process_able_date=process_able_date.date(), months_ahead=self._settings_config.extractor_settings.expiry_duration)
            logger.debug("Expiry suffixes loaded", extra={"suffixes": provider._expiry_suffixes})
            processed_data = self._processing_day(master_instrument, provider=provider, date=date_str, variation="intraday")
            date_map[process_able_date] = processed_data

            if app_settings.datalake_settings.push_to_cloud:
                for date, filepath in processed_data.items():
                    try:
                        self._s3_object.upload_files(filepath=filepath, bucket_name="marketdata-pipeline", destination_prefix=f"bronze_cache_storage_market_data/{client}/")
                        logger.info("S3 upload success", extra={"filepath": str(filepath)})
                    except Exception as e:
                        logger.warning("S3 upload failed", extra={"filepath": str(filepath), "error": str(e)}, exc_info=True)

            transform_data({date: processed_data})


        else:
            start_date = self._settings_config.extractor_settings.start_date
            end_date = self._settings_config.extractor_settings.end_date
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            end = datetime.strptime(end_date, "%Y-%m-%d").date()
            dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
            logger.info("Historical date range", extra={"start_date": start_date, "end_date": end_date, "days": len(dates)})

            for date in dates:
                date_str = datetime.strftime(date, "%Y-%m-%d")
                provider._expiry_suffixes = provider.get_expiry_suffixes(process_able_date=date, months_ahead=self._settings_config.extractor_settings.expiry_duration)
                logger.debug("Expiry suffixes loaded", extra={"date": date_str, "suffixes": provider._expiry_suffixes})
                processed_data = self._processing_day(master_instrument, provider=provider, date=str(date_str), variation="historical")
                date_map[date] = processed_data

                if app_settings.datalake_settings.push_to_cloud:
                    for date_key, filepath in processed_data.items():
                        try:
                            self._s3_object.upload_files(filepath=filepath, bucket_name="marketdata-pipeline", destination_prefix=f"bronze_cache_storage_market_data/{client}/")
                            logger.info("S3 upload success", extra={"filepath": str(filepath)})
                        except Exception as e:
                            logger.warning("S3 upload failed", extra={"filepath": str(filepath), "error": str(e)}, exc_info=True)

                transform_data({date: processed_data})

        logger.info("Extraction process completed", extra={"dates_processed": len(date_map)})
        return date_map

if __name__ == "__main__":
    main_runner = DailyDataExtractor(app_settings)
    date_map = main_runner.process()