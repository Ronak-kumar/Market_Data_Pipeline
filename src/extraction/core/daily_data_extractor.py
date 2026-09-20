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
from cloud import cloud_discovery, cloud_registry
from transformation.orchestrator import transform_data
from transformation.clients import transformer_discovery, transformer_registry

logger = get_logger(__name__)

class DailyDataExtractor:
    def __init__(self):
        self._initialize_adapters()

    def _initialize_adapters(self):
        self._settings_config = app_settings
        client_discovery()
        parser_discovery()
        transformer_discovery()
        cloud_discovery()

        client = self._settings_config.extractor_settings.client.lower()
        cloud_client = self._settings_config.cloud_settings.client.lower()
        self._cloud_push_flag = self._settings_config.cloud_settings.push_to_cloud

        try:
            self._provider = client_registry.get(client)()
            logger.info("Provider instantiated", extra={"client": client})
        except Exception as e:
            logger.error("Failed to instantiate provider", extra={"client": client, "error": str(e)}, exc_info=True)
            raise

        try:
            self._parser = parser_registry.get(client)()
            logger.info("Parser instantiated", extra={"client": client})
        except Exception as e:
            logger.error("Failed to instantiate Parser", extra={"client": client, "error": str(e)}, exc_info=True)
            raise

        try:
            self._transformer = transformer_registry.get(client)()
            logger.info("Transformer Object instantiated", extra={"client": client})
        except Exception as e:
            logger.error("Failed to instantiate Transformer", extra={"client": client, "error": str(e)}, exc_info=True)
            raise

        try:
            self._cloud_object = cloud_registry.get(cloud_client)()
            logger.info("Cloud Object instantiated", extra={"client": cloud_client})
        except Exception as e:
            logger.error("Failed to instantiate Cloud", extra={"client": cloud_client, "error": str(e)}, exc_info=True)
            raise

        logger.info("DailyDataExtractor initialized", extra={"client": client})

    def _push_to_cloud(self, processed_data: dict) -> None:
        if not self._cloud_push_flag:
            return
        
        for _, filepath in processed_data.items():
            try:
                storage_object = self._settings_config.cloud_settings.storage_name
                destination_folder = self._settings_config.cloud_settings.destination_folder
                self._cloud_object.upload_files(filepath=filepath, bucket_name=storage_object, destination_prefix=destination_folder)
                logger.info("Cloud upload success", extra={"filepath": str(filepath)})
            except Exception as e:
                logger.warning("Cloud upload failed", extra={"filepath": str(filepath), "error": str(e)}, exc_info=True)


    def process(self) -> Dict:
        logger.info("Starting extraction process")
        client = self._settings_config.extractor_settings.client.lower()

        try:
            master_instrument = self._parser.parse(self._provider.master_instrument_data)
            logger.info("Master instrument parsed", extra={"rows": master_instrument.height})
        except Exception as e:
            logger.error("Failed to parse master instrument", extra={"client": client, "error": str(e)}, exc_info=True)
            return {}

        date_map = {}

        if self._settings_config.extractor_settings.start_date == "" or self._settings_config.extractor_settings.end_date == "":
            logger.info("No date range specified, processing current day (intraday)")
            process_able_date = datetime.now()
            processed_data = self._day_process(date=process_able_date, master_instrument=master_instrument, client=client, variation="intraday")
            date_map[process_able_date] = processed_data

        else:
            start_date = self._settings_config.extractor_settings.start_date
            end_date = self._settings_config.extractor_settings.end_date
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            end = datetime.strptime(end_date, "%Y-%m-%d").date()
            dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
            logger.info("Historical date range", extra={"start_date": start_date, "end_date": end_date, "days": len(dates)})

            for date in dates:
                processed_data = self._day_process(date=date, master_instrument=master_instrument, client=client, variation="historical")
                date_map[date] = processed_data
                
        logger.info("Extraction process completed", extra={"dates_processed": len(date_map)})
        return date_map
    
    def _day_process(self, date, master_instrument, client, variation) -> dict:
        date_str = datetime.strftime(date, "%Y-%m-%d")
        self._provider._expiry_suffixes = self._provider.get_expiry_suffixes(process_able_date=date, months_ahead=self._settings_config.extractor_settings.expiry_duration)
        logger.debug("Expiry suffixes loaded", extra={"date": date_str, "suffixes": self._provider._expiry_suffixes})


        # Process day data
        logger.info("Processing date...", extra={"date": date_str, "variation": variation})
        processed_data = self._processing_day(master_instrument, provider=self._provider, date=date_str, variation=variation)
        # Push processed data to datalake
        self._push_to_cloud(processed_data)
        # Create silver data
        transform_data({date: processed_data}, self._transformer)

        return processed_data
    
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


if __name__ == "__main__":
    main_runner = DailyDataExtractor()
    date_map = main_runner.process()