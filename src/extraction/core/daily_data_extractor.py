
from shared.config import app_settings
import polars as pl
from extraction.clients.discovery import client_discovery
from extraction.clients.registry import client_registry
from extraction.parser.base_parser import UpstoxInstrumentParser
from shared.observability import get_logger
from tqdm import tqdm
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict
from zoneinfo import ZoneInfo
from shared.utils.aws_s3_manager import S3BucketManager
from transformation.orchestrator import transform_data

logger = get_logger(__file__)
class DailyDataExtractor:
    def __init__(self, app_settings):
        self._settings_config = app_settings
        client_discovery()
        self._s3_object = S3BucketManager()


    def _processing_day(self, master_instrument, provider, date, variation) -> Dict[str, pl.DataFrame]:

        segment_map = {segment: pl.DataFrame() for segment in self._settings_config.extractor_settings.processable_segments}
        if len(segment_map) == 0:
            logger.warning(f"No segment selected for processing please select segments and try again")


        data_fetching_interval = self._settings_config.extractor_settings.interval
        for segment, _ in segment_map.items():
            logger.info(f"Processing segment {segment}")
            segment_df = master_instrument.filter(pl.col("segment") == segment)
            pbar = tqdm(
                        segment_df.iter_rows(named=True),
                        total=segment_df.height,
                        desc=f"{segment}",
                        unit="key",
                        leave=False,
                    )

            segment_rows = []
            for row in pbar:
                pbar.set_postfix(key=row["instrument_key"], status="fetching")

                if "fo" in segment.lower():
                    if not any(exp in row["trading_symbol"] for exp in provider._expiry_suffixes):
                        continue

                ### Implementation Variation (Intraday, Historical, Expired Historical)
                if row["expiry"] != None and datetime.now().date() > datetime.fromtimestamp(row["expiry"] / 1000, tz=ZoneInfo("Asia/Kolkata"),).date():
                    candles, name = provider.fetch_expired_historical_instrument(context = row, interval = data_fetching_interval, start_date=date, end_date=date)
                elif variation == "intraday":
                    candles, name = provider.fetch_instrument(context = row, interval = data_fetching_interval)
                elif variation == "historical":
                    candles, name = provider.fetch_historical_instrument(context = row, interval = data_fetching_interval, start_date=date, end_date=date)

                if candles == None or name == None:
                    continue

                for row in candles:
                    dt = datetime.fromisoformat(row[0])

                    segment_rows.append([
                        name,
                        dt.strftime("%d-%m-%Y"),
                        dt.strftime("%H:%M:%S"),
                        *row[1:7],
                    ])

            segment_frame = pl.DataFrame(segment_rows, schema=["Ticker", "Date", "Time", "Open", "High", "Low", "Close", "Volume", "Open Interest"])
            
            saving_path = Path(__file__).parent.parent / "cache" / self._settings_config.extractor_settings.client.lower()/ "Bronze" / date 
            saving_path.mkdir(parents=True, exist_ok=True)
            segment_frame.write_parquet(saving_path / f"{segment}.parquet")
            segment_map[segment] = saving_path / f"{segment}.parquet"


        return segment_map

    def process(self) -> None:
        client = self._settings_config.extractor_settings.client.lower()
        try:
            provider = client_registry.get(client)()
        except Exception as e:
            logger.error(f"No client available for {client}, Exception cause: {e}")
            return {}

        parser = UpstoxInstrumentParser()
        try:
            master_instrument = parser.parse(provider.master_instrument_data)
        except Exception as e:
            logger.error(f"Unable to extract master instrument for {client}, Exception cause: {e}")
            return {}

        date_map = {}

        if self._settings_config.extractor_settings.start_date == "" or self._settings_config.extractor_settings.end_date == "":
            process_able_date = datetime.now()
            date_str = process_able_date.strftime("%Y-%m-%d")
            provider._expiry_suffixes = provider.get_expiry_suffixes(process_able_date=process_able_date.date(), months_ahead=self._settings_config.extractor_settings.expiry_duration)
            processed_data = self._processing_day(master_instrument, provider=provider, date=date_str, variation="intraday")
            date_map[process_able_date] = processed_data

            for date, filepath in processed_data.items():
                try:
                    # self._s3_object.upload_files(filepath=filepath, bucket_name="marketdata-pipeline", destination_prefix=f"bronze_cache_storage_market_data/{client}/")
                    logger.info(f"[S3 INFO] {filepath} | Succesfully exported file to s3 bucket")
                except Exception as e:
                    logger.warning(f"[S3 Error] {filepath} | Unable exported file to s3 bucket | Exception : {e}")

        else:
            start_date = self._settings_config.extractor_settings.start_date
            end_date = self._settings_config.extractor_settings.end_date
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            end = datetime.strptime(end_date, "%Y-%m-%d").date()
            dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]

            for date in dates:
                date_str = datetime.strftime(date, "%Y-%m-%d")
                provider._expiry_suffixes = provider.get_expiry_suffixes(process_able_date=date, months_ahead=self._settings_config.extractor_settings.expiry_duration)
                processed_data = self._processing_day(master_instrument, provider=provider, date=str(date_str), variation="historical")
                date_map[date] = processed_data

                for date, filepath in  processed_data.items():
                    try:
                        # self._s3_object.upload_files(filepath=filepath, bucket_name="marketdata-pipeline", destination_prefix=f"bronze_cache_storage_market_data/{client}/")
                        logger.info(f"[S3 INFO] {filepath} | Succesfully exported file to s3 bucket")
                    except Exception as e:
                        logger.warning(f"[S3 Error] {filepath} | Unable exported file to s3 bucket | Exception : {e}")

        return date_map

if __name__ == "__main__":
    main_runner = DailyDataExtractor(app_settings)
    date_map = main_runner.process()
    transform_data(date_map)
