from shared.config import app_settings
import polars as pl
from extraction.clients import client_discovery
from extraction.clients import client_registry
from extraction.parser import parser_registry, parser_discovery
from shared.observability import get_logger
from tqdm import tqdm
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any
from cloud import cloud_discovery, cloud_registry
from transformation.orchestrator import transform_data
from transformation.clients import transformer_discovery, transformer_registry
from extraction.models.data_models import ClientObject, CloudObject, ExtractionConfig

logger = get_logger(__name__)

class DailyDataExtractor:
    def __init__(self):
        self._settings_config = app_settings
        self._initialize_client()
        self._initialize_cloud_clients()
        self._config()
        self.segment_type_map = {segment: segment_type for segment_type, segments in self._settings_config.broker_configuration.processable_segments.items() for segment in segments}

    def _config(self):
        expiry_suffix_duration = self._settings_config.extractor_settings.expiry_duration
        end_date = self._settings_config.extractor_settings.end_date
        start_date = self._settings_config.extractor_settings.start_date
        processable_segments = self._settings_config.broker_configuration.processable_segments
        extraction_interval = self._settings_config.extractor_settings.interval
        extraction_variation = "intraday" if end_date == "" and start_date == "" else "historical" 

        self.extraction_config = ExtractionConfig(expiry_suffix_duration, end_date, start_date, processable_segments, extraction_interval, extraction_variation)

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

        self.client_object = ClientObject(client_name=client, provider=provider, parser=parser,
                                           transformer=transformer, master_instrument=master_instrument)

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

        self.cloud_object = CloudObject(client_name=cloud_client, push_to_cloud_flag=cloud_push_flag, cloud_instance=cloud_object,
                                         storage_object=storage_object, destination_folder=destination_folder)


    def _push_to_cloud(self, processed_data: dict) -> None:
        if not self.cloud_object.push_to_cloud_flag:
            return
        
        for _, filepath in processed_data.items():
            try:
                storage_object = self.cloud_object.storage_object
                destination_folder = self.cloud_object.destination_folder
                self._cloud_object.upload_files(filepath=filepath, bucket_name=storage_object, destination_prefix=destination_folder)
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
        """ Per day processing following 3 steps"""
        date_str = datetime.strftime(date, "%Y-%m-%d")
        self.client_object.provider._expiry_suffixes = self.client_object.provider.get_expiry_suffixes(process_able_date=date, months_ahead=self.extraction_config.expiry_suffix_duration)
        logger.debug("Expiry suffixes loaded", extra={"date": date_str, "suffixes": self.extraction_config.expiry_suffix_duration})

        # Process day data
        logger.info("Processing date...", extra={"date": date_str, "variation": variation})
        processed_data = self._processing_day(master_instrument, provider=self.client_object.provider, date=date_str, variation=variation)
        # Push processed data to datalake
        self._push_to_cloud(processed_data)
        # Create silver data
        transform_data({date: processed_data}, self.client_object.transformer, self.segment_type_map)

        return processed_data

    def _processing_day(self, master_instrument, provider, date, variation) -> Dict[str, pl.DataFrame]:
        logger.info("Starting daily processing", extra={"date": date, "variation": variation})

        segment_map = {segment: pl.DataFrame() for segment in self.segment_type_map.keys()}
        if len(segment_map) == 0:
            logger.warning("No segment selected for processing", extra={"processable_segments": self.extraction_config.processable_segments})
            return segment_map

        data_fetching_interval = self.extraction_config.extraction_interval
        logger.debug("Processing configuration", extra={"interval": data_fetching_interval, "segments": list(segment_map.keys())})

        for segment, _ in segment_map.items():
            logger.info("Processing segment", extra={"segment": segment})
            segment_df = master_instrument.filter(pl.col("segment") == segment)
            logger.debug("Segment filtered", extra={"segment": segment, "instrument_count": segment_df.height})

            segment_rows, fetched_count, skipped_count, error_count = self._fetch_segment(segment_df, segment, provider, date, variation)

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

            saving_path = Path(__file__).parent.parent / "cache" / self.client_object.client_name / "Bronze" / date
            saving_path.mkdir(parents=True, exist_ok=True)
            output_path = saving_path / f"{segment}.parquet"
            segment_frame.write_parquet(output_path)
            logger.info("Segment written to Bronze layer", extra={"segment": segment, "output_path": str(output_path), "rows": segment_frame.height})
            segment_map[segment] = output_path

        return segment_map

    def _fetch_segment(self, segment_df: pl.DataFrame, segment: str, provider, date, variation: str) -> list[list]:
        rows = []
        fetched_count =  skipped_count = error_count = 0

        pbar = tqdm(segment_df.iter_rows(named=True), total=segment_df.height,
            desc=segment, unit="key", leave=False)

        for row in pbar:
            trading_symbol = row.get("trading_symbol", "")

            if self._should_skip_instrument(row, segment, provider):
                skipped_count += 1
                continue

            try:
                candles = provider.fetch_instrument(
                    row=row,
                    variation=variation,
                    date=date,
                    interval=self.extraction_config.extraction_interval
                )

            except Exception:
                error_count += 1
                logger.error("Failed to fetch candles",
                    extra={
                        "instrument_key": row["instrument_key"],
                        "trading_symbol": row.get("trading_symbol", ""),
                    })
                continue

            if not candles:
                skipped_count += 1
                logger.info(f'No candles returned {trading_symbol}')
                continue

            rows.extend(candles)
            fetched_count += 1

        return rows, fetched_count, skipped_count, error_count

    def _should_skip_instrument(self, row: dict, segment: str, provider: Any) -> bool:
        # F&O filtering
        segment_type = self.segment_type_map[segment]
        if "fo" in segment_type.lower():
            trading_symbol = row.get("trading_symbol", "")

            if not any(
                expiry in trading_symbol
                for expiry in provider._expiry_suffixes):
                return True

        return False
    
if __name__ == "__main__":
    main_runner = DailyDataExtractor()
    date_map = main_runner.process()