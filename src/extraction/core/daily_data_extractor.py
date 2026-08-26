
from extraction.config import app_settings
import polars as pl
from extraction.clients.discovery import client_discovery
from extraction.clients.registry import client_registry
from extraction.parser.base_parser import UpstoxInstrumentParser
from extraction.observability import get_logger
from tqdm import tqdm
from datetime import datetime
import requests
import time as tm


logger = get_logger(__file__)
class DailyDataExtractor:
    def __init__(self, app_settings):
        self._settings_config = app_settings
        client_discovery()

    def _processing_day(self, master_instrument, provider, date):
        segmanet_map = {segment: pl.DataFrame() for segment in self._settings_config.extractor_settings.processable_segments}
        if len(segmanet_map) == 0:
            logger.warning(f"No segment selected for processing please select segments and try again")


        data_fetching_interval = self._settings_config.extractor_settings.interval
        for segment, df in segmanet_map.items():
            logger.info(f"Processing segment {segment}")

            segment_df = master_instrument.filter(pl.col("segment") == segment)
            pbar = tqdm(
                        segment_df.iter_rows(named=True),
                        total=segment_df.height,
                        desc=f"{segment}",
                        unit="key",
                        leave=False,
                    )

            all_rows = []
            for row in pbar:
                instrument_key = row["instrument_key"]
                retries = provider.max_retries
                for attempt in range(1, retries + 1):
                    try:

                        response = provider.fetch_historical_instrument(instrument_key = instrument_key, interval = data_fetching_interval, start_date=date, end_date=date)

                        if response.status_code == 200:
                            break

                        if response.status_code in (400, 401, 403, 404):
                            logger.warning(
                                f"[FATAL] {instrument_key} | {response.status_code} | {response.text}"
                            )
                            break

                        logger.warning(f"[RETRY] {instrument_key} | {response.status_code} | attempt {attempt}/{retries}")

                    except requests.exceptions.Timeout:
                        tm.sleep((attempt * 2))
                        logger.warning(f"[TIMEOUT] {instrument_key} | attempt {attempt}/{retries}")

    def _process_segment(self):
        pass
    def process(self):
        client = self._settings_config.extractor_settings.client.lower()
        try:
            provider = client_registry.get(client)()
        except Exception as e:
            logger.error(f"No client vailable for {client}, Exception cause: {e}")

        parser = UpstoxInstrumentParser()
        try:
            master_instrument = parser.parse(provider.master_instrument_data)
        except Exception as e:
            logger.error(f"Unable to extract marster instrument for {client}, Exception cause: {e}")


        if self._settings_config.extractor_settings.start_date == "" or self._settings_config.extractor_settings.end_date == "":
            process_able_date = datetime.now()
            date_str = process_able_date.strftime("%Y-%m-%d")
            self._processing_day(master_instrument, provider=provider, date=date_str)

        else:
            start_date = self._settings_config.extractor_settings.start_date
            end_date = self._settings_config.extractor_settings.end_date
            for date in range(start_date, end_date):
                date_str = "2026-08-11"

                self._processing_day(master_instrument, provider=provider, date=date_str)

        
                        


DailyDataExtractor(app_settings).process()