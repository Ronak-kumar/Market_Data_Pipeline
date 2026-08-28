
from extraction.config import app_settings
import polars as pl
from extraction.clients.discovery import client_discovery
from extraction.clients.registry import client_registry
from extraction.parser.base_parser import UpstoxInstrumentParser
from extraction.observability import get_logger
from tqdm import tqdm
from datetime import datetime
import numpy as np
from pathlib import Path
from typing import Dict


logger = get_logger(__file__)
class DailyDataExtractor:
    def __init__(self, app_settings):
        self._settings_config = app_settings
        client_discovery()


    def _processing_day(self, master_instrument, provider, date) -> Dict[str, pl.DataFrame]:

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
                    if not any(exp in row["trading_symbol"] for exp in provider._expiry_suffixe):
                        continue

                candles, name = provider.fetch_historical_instrument(context = row, interval = data_fetching_interval, start_date=date, end_date=date)
                if candles == None or name == None:
                    continue

                segment_rows.extend([
                    [
                        name,
                        datetime.fromisoformat(row[0]).strftime('%d-%m-%Y'),
                        datetime.fromisoformat(row[0]).strftime('%H:%M:%S'),
                        row[1], row[2], row[3], row[4], row[5], row[6]
                    ]
                    for row in candles
                ])

            segment_map[segment] = pl.DataFrame(segment_rows, schema=["Ticker", "Date", "Time", "Open", "High", "Low", "Close", "Volume", "Open Interest"])
            
            saving_path = Path(__file__).parent.parent / "cache"/ date 
            saving_path.mkdir(parents=True, exist_ok=True)
            segment_map[segment].write_parquet(saving_path / f"{segment}.parquet")

        return segment_map

    def process(self) -> None:
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
            
            dates = np.arange(
                np.datetime64(start_date),
                np.datetime64(end_date) + np.timedelta64(1, "D"),
                np.timedelta64(1, "D")
                )
            for date in dates:
                provider._expiry_suffixe = provider.get_expiry_suffixes(date)
                processed_data = self._processing_day(master_instrument, provider=provider, date=str(date))

DailyDataExtractor(app_settings).process()