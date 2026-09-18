import polars as pl
from datetime import date
from typing import Iterable

class DataCleaner:

    def base_cleaner(self, df: pl.DataFrame) -> pl.DataFrame: 
        try:
            df = IntradayRangeFiller().range_filler(pl.from_pandas(df)).to_pandas()
        except:
            df["Date"] = df["Date"].str.replace("/", "-")
            df = IntradayRangeFiller().range_filler(pl.from_pandas(df)).to_pandas()

        return df

    def clean_and_transform_fo(self, df):
        # df['Volume'] = df['Volume'].clip(lower=0)  # make sure no negatives

        
        valid_expiries = [
            date(2026, 9, 25),
            date(2026, 9, 30),
            date(2026, 10, 1),
        ]

        expiry_tokens = sorted(
            {
                expiry.strftime(fmt).upper()
                for expiry in valid_expiries
                for fmt in ("%d%b%Y", "%d%b%y", "%d%b")
            },
            key=len,
            reverse=True,
        )

        expiry_pattern = "(" + "|".join(expiry_tokens) + ")"


        df = df.with_columns(
            pl.col("_ticker_without_type")
            .str.extract(
                expiry_pattern,
                1,
            )
            .alias("Expiry")
        )

        ### Expiry extaction
        
        df[['Symbol', 'Expiry', 'Strike', 'Instrument_type']] = df['Ticker'].str.extract(
            r'^([A-Z]+)(\d{1,2}[A-Z]{3}\d{2})(\d+)(CE|PE|FUT)$'
        )

        df['Expiry'] = pd.to_datetime(df['Expiry'], format='%d%b%y', errors='coerce').dt.date
        df = df.dropna(subset=['Strike'])
        df['Strike'] = df['Strike'].astype(int)

        expected_columns = [
            "Timestamp", "Ticker", "Open", "High", "Low", "Close",
            "Instrument_type", "Expiry", "Strike", "Volume",
            "Open_Interest", "Symbol", "Exchange"
        ]

        ### End Validation ###
        # symbols = set(df["Symbol"].unique())
        # exchange = set(df["Exchange"].unique())

        # group1 = {"NIFTY", "BANKNIFTY", "FINNIFTY"}
        # group2 = {"BANKEX", "SENSEX"}

        # if not ((group1.issubset(symbols) and "NSE" in exchange) or
        #         (group2.issubset(symbols) and "BSE" in exchange)):
        #     raise Exception("Invalid Symbol")
        ################################

        return df[expected_columns]
