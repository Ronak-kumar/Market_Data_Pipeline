import polars as pl
from polars import DataFrame
from datetime import datetime, time, date
from shared.observability import get_logger

logger = get_logger(__name__)


class IntradayRangeFiller:
    @staticmethod

    def range_filler(df: DataFrame, date: date, start_time: str, end_time: str, interval: int, symbol_col: str= "Ticker"):
        logger.info("Starting intraday range filling", extra={"start_time": start_time, "end_time": end_time, "input_rows": df.height, "input_columns": df.columns})

        start_time = datetime.strptime(start_time, "%H:%M").time()
        end_time = datetime.strptime(end_time, "%H:%M").time()
        interval = f"{interval}m"
        # df = df.with_columns(pl.col("Timestamp").cast(pl.Datetime("us")))
        df = df.drop(["Date", "Time"])

        result_df_list = []
        base_date = date
        
        # Define full timestamp range for market hours
        start = datetime.combine(base_date, start_time)
        end = datetime.combine(base_date, end_time)
        logger.debug("Market hours", extra={"date": str(base_date), "start": str(start), "end": str(end)})

        # Create 1-min interval timestamp series using range
        minutes_range = pl.datetime_range(start=start, end=end, interval=interval, eager=True)

        # Convert to DataFrame with Date and Time columns
        minutes_range = pl.DataFrame({"Timestamp": minutes_range}).with_columns([
            pl.col("Timestamp").dt.strftime("%d-%m-%Y").alias("Date"),
            pl.col("Timestamp").dt.strftime("%H:%M:%S").alias("Time")
        ])

        # Get unique tickers
        unique_ticker = df.select(symbol_col).unique().to_series().to_list()
        logger.debug("Processing tickers for date", extra={"date": str(base_date), "ticker_count": len(unique_ticker)})

        # Initialize output
        processed_tickers = 0
        for ticker in unique_ticker:
            logger.debug("Processing ticker", extra={"ticker": ticker, "date": str(base_date)})
            sliced_df = df.filter(pl.col(symbol_col) == ticker)

            # Join with full timestamp range
            merged = minutes_range.join(sliced_df, on="Timestamp", how="left")

            # Forward fill only needed columns with the last recorded close
            merged = merged.sort("Timestamp")

            # Previous Close
            merged = merged.with_columns(
                pl.col("Close").forward_fill().alias("_previous_close")
            )

            # Fill missing OHLC from previous Close
            merged = merged.with_columns(
                pl.col("Open").fill_null(pl.col("_previous_close")).alias("Open"),
                pl.col("High").fill_null(pl.col("_previous_close")).alias("High"),
                pl.col("Low").fill_null(pl.col("_previous_close")).alias("Low"),
                pl.col("Close").fill_null(pl.col("_previous_close")).alias("Close"),

                # Fill missing Volume / Open Interest with 0
                pl.col("Volume").fill_null(0).alias("Volume"),
                pl.col("Open_Interest").fill_null(0).alias("Open_Interest"),

                # Fill missing metadata from previous row
                pl.col("Exchange").forward_fill().alias("Exchange"),
                pl.col("Ticker").forward_fill().alias("Ticker"),
            ).drop("_previous_close")
               

            # Drop rows where all OHLC fields are null (optional, depends on your needs)
            merged = merged.drop_nulls()

            # Append to result
            result_df_list.append(merged)
            processed_tickers += 1

        logger.debug("Date processing complete", extra={"date": str(base_date), "tickers_processed": processed_tickers})

        # Concatenate all results
        final_df = pl.concat(result_df_list)
        # final_df = final_df.drop(["Date", "Time"])
        logger.info("Intraday range filling completed", extra={"output_rows": final_df.height, "output_columns": final_df.columns})
        return final_df