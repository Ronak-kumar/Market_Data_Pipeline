import polars as pl
from datetime import datetime, time
from shared.observability import get_logger

logger = get_logger(__name__)


class IntradayRangeFiller:
    @staticmethod
    def range_filler(df, range_of="NFO"):
        logger.info("Starting intraday range filling", extra={"range_of": range_of, "input_rows": df.height, "input_columns": df.columns})
        # Convert 'Timestamp' column from 'Date' + 'Time'
        try:
            df = df.with_columns([
                    pl.concat_str([
                        pl.col("Date").str.strip_chars().str.strip_chars().str.strip_chars(),
                        # Remove stray whitespace if any
                        pl.lit(" "),
                        pl.col("Time")
                    ]).str.strptime(pl.Datetime, format="%d-%m-%Y %H:%M:%S").alias("Timestamp")
                ])
        except Exception as e:
            logger.warning("First timestamp format failed, trying alternative", extra={"error": str(e)})
            df = df.with_columns([
                pl.concat_str([
                    pl.col("Date").str.strip_chars().str.strip_chars().str.strip_chars(),
                    # Remove stray whitespace if any
                    pl.lit(" "),
                    pl.col("Time")
                ]).str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S").alias("Timestamp")
            ])
            df = df.with_columns(pl.col("Timestamp").cast(pl.Datetime("us")))

        date_to_process = df["Date"].unique()
        logger.debug("Unique dates to process", extra={"dates": date_to_process.to_list()})
        result_df_list = []
        df = df.drop(["Date", "Time"])

        for date in date_to_process:
            try:
                base_date = datetime.strptime(date, "%d-%m-%Y").date()
            except Exception:
                base_date = datetime.strptime(date, "%Y-%m-%d").date()

            # Define full timestamp range for market hours
            if range_of == "NFO":
                start = datetime.combine(base_date, time(9, 15))
                end = datetime.combine(base_date, time(15, 40))
                logger.debug("NFO market hours", extra={"date": str(base_date), "start": str(start), "end": str(end)})
            else:
                start = datetime.combine(base_date, time(9, 00))
                end = datetime.combine(base_date, time(23, 30))
                logger.debug("Extended market hours", extra={"date": str(base_date), "start": str(start), "end": str(end)})

            # Create 1-min interval timestamp series using range
            minutes_range = pl.datetime_range(start=start, end=end, interval="1m", eager=True)

            # Convert to DataFrame with Date and Time columns
            minutes_range = pl.DataFrame({"Timestamp": minutes_range}).with_columns([
                pl.col("Timestamp").dt.strftime("%d-%m-%Y").alias("Date"),
                pl.col("Timestamp").dt.strftime("%H:%M:%S").alias("Time")
            ])

            # Get unique tickers
            unique_ticker = df.select("Ticker").unique().to_series().to_list()
            logger.debug("Processing tickers for date", extra={"date": str(base_date), "ticker_count": len(unique_ticker)})

            # Initialize output
            processed_tickers = 0
            for ticker in unique_ticker:
                logger.debug("Processing ticker", extra={"ticker": ticker, "date": str(base_date)})
                sliced_df = df.filter(pl.col("Ticker") == ticker)

                # Join with full timestamp range
                merged = minutes_range.join(sliced_df, on="Timestamp", how="left")

                # Forward fill only needed columns
                merged = merged.sort("Timestamp").fill_null(strategy="forward")

                # Drop rows where all OHLC fields are null (optional, depends on your needs)
                merged = merged.drop_nulls()

                # Append to result
                result_df_list.append(merged)
                processed_tickers += 1

            logger.debug("Date processing complete", extra={"date": str(base_date), "tickers_processed": processed_tickers})

        # Concatenate all results
        final_df = pl.concat(result_df_list)
        final_df = final_df.drop(["Timestamp"])
        logger.info("Intraday range filling completed", extra={"output_rows": final_df.height, "output_columns": final_df.columns})
        return final_df