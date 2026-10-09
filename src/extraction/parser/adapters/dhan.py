from extraction.parser import InstrumentParser, parser_registry
import polars as pl
from shared.observability import get_logger
import re

logger = get_logger(__name__)


class InstrumentValidationError(ValueError):
    """Raised when broker symbol validation fails or schema format mutates."""
    pass


@parser_registry.register("dhan")
class DhanInstrumentParser(InstrumentParser):

    # Dhan segment codes from instrument master
    AVAILABLE_SEGMENTS = [
        "NSE_EQ", "NSE_FNO", "BSE_EQ", "BSE_FNO", 
        "NSE_IDX", "BSE_IDX", "MCX_FNO", "MCX_COM",
        "NSE_CUR", "BSE_CUR", "NSE_COM", "BSE_COM"
    ]

    def parse(self, data) -> pl.DataFrame:
        logger.info("Parsing Dhan instrument data", extra={"input_type": type(data).__name__, "input_length": len(data) if hasattr(data, '__len__') else "unknown"})
        df = pl.DataFrame(data)
        result = self._normalize(df)
        logger.info("Dhan instrument parsing complete", extra={"rows": result.height})
        return result

    def _normalize(self, df: pl.DataFrame) -> pl.DataFrame:
        logger.debug("Normalizing Dhan instrument schema", extra={"input_columns": df.columns, "input_rows": df.height})
        
        # Validate required columns exist
        required_cols = ["SECURITY_ID", "EXCH_ID", "SEGMENT", "INSTRUMENT", 
                         "SYMBOL_NAME", "SEM_TRADING_SYMBOL", "DISPLAY_NAME", "INSTRUMENT_TYPE"]
        
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            logger.warning("Some expected columns missing from Dhan data", extra={"missing": missing})
        
        # Rename columns to match expected schema
        column_mapping = {
            "SECURITY_ID": "instrument_key",
            "EXCH_ID": "exchange",
            "SEGMENT": "segment_code",
            "INSTRUMENT": "instrument_type",
            "SYMBOL_NAME": "symbol_name",
            "UNDERLYING_SYMBOL": "underlying_symbol",
            "DISPLAY_NAME": "display_name",
            "INSTRUMENT_TYPE": "instrument_type_exch",
            "SEM_EXPIRY_CODE": "expiry_code",
            "SM_EXPIRY_DATE": "expiry_date",
            "LOT_SIZE": "lot_size",
            "TICK_SIZE": "tick_size",
            "UNDERLYING_SECURITY_ID": "underlying_security_id",
            "UNDERLYING_SYMBOL": "underlying_symbol",
            "ISIN": "isin",
            "SERIES": "series",
            "STRIKE_PRICE": "strike_price",
            "OPTION_TYPE": "option_type",

            "SEM_TRADING_SYMBOL": "trading_symbol",
        }
        
        # Apply renaming for columns that exist
        rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(rename_dict)
        
        # Create standardized segment field
        df = self._create_segment_field(df)
        
        # Create trading_symbol
        df = self._create_trading_symbol(df)
        
        df = self._validate_symbols(df)        

        logger.debug("Dhan schema normalization complete", extra={"output_columns": df.columns, "output_rows": df.height})
        return df

    def _create_segment_field(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create standardized segment field from exchange + segment_code."""
        # Dhan segment codes: E=Equity, D=Derivatives, I=Index, C=Currency, M=Commodity
        # Exchange: NSE, BSE, MCX
        df = df.with_columns((pl.col("exchange") + "_" + pl.col("segment_code")).alias("exchange_segment"))
        
        segment_expr = (
            pl.when(pl.col("exchange") == "NSE").then(
                pl.when(pl.col("segment_code") == "E").then(pl.lit("NSE_EQ"))
                .when(pl.col("segment_code") == "D").then(pl.lit("NSE_FO"))
                .when(pl.col("segment_code") == "I").then(pl.lit("NSE_INDEX"))
                .when(pl.col("segment_code") == "C").then(pl.lit("NSE_CURRENCY"))
                .when(pl.col("segment_code") == "M").then(pl.lit("NSE_COM"))
                .otherwise(pl.lit("None"))
            )
            .when(pl.col("exchange") == "BSE").then(
                pl.when(pl.col("segment_code") == "E").then(pl.lit("BSE_EQ"))
                .when(pl.col("segment_code") == "D").then(pl.lit("BSE_FO"))
                .when(pl.col("segment_code") == "I").then(pl.lit("BSE_INDEX"))
                .when(pl.col("segment_code") == "C").then(pl.lit("BSE_CURRENCY"))
                .when(pl.col("segment_code") == "M").then(pl.lit("BSE_COM"))
                .otherwise(pl.lit("None"))
            )
            .when(pl.col("exchange") == "MCX").then(
                pl.when(pl.col("segment_code") == "D").then(pl.lit("MCX_FO"))
                .when(pl.col("segment_code") == "M").then(pl.lit("MCX_COMMODITY"))
                .otherwise(pl.lit("None"))
            )
            .otherwise(pl.lit("None"))
        )
        
        return df.with_columns(segment_expr.alias("segment"))

    def _create_trading_symbol(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create trading_symbol from symbol_name and exchange."""
        ## Create trading_symbol based on symbol_name, expiry_date, strike_price, and option_type0
        df = df.with_columns(
            pl.col("option_type").replace({"XX": ""}).alias("option_type"),
            pl.col("expiry_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("expiry_date")
        )

        ## Normalize expiry_date to format DDMMMYY (e.g., 15JAN24)
        df = df.with_columns(
            pl.when(
                pl.col("expiry_date").is_null()
                | (pl.col("expiry_date") < pl.date(2000, 1, 1))
            )
            .then(pl.lit(""))
            .otherwise(
                pl.col("expiry_date")
                .dt.strftime("%d%b%y")
                .str.to_uppercase()
            )
            .alias("expiry_normalized")
        )

        ## Strike values normalization: Remove trailing .0 and convert negative strikes to empty string
        df = df.with_columns(
            pl.col("strike_price")
            .cast(pl.Float64, strict=False)
            .alias("_strike_price")
        ).with_columns(
            pl.when(
                pl.col("_strike_price").is_null() |
                (pl.col("_strike_price") < 0)
            )
            .then(pl.lit(""))
            .otherwise(
                pl.col("_strike_price")
                .cast(pl.String)
                .str.replace(r"\.0+$", "")
            )
            .alias("strike_price")
        ).drop("_strike_price")

        trading_expr = (
            pl.concat_list([
                pl.col("underlying_symbol").fill_null(""),
                pl.col("expiry_normalized").fill_null(""),
                pl.col("strike_price").fill_null(""),
                pl.col("option_type").fill_null(""),
            ])
            .list.eval(
                pl.element().filter(pl.element() != "")
            )
            .list.join("-")
        )
        
        return df.with_columns(trading_expr.alias("trading_symbol"))

    def _validate_symbols(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Extracts components using strict regex patterns and reassembles them 
        to create the symbol_name (excluding the exchange).
        """
        EQUITY_PATTERN = r"^(?P<underlying>[A-Za-z0-9& _-]+)$"
        FUTURE_PATTERN = r"^(?P<underlying>[A-Z0-9&_-]+)-(?P<expiry>\d{2}[A-Za-z]{3}\d{2})-FUT$"
        OPTION_PATTERN = r"^(?P<underlying>[A-Z0-9&_-]+)-(?P<expiry>\d{2}[A-Za-z]{3}\d{2})-(?P<strike>\d+(?:\.\d+)?)-(?P<option_type>CE|PE)$"

        temp_df = df.with_columns(
            # -------------------------
            # 1. Symbol name
            # -------------------------
            symbol_name=(
                pl.when(pl.col("trading_symbol").str.contains(OPTION_PATTERN).fill_null(False))
                .then(pl.col("trading_symbol").str.replace(OPTION_PATTERN, "$1-$4-$2-$3").str.replace_all(" ", ""))
                
                .when(pl.col("trading_symbol").str.contains(FUTURE_PATTERN).fill_null(False))
                .then(pl.col("trading_symbol").str.replace(FUTURE_PATTERN, "$1-$2-FUT").str.replace_all(" ", ""))
                
                .when(pl.col("trading_symbol").str.contains(EQUITY_PATTERN).fill_null(False))
                .then(pl.col("trading_symbol").str.replace(EQUITY_PATTERN, "$1"))
                
                .otherwise(pl.lit(None, dtype=pl.String))
            ).str.to_uppercase(),

            # -------------------------
            # 2. Asset class
            # -------------------------
            symbol_asset_class=(
                pl.when(pl.col("trading_symbol").str.contains(OPTION_PATTERN).fill_null(False))
                .then(pl.lit("FO"))
                
                .when(pl.col("trading_symbol").str.contains(FUTURE_PATTERN).fill_null(False))
                .then(pl.lit("FO"))
                
                .when(pl.col("trading_symbol").str.contains(EQUITY_PATTERN).fill_null(False))
                .then(pl.lit("CASH"))
                
                .otherwise(pl.lit(None, dtype=pl.String))
            )

        ).with_columns(
            # -------------------------
            # 3. Expiry fetch (from symbol_name)
            # -------------------------
            symbol_expiry=(
                pl.col("symbol_name")
                .str.extract(r"-(\d{2}[A-Z]{3}\d{2})-", 1)
                .str.to_date(format="%d%b%y", strict=False))
        )

        # THE SECURITY LAYER: Catch the broken schemas before they are dropped
        invalid_rows = temp_df.filter(pl.col("symbol_name").is_null())
    
        if not invalid_rows.is_empty():
            bad_samples = invalid_rows["trading_symbol"].head(5).to_list()
            logger.warning(f"Invalid rows found in the master insrument symbol processing, lengh of invalid rows are {len(invalid_rows)}")
            if len(invalid_rows) > 100:
                raise InstrumentValidationError(
                    f"CRITICAL: Schema mutation detected! {invalid_rows.height} symbols failed regex validation.\n"
                    f"The broker likely changed their format. Review these raw symbols: {bad_samples}"
                )

        return temp_df.drop_nulls(subset=["symbol_name"])