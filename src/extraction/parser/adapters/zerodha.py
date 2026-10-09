from extraction.parser import InstrumentParser, parser_registry
import polars as pl
from shared.observability import get_logger

logger = get_logger(__name__)


class InstrumentValidationError(ValueError):
    """Raised when broker symbol validation fails or schema format mutates."""
    pass


@parser_registry.register("zerodha")
class ZerodhaInstrumentParser(InstrumentParser):

    # Zerodha segments
    AVAILABLE_SEGMENTS = [
        "NSE_EQ", "NSE_FNO", "BSE_EQ", "BSE_FNO", 
        "NSE_INDEX", "BSE_INDEX", "MCX_FNO", "NSE_CURRENCY", "BSE_CURRENCY"
    ]

    def parse(self, data) -> pl.DataFrame:
        logger.info("Parsing Zerodha instrument data", extra={"input_type": type(data).__name__, "input_length": len(data) if hasattr(data, '__len__') else "unknown"})
        df = pl.DataFrame(data)
        result = self._normalize(df)
        logger.info("Zerodha instrument parsing complete", extra={"rows": result.height})
        return result

    def _normalize(self, df: pl.DataFrame) -> pl.DataFrame:
        logger.debug("Normalizing Zerodha instrument schema", extra={"input_columns": df.columns, "input_rows": df.height})
        
        # Validate required columns exist
        required_cols = ["instrument_token", "exchange_token", "tradingsymbol", "name", "expiry", 
                         "strike", "tick_size", "lot_size", "instrument_type", "segment", "exchange"]
        
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            logger.warning("Some expected columns missing from Zerodha data", extra={"missing": missing})
        
        # Rename columns to match expected schema
        column_mapping = {
            "instrument_token": "instrument_key",
            "exchange_token": "exchange_token",
            "tradingsymbol": "trading_symbol",
            "name": "symbol_name",
            "expiry": "expiry_date",
            "strike": "strike_price",
            "tick_size": "tick_size",
            "lot_size": "lot_size",
            "instrument_type": "instrument_type",
            "segment": "segment_code",
            "exchange": "exchange",
        }
        
        # Apply renaming for columns that exist
        rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(rename_dict)
        
        # Create standardized segment field
        df = self._create_segment_field(df)

        # Create trading_symbol
        df = self._create_trading_symbol(df)
        
        df = self._validate_symbols(df)
        
        # Create symbol_asset_class
        df = self._create_asset_class(df)
        
        # Parse expiry date
        if "expiry_date" in df.columns:
            df = df.with_columns(
                pl.col("expiry_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("symbol_expiry")
            )
        else:
            df = df.with_columns(pl.lit(None).cast(pl.Date).alias("symbol_expiry"))
        
        # Ensure instrument_key is string
        df = df.with_columns(pl.col("instrument_key").cast(pl.Utf8))
        
        # Create display_name
        if "display_name" not in df.columns:
            df = df.with_columns(pl.col("symbol_name").alias("display_name"))
        
        # Select and order final columns
        final_cols = [
            "instrument_key", "trading_symbol", "symbol_name", "display_name",
            "exchange", "segment", "segment_code", "instrument_type",
            "expiry_date", "symbol_expiry", "strike_price", "lot_size", "tick_size",
            "symbol_asset_class"
        ]
        
        # Keep only columns that exist
        final_cols = [c for c in final_cols if c in df.columns]
        df = df.select(final_cols)
        
        logger.debug("Zerodha schema normalization complete", extra={"output_columns": df.columns, "output_rows": df.height})
        return df

    def _create_segment_field(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create standardized segment field from exchange + segment_code + instrument_type."""
        # Zerodha segments: NSE, BSE, NFO, BFO, MCX, CDS, BCD
        # instrument_type: EQ, FUT, CE, PE, etc.
        
        segment_expr = (
            pl.when(pl.col("exchange") == "NSE").then(
                pl.when(pl.col("instrument_type") == "EQ").then(pl.lit("NSE_EQ"))
                .when(pl.col("instrument_type").is_in(["FUT", "CE", "PE"])).then(pl.lit("NSE_FNO"))
                .otherwise(pl.lit("NSE_EQ"))
            )
            .when(pl.col("exchange") == "BSE").then(
                pl.when(pl.col("instrument_type") == "EQ").then(pl.lit("BSE_EQ"))
                .when(pl.col("instrument_type").is_in(["FUT", "CE", "PE"])).then(pl.lit("BSE_FNO"))
                .otherwise(pl.lit("BSE_EQ"))
            )
            .when(pl.col("exchange") == "NFO").then(pl.lit("NSE_FNO"))
            .when(pl.col("exchange") == "BFO").then(pl.lit("BSE_FNO"))
            .when(pl.col("exchange") == "MCX").then(pl.lit("MCX_FNO"))
            .when(pl.col("exchange") == "CDS").then(pl.lit("NSE_CURRENCY"))
            .when(pl.col("exchange") == "BCD").then(pl.lit("BSE_CURRENCY"))
            .otherwise(pl.lit("NSE_EQ"))
        )
        
        return df.with_columns(segment_expr.alias("segment"))

    def _create_trading_symbol(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create trading_symbol from symbol_name and exchange."""
        ## Create trading_symbol based on symbol_name, expiry_date, strike_price, and option_type0
        df = df.with_columns(
            pl.col("instrument_type").replace({"EQ": ""}).alias("option_type"),
            pl.col("expiry_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("expiry_date")
        )

        df = df.with_columns(
            pl.col("symbol_name").alias("symbol_name_base"),
            pl.col("trading_symbol").alias("trading_symbol_base"),)

        df= df.with_columns(pl.when(pl.col("instrument_type").is_in(["EQ"])).then(pl.col("trading_symbol_base")).otherwise(pl.col("symbol_name_base")).alias("symbol_name"))

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
                (pl.col("_strike_price") <= 0)
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
                pl.col("symbol_name").fill_null(""),
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