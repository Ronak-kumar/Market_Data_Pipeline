from extraction.parser import InstrumentParser, parser_registry
import polars as pl
from shared.observability import get_logger
import re

logger = get_logger(__name__)

class InstrumentValidationError(ValueError):
    """Raised when broker symbol validation fails or schema format mutates."""
    pass

@parser_registry.register("upstox")
class UpstoxInstrumentParser(InstrumentParser):

    AVAILABLE_SEGMENTS = ['MCX_FO', 'BCD_FO', 'BSE_EQ', 'NSE_INDEX', 'GLOBAL_INDEX', 'NSE_EQ', 'BSE_INDEX', 'BSE_FO', 'NCD_FO', 'GLOBAL_INDICATOR', 'NSE_COM', 'NSE_FO']

    def parse(self, data) -> pl.DataFrame:
        logger.info("Parsing Upstox instrument data", extra={"input_type": type(data).__name__, "input_length": len(data) if hasattr(data, '__len__') else "unknown"})
        df = pl.DataFrame(data)
        result = self._normalize(df)
        logger.info("Upstox instrument parsing complete", extra={"rows": result.height})
        return result

    def _normalize(self, df: pl.DataFrame) -> pl.DataFrame:
        logger.debug("Normalizing instrument schema", extra={"input_columns": df.columns, "input_rows": df.height})
        df = self._validate_symbols(df)

        result = df
        logger.debug("Schema normalization complete", extra={"output_columns": result.columns, "output_rows": result.height})
        return result

    def _validate_symbols(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Extracts components using strict regex patterns and reassembles them 
        to create the symbol_name (excluding the exchange).
        """
        EQUITY_PATTERN = r"^(?P<underlying>[A-Za-z0-9& _-]+)$"
        FUTURE_PATTERN = r"^(?P<underlying>[A-Z0-9&_-]+) FUT (?P<expiry>\d{2} [A-Za-z]{3} \d{2})$"
        OPTION_PATTERN = r"^(?P<underlying>[A-Z0-9&_-]+) (?P<strike>\d+(?:\.\d+)?) (?P<option_type>CE|PE) (?P<expiry>\d{2} [A-Za-z]{3} \d{2})$"

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