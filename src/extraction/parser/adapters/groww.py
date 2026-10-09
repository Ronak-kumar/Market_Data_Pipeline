from extraction.parser import InstrumentParser, parser_registry
import polars as pl
from shared.observability import get_logger
import re

logger = get_logger(__name__)

class InstrumentValidationError(ValueError):
    """Raised when broker symbol validation fails or schema format mutates."""
    pass

@parser_registry.register("groww")
class GrowwInstrumentParser(InstrumentParser):
    AVAILABLE_SEGMENT = ['NSE_EQ', 'BSE_FO', 'BSE_INDEX', 'NSE_FO', 'BSE_EQ', 'MCX_FO', 'NSE_INDEX']

    def parse(self, data, ) -> pl.DataFrame:
        logger.info("Parsing Groww instrument data", extra={"input_type": type(data).__name__, "input_length": len(data) if hasattr(data, '__len__') else "unknown"})
        df = pl.DataFrame(data)
        result = self._normalize(df)
        logger.info("Groww instrument parsing complete", extra={"rows": result.height})
        return result

    def _normalize(self, df: pl.DataFrame) -> pl.DataFrame:
        logger.debug("Normalizing instrument schema", extra={"input_columns": df.columns, "input_rows": df.height})

        ## Segment Modification ##
        df = df.with_columns([(pl.col("segment")).alias("base_segment")])

        df = self._create_segment_field(df)

        df = self._validate_symbols(df)

        result = df
        logger.debug("Schema normalization complete", extra={"output_columns": result.columns, "output_rows": result.height})
        return result

    def _create_segment_field(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create standardized segment field from exchange + segment_code."""
        # Dhan segment codes: E=Equity, D=Derivatives, I=Index, C=Currency, M=Commodity
        # Exchange: NSE, BSE, MCX
        
        segment_expr = (
            pl.when(pl.col("exchange") == "NSE")
            .then(
                pl.when((pl.col("segment") == "CASH") & (pl.col("instrument_type") == "EQ"))
                .then(pl.lit("NSE_EQ"))

                .when(
                    (pl.col("segment") == "CASH")
                    & (pl.col("instrument_type") == "IDX")
                )
                .then(pl.lit("NSE_INDEX"))

                .when((pl.col("segment") == "FNO") | (pl.col("segment") == "COMMODITY"))
                .then(pl.lit("NSE_FO"))

                .otherwise(pl.lit("None"))
            )

            .when(pl.col("exchange") == "BSE")
            .then(
                pl.when(
                    (pl.col("segment") == "CASH")
                    & (pl.col("instrument_type") == "EQ")
                )
                .then(pl.lit("BSE_EQ"))

                .when(
                    (pl.col("segment") == "CASH")
                    & (pl.col("instrument_type") == "IDX")
                )
                .then(pl.lit("BSE_INDEX"))

                .when((pl.col("segment") == "FNO") | (pl.col("segment") == "COMMODITY"))
                .then(pl.lit("BSE_FO"))

                .otherwise(pl.lit("None"))
            )

            .when(pl.col("exchange") == "MCX")
            .then(
                pl.when(pl.col("segment") == "COMMODITY")
                .then(pl.lit("MCX_FO"))

                .otherwise(pl.lit("None"))
            )

            .otherwise(pl.lit("None"))
        )

        
        return df.with_columns(segment_expr.alias("segment"))
    def _validate_symbols(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Extracts components using strict regex patterns and reassembles them 
        to create the symbol_name (excluding the exchange).
        """
        EQUITY_PATTERN = r"^(?P<exchange>NSE|BSE)-(?P<underlying>[A-Za-z0-9& _-]+)$"
        FUTURE_PATTERN = r"^(?P<exchange>NSE|BSE|MCX)-(?P<underlying>[A-Z0-9&_-]+)-(?P<expiry>\d{2}[A-Za-z]{3}\d{2})-FUT$"
        OPTION_PATTERN = r"^(?P<exchange>NSE|BSE|MCX)-(?P<underlying>[A-Z0-9&_-]+)-(?P<expiry>\d{2}[A-Za-z]{3}\d{2})-(?P<strike>\d+(?:\.\d+)?)-(?P<option_type>CE|PE)$"

        temp_df = df.with_columns(
            # -------------------------
            # 1. Symbol name
            # -------------------------
            symbol_name=(
                pl.when(pl.col("groww_symbol").str.contains(OPTION_PATTERN).fill_null(False))
                .then(pl.col("groww_symbol").str.replace(OPTION_PATTERN, "$2-$3-$4-$5"))
                
                .when(pl.col("groww_symbol").str.contains(FUTURE_PATTERN).fill_null(False))
                .then(pl.col("groww_symbol").str.replace(FUTURE_PATTERN, "$2-$3-FUT"))
                
                .when(pl.col("groww_symbol").str.contains(EQUITY_PATTERN).fill_null(False))
                .then(pl.col("groww_symbol").str.replace(EQUITY_PATTERN, "$2"))
                
                .otherwise(pl.lit(None, dtype=pl.String))
            ).str.to_uppercase(),

            # -------------------------
            # 2. Asset class
            # -------------------------
            symbol_asset_class=(
                pl.when(pl.col("groww_symbol").str.contains(OPTION_PATTERN).fill_null(False))
                .then(pl.lit("FO"))
                
                .when(pl.col("groww_symbol").str.contains(FUTURE_PATTERN).fill_null(False))
                .then(pl.lit("FO"))
                
                .when(pl.col("groww_symbol").str.contains(EQUITY_PATTERN).fill_null(False))
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
            bad_samples = invalid_rows["groww_symbol"].head(5).to_list()
            logger.warning(f"Invalid rows found in the master insrument symbol processing, lengh of invalid rows are {len(invalid_rows)}")
            if len(invalid_rows) > 100:
                raise InstrumentValidationError(
                    f"CRITICAL: Schema mutation detected! {invalid_rows.height} symbols failed regex validation.\n"
                    f"The broker likely changed their format. Review these raw symbols: {bad_samples}"
                )

        return temp_df.drop_nulls(subset=["symbol_name"])