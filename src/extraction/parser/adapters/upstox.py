from extraction.parser import InstrumentParser, parser_registry
import polars as pl
from shared.observability import get_logger

logger = get_logger(__name__)

@parser_registry.register("upstox")
class UpstoxInstrumentParser(InstrumentParser):
    def parse(self, data) -> pl.DataFrame:
        logger.info("Parsing Upstox instrument data", extra={"input_type": type(data).__name__, "input_length": len(data) if hasattr(data, '__len__') else "unknown"})
        df = pl.DataFrame(data)
        result = self._normalize(df)
        logger.info("Upstox instrument parsing complete", extra={"rows": result.height})
        return result