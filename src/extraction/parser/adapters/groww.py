from extraction.parser import InstrumentParser, parser_registry
import polars as pl
from shared.observability import get_logger

logger = get_logger(__name__)

@parser_registry.register("groww")
class GrowwInstrumentParser(InstrumentParser):
    def parse(self, data) -> pl.DataFrame:
        logger.info("Parsing Groww instrument data", extra={"input_type": type(data).__name__, "input_length": len(data) if hasattr(data, '__len__') else "unknown"})
        df = pl.DataFrame(data)
        result = self._normalize(df)
        logger.info("Groww instrument parsing complete", extra={"rows": result.height})
        return result
