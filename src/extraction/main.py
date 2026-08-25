
from extraction.config import app_settings
import polars as pl
def get_provider_and_parser(client_name: str):
    if client_name == "Upstox":
        from extraction.parser.base_parser import UpstoxInstrumentParser
        from extraction.clients import UpstoxAdapter
        provider = UpstoxAdapter(timeout=5, max_retries=3)
        parser = UpstoxInstrumentParser()


    elif client_name == "Groww":
        from extraction.parser.base_parser import GrowwInstrumentParser
        from extraction.clients import GrowwAdapter
        provider = GrowwAdapter(timeout=5, max_retries=3)
        parser = GrowwInstrumentParser()

    else:
        raise ValueError(f"Unsupported client: {client_name}")
    
    return provider, parser


if __name__ == "__main__":
    print(app_settings)
    provider, parser = get_provider_and_parser(app_settings.extractor_settings.client)
    df = parser.parse(provider.master_instrument_data)
    processable_segments = {}
    for segment in app_settings.extractor_settings.processable_segments:
        processable_segments[segment] = df.filter(pl.col("segment") == segment)


    for segment, segment_df in processable_segments.items():
        print(f"Processing Segment: {segment}")
        for row in segment_df.iter_rows():
            print(row)
    print(df)