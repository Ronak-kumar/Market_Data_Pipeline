
from extraction.parser.base_parser import UpstoxInstrumentParser
from extraction.clients import UpstoxAdapter
provider = UpstoxAdapter(timeout=5, max_retries=3)
parser = UpstoxInstrumentParser()

instrumenmt_dict = provider.master_instrument_data
df = parser.parse(instrumenmt_dict)
print(df)