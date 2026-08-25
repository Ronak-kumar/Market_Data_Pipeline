from extraction.clients import adapters
from extraction.utils import discover_modules
def client_discovery():
    discover_modules(adapters)

