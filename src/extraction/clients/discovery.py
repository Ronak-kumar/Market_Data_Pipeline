from extraction.clients import adapters
from extraction.utils.discovery import discover_modules
def client_discovery():
    discover_modules(adapters)

