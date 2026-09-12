from transformation.clients import adapters
from shared.utils import discover_modules


def client_discovery():
    discover_modules(adapters)
