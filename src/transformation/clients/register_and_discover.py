from shared.utils import Registry
from transformation.clients import adapters
from shared.utils import discover_modules

def transformer_discovery():
    discover_modules(adapters)

transformer_registry = Registry()
