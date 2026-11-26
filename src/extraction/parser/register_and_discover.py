from shared.utils import Registry
from extraction.parser import adapters
from shared.utils import discover_modules

def parser_discovery():
    discover_modules(adapters)

parser_registry = Registry()
