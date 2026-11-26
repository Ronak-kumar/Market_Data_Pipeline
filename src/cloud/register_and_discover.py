from cloud import adapters
from shared.utils import discover_modules
from shared.utils import Registry

def cloud_discovery():
    discover_modules(adapters)

cloud_registry = Registry()