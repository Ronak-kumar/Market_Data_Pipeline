from cloud import adapters
from shared.utils import discover_modules


def cloud_discovery():
    discover_modules(adapters)
