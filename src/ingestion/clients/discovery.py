from ingestion.clients import adapters
from shared.utils import discover_modules


def ingestion_discovery():
    """Discover and register all ingestion adapters."""
    discover_modules(adapters)
