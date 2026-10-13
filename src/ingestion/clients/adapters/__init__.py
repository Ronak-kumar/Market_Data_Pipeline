"""
Ingestion Adapters Package
"""
# Adapters are auto-discovered via ingestion_discovery()
# Import side-effects register them with ingestion_registry

from ingestion.clients.adapters import clickhouse
from ingestion.clients.adapters import postgresql
from ingestion.clients.adapters import timescaledb

__all__ = []
