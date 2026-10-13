"""
Ingestion Clients Package
"""
from ingestion.clients.base import DataIngestor, IngestionResult
from ingestion.clients.registry import ingestion_registry
from ingestion.clients.discovery import ingestion_discovery

__all__ = [
    "DataIngestor",
    "IngestionResult", 
    "ingestion_registry",
    "ingestion_discovery",
]
