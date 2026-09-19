# Market Data Pipeline

**Multi-broker historical market data extraction → transformation → data lake ingestion platform**

Built for Indian equities/F&O/commodity markets. Production-grade, broker-agnostic, cloud-native.

---

## 🎯 Overview

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Extraction** | Upstox, Groww (pluggable adapters) | Fetch instruments + historical candles (intraday, historical, expired) |
| **Transformation** | Polars (→ Databricks/Spark) | Bronze → Silver → Gold normalization, validation, enrichment |
| **Storage** | Parquet on S3 (boto3) | Partitioned data lake: `bronze/`, `silver/`, `gold/` |
| **Ingestion** | Multi-DB (ClickHouse, PostgreSQL, TimescaleDB) | Query-optimized loads for backtesting, research, dashboards |
| **Orchestration** | Config-driven, date-range aware | Intraday (T+0) and historical (T-N) backfill |

---

## 🏗 Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Broker APIs │────▶│  Extraction  │────▶│  Bronze     │────▶│  Silver     │
│  (Upstox,    │     │  (async,     │     │  (raw       │     │  (validated,│
│   Groww)     │     │   retries,   │     │   parquet,  │     │   typed,    │
│              │     │   token mgmt)│     │   partitioned)│    │   enriched) │
└─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘
                                                                    │
                              ┌─────────────┐                      │
                              │  Databases  │◀─────────────────────┘
                              │ (ClickHouse,│     ┌─────────────┐
                              │  PostgreSQL, │     │  Gold       │
                              │  TimescaleDB)│     │  (aggregated,│
                              └─────────────┘     │   features,  │
                                    ▲             │   ML-ready)  │
                                    │             └─────────────┘
                                    └──────────────┘
```

### Medallion Architecture (Parquet on S3)

```
s3://marketdata-pipeline/
├── bronze/                    # Raw, immutable, broker-format
│   ├── upstox/
│   │   ├── 2026-09-18/
│   │   │   ├── NSE_INDEX.parquet
│   │   │   ├── NSE_FO.parquet
│   │   │   └── ...
│   │   └── 2026-09-19/
│   └── groww/
│       └── ...
├── silver/                    # Validated, typed, unified schema
│   ├── upstox/
│   │   ├── 2026-09-18/
│   │   │   ├── NSE_INDEX.parquet   # equity schema
│   │   │   ├── NSE_FO.parquet      # fno schema (symbol, expiry, strike, type)
│   │   │   └── MCX_FO.parquet
│   │   └── ...
│   └── groww/
└── gold/                      # Business-ready, aggregated
    ├── ohlcv_1m/              # 1-min bars, all symbols
    ├── ohlcv_5m/
    ├── daily_bars/
    ├── features/              # Technical indicators, regime labels
    └── universe/              # Tradable universe per strategy
```

**Partitioning:** `broker/date/segment.parquet` — enables partition pruning in Athena/Trino/Spark

**Compression:** ZSTD level 3 (best ratio/speed for columnar)

**Schema evolution:** Each parquet file embeds `schema_version` in metadata; readers handle forward/backward compatibility

---

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Upstox API credentials (access token)
- AWS credentials for S3 (or MinIO for local)
- uv (recommended) or pip

### Installation
```bash
git clone <repo>
cd Market_Data_Pipeline
uv sync  # or: pip install -e ".[dev]"
```

### Configuration
```yaml
# config/settings.yaml
application:
  name: "Market Data Pipeline"
  environment: "production"
  debug: false

extractor_settings:
  client: "upstox"              # or "groww"
  interval: 1                   # minutes (1, 5, 15, 30, 60)
  max_retries: 3
  expiry_duration: 3            # months of F&O expiries to fetch
  processable_segments:
    - "NSE_INDEX"
    - "NSE_FO"
    - "BSE_INDEX"
    - "BSE_FO"
    - "MCX_FO"
  start_date: "2026-09-01"      # empty = today (intraday)
  end_date: "2026-09-18"

transformation_settings:
  spot_name_mapping:
    "NIFTY 50": "NIFTY50"
    "NIFTY BANK": "BANKNIFTY"
    # ...
  session_bounds:
    indices: {start: "09:15", end: "15:40"}
    fno: {start: "09:15", end: "15:40"}
    mcx: {start: "09:00", end: "23:30"}

s3_settings:
  bucket: "marketdata-pipeline"
  bronze_prefix: "bronze_cache_storage_market_data"
  silver_prefix: "silver_cache_storage_market_data"
  gold_prefix: "gold_cache_storage_market_data"
```

### Run Extraction (Bronze)
```bash
# Intraday (today)
uv run python -m extraction.core.daily_data_extractor

# Historical backfill
# Set start_date/end_date in config, then:
uv run python -m extraction.core.daily_data_extractor
```

### Run Transformation (Bronze → Silver)
```bash
uv run python -m transformation.orchestrator
```

### Run Gold Aggregation (Silver → Gold)
```bash
# TODO: implement gold layer builders
uv run python -m transformation.gold_builder
```

### Ingest to Database
```bash
# TODO: implement DB ingestors
uv run python -m ingestion.clickhouse_loader
```

---

## 🔌 Adding a New Broker

1. **Create extraction adapter** (`extraction/clients/adapters/{broker}.py`):
   ```python
   from extraction.clients import MarketDataProvider
   from extraction.clients.registry import client_registry

   @client_registry.register("broker_name")
   class BrokerAdapter(MarketDataProvider):
       MASTER_URL = "..."
       # Implement: _load_token, _fetch_master_instrument, fetch_instrument, 
       # fetch_historical_instrument, fetch_expired_historical_instrument, _normalize_response
   ```

2. **Create parser** (`extraction/parser/base_parser.py`):
   ```python
   class BrokerInstrumentParser(InstrumentParser):
       def parse(self, data) -> pl.DataFrame: ...
   ```

3. **Create transformation adapter** (`transformation/clients/adapters/{broker}.py`):
   ```python
   from transformation.clients import DataTransformer, client_registry

   @client_registry.register("broker_name")
   class BrokerTransformer(DataTransformer):
       def fno_transformation(self, df): ...
       def equity_transformation(self, df): ...
       def mcx_transformation(self, df): ...
   ```

4. **Add to config** — set `extractor_settings.client: "broker_name"`

**No other code changes needed** — registry auto-discovers adapters.

---

## 📊 Data Contracts

### Bronze (Raw)
| Column | Type | Source |
|--------|------|--------|
| Ticker | String | Broker-specific normalized name |
| Date | String (DD-MM-YYYY) | Candle timestamp |
| Time | String (HH:MM:SS) | Candle timestamp |
| Open | Float32 | Broker |
| High | Float32 | Broker |
| Low | Float32 | Broker |
| Close | Float32 | Broker |
| Volume | UInt32 | Broker |
| Open Interest | UInt32 | Broker (0 for equity) |

### Silver (Validated + Enriched)
**Equity:**
| Column | Type |
|--------|------|
| Timestamp | Datetime (tz-aware, IST) |
| Symbol | String (standardized) |
| Open/High/Low/Close | Float32 |
| Exchange | String (NSE/BSE) |

**F&O:**
| Column | Type |
|--------|------|
| Timestamp | Datetime (tz-aware, IST) |
| Ticker | String (original) |
| Symbol | String (underlying: NIFTY, BANKNIFTY) |
| Expiry | String (DDMMMYY) |
| Strike | String (0 for FUT) |
| Instrument_type | String (CE/PE/FUT) |
| Open/High/Low/Close | Float32 |
| Volume | UInt32 |
| Open_Interest | UInt32 |
| Exchange | String (NSE/BSE/MCX) |

### Gold (Aggregated)
- `ohlcv_1m/`: 1-min bars, all symbols, continuous contracts for F&O
- `features/`: RSI, MACD, ATR, VWAP, volume profiles, regime labels
- `universe/`: Strategy-specific tradable lists with metadata

---

## ⚙️ Configuration Reference

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `extractor_settings.client` | str | required | `upstox` \| `groww` |
| `extractor_settings.interval` | int | required | Candle interval in minutes |
| `extractor_settings.max_retries` | int | 3 | HTTP retry attempts |
| `extractor_settings.expiry_duration` | int | 3 | Months of F&O expiries to include |
| `extractor_settings.processable_segments` | list[str] | required | Segments to process |
| `extractor_settings.start_date` | str | "" | Historical start (YYYY-MM-DD); empty = today |
| `extractor_settings.end_date` | str | "" | Historical end (YYYY-MM-DD); empty = today |

---

## 🧪 Testing

```bash
# Unit tests
uv run pytest tests/unit -v

# Contract tests (Upstox API response validation)
uv run pytest tests/contracts -v

# Integration tests (requires credentials)
uv run pytest tests/integration -v

# Type checking
uv run mypy src/

# Linting
uv run ruff check src/
```

---

## 📈 Performance Targets

| Metric | Target |
|--------|--------|
| 10k instruments (intraday) | < 2 minutes |
| 10k instruments (historical, 1 day) | < 5 minutes |
| Bronze → Silver transformation | < 30 seconds per segment |
| S3 upload (per segment) | < 10 seconds |
| End-to-end (extraction + transform) | < 10 minutes/day |

---

## 🔐 Security

- **No secrets in code** — tokens via env vars / AWS Secrets Manager / HashiCorp Vault
- **IAM least privilege** — S3 write-only for bronze/silver prefixes
- **Network** — VPC endpoints for S3, broker APIs via allowlisted IPs
- **Audit** — All API calls logged with request/response metadata (no PII)

---

## 🗺 Roadmap

- [ ] **Async extraction** (aiohttp + semaphore) — 10x throughput
- [ ] **Databricks/Spark Silver→Gold** — distributed feature engineering
- [ ] **ClickHouse ingestor** — partition-aware bulk loads
- [ ] **Schema registry** (Confluent/Avro) — contract enforcement
- [ ] **Airflow/Dagster orchestration** — DAG-based scheduling, retries, SLAs
- [ ] **Data quality dashboard** — Great Expectations + Grafana
- [ ] **CDC for instrument master** — incremental updates, not full reload
- [ ] **Multi-region S3 replication** — DR compliance

---

## 🤝 Contributing

1. Fork → feature branch → PR
2. All PRs require: tests, type hints, ruff/mypy clean
3. Follow existing adapter patterns for new brokers
4. Update README for new features

---

## 📄 License

Proprietary — Finesse Advisory Services internal use only.