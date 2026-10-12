-- F&O Table Schema Template for ClickHouse
-- This template is used for all F&O segments (NSE_FNO, BSE_FNO, MCX_FNO, etc.)

CREATE TABLE IF NOT EXISTS {database}.{table_name} (
    `Timestamp` DateTime('Asia/Kolkata'),
    `Ticker` LowCardinality(String) CODEC(ZSTD(1)),
    `Open` Float32 CODEC(Gorilla(4), ZSTD(1)),
    `High` Float32 CODEC(Gorilla(4), ZSTD(1)),
    `Low` Float32 CODEC(Gorilla(4), ZSTD(1)),
    `Close` Float32 CODEC(Gorilla(4), ZSTD(1)),
    `Instrument_type` LowCardinality(String) CODEC(ZSTD(1)),
    `Expiry` Date,
    `Strike` Float32 CODEC(Gorilla(4), ZSTD(1)),
    `Volume` UInt32 CODEC(T64, ZSTD(1)),
    `Open_Interest` UInt32 CODEC(T64, ZSTD(1)),
    `Symbol` LowCardinality(String) CODEC(ZSTD(1)),
    `Exchange` LowCardinality(String) CODEC(ZSTD(1)),
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(Timestamp)
ORDER BY (Symbol,
 Expiry,
 Timestamp,
 Strike,
 Instrument_type)
SETTINGS index_granularity = 8192;