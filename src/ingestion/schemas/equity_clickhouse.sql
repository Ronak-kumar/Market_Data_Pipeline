-- Equity/Cash Table Schema Template for ClickHouse
-- This template is used for all equity segments (NSE_EQ, BSE_EQ, NSE_INDEX, BSE_INDEX, etc.)

CREATE TABLE IF NOT EXISTS {database}.{table_name} (
    `Timestamp` DateTime('Asia/Kolkata'),
    `Symbol` LowCardinality(String) CODEC(ZSTD(1)),
    `Open` Float32 CODEC(Gorilla(4), ZSTD(1)),
    `High` Float32 CODEC(Gorilla(4), ZSTD(1)),
    `Low` Float32 CODEC(Gorilla(4), ZSTD(1)),
    `Close` Float32 CODEC(Gorilla(4), ZSTD(1)),
    `Volume` UInt32 DEFAULT 0 CODEC(T64, ZSTD(1)),
    `Open_Interest` UInt32 DEFAULT 0 CODEC(T64, ZSTD(1)),
    `Exchange` LowCardinality(String) CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(Timestamp)
ORDER BY (Symbol, Timestamp)
SETTINGS index_granularity = 8192;
