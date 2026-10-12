-- Equity/Cash Table Schema Template for PostgreSQL/TimescaleDB
-- This template is used for all equity segments

CREATE TABLE IF NOT EXISTS {table_name} (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    open_interest DOUBLE PRECISION DEFAULT 0,
    exchange TEXT NOT NULL,
    lot_size INTEGER DEFAULT 1,
    tick_size DOUBLE PRECISION DEFAULT 0.01,
    PRIMARY KEY (timestamp, symbol)
);

-- TimescaleDB hypertable conversion (run after table creation)
-- SELECT create_hypertable('{table_name}', 'timestamp',
--     chunk_time_interval => INTERVAL '1 day',
--     if_not_exists => TRUE,
--     migrate_data => TRUE
-- );
