-- F&O Table Schema Template for PostgreSQL/TimescaleDB
-- This template is used for all F&O segments

CREATE TABLE IF NOT EXISTS {table_name} (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    underlying TEXT NOT NULL,
    expiry DATE NOT NULL,
    strike DOUBLE PRECISION NOT NULL,
    instrument_type TEXT NOT NULL,  -- 'CE', 'PE', 'FUT'
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    open_interest DOUBLE PRECISION DEFAULT 0,
    exchange TEXT NOT NULL,
    strike_price DOUBLE PRECISION GENERATED ALWAYS AS (strike) STORED,
    lot_size INTEGER DEFAULT 0,
    tick_size DOUBLE PRECISION DEFAULT 0,
    PRIMARY KEY (timestamp, symbol, underlying, expiry, strike, instrument_type)
);

-- TimescaleDB hypertable conversion (run after table creation)
-- SELECT create_hypertable('{table_name}', 'timestamp', 
--     chunk_time_interval => INTERVAL '1 day',
--     if_not_exists => TRUE,
--     migrate_data => TRUE
-- );

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_{table_name}_underlying ON {table_name} (underlying);
CREATE INDEX IF NOT EXISTS idx_{table_name}_expiry ON {table_name} (expiry);
CREATE INDEX IF NOT EXISTS idx_{table_name}_instrument_type ON {table_name} (instrument_type);
