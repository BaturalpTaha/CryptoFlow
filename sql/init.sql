-- ============================================================
-- CryptoFlow – PostgreSQL initialisation script
-- Runs on first boot via /docker-entrypoint-initdb.d/
-- POSTGRES_DB=cryptoflow, so we are already inside that DB.
-- ============================================================

-- Main time-series table
CREATE TABLE IF NOT EXISTS market_data (
    id              SERIAL PRIMARY KEY,
    symbol          VARCHAR(20)      NOT NULL,
    timestamp       TIMESTAMP        NOT NULL,
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    volume          DOUBLE PRECISION NOT NULL,
    sma_20          DOUBLE PRECISION,
    rsi_14          DOUBLE PRECISION,
    bb_high         DOUBLE PRECISION,
    bb_low          DOUBLE PRECISION,
    is_anomaly      BOOLEAN          NOT NULL DEFAULT FALSE,
    anomaly_reason  VARCHAR(512),
    UNIQUE (symbol, timestamp)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_md_symbol_time
    ON market_data (symbol, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_md_anomaly
    ON market_data (is_anomaly)
    WHERE is_anomaly = TRUE;

-- Convenience view: latest bar per symbol
CREATE OR REPLACE VIEW latest_prices AS
SELECT DISTINCT ON (symbol)
    symbol, timestamp, open, high, low, close, volume,
    sma_20, rsi_14, bb_high, bb_low
FROM market_data
ORDER BY symbol, timestamp DESC;
