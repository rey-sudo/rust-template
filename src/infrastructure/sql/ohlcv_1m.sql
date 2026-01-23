\c service_ingest

CREATE TABLE IF NOT EXISTS ohlcv_1m (
    symbol TEXT NOT NULL,

    open_time BIGINT NOT NULL,   -- minute boundary (Unix ms)
    close_time BIGINT NOT NULL,  -- last tick timestamp in that minute

    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low  DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (symbol, open_time)
);


CREATE INDEX idx_ohlcv_1m_symbol_open_time_desc
ON ohlcv_1m (symbol, open_time DESC);

CREATE INDEX idx_ohlcv_1m_open_time
ON ohlcv_1m (open_time);

