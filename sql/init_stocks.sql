-- Create a separate database for stock data alongside Airflow's metadata DB
-- Use psql's \gexec to avoid running CREATE DATABASE inside a transaction
SELECT 'CREATE DATABASE stocks'
WHERE NOT EXISTS (
  SELECT FROM pg_database WHERE datname = 'stocks'
)\gexec

\connect stocks

CREATE TABLE IF NOT EXISTS stock_prices (
    symbol TEXT NOT NULL,
    trading_day DATE NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    adjusted_close NUMERIC,
    volume BIGINT,
    PRIMARY KEY (symbol, trading_day)
);

CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol ON stock_prices(symbol);


