-- Create a new table 'crypto_prices' with a primary key and columns
CREATE TABLE crypto_prices (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(150) NOT NULL,
    symbol varchar(50) NOT NULL,
    name varchar(150) NOT NULL,
    price_usd DECIMAL(20, 8) NOT NULL,
    market_cap_usd BIGINT,
    volume_24h BIGINT,
    change_pct_24h DECIMAL(8, 4),
    last_updated_api TIMESTAMPTZ,
    extracted_at TIMESTAMPTZ DEFAULT NOW()
);