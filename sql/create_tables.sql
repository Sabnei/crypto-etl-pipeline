-- Run this script ONCE in Azure PostgreSQL to set up the schema
-- Connect using: psql "host=sabnei-postgres.postgres.database.azure.com port=5432 dbname=postgres user=portfolioadmin sslmode=require"
-- Or from pgAdmin connected to the Azure server

-- 1. Create the project schema
CREATE SCHEMA IF NOT EXISTS crypto;

-- 2. Create the table inside the schema
--    Next time you add a project, repeat this with another schema:
--    CREATE SCHEMA IF NOT EXISTS weather;
--    CREATE TABLE weather.observations (...);

CREATE TABLE IF NOT EXISTS crypto.crypto_prices (
    id               SERIAL PRIMARY KEY,
    coin_id          VARCHAR(150)   NOT NULL,
    symbol           VARCHAR(50)    NOT NULL,
    name             VARCHAR(150)   NOT NULL,
    price_usd        DECIMAL(20, 8) NOT NULL,
    market_cap_usd   BIGINT,
    volume_24h       BIGINT,
    change_pct_24h   DECIMAL(8, 4),
    last_updated_api TIMESTAMPTZ,
    extracted_at     TIMESTAMPTZ    DEFAULT NOW()
);

-- 3. Verify that everything was created correctly
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'crypto';