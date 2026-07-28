-- ============================================================
-- Schemas
-- ============================================================
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;


-- ============================================================
-- RAW LAYER: Collect raw API response
-- ============================================================
CREATE TABLE IF NOT EXISTS raw.api_response (
    id SERIAL PRIMARY KEY,
    fetched_at TIMESTAMP NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL DEFAULT 'frankfurter',
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_fetched_at
ON raw.api_response (fetched_at);


-- ============================================================
-- STAGING LAYER: flat, typed, deduplicated
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.stg_exchange_rate (
    source_date DATE NOT NULL,
    base_currency CHAR(3) NOT NULL,
    target_currency CHAR(3) NOT NULL,
    rate NUMERIC(12, 6) NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_date, base_currency, target_currency)
);


-- ============================================================
-- MART LAYER: Star schema — dimensional model
-- fact_exchange_rate (raw measure) + dim_date / dim_currency.
-- Flexible slicing; parallel to fx_daily by design (both from staging).
-- dim_currency uses natural key (ISO 4217 code) — immutable, single-source.
-- ============================================================
CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year_number INT NOT NULL,
    quarter_number INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    week_of_year INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.dim_currency (
    currency_code CHAR(3) PRIMARY KEY,
    currency_name VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.fact_exchange_rate (
    date_key INT NOT NULL REFERENCES mart.dim_date (date_key),
    base_currency CHAR(3) NOT NULL REFERENCES mart.dim_currency (currency_code),
    target_currency CHAR(3) NOT NULL REFERENCES mart.dim_currency (
        currency_code
    ),
    rate NUMERIC(12, 6) NOT NULL,
    refreshed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date_key, base_currency, target_currency)
);
