-- =========================================================
-- PORTFOLIO MANAGEMENT SYSTEM - COMPLETE SCHEMA + SEED DATA
-- PostgreSQL | React + FastAPI + psycopg2
--
-- This single file replaces sql/01 through sql/06 plus every ad-hoc
-- migration run afterward (column rename, issuer/classification
-- tables, historical-support columns). Run this against a fresh
-- database and the result matches the current live DB exactly.
--
-- After this file: run scripts/import_historical_prices.py against
-- your CSVs, then scripts/seed_demo_portfolios.py for the 3 demo
-- portfolios. Do NOT run sql/01-08 individually anymore - this file
-- supersedes them.
-- =========================================================

BEGIN;

-- =========================================================
-- SECTION 1: CORE TABLES
-- =========================================================

CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role VARCHAR(30) NOT NULL DEFAULT 'FUND_MANAGER',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS asset_classes (
    asset_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asset_code VARCHAR(30) NOT NULL UNIQUE,
    asset_class VARCHAR(80) NOT NULL,
    asset_description TEXT,
    sub_asset_class VARCHAR(100),
    sub_asset_description TEXT,
    risk VARCHAR(30),
    investment_horizon VARCHAR(30),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_asset_risk CHECK (
        risk IS NULL OR risk IN ('LOW', 'LOW_MODERATE', 'MEDIUM', 'MODERATE', 'HIGH', 'VERY_HIGH')
    )
);

CREATE TABLE IF NOT EXISTS investment_themes (
    theme_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    theme_name VARCHAR(80) NOT NULL UNIQUE,
    description TEXT,
    risk_level VARCHAR(30) NOT NULL,
    investment_horizon VARCHAR(30) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_theme_risk CHECK (
        risk_level IN ('LOW', 'LOW_MODERATE', 'MODERATE', 'HIGH', 'VERY_HIGH')
    )
);

CREATE TABLE IF NOT EXISTS theme_allocation (
    theme_id BIGINT NOT NULL REFERENCES investment_themes(theme_id),
    asset_id BIGINT NOT NULL REFERENCES asset_classes(asset_id),
    allocation_pct NUMERIC(5,2) NOT NULL,
    PRIMARY KEY (theme_id, asset_id),
    CONSTRAINT chk_theme_allocation_pct CHECK (allocation_pct BETWEEN 0 AND 100)
);

-- ---------------------------------------------------------
-- Issuers + GICS classification (added after initial build).
-- Must exist before `securities`, which references issuer_id.
-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS issuers (
    issuer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    issuer_name VARCHAR(250) NOT NULL,
    country VARCHAR(80) NOT NULL DEFAULT 'India',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_issuer_name_country UNIQUE (issuer_name, country)
);

CREATE TABLE IF NOT EXISTS issuer_gics_classifications (
    classification_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    issuer_id BIGINT NOT NULL REFERENCES issuers(issuer_id),
    gics_sector_code VARCHAR(10) NOT NULL,
    gics_industry_group_code VARCHAR(10) NOT NULL,
    gics_industry_code VARCHAR(10) NOT NULL,
    gics_sub_industry_code VARCHAR(10) NOT NULL,
    source VARCHAR(500) NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_gics_effective_dates CHECK (effective_to IS NULL OR effective_to >= effective_from),
    CONSTRAINT uq_issuer_gics_effective_from UNIQUE (issuer_id, effective_from)
);

CREATE INDEX IF NOT EXISTS idx_issuer_gics_history ON issuer_gics_classifications (issuer_id, effective_from DESC);

-- ---------------------------------------------------------
-- Securities master (issuer_id nullable - only company equities
-- ever get one; ETFs/REITs/Gold/Debt never do, by design).
-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS securities (
    security_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    exchange VARCHAR(10) NOT NULL DEFAULT 'NSE',
    symbol VARCHAR(40) NOT NULL,
    security_series VARCHAR(20),
    security_code VARCHAR(50),
    isin VARCHAR(20),
    security_name VARCHAR(250) NOT NULL,
    asset_id BIGINT NOT NULL REFERENCES asset_classes(asset_id),
    issuer_id BIGINT REFERENCES issuers(issuer_id),
    sector VARCHAR(120),
    industry VARCHAR(150),
    equity_category VARCHAR(30),
    country VARCHAR(80) NOT NULL DEFAULT 'India',
    currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    last_price NUMERIC(20,6),
    last_price_date DATE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_security_exchange_symbol UNIQUE (exchange, symbol),
    CONSTRAINT chk_security_exchange CHECK (exchange IN ('NSE', 'BSE')),
    CONSTRAINT chk_security_equity_category CHECK (
        equity_category IS NULL OR equity_category IN ('LARGE_CAP', 'MID_CAP', 'SMALL_CAP')
    ),
    CONSTRAINT chk_security_price CHECK (last_price IS NULL OR last_price >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_securities_exchange_isin
    ON securities (exchange, isin) WHERE isin IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_securities_issuer_id
    ON securities (issuer_id) WHERE issuer_id IS NOT NULL;

-- ---------------------------------------------------------
-- Portfolios (inception_date included from the start - it's what
-- lets a portfolio be backdated for a real multi-year backtest).
-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS portfolios (
    portfolio_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(user_id),
    portfolio_name VARCHAR(150) NOT NULL,
    portfolio_type VARCHAR(20) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    exchange VARCHAR(10) NOT NULL DEFAULT 'NSE',
    benchmark_symbol VARCHAR(40) NOT NULL DEFAULT '^NSEI',
    theme_id BIGINT NOT NULL REFERENCES investment_themes(theme_id),
    rebalancing_frequency VARCHAR(20) NOT NULL,
    initial_investment NUMERIC(20,2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'NEW',
    inception_date DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at TIMESTAMPTZ,
    CONSTRAINT chk_portfolio_type CHECK (portfolio_type IN ('WEIGHTAGE', 'AMOUNT')),
    CONSTRAINT chk_portfolio_exchange CHECK (exchange IN ('NSE', 'BSE')),
    CONSTRAINT chk_rebalancing_frequency CHECK (
        rebalancing_frequency IN ('DAILY','MONTHLY','QUARTERLY','HALF_YEARLY','YEARLY')
    ),
    CONSTRAINT chk_portfolio_investment CHECK (initial_investment > 0),
    CONSTRAINT chk_portfolio_status CHECK (status IN ('NEW', 'ACTIVE', 'CLOSED'))
);

-- ---------------------------------------------------------
-- Holdings (as_of_date included from the start, alongside the
-- optional per-security target_weight_pct/target_amount your ad-hoc
-- SQL populated for the demo portfolios).
-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS holdings (
    holding_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    portfolio_id BIGINT NOT NULL REFERENCES portfolios(portfolio_id),
    security_id BIGINT NOT NULL REFERENCES securities(security_id),
    quantity NUMERIC(24,8) NOT NULL,
    avg_price NUMERIC(20,6) NOT NULL,
    target_weight_pct NUMERIC(7,4),
    target_amount NUMERIC(20,2),
    as_of_date DATE NOT NULL DEFAULT CURRENT_DATE,
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_portfolio_security UNIQUE (portfolio_id, security_id),
    CONSTRAINT chk_holding_quantity CHECK (quantity > 0),
    CONSTRAINT chk_holding_avg_price CHECK (avg_price >= 0),
    CONSTRAINT chk_holding_target_weight CHECK (target_weight_pct IS NULL OR target_weight_pct BETWEEN 0 AND 100),
    CONSTRAINT chk_holding_target_amount CHECK (target_amount IS NULL OR target_amount >= 0)
);

CREATE TABLE IF NOT EXISTS transactions (
    txn_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    portfolio_id BIGINT NOT NULL REFERENCES portfolios(portfolio_id),
    security_id BIGINT NOT NULL REFERENCES securities(security_id),
    action VARCHAR(10) NOT NULL,
    quantity NUMERIC(24,8) NOT NULL,
    price NUMERIC(20,6) NOT NULL,
    txn_date DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_transaction_action CHECK (action IN ('BUY', 'SELL')),
    CONSTRAINT chk_transaction_quantity CHECK (quantity > 0),
    CONSTRAINT chk_transaction_price CHECK (price >= 0)
);

-- ---------------------------------------------------------
-- Price history (close_price - NOT adjusted_close: this is raw
-- NSE close, never split/dividend-adjusted).
-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS price_history (
    security_id BIGINT NOT NULL REFERENCES securities(security_id),
    price_date DATE NOT NULL,
    close_price NUMERIC(20,6) NOT NULL,
    source VARCHAR(50),
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (security_id, price_date),
    CONSTRAINT chk_close_price CHECK (close_price >= 0)
);

CREATE TABLE IF NOT EXISTS benchmark_history (
    benchmark_symbol VARCHAR(40) NOT NULL,
    price_date DATE NOT NULL,
    close_price NUMERIC(20,6) NOT NULL,
    source VARCHAR(50),
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (benchmark_symbol, price_date),
    CONSTRAINT chk_benchmark_price CHECK (close_price >= 0)
);

CREATE TABLE IF NOT EXISTS data_import_runs (
    import_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_name VARCHAR(80) NOT NULL,
    source_name VARCHAR(100),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    rows_read BIGINT NOT NULL DEFAULT 0,
    rows_inserted BIGINT NOT NULL DEFAULT 0,
    rows_updated BIGINT NOT NULL DEFAULT 0,
    rows_rejected BIGINT NOT NULL DEFAULT 0,
    error_message TEXT,
    CONSTRAINT chk_import_status CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED'))
);

-- =========================================================
-- SECTION 2: INDEXES
-- =========================================================

CREATE INDEX IF NOT EXISTS idx_portfolios_user_status ON portfolios (user_id, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_holdings_portfolio ON holdings (portfolio_id, holding_id);
CREATE INDEX IF NOT EXISTS idx_securities_asset ON securities (asset_id, is_active, symbol);
CREATE INDEX IF NOT EXISTS idx_securities_sector ON securities (sector, industry);
CREATE INDEX IF NOT EXISTS idx_securities_name ON securities (security_name);
CREATE INDEX IF NOT EXISTS idx_price_history_date ON price_history (price_date, security_id);
CREATE INDEX IF NOT EXISTS idx_transactions_portfolio_date ON transactions (portfolio_id, txn_date DESC);
CREATE INDEX IF NOT EXISTS idx_import_runs_started ON data_import_runs (started_at DESC);

-- =========================================================
-- SECTION 3: REFERENCE DATA - demo user, asset classes, themes
-- =========================================================

INSERT INTO users (name, email, password_hash, role)
VALUES (
    'Demo Fund Manager',
    'manager@pms.local',
    -- bcrypt hash placeholder - regenerate with:
    -- python -c "from app.common.security_utils import hash_password; print(hash_password('yourpassword'))"
    '$2b$12$KIXQx7z5m0rF3nQhF8T2UeYQZ1B9r6f8yQ2yLh3M8kQnF4wQe1S9O',
    'FUND_MANAGER'
)
ON CONFLICT (email) DO NOTHING;

INSERT INTO asset_classes (asset_code, asset_class, asset_description, sub_asset_class, risk, investment_horizon)
VALUES
('EQUITY',    'Equity',            'Individual listed equity securities',    'Listed Stocks', 'HIGH',   'LONG'),
('BROAD_ETF', 'Broad Market ETF',  'Broad-market exchange-traded funds',     'Index ETF',     'MEDIUM', 'LONG'),
('GOLD',      'Gold',              'Gold-linked exchange-traded instruments','Gold ETF',      'LOW',    'LONG'),
('REIT',      'Real Estate',       'Listed real estate investment trusts',   'REIT',          'MEDIUM', 'LONG'),
('DEBT',      'Debt',              'Debt and fixed-income instruments',      'Debt ETF',      'LOW',    'SHORT')
ON CONFLICT (asset_code) DO NOTHING;

INSERT INTO investment_themes (theme_name, description, risk_level, investment_horizon)
VALUES
('Conservative',             'Lower equity exposure with debt and diversified assets', 'LOW',          'LONG'),
('Moderately Conservative',  'Moderate equity exposure with diversified holdings',     'LOW_MODERATE', 'LONG'),
('Moderately Aggressive',    'Growth-oriented portfolio with diversified exposure',    'MODERATE',     'LONG'),
('Aggressive',               'High equity exposure with alternative asset allocation', 'HIGH',         'LONG'),
('Very Aggressive',          'Very high equity exposure',                              'VERY_HIGH',    'LONG')
ON CONFLICT (theme_name) DO NOTHING;

WITH allocation_seed (theme_name, asset_code, allocation_pct) AS (
    VALUES
    ('Conservative', 'EQUITY', 20.00::NUMERIC), ('Conservative', 'BROAD_ETF', 40.00::NUMERIC),
    ('Conservative', 'GOLD', 0.00::NUMERIC), ('Conservative', 'REIT', 20.00::NUMERIC), ('Conservative', 'DEBT', 20.00::NUMERIC),

    ('Moderately Conservative', 'EQUITY', 40.00::NUMERIC), ('Moderately Conservative', 'BROAD_ETF', 20.00::NUMERIC),
    ('Moderately Conservative', 'GOLD', 5.00::NUMERIC), ('Moderately Conservative', 'REIT', 20.00::NUMERIC), ('Moderately Conservative', 'DEBT', 15.00::NUMERIC),

    ('Moderately Aggressive', 'EQUITY', 60.00::NUMERIC), ('Moderately Aggressive', 'BROAD_ETF', 20.00::NUMERIC),
    ('Moderately Aggressive', 'GOLD', 5.00::NUMERIC), ('Moderately Aggressive', 'REIT', 10.00::NUMERIC), ('Moderately Aggressive', 'DEBT', 5.00::NUMERIC),

    ('Aggressive', 'EQUITY', 70.00::NUMERIC), ('Aggressive', 'BROAD_ETF', 0.00::NUMERIC),
    ('Aggressive', 'GOLD', 10.00::NUMERIC), ('Aggressive', 'REIT', 10.00::NUMERIC), ('Aggressive', 'DEBT', 10.00::NUMERIC),

    ('Very Aggressive', 'EQUITY', 90.00::NUMERIC), ('Very Aggressive', 'BROAD_ETF', 0.00::NUMERIC),
    ('Very Aggressive', 'GOLD', 5.00::NUMERIC), ('Very Aggressive', 'REIT', 5.00::NUMERIC), ('Very Aggressive', 'DEBT', 0.00::NUMERIC)
)
INSERT INTO theme_allocation (theme_id, asset_id, allocation_pct)
SELECT t.theme_id, a.asset_id, s.allocation_pct
FROM allocation_seed s
JOIN investment_themes t ON t.theme_name = s.theme_name
JOIN asset_classes a ON a.asset_code = s.asset_code
ON CONFLICT (theme_id, asset_id) DO UPDATE SET allocation_pct = EXCLUDED.allocation_pct;

-- =========================================================
-- SECTION 4: SECURITIES MASTER - full 22-instrument universe
-- (original 19 + ITBEES/EBBETF0430/MINDSPACE added later)
-- =========================================================

INSERT INTO securities (exchange, symbol, isin, security_name, asset_id, sector, industry, equity_category, country, currency)
SELECT 'NSE', v.symbol, v.isin, v.name, a.asset_id, v.sector, v.industry, v.equity_category, 'India', 'INR'
FROM (VALUES
    ('RELIANCE',  'INE002A01018', 'Reliance Industries Ltd',            'Energy',     'Oil & Gas',         'LARGE_CAP'),
    ('HDFCBANK',  'INE040A01034', 'HDFC Bank Ltd',                      'Financials', 'Banking',           'LARGE_CAP'),
    ('ICICIBANK', 'INE090A01021', 'ICICI Bank Ltd',                     'Financials', 'Banking',           'LARGE_CAP'),
    ('INFY',      'INE009A01021', 'Infosys Ltd',                        'IT',         'IT Services',       'LARGE_CAP'),
    ('TCS',       'INE467B01029', 'Tata Consultancy Services Ltd',      'IT',         'IT Services',       'LARGE_CAP'),
    ('BHARTIARTL','INE397D01024', 'Bharti Airtel Ltd',                  'Telecom',    'Telecom Services',  'LARGE_CAP'),
    ('LT',        'INE018A01030', 'Larsen & Toubro Ltd',                'Industrials','Construction',      'LARGE_CAP'),
    ('ITC',       'INE154A01025', 'ITC Ltd',                            'Consumer',   'FMCG',              'LARGE_CAP'),
    ('SBIN',      'INE062A01020', 'State Bank of India',                'Financials', 'Banking',           'LARGE_CAP'),
    ('AXISBANK',  'INE238A01034', 'Axis Bank Ltd',                      'Financials', 'Banking',           'LARGE_CAP'),
    ('KOTAKBANK', 'INE237A01028', 'Kotak Mahindra Bank Ltd',            'Financials', 'Banking',           'LARGE_CAP'),
    ('MARUTI',    'INE585B01010', 'Maruti Suzuki India Ltd',            'Consumer',   'Automobile',        'LARGE_CAP'),
    ('SUNPHARMA', 'INE044A01036', 'Sun Pharmaceutical Industries Ltd',  'Healthcare', 'Pharmaceuticals',   'LARGE_CAP'),
    ('HCLTECH',   'INE860A01027', 'HCL Technologies Ltd',               'IT',         'IT Services',       'LARGE_CAP'),
    ('TITAN',     'INE280A01028', 'Titan Company Ltd',                  'Consumer',   'Consumer Durables', 'LARGE_CAP')
) AS v(symbol, isin, name, sector, industry, equity_category)
JOIN asset_classes a ON a.asset_code = 'EQUITY'
ON CONFLICT (exchange, symbol) DO NOTHING;

INSERT INTO securities (exchange, symbol, security_name, asset_id, sector, country, currency)
SELECT 'NSE', v.symbol, v.name, a.asset_id, v.sector, 'India', 'INR'
FROM (VALUES
    ('NIFTYBEES',   'Nippon India ETF Nifty BeES',        'BROAD_ETF', 'Diversified'),
    ('GOLDBEES',    'Nippon India ETF Gold BeES',         'GOLD',      'Commodities'),
    ('LIQUIDBEES',  'Nippon India ETF Liquid BeES',       'DEBT',      'Money Market'),
    ('EMBASSY',     'Embassy Office Parks REIT',          'REIT',      'Real Estate'),
    ('ITBEES',      'ITBEES',                             'BROAD_ETF', 'Sector - IT'),
    ('EBBETF0430',  'EBBETF0430',                         'DEBT',      'Target Maturity Bond'),
    ('MINDSPACE',   'Mindspace Business Parks REIT',      'REIT',      'Real Estate')
) AS v(symbol, name, asset_code, sector)
JOIN asset_classes a ON a.asset_code = v.asset_code
ON CONFLICT (exchange, symbol) DO NOTHING;

COMMIT;

-- =========================================================
-- SECTION 5: RUN AFTER price_history IS POPULATED
-- (via scripts/import_historical_prices.py) - NOT part of the
-- initial bootstrap, safe to run any time after an import.
-- =========================================================

-- UPDATE securities s
-- SET last_price = ph.close_price, last_price_date = ph.price_date
-- FROM (
--     SELECT DISTINCT ON (security_id) security_id, close_price, price_date
--     FROM price_history
--     ORDER BY security_id, price_date DESC
-- ) ph
-- WHERE ph.security_id = s.security_id;



--------abi doing routing-----


SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'users'
  AND table_schema = 'public'
ORDER BY ordinal_position;
SELECT *
FROM public.users
LIMIT 10;

SELECT user_id, name, email, role, is_active
FROM public.users
WHERE email = 'manager@pms.local';

BEGIN;

UPDATE public.users
SET password_hash = '$2b$12$V0Do5RqgyJfwEdge5sAJvOkjgJNI5R2gyKSbj1hD4ca9fEHdlgbgO'
WHERE email = 'manager@pms.local'
RETURNING user_id, email, is_active;

COMMIT;


UPDATE public.users
SET email = 'manager@pmsdemo.in'
WHERE email = 'manager@pms.local'
RETURNING user_id, name, email, role, is_active;



SELECT
    s.security_id,
    s.symbol,
    s.last_price AS stored_last_price,
    latest.price_date AS latest_price_date,
    latest.close_price AS latest_close_price
FROM securities s
LEFT JOIN LATERAL (
    SELECT
        ph.price_date,
        ph.close_price
    FROM price_history ph
    WHERE ph.security_id = s.security_id
    ORDER BY ph.price_date DESC
    LIMIT 1
) latest ON TRUE
ORDER BY s.symbol;

BEGIN;

UPDATE securities AS s
SET last_price = latest.close_price
FROM (
    SELECT DISTINCT ON (security_id)
        security_id,
        close_price
    FROM price_history
    ORDER BY security_id, price_date DESC
) AS latest
WHERE s.security_id = latest.security_id;

COMMIT;

SELECT
    security_id,
    symbol,
    last_price
FROM securities
ORDER BY symbol;



SELECT
    s.symbol,
    s.last_price,
    p.close_price AS latest_close,
    p.price_date AS latest_price_date,
    s.last_price = p.close_price AS price_matches
FROM public.securities AS s
JOIN LATERAL (
    SELECT close_price, price_date
    FROM public.price_history
    WHERE security_id = s.security_id
    ORDER BY price_date DESC
    LIMIT 1
) AS p ON TRUE
WHERE s.is_active = TRUE
ORDER BY s.symbol;


SELECT
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN (
      'portfolios',
      'holdings',
      'transactions',
      'securities',
      'price_history',
      'benchmark_history',
      'issuers',
      'issuer_gics_classifications',
      'asset_classes',
      'investment_themes',
      'theme_allocation',
      'data_import_runs'
  )
ORDER BY table_name, ordinal_position;

SELECT
    s.symbol,
    s.issuer_id,
    ac.asset_class,
    i.issuer_name,
    gc.gics_sector_code,
    gc.effective_from,
    gc.effective_to,
    CASE
        WHEN s.issuer_id IS NULL
            THEN 'Missing issuer mapping'
        WHEN gc.classification_id IS NULL
            THEN 'No currently effective GICS classification'
        ELSE 'Classification found'
    END AS classification_status
FROM holdings h
JOIN securities s
    ON s.security_id = h.security_id
JOIN asset_classes ac
    ON ac.asset_id = s.asset_id
LEFT JOIN issuers i
    ON i.issuer_id = s.issuer_id
LEFT JOIN issuer_gics_classifications gc
    ON gc.issuer_id = s.issuer_id
   AND gc.effective_from <= CURRENT_DATE
   AND (
       gc.effective_to IS NULL
       OR gc.effective_to >= CURRENT_DATE
   )
WHERE h.portfolio_id = 1
  AND ac.asset_class = 'Equity'
ORDER BY s.symbol, gc.effective_from DESC;

SELECT
    security_id,
    symbol,
    issuer_id,
    asset_id
FROM securities
WHERE symbol = 'RELIANCE';

SELECT *
FROM issuers
ORDER BY issuer_id
LIMIT 20;

BEGIN;

WITH new_issuer AS (
    INSERT INTO issuers (issuer_name, country)
    VALUES ('Reliance Industries Limited', 'India')
    RETURNING issuer_id
)
UPDATE securities AS s
SET issuer_id = ni.issuer_id
FROM new_issuer AS ni
WHERE s.security_id = 1
  AND s.symbol = 'RELIANCE';

COMMIT;

SELECT
    s.security_id,
    s.symbol,
    s.issuer_id,
    i.issuer_name,
    i.country
FROM securities AS s
LEFT JOIN issuers AS i
    ON i.issuer_id = s.issuer_id
WHERE s.symbol = 'RELIANCE';

BEGIN;

INSERT INTO issuer_gics_classifications (
    issuer_id,
    gics_sector_code,
    effective_from,
    effective_to
)
VALUES (
    1,
    '10',
    CURRENT_DATE,
    NULL
);

COMMIT;
SELECT *
FROM issuer_gics_classifications
WHERE issuer_id = 1;

INSERT INTO issuer_gics_classifications (
    issuer_id,
    gics_sector_code,
    gics_industry_group_code,
    gics_industry_code,
    gics_sub_industry_code,
    source,
    effective_from,
    effective_to
)
SELECT
    1,
    '10',
    '1010',
    '101020',
    '10102010',
    'Manual',
    CURRENT_DATE,
    NULL
WHERE NOT EXISTS (
    SELECT 1
    FROM issuer_gics_classifications
    WHERE issuer_id = 1
      AND effective_from <= CURRENT_DATE
      AND (
          effective_to IS NULL
          OR effective_to >= CURRENT_DATE
      )
);
ROLLBACK;

SELECT
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'issuer_gics_classifications'
ORDER BY ordinal_position;


INSERT INTO issuer_gics_classifications (
    issuer_id,
    gics_sector_code,
    gics_industry_group_code,
    gics_industry_code,
    gics_sub_industry_code,
    source,
    effective_from,
    effective_to
)
SELECT
    1,
    '10',
    '1010',
    '101020',
    '10102010',
    'Manual',
    CURRENT_DATE,
    NULL
WHERE NOT EXISTS (
    SELECT 1
    FROM issuer_gics_classifications
    WHERE issuer_id = 1
      AND effective_from <= CURRENT_DATE
      AND (
          effective_to IS NULL
          OR effective_to >= CURRENT_DATE
      )
);

SELECT
    issuer_id,
    gics_sector_code,
    gics_industry_group_code,
    gics_industry_code,
    gics_sub_industry_code,
    source,
    effective_from,
    effective_to
FROM issuer_gics_classifications
WHERE issuer_id = 1;

UPDATE issuer_gics_classifications
SET effective_from = DATE '2022-10-03'
WHERE issuer_id = 1
  AND gics_sector_code = '10'
  AND source = 'Manual';

SELECT
    issuer_id,
    gics_sector_code,
    effective_from,
    effective_to
FROM issuer_gics_classifications
WHERE issuer_id = 1;

SELECT DISTINCT
    s.security_id,
    s.symbol,
    ac.asset_class,
    s.issuer_id,
    i.issuer_name,
    gc.gics_sector_code,
    gc.gics_industry_group_code,
    gc.gics_industry_code,
    gc.gics_sub_industry_code,
    gc.effective_from,
    gc.effective_to,
    CASE
        WHEN ac.asset_class <> 'Equity'
            THEN 'GICS sector mapping not required for this asset'
        WHEN s.issuer_id IS NULL
            THEN 'Missing issuer mapping'
        WHEN gc.classification_id IS NULL
            THEN 'Missing current GICS classification'
        ELSE 'Mapped'
    END AS mapping_status
FROM holdings h
JOIN securities s
    ON s.security_id = h.security_id
JOIN asset_classes ac
    ON ac.asset_id = s.asset_id
LEFT JOIN issuers i
    ON i.issuer_id = s.issuer_id
LEFT JOIN LATERAL (
    SELECT
        c.classification_id,
        c.gics_sector_code,
        c.gics_industry_group_code,
        c.gics_industry_code,
        c.gics_sub_industry_code,
        c.effective_from,
        c.effective_to
    FROM issuer_gics_classifications c
    WHERE c.issuer_id = s.issuer_id
      AND c.effective_from <= CURRENT_DATE
      AND (
          c.effective_to IS NULL
          OR c.effective_to >= CURRENT_DATE
      )
    ORDER BY c.effective_from DESC
    LIMIT 1
) gc ON TRUE
WHERE h.portfolio_id = 1
ORDER BY ac.asset_class, s.symbol;

SELECT
    table_name,
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN (
      'securities',
      'asset_classes',
      'issuers',
      'issuer_gics_classifications'
  )
ORDER BY table_name, ordinal_position;

SELECT
    s.security_id,
    s.symbol,
    s.security_name,
    s.exchange,
    s.asset_id,
    ac.asset_class,
    s.issuer_id,
    i.issuer_name,

    g.gics_sector_code,
    g.gics_industry_group_code,
    g.gics_industry_code,
    g.gics_sub_industry_code,
    g.source,
    g.effective_from,
    g.effective_to,

    CASE
        WHEN s.issuer_id IS NULL
            THEN 'Missing issuer link'

        WHEN i.issuer_id IS NULL
            THEN 'Issuer not found'

        WHEN g.classification_id IS NULL
            THEN 'Missing current GICS'

        WHEN g.gics_sector_code IS NULL
          OR g.gics_industry_group_code IS NULL
          OR g.gics_industry_code IS NULL
          OR g.gics_sub_industry_code IS NULL
            THEN 'Incomplete GICS'

        ELSE 'Mapped'
    END AS gics_status

FROM securities s

JOIN asset_classes ac
    ON ac.asset_id = s.asset_id

LEFT JOIN issuers i
    ON i.issuer_id = s.issuer_id

LEFT JOIN LATERAL (
    SELECT
        c.classification_id,
        c.gics_sector_code,
        c.gics_industry_group_code,
        c.gics_industry_code,
        c.gics_sub_industry_code,
        c.source,
        c.effective_from,
        c.effective_to
    FROM issuer_gics_classifications c
    WHERE c.issuer_id = s.issuer_id
      AND c.effective_from <= CURRENT_DATE
      AND (
          c.effective_to IS NULL
          OR c.effective_to >= CURRENT_DATE
      )
    ORDER BY c.effective_from DESC,
             c.classification_id DESC
    LIMIT 1
) g ON TRUE

WHERE s.is_active = TRUE
  AND LOWER(TRIM(ac.asset_class)) = 'equity'

ORDER BY
    gics_status,
    s.symbol;

SELECT
    issuer_id,
    issuer_name,
    country
FROM issuers
WHERE LOWER(TRIM(issuer_name)) IN (
    LOWER('Axis Bank Ltd'),
    LOWER('Bharti Airtel Ltd'),
    LOWER('HCL Technologies Ltd'),
    LOWER('HDFC Bank Ltd'),
    LOWER('ICICI Bank Ltd'),
    LOWER('Infosys Ltd'),
    LOWER('ITC Ltd'),
    LOWER('Kotak Mahindra Bank Ltd'),
    LOWER('Larsen & Toubro Ltd'),
    LOWER('Maruti Suzuki India Ltd'),
    LOWER('State Bank of India'),
    LOWER('Sun Pharmaceutical Industries Ltd'),
    LOWER('Tata Consultancy Services Ltd'),
    LOWER('Titan Company Ltd')
)
ORDER BY issuer_name;


BEGIN;

-- 1. Define the issuer records we need.
WITH issuer_data (symbol, issuer_name) AS (
    VALUES
        ('AXISBANK',   'Axis Bank Ltd'),
        ('BHARTIARTL', 'Bharti Airtel Ltd'),
        ('HCLTECH',    'HCL Technologies Ltd'),
        ('HDFCBANK',   'HDFC Bank Ltd'),
        ('ICICIBANK',  'ICICI Bank Ltd'),
        ('INFY',       'Infosys Ltd'),
        ('ITC',        'ITC Ltd'),
        ('KOTAKBANK',  'Kotak Mahindra Bank Ltd'),
        ('LT',         'Larsen & Toubro Ltd'),
        ('MARUTI',     'Maruti Suzuki India Ltd'),
        ('SBIN',       'State Bank of India'),
        ('SUNPHARMA',  'Sun Pharmaceutical Industries Ltd'),
        ('TCS',        'Tata Consultancy Services Ltd'),
        ('TITAN',      'Titan Company Ltd')
)
INSERT INTO issuers (issuer_name, country)
SELECT d.issuer_name, 'India'
FROM issuer_data d
WHERE NOT EXISTS (
    SELECT 1
    FROM issuers i
    WHERE LOWER(TRIM(i.issuer_name))
        = LOWER(TRIM(d.issuer_name))
);

-- 2. Link each equity to its issuer.
WITH issuer_data (symbol, issuer_name) AS (
    VALUES
        ('AXISBANK',   'Axis Bank Ltd'),
        ('BHARTIARTL', 'Bharti Airtel Ltd'),
        ('HCLTECH',    'HCL Technologies Ltd'),
        ('HDFCBANK',   'HDFC Bank Ltd'),
        ('ICICIBANK',  'ICICI Bank Ltd'),
        ('INFY',       'Infosys Ltd'),
        ('ITC',        'ITC Ltd'),
        ('KOTAKBANK',  'Kotak Mahindra Bank Ltd'),
        ('LT',         'Larsen & Toubro Ltd'),
        ('MARUTI',     'Maruti Suzuki India Ltd'),
        ('SBIN',       'State Bank of India'),
        ('SUNPHARMA',  'Sun Pharmaceutical Industries Ltd'),
        ('TCS',        'Tata Consultancy Services Ltd'),
        ('TITAN',      'Titan Company Ltd')
)
UPDATE securities s
SET issuer_id = i.issuer_id
FROM issuer_data d
JOIN issuers i
  ON LOWER(TRIM(i.issuer_name))
   = LOWER(TRIM(d.issuer_name))
WHERE s.symbol = d.symbol
  AND s.issuer_id IS NULL;

-- 3. Verify all 14 links before committing.
SELECT
    s.symbol,
    s.security_id,
    s.issuer_id,
    i.issuer_name,
    i.country,
    CASE
        WHEN i.issuer_id IS NOT NULL THEN 'Issuer linked'
        ELSE 'MISSING ISSUER LINK'
    END AS status
FROM securities s
LEFT JOIN issuers i
    ON i.issuer_id = s.issuer_id
WHERE s.symbol IN (
    'AXISBANK', 'BHARTIARTL', 'HCLTECH', 'HDFCBANK',
    'ICICIBANK', 'INFY', 'ITC', 'KOTAKBANK',
    'LT', 'MARUTI', 'SBIN', 'SUNPHARMA', 'TCS', 'TITAN'
)
ORDER BY s.symbol;

COMMIT;

SELECT
    s.symbol,
    s.issuer_id,
    i.issuer_name,
    CASE
        WHEN s.issuer_id IS NOT NULL
         AND i.issuer_id IS NOT NULL
        THEN 'Linked'
        ELSE 'Missing link'
    END AS status
FROM securities s
LEFT JOIN issuers i
    ON i.issuer_id = s.issuer_id
WHERE s.symbol IN (
    'AXISBANK', 'BHARTIARTL', 'HCLTECH', 'HDFCBANK',
    'ICICIBANK', 'INFY', 'ITC', 'KOTAKBANK',
    'LT', 'MARUTI', 'SBIN', 'SUNPHARMA', 'TCS', 'TITAN'
)
ORDER BY s.symbol;


BEGIN;

WITH gics_data (
    issuer_id,
    symbol,
    sector_code,
    industry_group_code,
    industry_code,
    sub_industry_code
) AS (
    VALUES
        (8,  'AXISBANK',   '40', '4010', '401010', '40101010'),
        (10, 'BHARTIARTL', '50', '5010', '501020', '50102010'),
        (12, 'HCLTECH',    '45', '4510', '451020', '45102010'),
        (13, 'HDFCBANK',   '40', '4010', '401010', '40101010'),
        (4,  'ICICIBANK',  '40', '4010', '401010', '40101010'),
        (5,  'INFY',       '45', '4510', '451020', '45102010'),
        (7,  'ITC',        '30', '3020', '302030', '30203010'),
        (14, 'KOTAKBANK',  '40', '4010', '401010', '40101010'),
        (3,  'LT',         '20', '2010', '201030', '20103010'),
        (15, 'MARUTI',     '25', '2510', '251020', '25102010'),
        (11, 'SBIN',       '40', '4010', '401010', '40101010'),
        (6,  'SUNPHARMA',  '35', '3520', '352020', '35202010'),
        (2,  'TCS',        '45', '4510', '451020', '45102010'),
        (9,  'TITAN',      '25', '2520', '252030', '25203010')
)
INSERT INTO issuer_gics_classifications (
    issuer_id,
    gics_sector_code,
    gics_industry_group_code,
    gics_industry_code,
    gics_sub_industry_code,
    source,
    effective_from,
    effective_to
)
SELECT
    d.issuer_id,
    d.sector_code,
    d.industry_group_code,
    d.industry_code,
    d.sub_industry_code,
    'Manual - demo mapping',
    DATE '2022-10-03',
    NULL
FROM gics_data d
WHERE NOT EXISTS (
    SELECT 1
    FROM issuer_gics_classifications c
    WHERE c.issuer_id = d.issuer_id
      AND c.effective_from <= CURRENT_DATE
      AND (
          c.effective_to IS NULL
          OR c.effective_to >= CURRENT_DATE
      )
);

COMMIT;

SELECT
    s.symbol,
    i.issuer_name,
    c.gics_sector_code,
    c.gics_industry_group_code,
    c.gics_industry_code,
    c.gics_sub_industry_code,
    c.source,
    c.effective_from,
    CASE
        WHEN c.classification_id IS NULL
            THEN 'MISSING GICS'
        WHEN c.gics_sector_code IS NULL
          OR c.gics_industry_group_code IS NULL
          OR c.gics_industry_code IS NULL
          OR c.gics_sub_industry_code IS NULL
            THEN 'INCOMPLETE GICS'
        ELSE 'MAPPED'
    END AS gics_status
FROM securities s
JOIN asset_classes ac
    ON ac.asset_id = s.asset_id
LEFT JOIN issuers i
    ON i.issuer_id = s.issuer_id
LEFT JOIN LATERAL (
    SELECT c.*
    FROM issuer_gics_classifications c
    WHERE c.issuer_id = s.issuer_id
      AND c.effective_from <= CURRENT_DATE
      AND (
          c.effective_to IS NULL
          OR c.effective_to >= CURRENT_DATE
      )
    ORDER BY c.effective_from DESC,
             c.classification_id DESC
    LIMIT 1
) c ON TRUE
WHERE LOWER(TRIM(ac.asset_class)) = 'equity'
  AND s.is_active = TRUE
ORDER BY s.symbol;