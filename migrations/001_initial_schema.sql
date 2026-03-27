-- Supabase Migration: Initial schema for NIFTY Options Scraper
-- Replaces SQLite with normalized Postgres schema

-- =============================================================
-- Reference Tables
-- =============================================================

CREATE TABLE IF NOT EXISTS expiries (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    expiry_date DATE NOT NULL,
    expiry_flag TEXT NOT NULL CHECK (expiry_flag IN ('MONTH', 'WEEK')),
    lot_size INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(symbol, expiry_date)
);

CREATE TABLE IF NOT EXISTS strikes (
    id SERIAL PRIMARY KEY,
    expiry_id INTEGER NOT NULL REFERENCES expiries(id) ON DELETE CASCADE,
    strike INTEGER NOT NULL,
    atm_offset INTEGER NOT NULL,
    UNIQUE(expiry_id, strike)
);

-- =============================================================
-- Data Tables
-- =============================================================

CREATE TABLE IF NOT EXISTS raw_candles (
    id BIGSERIAL PRIMARY KEY,
    strike_id INTEGER NOT NULL REFERENCES strikes(id) ON DELETE CASCADE,
    option_type TEXT NOT NULL CHECK (option_type IN ('CE', 'PE')),
    timestamp TIMESTAMPTZ NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    oi INTEGER,
    iv REAL,
    spot REAL,
    UNIQUE(strike_id, option_type, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_raw_candles_timestamp ON raw_candles(timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_candles_strike_type_ts ON raw_candles(strike_id, option_type, timestamp);

CREATE TABLE IF NOT EXISTS derived_metrics (
    strike_id INTEGER NOT NULL REFERENCES strikes(id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL,
    ce_ltp REAL,
    pe_ltp REAL,
    ce_ltp_chg REAL,
    pe_ltp_chg REAL,
    pe_ltp_chg_pct REAL,
    ce_volume INTEGER,
    pe_volume INTEGER,
    ce_oi INTEGER,
    pe_oi INTEGER,
    ce_oi_chg INTEGER,
    pe_oi_chg INTEGER,
    ce_iv REAL,
    pe_iv REAL,
    ce_iv_chg REAL,
    pe_iv_chg REAL,
    pe_ce_oi INTEGER,
    pe_ce_oi_chg INTEGER,
    pcr_oi REAL,
    pcr_oi_chg REAL,
    pcr_vol REAL,
    PRIMARY KEY (strike_id, timestamp)
);

CREATE TABLE IF NOT EXISTS aggregate_metrics (
    expiry_id INTEGER NOT NULL REFERENCES expiries(id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL,
    spot REAL,
    spot_chg REAL,
    spot_chg_pct REAL,
    fair_price REAL,
    fair_price_chg REAL,
    atm_iv REAL,
    ivr REAL,
    ivp REAL,
    max_pain INTEGER,
    overall_pcr REAL,
    lot_size INTEGER,
    total_ce_oi INTEGER,
    total_pe_oi INTEGER,
    total_oi_net INTEGER,
    total_ce_oi_chg INTEGER,
    total_pe_oi_chg INTEGER,
    total_oi_chg_net INTEGER,
    total_bullish_oi INTEGER,
    total_bearish_oi INTEGER,
    otm_ce_oi INTEGER,
    otm_pe_oi INTEGER,
    otm_oi_net INTEGER,
    otm_ce_oi_chg INTEGER,
    otm_pe_oi_chg INTEGER,
    otm_oi_chg_net INTEGER,
    itm_ce_oi INTEGER,
    itm_pe_oi INTEGER,
    itm_oi_net INTEGER,
    itm_ce_oi_chg INTEGER,
    itm_pe_oi_chg INTEGER,
    itm_oi_chg_net INTEGER,
    PRIMARY KEY (expiry_id, timestamp)
);

CREATE TABLE IF NOT EXISTS iv_history (
    expiry_id INTEGER NOT NULL REFERENCES expiries(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    atm_iv REAL,
    spot REAL,
    atm_strike INTEGER,
    PRIMARY KEY (expiry_id, date)
);

CREATE TABLE IF NOT EXISTS verification_log (
    strike_id INTEGER NOT NULL REFERENCES strikes(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    dhan_close REAL,
    nse_close REAL,
    dhan_oi INTEGER,
    nse_oi INTEGER,
    dhan_volume INTEGER,
    nse_volume INTEGER,
    close_match BOOLEAN,
    oi_match BOOLEAN,
    volume_match BOOLEAN,
    notes TEXT,
    PRIMARY KEY (strike_id, date)
);

-- =============================================================
-- Views
-- =============================================================

CREATE OR REPLACE VIEW v_eod_snapshot AS
SELECT DISTINCT ON (rc.strike_id, rc.option_type, rc.timestamp::date)
    rc.strike_id, s.strike, s.atm_offset, rc.option_type,
    rc.timestamp::date AS date,
    rc.close, rc.oi, rc.volume, rc.spot,
    e.symbol, e.expiry_date
FROM raw_candles rc
JOIN strikes s ON s.id = rc.strike_id
JOIN expiries e ON e.id = s.expiry_id
ORDER BY rc.strike_id, rc.option_type, rc.timestamp::date, rc.timestamp DESC;

CREATE OR REPLACE VIEW v_pcr_trend AS
SELECT am.timestamp, am.overall_pcr, am.spot, e.symbol, e.expiry_date
FROM aggregate_metrics am
JOIN expiries e ON e.id = am.expiry_id
ORDER BY am.timestamp;

CREATE OR REPLACE VIEW v_oi_buildup AS
SELECT dm.timestamp, s.strike, s.atm_offset,
    dm.ce_oi, dm.pe_oi, dm.ce_oi_chg, dm.pe_oi_chg,
    e.symbol, e.expiry_date
FROM derived_metrics dm
JOIN strikes s ON s.id = dm.strike_id
JOIN expiries e ON e.id = s.expiry_id
ORDER BY dm.timestamp, s.strike;

-- =============================================================
-- Row Level Security
-- =============================================================

ALTER TABLE expiries ENABLE ROW LEVEL SECURITY;
ALTER TABLE strikes ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw_candles ENABLE ROW LEVEL SECURITY;
ALTER TABLE derived_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE aggregate_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE iv_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE verification_log ENABLE ROW LEVEL SECURITY;

-- Service role: full access
CREATE POLICY "service_all" ON expiries FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON strikes FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON raw_candles FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON derived_metrics FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON aggregate_metrics FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON iv_history FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "service_all" ON verification_log FOR ALL USING (true) WITH CHECK (true);
