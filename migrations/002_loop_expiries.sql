-- Migration 002: Dynamic table creation for loop_expiries scraper
-- Creates a reusable function that generates year-specific tables.
-- Usage: SELECT create_loop_tables(2026);

CREATE OR REPLACE FUNCTION create_loop_tables(p_year INTEGER)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  -- Data table: flat denormalized raw candles
  EXECUTE format($t$
    CREATE TABLE IF NOT EXISTS full_expiries_%s (
      id BIGSERIAL PRIMARY KEY,
      expiry_date DATE NOT NULL,
      expiry_flag TEXT NOT NULL CHECK (expiry_flag IN ('MONTH', 'WEEK')),
      timestamp TIMESTAMPTZ NOT NULL,
      date DATE NOT NULL,
      time TIME NOT NULL,
      strike INTEGER NOT NULL,
      atm_offset INTEGER NOT NULL,
      option_type TEXT NOT NULL CHECK (option_type IN ('CE', 'PE')),
      open REAL,
      high REAL,
      low REAL,
      close REAL,
      volume INTEGER,
      oi INTEGER,
      iv REAL,
      spot REAL,
      UNIQUE(expiry_date, expiry_flag, timestamp, strike, option_type)
    )
  $t$, p_year);

  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS idx_fe_%s_expiry ON full_expiries_%s(expiry_date)',
    p_year, p_year
  );
  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS idx_fe_%s_ts ON full_expiries_%s(timestamp)',
    p_year, p_year
  );

  -- Progress tracking table
  EXECUTE format($t$
    CREATE TABLE IF NOT EXISTS scrape_progress_%s (
      expiry_date DATE NOT NULL,
      expiry_flag TEXT NOT NULL CHECK (expiry_flag IN ('MONTH', 'WEEK')),
      status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'in_progress', 'completed', 'failed', 'skipped')),
      rows_inserted INTEGER DEFAULT 0,
      api_calls_made INTEGER DEFAULT 0,
      started_at TIMESTAMPTZ,
      completed_at TIMESTAMPTZ,
      error_message TEXT,
      PRIMARY KEY (expiry_date, expiry_flag)
    )
  $t$, p_year);

  -- RLS: enable + service_role full access
  EXECUTE format('ALTER TABLE full_expiries_%s ENABLE ROW LEVEL SECURITY', p_year);
  EXECUTE format('ALTER TABLE scrape_progress_%s ENABLE ROW LEVEL SECURITY', p_year);

  EXECUTE format(
    'DROP POLICY IF EXISTS service_all ON full_expiries_%s', p_year
  );
  EXECUTE format(
    'CREATE POLICY service_all ON full_expiries_%s FOR ALL USING (true) WITH CHECK (true)',
    p_year
  );
  EXECUTE format(
    'DROP POLICY IF EXISTS service_all ON scrape_progress_%s', p_year
  );
  EXECUTE format(
    'CREATE POLICY service_all ON scrape_progress_%s FOR ALL USING (true) WITH CHECK (true)',
    p_year
  );
END;
$$;
