# Full Expiries Loop Scraper — Design Spec

**Date:** 2026-03-31
**Status:** Approved
**Scope:** Batch scrape ALL NIFTY expiries for any given year, store raw candle data in Supabase

---

## 1. Goal

Build a year-parameterized batch scraping system that:
- Fetches 1-min raw candle data for ALL NIFTY weekly + monthly expiries
- Covers full ATM-10 to ATM+10 strike range (21 strikes)
- Stores 2-week backdata per expiry in a flat `full_expiries_{year}` Supabase table
- Tracks progress per-expiry for resume on failure
- Runs on GitHub Actions with TQDM-like progress and hourly health monitoring
- Is dynamic: works for 2026, 2025, 2024, etc. with correct lot sizes and edge-case handling

## 2. Key Numbers

| Metric | Value |
|--------|-------|
| Expiries per year | ~52 weekly + ~12 monthly = ~64 |
| Strikes | 21 (ATM-10 through ATM+10) |
| API calls per expiry | 42 (21 strikes x CE/PE) |
| Rate limit delay | 0.25s per call |
| Time per expiry | ~1.5 min |
| Total time | ~96 min for 64 expiries |
| Rows per expiry | ~157,500 (10 trading days x 375 candles x 42) |
| GitHub Actions timeout | 180 min |

## 3. Module Structure

All new code in `DhanHQ_src/loop_expiries/`:

```
DhanHQ_src/loop_expiries/
  __init__.py          # Package init
  config.py            # Expiry date generation, strike ranges, lot sizes per year
  scraper.py           # Core loop: iterate expiries, fetch, store, track progress
  db.py                # Supabase client for full_expiries_{year} + scrape_progress_{year}
  runner.py            # CLI entry: python -m DhanHQ_src.loop_expiries.runner --year 2026
```

### Reuses from existing code (no changes):
- `DhanHQ_src/auth.py` — TOTP token generation
- `DhanHQ_src/fetcher.py` — `DhanClient`, `fetch_with_retry()`, `parse_api_response()`, `build_raw_rows()`
- `DhanHQ_src/config.py` — credentials, rate limits, API constants

### Does NOT use:
- `calculator.py` — no derived metrics
- `verifier.py` — no bhavcopy verification
- `main.py` — separate pipeline, doesn't touch existing normalized tables

## 4. Supabase Schema

### 4a. Dynamic table creation via Postgres function

A migration creates a reusable function:

```sql
CREATE OR REPLACE FUNCTION create_loop_tables(p_year INTEGER)
RETURNS VOID AS $$
BEGIN
  -- Data table
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

  EXECUTE format('CREATE INDEX IF NOT EXISTS idx_fe_%s_expiry ON full_expiries_%s(expiry_date)', p_year, p_year);
  EXECUTE format('CREATE INDEX IF NOT EXISTS idx_fe_%s_ts ON full_expiries_%s(timestamp)', p_year, p_year);

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

  -- RLS
  EXECUTE format('ALTER TABLE full_expiries_%s ENABLE ROW LEVEL SECURITY', p_year);
  EXECUTE format('ALTER TABLE scrape_progress_%s ENABLE ROW LEVEL SECURITY', p_year);
  EXECUTE format($t$
    CREATE POLICY IF NOT EXISTS service_all ON full_expiries_%s FOR ALL USING (true) WITH CHECK (true)
  $t$, p_year);
  EXECUTE format($t$
    CREATE POLICY IF NOT EXISTS service_all ON scrape_progress_%s FOR ALL USING (true) WITH CHECK (true)
  $t$, p_year);
END;
$$ LANGUAGE plpgsql;
```

Called from Python: `client.rpc('create_loop_tables', {'p_year': year}).execute()`

### 4b. Why dynamic tables per year

- Clean isolation: drop `full_expiries_2025` without affecting 2026
- Predictable table sizes and index performance
- User's explicit naming convention: `full_expiries_2026`
- Easy to query: `SELECT * FROM full_expiries_2026 WHERE expiry_date = '2026-03-30'`

## 5. Expiry Date Generation

`config.py` generates expiry dates for any year:

```python
def generate_expiry_dates(year: int) -> list[dict]:
    """Generate all NIFTY expiry dates for a given year.

    Returns list of dicts:
      expiry_date: str (YYYY-MM-DD, always a Thursday)
      expiry_flag: 'WEEK' or 'MONTH'
      from_date: str (expiry_date - 14 days)
      to_date: str (= expiry_date)

    Every Thursday is a WEEK expiry.
    Last Thursday of each month is ALSO a MONTH expiry (separate contract).
    """
```

### Lot size lookup

```python
NIFTY_LOT_SIZES = {
    2024: 25,   # Changed mid-year; using pre-Nov value
    2025: 75,
    2026: 65,
}
# Fallback: 75 for unknown years
```

## 6. Data Flow

### 6a. Startup

1. Parse CLI args: `--year 2026` (required), `--reset` (optional)
2. Call `create_loop_tables(year)` via Supabase RPC
3. Generate all expiry dates for the year
4. Seed `scrape_progress_{year}`: upsert all expiries as `pending` (skip existing `completed`)
5. Authenticate via TOTP → get DhanHQ access token

### 6b. Main loop

For each non-completed expiry (ordered by date):

1. Mark `in_progress` with `started_at = now()`
2. Set `from_date = expiry_date - 14 days`, `to_date = expiry_date`
3. Loop 21 strikes x 2 option types (42 API calls):
   - Use `fetch_with_retry()` from existing `fetcher.py`
   - Parse with `parse_api_response()` + `build_raw_rows()`
   - Track empty responses
4. Batch upsert all rows into `full_expiries_{year}` (500-row chunks)
5. Update progress:
   - All 42 calls empty → status=`skipped` (likely holiday)
   - Error → status=`failed`, store error_message, continue to next
   - Success → status=`completed`, store `rows_inserted` count

### 6c. Progress display (TQDM-style)

```
[14/64] 21.9% | 2026-04-09 WEEK | 6,543 rows | ETA: 52m
[15/64] 23.4% | 2026-04-09 MONTH | 6,210 rows | ETA: 51m
...
DONE: 52 completed, 0 failed, 12 skipped (holidays) | Total: 341,214 rows
```

In GitHub Actions: use `::group::` for each expiry, job summary at the end.

### 6d. Resume logic

- On restart, query `scrape_progress_{year}`
- Skip `completed` and `skipped` entries
- Retry `failed` and `in_progress` (crashed mid-expiry)
- Upsert in data table ensures no duplicates
- `--reset` flag: sets all entries back to `pending`

### 6e. Token management

- TOTP token valid for 24h
- Track token generation time
- If token age > 20h during loop, regenerate via `get_access_token()`

## 7. GitHub Workflows

### 7a. Main scraper: `.github/workflows/loop_expiries.yml`

```yaml
name: Loop Expiries Scraper
on:
  workflow_dispatch:
    inputs:
      year:
        description: 'Year to scrape (e.g. 2026)'
        required: true
        default: '2026'
        type: string
      reset:
        description: 'Reset progress and start fresh'
        required: false
        default: false
        type: boolean

jobs:
  scrape:
    runs-on: ubuntu-latest
    timeout-minutes: 180
    steps:
      - checkout, setup-python, install deps
      - Run scraper:
          python -m DhanHQ_src.loop_expiries.runner --year ${{ inputs.year }}
          (add --reset if inputs.reset)
      - Upload logs artifact
```

Env vars: same secrets as existing `scrape.yml`.

### 7b. Health monitor: `.github/workflows/loop_expiries_monitor.yml`

```yaml
name: Loop Expiries Monitor
on:
  schedule:
    - cron: '0 * * * *'  # every hour
  workflow_dispatch:
    inputs:
      year:
        description: 'Year to monitor'
        required: true
        default: '2026'

jobs:
  check:
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - checkout, setup-python, install deps
      - Check if main workflow is running:
          gh run list --workflow=loop_expiries.yml --status=in_progress --json status
      - If running: print "Still active", exit 0
      - If not running: query scrape_progress_{year} from Supabase
      - Report summary to job summary:
          X/64 completed | Y failed | Z skipped | W pending
      - If any failed: list expiry dates + error messages
      - Confirm total row count in full_expiries_{year}
```

## 8. CLI Interface

```bash
# Scrape all 2026 expiries
python -m DhanHQ_src.loop_expiries.runner --year 2026

# Reset progress and re-scrape
python -m DhanHQ_src.loop_expiries.runner --year 2026 --reset

# Scrape 2025 historical data
python -m DhanHQ_src.loop_expiries.runner --year 2025

# Start via GitHub CLI
gh workflow run loop_expiries.yml -f year=2026
gh workflow run loop_expiries.yml -f year=2025 -f reset=true
```

## 9. Migration

New file: `migrations/002_loop_expiries.sql`

Contains the `create_loop_tables(p_year INTEGER)` function definition. Applied once; the function is then called at runtime for each year.

## 10. Edge Cases

| Case | Handling |
|------|----------|
| Holiday (Thursday) | API returns 0 candles for all 42 calls → `skipped` |
| Expiry shifted to Wednesday | Date range (expiry-14d to expiry) still captures data; API handles shift |
| Token expires mid-run | Regenerate after 20h |
| API 429 / rate limit | `fetch_with_retry()` with exponential backoff (existing) |
| Partial failure | Each expiry independent; failure → `failed` status, continue next |
| GitHub Actions timeout | 180 min covers ~64 expiries; if exceeded, resume on next run |
| Different lot sizes per year | `NIFTY_LOT_SIZES` dict in config |
| No data for entire year | Final summary shows 0 completed, all skipped |
| Supabase row limits | Batch upsert in 500-row chunks (existing pattern) |

## 11. What This Does NOT Do

- No derived metrics (ce_ltp_chg, pcr_oi, etc.)
- No aggregate metrics (max_pain, IVR/IVP, etc.)
- No NSE bhavcopy verification
- No output table generation
- No EDA reports
- Does not modify existing pipeline or tables
