# NIFTY Options Historical Data Scraper

Fetches 1-minute intraday historical data for NIFTY expired options using the **DhanHQ Data API**, stores it in **Supabase (Postgres)**, computes derived metrics (IV Rank, IV Percentile, Max Pain, PCR, Fair Price via Black-Scholes), and verifies against NSE Bhavcopy.

## Why DhanHQ?

Getting reliable historical options data with IV, OI, and spot at minute-level granularity is surprisingly hard in India:

| Method | Problem |
|--------|---------|
| **NSE Bhavcopy** | EOD data only — no intraday, no IV |
| **Web scraping (NSE/MoneyControl)** | Rate-limited, fragile selectors, no historical depth |
| **Zerodha Kite API** | No expired options data — only active contracts |
| **Yahoo Finance / Google Finance** | No Indian F&O options data at minute level |
| **Paid data vendors (Global Datafeeds, etc.)** | Expensive, CSV dumps, no API |
| **DhanHQ Rolling Option API** | 5 years of expired options data, 1-min candles, IV/OI/spot included |

DhanHQ's `/v2/charts/rollingoption` endpoint fetches expired options data using ATM-relative strikes (ATM-4 to ATM+4) — no security ID lookups needed.

**Requires:** DhanHQ Data API subscription (Rs 499/month, or free with 25+ F&O trades/month).

## Pipeline

The scraper runs a 9-step pipeline orchestrated by `main.py`:

```
1. Initialize DB (Supabase or SQLite fallback)
2. Connect to DhanHQ API
3. Fetch 52-week IV baseline (ATM IV history, chunked in 30-day windows)
4. Fetch options data (9 strikes x CE/PE, 1-min candles)
5. Compute per-strike derived metrics (LTP/OI/IV changes, PCR)
6. Compute aggregate metrics (Max Pain, IVR/IVP, Fair Price, OTM/ITM splits)
7. Verify against NSE Bhavcopy (close/OI/volume comparison)
8. Build denormalized output table (Supabase only)
9. Run EDA checks and generate HTML report (Supabase only)
```

## Computed Metrics

| Category | Metrics |
|----------|---------|
| **Per-strike** | CE/PE LTP change, OI change, IV change, strike-level PCR (OI and volume) |
| **Aggregate** | Spot change (abs + %), ATM IV, IV Rank, IV Percentile, Max Pain, overall PCR |
| **Derived** | Black-Scholes fair price, total OI (CE/PE/net), OTM/ITM OI splits, bullish/bearish OI |
| **Verification** | DhanHQ vs NSE Bhavcopy close (0.05 tolerance), OI match, volume match |

## Database

**Primary: Supabase (Postgres)** — normalized schema with foreign keys and RLS.

```
expiries → strikes → raw_candles
                   → derived_metrics
         → aggregate_metrics
         → iv_history
         → verification_log
```

**3 views:** `v_eod_snapshot` (last candle per day), `v_pcr_trend` (PCR over time), `v_oi_buildup` (OI by strike).

**RLS:** service_role has full access, anon key is read-only.

**Fallback: SQLite** — used when Supabase env vars are missing (local dev / offline tests).

## Project Structure

```
OptionsScraper/
├── DhanHQ_src/
│   ├── auth.py            # TOTP auth + auto token generation
│   ├── config.py          # API credentials, scrape parameters, constants
│   ├── fetcher.py         # DhanHQ REST client (custom, not SDK)
│   ├── supabase_db.py     # Supabase Postgres backend (production)
│   ├── db.py              # SQLite backend (test fallback)
│   ├── calculator.py      # Derived metrics, Max Pain, IVR/IVP, Black-Scholes
│   ├── verifier.py        # NSE Bhavcopy download, parse, comparison
│   ├── main.py            # Pipeline orchestrator (9 steps)
│   └── requirements.txt
├── tests/
│   ├── test_db.py         # SQLite CRUD tests
│   ├── test_fetcher.py    # API parsing, row building
│   ├── test_calculator.py # Metrics, Black-Scholes, Max Pain
│   ├── test_verifier.py   # Bhavcopy parsing, comparison
│   ├── test_integration.py# End-to-end with mocked API
│   └── supabase/
│       ├── conftest.py    # Local Supabase fixtures (reset, seed)
│       ├── test_schema.py # Schema validation (tables, FKs, RLS)
│       ├── test_db.py     # SupabaseDB class tests
│       └── eda.py         # EDA report generator (HTML)
├── migrations/
│   ├── 001_initial_schema.sql  # Postgres schema (7 tables, 3 views, RLS)
│   └── apply_migration.py      # Direct migration runner
├── .github/workflows/
│   └── scrape.yml         # Scheduled (Mon-Fri 10:30 PM IST) + manual
└── data/                  # SQLite DB + Bhavcopy CSVs (gitignored)
```

## Setup

Configure required credentials via environment variables or GitHub Secrets — see `config.py` for the full list.

### Run

```bash
pip install -r DhanHQ_src/requirements.txt
python -m DhanHQ_src.main
```

Or trigger via **GitHub Actions** — runs automatically Mon-Fri at 10:30 PM IST, or manually via `workflow_dispatch`.

### Tests

```bash
# All tests (non-Supabase)
python -m pytest tests/ -v --ignore=tests/supabase

# Supabase integration tests (requires local Supabase stack)
npx supabase start
python -m pytest tests/supabase/ -v

# EDA report
python -m tests.supabase.eda
```
