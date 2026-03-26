# NIFTY Options Historical Data Scraper — Design Spec

## Overview

Python application to scrape NIFTY options historical data from DhanHQ's Expired Options Data API for the 30MAR2026 expiry, covering March 16-30 at 1-minute intraday intervals with 8 Near ATM strikes (ATM±4). Data is stored in SQLite and verified against NSE Bhavcopy.

## Data Source

**API:** DhanHQ Expired Options Data API (paid Data API add-on, ~Rs 499/month)

**Endpoint:** `POST /charts/rollingoption`

**Parameters for our use case:**

| Parameter | Value |
|-----------|-------|
| `securityId` | NIFTY underlying ID (from instrument master CSV) |
| `exchangeSegment` | `NSE_FNO` |
| `instrument` | `OPTIDX` |
| `expiryFlag` | `MONTH` |
| `interval` | `1` (1-minute candles) |
| `fromDate` | `2026-03-16` |
| `toDate` | `2026-03-30` |
| `strike` | `ATM-4` through `ATM+4` (excluding `ATM+0` duplicate = 8 values) |
| `drvOptionType` | `CALL` or `PUT` |
| `requiredData` | `["open", "high", "low", "close", "iv", "volume", "strike", "oi", "spot"]` |

**Iteration:** 8 strikes × 2 option types = 16 API calls for main data. Plus ~2 additional calls for 52-week ATM IV baseline (for IVR/IVP). Date range under 30-day limit so each combo is 1 call. Total: ~18 calls at 5 req/sec rate limit = ~4 seconds.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    config.py                            │
│  (API credentials, date range, strike config)           │
└──────────────┬──────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────┐
│              fetcher.py                                 │
│  - Authenticate with DhanHQ                             │
│  - Iterate strikes (ATM-4 to ATM+4) × (CE, PE)         │
│  - Call POST /charts/rollingoption                      │
│  - Parse parallel-array response into rows              │
│  - Rate limit handling (5 req/sec)                      │
└──────────────┬──────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────┐
│              db.py                                      │
│  - SQLite connection / schema creation                  │
│  - Insert raw option data                               │
│  - Upsert derived & aggregate metrics                   │
└──────────────┬──────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────┐
│              calculator.py                              │
│  - Per-strike derived metrics (IV Chg, OI Chg,          │
│    LTP Chg, PE-CE OI, PCR per strike)                   │
│  - Aggregate metrics (Total OI, OTM/ITM splits,         │
│    Max Pain, Bullish/Bearish OI, overall PCR)           │
│  - Fair Price (Black-Scholes)                           │
│  - IVR, IVP (from ATM IV history)                       │
│  - Greeks (Delta, Gamma, Theta, Vega)                   │
└──────────────┬──────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────┐
│              verifier.py                                │
│  - Download NSE Bhavcopy for each trading date          │
│  - Compare EOD Close, OI, Volume per strike             │
│  - Log mismatches to verification_log table             │
│  - Print summary report                                 │
└──────────────┬──────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────┐
│              main.py                                    │
│  - Orchestrate: fetch → store → calculate → verify      │
└─────────────────────────────────────────────────────────┘
```

## Database Schema (SQLite)

### Table: `raw_option_data`

One row per minute per strike per option type. This is the core data table.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `timestamp` | DATETIME | Candle timestamp (e.g., 2026-03-16 09:15:00) |
| `date` | DATE | Trading date (indexed) |
| `time` | TIME | Intraday time (e.g., 09:15:59) |
| `expiry_date` | DATE | 2026-03-30 |
| `strike` | INTEGER | Absolute strike price (e.g., 23100, 23200) |
| `option_type` | TEXT | 'CE' or 'PE' |
| `open` | REAL | Open price |
| `high` | REAL | High price |
| `low` | REAL | Low price |
| `close` | REAL | Close / LTP |
| `volume` | INTEGER | Trade volume |
| `oi` | INTEGER | Open Interest |
| `iv` | REAL | Implied Volatility |
| `spot` | REAL | NIFTY spot price at that minute |
| `atm_offset` | INTEGER | ATM-relative position (-4 to +4) |

**Unique constraint:** `(timestamp, strike, option_type)`
**Indexes:** `(date)`, `(strike, option_type)`, `(timestamp)`

### Table: `derived_metrics`

Per-minute per-strike pair (CE+PE combined). Maps to the per-row data in the icharts screenshot.

| Column | Type | Source / Description |
|--------|------|---------------------|
| `timestamp` | DATETIME | PK part 1 |
| `strike` | INTEGER | PK part 2 |
| `ce_ltp` | REAL | Call LTP (close) |
| `pe_ltp` | REAL | Put LTP (close) |
| `ce_ltp_chg` | REAL | Call LTP change from prev candle |
| `pe_ltp_chg` | REAL | Put LTP change from prev candle |
| `pe_ltp_chg_pct` | REAL | Put LTP change % |
| `ce_volume` | INTEGER | Call volume |
| `pe_volume` | INTEGER | Put volume |
| `ce_oi` | INTEGER | Call OI |
| `pe_oi` | INTEGER | Put OI |
| `ce_oi_chg` | INTEGER | Call OI change from prev candle |
| `pe_oi_chg` | INTEGER | Put OI change from prev candle |
| `ce_iv` | REAL | Call IV |
| `pe_iv` | REAL | Put IV |
| `ce_iv_chg` | REAL | Call IV change from prev candle |
| `pe_iv_chg` | REAL | Put IV change from prev candle |
| `pe_ce_oi` | INTEGER | Put OI - Call OI |
| `pe_ce_oi_chg` | INTEGER | PE-CE OI change from prev candle |
| `pcr_oi` | REAL | Per-strike PCR by OI (Put OI / Call OI) |
| `pcr_oi_chg` | REAL | PCR-OI change from prev candle |
| `pcr_vol` | REAL | Per-strike PCR by Volume |

**Primary key:** `(timestamp, strike)`

### Table: `aggregate_metrics`

Per-minute across ALL strikes. Maps to the top-right panel and bottom summary boxes.

| Column | Type | Source / Description |
|--------|------|---------------------|
| `timestamp` | DATETIME | PK |
| `spot` | REAL | NIFTY spot price |
| `spot_chg` | REAL | Spot change from prev close |
| `spot_chg_pct` | REAL | Spot change % |
| `fair_price` | REAL | Black-Scholes theoretical price |
| `fair_price_chg` | REAL | Fair price change |
| `atm_iv` | REAL | ATM Implied Volatility |
| `ivr` | REAL | IV Rank (52-week) |
| `ivp` | REAL | IV Percentile (52-week) |
| `max_pain` | INTEGER | Max Pain strike |
| `overall_pcr` | REAL | Overall PCR across all strikes |
| `lot_size` | INTEGER | Lot size (65 for NIFTY) |
| **Totals box** | | |
| `total_ce_oi` | INTEGER | Total Call OI |
| `total_pe_oi` | INTEGER | Total Put OI |
| `total_oi_net` | INTEGER | Net (Put - Call) |
| `total_ce_oi_chg` | INTEGER | Total Call OI change |
| `total_pe_oi_chg` | INTEGER | Total Put OI change |
| `total_oi_chg_net` | INTEGER | Net OI change |
| `total_bullish_oi` | INTEGER | Total Bullish OI |
| `total_bearish_oi` | INTEGER | Total Bearish OI |
| **OTM box** | | |
| `otm_ce_oi` | INTEGER | OTM Call OI |
| `otm_pe_oi` | INTEGER | OTM Put OI |
| `otm_oi_net` | INTEGER | OTM Net |
| `otm_ce_oi_chg` | INTEGER | OTM Call OI change |
| `otm_pe_oi_chg` | INTEGER | OTM Put OI change |
| `otm_oi_chg_net` | INTEGER | OTM Net OI change |
| **ITM box** | | |
| `itm_ce_oi` | INTEGER | ITM Call OI |
| `itm_pe_oi` | INTEGER | ITM Put OI |
| `itm_oi_net` | INTEGER | ITM Net |
| `itm_ce_oi_chg` | INTEGER | ITM Call OI change |
| `itm_pe_oi_chg` | INTEGER | ITM Put OI change |
| `itm_oi_chg_net` | INTEGER | ITM Net OI change |

### Table: `verification_log`

NSE Bhavcopy comparison — one row per date per strike per option type.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Trading date |
| `strike` | INTEGER | Strike price |
| `option_type` | TEXT | CE/PE |
| `dhan_close` | REAL | EOD Close from DhanHQ |
| `nse_close` | REAL | Close from NSE Bhavcopy |
| `dhan_oi` | INTEGER | EOD OI from DhanHQ |
| `nse_oi` | INTEGER | OI from NSE Bhavcopy |
| `dhan_volume` | INTEGER | Total day volume from DhanHQ |
| `nse_volume` | INTEGER | Volume from NSE Bhavcopy |
| `close_match` | BOOLEAN | Within tolerance? |
| `oi_match` | BOOLEAN | Exact match? |
| `volume_match` | BOOLEAN | Exact match? |
| `notes` | TEXT | Mismatch details if any |

**Primary key:** `(date, strike, option_type)`

## Derived Metrics Calculation Logic

### Per-strike (derived_metrics table)

| Metric | Formula |
|--------|---------|
| LTP Chg | `current_close - prev_close` (prev = previous candle same day, or last candle prev day for 09:15) |
| LTP Chg % | `(ltp_chg / prev_close) * 100` |
| OI Chg | `current_oi - prev_oi` |
| IV Chg | `current_iv - prev_iv` |
| PE-CE OI | `put_oi - call_oi` for same strike |
| PE-CE OI Chg | `current_pe_ce_oi - prev_pe_ce_oi` |
| PCR-OI | `put_oi / call_oi` per strike |
| PCR-OI Chg | `current_pcr_oi - prev_pcr_oi` |
| PCR-Vol | `put_volume / call_volume` per strike |

### Aggregate (aggregate_metrics table)

| Metric | Formula |
|--------|---------|
| Total OI | Sum of OI across all strikes for CE and PE separately |
| OTM/ITM split | Strike > spot → OTM for CE, ITM for PE; Strike < spot → ITM for CE, OTM for PE |
| Max Pain | Strike where total payout to option buyers is minimized |
| Overall PCR | `total_pe_oi / total_ce_oi` |
| Bullish OI | Sum of (Call OI where OI is increasing + Put OI where OI is decreasing) |
| Bearish OI | Sum of (Put OI where OI is increasing + Call OI where OI is decreasing) |
| Fair Price | Black-Scholes theoretical from ATM IV + spot |
| IVR | `(current_atm_iv - 52w_low_iv) / (52w_high_iv - 52w_low_iv) * 100` |
| IVP | `% of days in past year where IV < current_atm_iv` |

**IVR/IVP baseline:** Fetch 52 weeks (~252 trading days) of daily ATM IV from DhanHQ using the same expired options endpoint with `DAY` interval, going back to ~March 2025. This builds the historical IV distribution needed for IVR/IVP calculations. Stored in a separate `iv_history` table.

## Verification Strategy

### Step 1: Download NSE Bhavcopy
- NSE publishes daily F&O bhavcopy CSV at `https://nsearchives.nseindia.com/content/fo/`
- File format: `fo_bhavcopy_DDMMYYYY.csv` or similar
- Contains: Symbol, Expiry, Strike, Option Type, Close, OI, Volume, etc.

### Step 2: Compare
For each trading date in our range:
1. Extract EOD values from DhanHQ data (last candle of the day: 15:29 or 15:30)
2. Extract matching rows from NSE Bhavcopy
3. Compare Close price (within Rs 0.05 tolerance), OI (exact match), Volume (exact match)

### Step 3: Report
- Print summary: X/Y values matched, list mismatches
- Store results in `verification_log` table

## Project Structure

```
OptionsScraper/
├── DhanHQ_src/
│   ├── config.py          # API keys, date ranges, strike config
│   ├── fetcher.py         # DhanHQ API client & data fetching
│   ├── db.py              # SQLite schema & CRUD operations
│   ├── calculator.py      # Derived & aggregate metric calculations
│   ├── verifier.py        # NSE Bhavcopy download & comparison
│   ├── main.py            # CLI entry point & orchestration
│   └── requirements.txt   # dhanhq, pandas, scipy, requests
├── data/
│   ├── nifty_options.db    # SQLite database
│   └── bhavcopy/           # Downloaded NSE bhavcopy CSVs
└── .env                    # API credentials (gitignored)
```

## Dependencies

- `dhanhq` — DhanHQ Python SDK
- `pandas` — Data manipulation
- `scipy` — Black-Scholes calculations (for Fair Price, Greeks)
- `requests` — HTTP client (for NSE Bhavcopy download)
- `python-dotenv` — Environment variable management for API keys

## Setup Steps

1. Enable DhanHQ API access: Go to https://dhanhq.co → Settings → API & Integrations
2. Subscribe to Data API add-on (~Rs 499/month)
3. Generate access token and note client ID
4. Create `.env` file with `DHAN_CLIENT_ID` and `DHAN_ACCESS_TOKEN`
5. `pip install -r requirements.txt`
6. `python main.py` — runs full pipeline: fetch → store → calculate → verify

## Rate Limits & Error Handling

- DhanHQ Data API: 5 requests/second, 100,000/day
- Add 0.2s delay between requests to stay within rate limit
- Retry on 429 (rate limit) with exponential backoff
- Log all API errors with request params for debugging
- Validate response structure before inserting into DB
