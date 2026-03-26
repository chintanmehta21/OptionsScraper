# NIFTY Options Historical Data Scraper

Fetches 1-minute intraday historical data for NIFTY expired options using the **DhanHQ Data API**, computes derived metrics (IV Rank, IV Percentile, Max Pain, PCR, Fair Price via Black-Scholes), and verifies against NSE Bhavcopy.

## Why DhanHQ?

Getting reliable historical options data with IV, OI, and spot at minute-level granularity is surprisingly hard in India. Here's what was tried before landing on DhanHQ:

| Method | Problem |
|--------|---------|
| **NSE Bhavcopy** | EOD data only — no intraday, no IV |
| **Web scraping (NSE/MoneyControl)** | Rate-limited, fragile selectors, no historical depth |
| **Zerodha Kite API** | No expired options data — only active contracts |
| **Yahoo Finance / Google Finance** | No Indian F&O options data at minute level |
| **Paid data vendors (Global Datafeeds, etc.)** | Expensive, CSV dumps, no API |
| **DhanHQ Rolling Option API** | Works — 5 years of expired options data, 1-min candles, IV/OI/spot included |

DhanHQ's `/v2/charts/rollingoption` endpoint is purpose-built for this: fetch expired options data using ATM-relative strikes (ATM-4 to ATM+4) without needing to look up individual security IDs.

**Requires:** DhanHQ Data API subscription (Rs 499/month, or free with 25+ F&O trades/month).

## Project Structure

```
OptionsScraper/
├── DhanHQ_src/
│   ├── config.py          # API credentials, scrape parameters, constants
│   ├── fetcher.py         # DhanHQ API client — fetch expired options & IV baseline
│   ├── db.py              # SQLite schema (5 tables) and CRUD operations
│   ├── calculator.py      # Derived metrics, Max Pain, IVR/IVP, Black-Scholes
│   ├── verifier.py        # NSE Bhavcopy download, parse, and comparison
│   ├── main.py            # CLI orchestrator — runs full pipeline
│   └── requirements.txt
├── tests/
│   ├── test_db.py
│   ├── test_fetcher.py
│   ├── test_calculator.py
│   ├── test_verifier.py
│   └── test_integration.py
├── .github/workflows/
│   └── scrape.yml         # GitHub Actions — scheduled + manual trigger
└── data/                  # SQLite DB + Bhavcopy CSVs (gitignored)
```

## Pipeline

```
DhanHQ API → fetch (9 strikes x CE/PE, 1-min) → SQLite → compute metrics → verify vs NSE Bhavcopy
```

## Setup

1. Set environment variables: `DHAN_CLIENT_ID`, `DHAN_ACCESS_TOKEN`
2. `pip install -r DhanHQ_src/requirements.txt`
3. `python -m DhanHQ_src.main`

Or trigger via GitHub Actions (secrets configured in repo settings).
