# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commit Messages
- Keep commit messages short: max 5-6 words
- Never add "Co-Authored-By" or attribution lines
- No multi-line commit messages unless absolutely necessary

## Commands

```bash
# Install dependencies
pip install -r DhanHQ_src/requirements.txt

# Run full pipeline (requires DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN env vars)
python -m DhanHQ_src.main

# Run all tests
python -m pytest tests/ -v

# Run a single test file
python -m pytest tests/test_calculator.py -v

# Run a single test
python -m pytest tests/test_calculator.py::test_compute_max_pain -v

# Run Supabase integration tests (requires local Supabase stack or test env vars)
python -m pytest tests/supabase/ -v

# Run only non-Supabase tests (no external deps needed)
python -m pytest tests/ -v --ignore=tests/supabase
```

## Architecture

NIFTY options scraper: fetches 1-min expired options data from DhanHQ, stores in Supabase (Postgres), computes metrics, verifies against NSE Bhavcopy. Falls back to SQLite only when Supabase env vars are missing.

**Pipeline flow:** `fetcher.py` → `supabase_db.py` → `calculator.py` → `verifier.py`, orchestrated by `main.py`.

**Database backend selection:** `main.py` checks for `SUPABASE_URL` + `SUPABASE_SERVICE_ROLE_KEY`. If both are set, it uses `SupabaseDB` (`supabase_db.py`). Otherwise it falls back to SQLite `Database` (`db.py`). Supabase is the primary/production backend.

### Key design decisions

- **DhanHQ REST API directly, not the SDK.** The installed `dhanhq` package (v2.0.2) lacks `expired_options_data()` and `DhanContext`. `fetcher.py` uses a custom `DhanClient` class that POSTs directly to `/v2/charts/rollingoption`. Do not revert to `from dhanhq import DhanContext, dhanhq` — it will break.

- **ATM-relative strikes.** The DhanHQ rolling option API uses strike strings like `"ATM-4"` through `"ATM+4"` — no security ID lookups needed for expired contracts.

- **API responses are parallel arrays.** DhanHQ returns `{"open": [...], "close": [...], "timestamp": [...]}` — not rows. `parse_api_response()` transposes these into row dicts. Timestamps are epoch seconds converted to IST.

- **30-day chunking.** DhanHQ limits queries to 30 days per call. `fetch_iv_baseline()` already chunks the 52-week range. Any new date-range fetching must do the same.

- **Supabase is the primary database.** `SupabaseDB` in `supabase_db.py` uses the supabase-py SDK with a normalized schema (expiries → strikes → raw_candles). It handles FK mapping (strike → strike_id), batch upsert in 500-row chunks, and IST timestamp conversion. The schema is managed via `migrations/001_initial_schema.sql`.

- **SQLite `Database` class is kept for offline tests only.** Unit tests create temp SQLite DBs via the `db` fixture. Do not remove `db.py` — it's the test backend. For any new DB features, implement in `supabase_db.py` first (production), then optionally in `db.py` (tests).

- **Supabase schema is normalized with FKs.** `ensure_expiry()` and `ensure_strikes()` must be called before any data inserts (pipeline does this in Step 1b). CASCADE delete flows: `expiries → strikes → raw_candles/derived_metrics/verification_log`. RLS is enabled on all tables (service_role: full access, anon: read-only).

- **`build_raw_rows()` handles both CALL/PUT and CE/PE.** The API uses `"CALL"`/`"PUT"`, the DB uses `"CE"`/`"PE"`. Passing `"CE"` directly also works (passthrough). Don't "fix" this to only accept one format.

### Environment variables

| Variable | Purpose |
|----------|---------|
| `DHAN_CLIENT_ID` | DhanHQ numeric client ID |
| `DHAN_ACCESS_TOKEN` | DhanHQ JWT token (expires every 24h) |
| `SUPABASE_URL` | Supabase project URL (required for production) |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase service role key (required for production) |
| `SUPABASE_ANON_KEY` | Supabase anon key (for read-only/RLS testing) |

GitHub Actions workflow at `.github/workflows/scrape.yml` runs on schedule (Mon-Fri 10:30 PM IST) and manual dispatch. Secrets are configured in repo settings.
