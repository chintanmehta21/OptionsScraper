# SENSEX Scraping — Design Spec

**Date:** 2026-05-07
**Status:** Draft (awaiting user review)
**Scope:** Add SENSEX parquet scraping pipeline mirroring the NIFTY parquet flow; refactor `scrape_single_expiry` to be symbol-agnostic; migrate NIFTY parquet output path under a `NIFTY/` subfolder.

---

## 1. Goal

Reproduce, for SENSEX, the same parquet-output pipeline that exists today for NIFTY:
- A full-year backfill runner (`run_parquet.py`).
- A current-year incremental updater (`update_2026_parquet.py`).
- Output: per-year parquet with the **same 16-column schema** as NIFTY, written to `G:/My Drive/OptionsData/SENSEX/sensex_options_{year}.parquet`.
- Reuse common code (auth, fetcher, classifier, scraper) without duplication.
- **NIFTY pipeline must continue to work without behavioural change.**

---

## 2. Non-goals (explicit)

- No SENSEX Supabase ingestion. No SENSEX `loop_expiries.db` / `runner.py` Supabase variant. Parquet only.
- No SENSEX main pipeline (`main.py` aggregate metrics, EDA, verifier). NIFTY-only.
- No SENSEX verifier (BSE bhavcopy reconciliation). Out of scope.
- No CI workflow for SENSEX (manual local runs only, like the existing NIFTY parquet scripts).
- No backfill of SENSEX years 2020–2023. Data does not exist on accessible bhavcopies for those years; pre-May-2023 SENSEX weekly options didn't exist on BSE.

---

## 3. Verified facts (source of truth)

| Item | Value | Source |
|---|---|---|
| SENSEX index `securityId` | `51` | DhanHQ scrip master row `BSE,I,51,INDEX,...,SENSEX,1.0,Sensex` |
| DhanHQ `exchangeSegment` (F&O) | `"BSE_FNO"` | DhanHQ API convention |
| DhanHQ `instrument` | `"OPTIDX"` | Same as NIFTY; SENSEX scrip rows are `OPTIDX` |
| BSE Bhavcopy URL | `https://www.bseindia.com/download/Bhavcopy/Derivative/BhavCopy_BSE_FO_0_0_0_{YYYYMMDD}_F_0000.CSV` | Probed; HTTP 200; returns CSV directly (not zipped) |
| Bhavcopy filter | `FinInstrmTp=="IDO" AND TckrSymb=="SENSEX"` | Verified against 2026-04-30 file |
| Expiry column | `XpryDt` (ISO `YYYY-MM-DD`) | Bhavcopy schema |
| Lot size 2024 | 10 | 2024-01-30 bhavcopy `NewBrdLotQty=10` |
| Lot size 2025–2026 | 20 | 2025-12-30 + 2026-04-30 bhavcopies |
| Strike grid | ATM-10..ATM+10 (21 strikes) | Same as NIFTY (`LOOP_STRIKES`) |
| Strike spacing | 100 (vs NIFTY 50) | API resolves server-side from `ATM±N` strings |

---

## 4. Data coverage caveat

| Year | Coverage | Outcome |
|---|---|---|
| 2020–2022 | None | SENSEX weeklies did not exist; old bhavcopy URLs return HTML 404. Skipped by user choice. |
| 2023 | None | New bhavcopy URL 404s for early 2023; SENSEX weeklies launched mid-May 2023 with thin volume. Skipped by user choice. |
| 2024 | Full | Bhavcopy works; lot=10 |
| 2025 | Full | Bhavcopy works; lot=20 |
| 2026 | Active | Incremental via `update_2026_parquet.py` |

`fetch_all_expiry_dates(year)` for skipped years will return `[]`; the runner will log "no expiries found" and exit cleanly. No parquet file created. No NIFTY breakage.

---

## 5. Architecture

### 5.1 Folder layout

```
DhanHQ_src/
├── loop_expiries/                 # existing, NIFTY (refactored)
│   ├── scraper.py                 # CHANGED: scrape_single_expiry now takes symbol_cfg
│   ├── run_parquet.py             # CHANGED: GDRIVE_BASE → .../NIFTY; passes NIFTY symbol_cfg
│   ├── update_2026_parquet.py     # CHANGED: GDRIVE_BASE → .../NIFTY; passes NIFTY symbol_cfg
│   ├── runner.py                  # CHANGED: passes NIFTY symbol_cfg through run_loop
│   ├── expiry_fetcher.py          # UNCHANGED (NIFTY-specific NSE bhavcopy)
│   ├── db.py                      # UNCHANGED (Supabase, NIFTY-only)
│   └── config.py                  # UNCHANGED
└── Sensex_Scraping/               # NEW
    ├── __init__.py
    ├── config.py                  # SENSEX symbol_cfg, paths, lot sizes
    ├── expiry_fetcher.py          # BSE bhavcopy fetcher
    ├── run_parquet.py             # Full-year SENSEX backfill
    └── update_2026_parquet.py     # Current-year SENSEX incremental
```

### 5.2 Refactor: `scrape_single_expiry`

**File:** `DhanHQ_src/loop_expiries/scraper.py`

**Old signature:**
```python
def scrape_single_expiry(dhan, expiry: dict) -> tuple[list[dict], int, int]:
    ...
    for strike in LOOP_STRIKES:
        for option_type in LOOP_OPTION_TYPES:
            response = fetch_with_retry(
                dhan,
                security_id=NIFTY_SECURITY_ID,
                exchange_segment=EXCHANGE_SEGMENT,
                instrument_type=INSTRUMENT_TYPE,
                ...
            )
```

**New signature:**
```python
def scrape_single_expiry(
    dhan,
    expiry: dict,
    symbol_cfg: dict,
    strikes: list[str] | None = None,
) -> tuple[list[dict], int, int]:
    ...
    _strikes = strikes if strikes is not None else LOOP_STRIKES
    strike_offsets = {s: i - len(_strikes) // 2 for i, s in enumerate(_strikes)}
    ...
    for strike in _strikes:
        for option_type in LOOP_OPTION_TYPES:
            response = fetch_with_retry(
                dhan,
                security_id=symbol_cfg["security_id"],
                exchange_segment=symbol_cfg["exchange_segment"],
                instrument_type=symbol_cfg.get("instrument", INSTRUMENT_TYPE),
                ...
            )
```

`symbol_cfg` is a plain dict with three keys: `security_id`, `exchange_segment`, `instrument`. Passing a dict (vs introducing a dataclass) keeps the change tiny and JSON-serialisable for tests.

`strikes` param is optional — defaults to `LOOP_STRIKES` so existing NIFTY behaviour is preserved when not passed. SENSEX passes `LOOP_STRIKES` explicitly (same value); the param exists for future symbols with different grids.

**Imports change:**
- Remove `NIFTY_SECURITY_ID`, `EXCHANGE_SEGMENT` from `DhanHQ_src.loop_expiries.scraper` imports — `symbol_cfg["security_id"]` and `symbol_cfg["exchange_segment"]` replace them.
- Keep `INSTRUMENT_TYPE` import as a fallback for `symbol_cfg.get("instrument", INSTRUMENT_TYPE)`.

**Helper reuse (no duplication):**
SENSEX `run_parquet.py` and `update_2026_parquet.py` import `scrape_single_expiry`, `_fmt_duration`, `_progress_bar` from `DhanHQ_src.loop_expiries.scraper` — same pattern the existing NIFTY parquet scripts already use. No copy of these helpers.

### 5.3 All callers updated (gap audit)

| File | Line | Change |
|---|---|---|
| `loop_expiries/scraper.py:187` (`run_loop`) | inside loop | Pass `NIFTY_SYMBOL_CFG` (new constant in `loop_expiries/config.py`) |
| `loop_expiries/run_parquet.py:165` | inside loop | Pass `NIFTY_SYMBOL_CFG` |
| `loop_expiries/update_2026_parquet.py:293` | inside loop | Pass `NIFTY_SYMBOL_CFG` |
| `tests/test_loop_scraper.py:173` | direct call | Pass `NIFTY_SYMBOL_CFG` |
| `tests/test_loop_scraper.py:190` | direct call | Pass `NIFTY_SYMBOL_CFG` |
| `tests/test_loop_scraper.py:202,221,243` | `@patch` mocks | No change; `mock_scrape.assert_called_with(...)` checks (if any) updated |

**New constant `NIFTY_SYMBOL_CFG`** in `loop_expiries/config.py`:
```python
from DhanHQ_src.config import NIFTY_SECURITY_ID, EXCHANGE_SEGMENT, INSTRUMENT_TYPE

NIFTY_SYMBOL_CFG = {
    "security_id": NIFTY_SECURITY_ID,
    "exchange_segment": EXCHANGE_SEGMENT,
    "instrument": INSTRUMENT_TYPE,
}
```

### 5.4 NIFTY parquet path migration

| File | Old | New |
|---|---|---|
| `loop_expiries/run_parquet.py:45` | `GDRIVE_BASE = "G:/My Drive/OptionsData"` | `GDRIVE_BASE = "G:/My Drive/OptionsData/NIFTY"` |
| `loop_expiries/run_parquet.py:237` | help text | update default-path help text |
| `loop_expiries/run_parquet.py:244` | default arg | derives from new GDRIVE_BASE — no separate change |
| `loop_expiries/update_2026_parquet.py:77` | `GDRIVE_BASE = "G:/My Drive/OptionsData"` | `GDRIVE_BASE = "G:/My Drive/OptionsData/NIFTY"` |

`_parquet_path(year)` and `_staging_path(year)` derive from `GDRIVE_BASE`/`STAGING_DIR`; no inner change required.

User has already moved the existing 9 parquet files into the new `NIFTY/` folder, so the next NIFTY run will read/write the correct location.

---

## 6. New SENSEX modules

### 6.1 `Sensex_Scraping/config.py`

```python
"""Configuration for SENSEX parquet scraping."""

from DhanHQ_src.loop_expiries.config import LOOP_STRIKES, LOOP_OPTION_TYPES

SENSEX_SYMBOL_CFG = {
    "security_id": 51,
    "exchange_segment": "BSE_FNO",
    "instrument": "OPTIDX",
}

# Same strike grid as NIFTY: ATM-10 through ATM+10 (21 strikes)
SENSEX_STRIKES = LOOP_STRIKES
SENSEX_OPTION_TYPES = LOOP_OPTION_TYPES

# Output paths
GDRIVE_BASE = "G:/My Drive/OptionsData/SENSEX"
PARQUET_FILENAME_TEMPLATE = "sensex_options_{year}.parquet"
STAGING_FILENAME_TEMPLATE = "sensex_options_{year}.parquet"
PROGRESS_FILENAME_TEMPLATE = "sensex_options_{year}_progress.json"

# Bhavcopy
BSE_BHAVCOPY_URL = (
    "https://www.bseindia.com/download/Bhavcopy/Derivative/"
    "BhavCopy_BSE_FO_0_0_0_{yyyymmdd}_F_0000.CSV"
)
BSE_BHAVCOPY_CACHE_PREFIX = "bse_fo_"  # cached as data/bhavcopy/bse_fo_{date_str}.csv
```

Lot sizes are intentionally **omitted** — the parquet schema does not include `lot_size`, and SENSEX is not run through the aggregate-metrics pipeline that needs it. If a future feature needs lot sizes, they can be added then.

### 6.2 `Sensex_Scraping/expiry_fetcher.py`

Implements `fetch_all_expiry_dates(year)` → `list[dict]` with the same dict shape as NIFTY's:
```python
{"expiry_date": "2026-05-07", "expiry_flag": "WEEK", "from_date": "2026-04-23", "to_date": "2026-05-07"}
```

Reuses `classify_expiry_dates` from `DhanHQ_src.loop_expiries.expiry_fetcher` (symbol-agnostic).

**Implementation outline:**
```python
import csv, os, time, logging
from datetime import date, timedelta
import requests

from DhanHQ_src.config import BHAVCOPY_DIR
from DhanHQ_src.loop_expiries.expiry_fetcher import classify_expiry_dates
from DhanHQ_src.Sensex_Scraping.config import (
    BSE_BHAVCOPY_URL, BSE_BHAVCOPY_CACHE_PREFIX,
)

BSE_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.bseindia.com/"}

def _download_bse_bhavcopy(date_str: str) -> str | None:
    """Download BSE F&O bhavcopy for one date. Returns CSV path or None."""
    os.makedirs(BHAVCOPY_DIR, exist_ok=True)
    cached = os.path.join(BHAVCOPY_DIR, f"{BSE_BHAVCOPY_CACHE_PREFIX}{date_str}.csv")
    if os.path.exists(cached):
        return cached

    yyyymmdd = date.fromisoformat(date_str).strftime("%Y%m%d")
    url = BSE_BHAVCOPY_URL.format(yyyymmdd=yyyymmdd)
    try:
        resp = requests.get(url, headers=BSE_HEADERS, timeout=30)
        if resp.status_code != 200 or not resp.content:
            return None
        # BSE serves CSV directly — but its 404 returns HTML with 200 status.
        # Sniff the first line to ensure it's the bhavcopy header.
        first_line = resp.content[:200].decode("utf-8", errors="replace")
        if "TradDt" not in first_line or "TckrSymb" not in first_line:
            return None
        with open(cached, "wb") as f:
            f.write(resp.content)
        return cached
    except Exception as e:
        logger.debug("BSE bhavcopy download failed %s: %s", url, e)
        return None

def _extract_sensex_expiry_dates(csv_path: str) -> list[str]:
    """Filter IDO/SENSEX rows, collect XpryDt."""
    out: set[str] = set()
    with open(csv_path, "r") as f:
        for row in csv.DictReader(f):
            if (row.get("FinInstrmTp", "").strip() == "IDO"
                    and row.get("TckrSymb", "").strip() == "SENSEX"):
                xpry = row.get("XpryDt", "").strip()
                if xpry:
                    out.add(xpry[:10])
    return sorted(out)

def fetch_expiry_dates_from_bhavcopy(year: int) -> list[str]:
    """Walk one bhavcopy per month for the year, merge SENSEX expiries."""
    all_dates: set[str] = set()
    today = date.today()
    for month in range(1, 13):
        for day_offset in range(2, 8):
            try:
                target = date(year, month, day_offset)
            except ValueError:
                continue
            if target >= today:
                break
            if target.weekday() >= 5:
                continue
            csv_path = _download_bse_bhavcopy(target.isoformat())
            if csv_path:
                year_dates = [d for d in _extract_sensex_expiry_dates(csv_path)
                              if d.startswith(str(year))]
                all_dates.update(year_dates)
                break
            time.sleep(1)
    return sorted(all_dates)

def fetch_all_expiry_dates(year: int) -> list[dict]:
    raw_dates = fetch_expiry_dates_from_bhavcopy(year)
    return classify_expiry_dates(raw_dates, year)
```

**Why sniff the first line:** BSE returns HTTP 200 with an HTML "page not found" body for missing dates. Sniffing `TradDt` + `TckrSymb` in the head bytes is a reliable cheap check.

### 6.3 `Sensex_Scraping/run_parquet.py` — full-year backfill

Structural copy of `loop_expiries/run_parquet.py`, with:
- `GDRIVE_BASE` from `Sensex_Scraping.config`
- Imports `fetch_all_expiry_dates` from `Sensex_Scraping.expiry_fetcher` (not loop_expiries)
- Passes `SENSEX_SYMBOL_CFG` to `scrape_single_expiry`
- Output filename: `sensex_options_{year}.parquet`
- Staging: `data/staging/sensex_options_{year}.parquet`
- Progress sidecar: `sensex_options_{year}_progress.json`

CLI: `python -m DhanHQ_src.Sensex_Scraping.run_parquet --year 2025`

### 6.4 `Sensex_Scraping/update_2026_parquet.py` — current-year incremental

Structural copy of `loop_expiries/update_2026_parquet.py`, with the same path/symbol changes as 6.3.

CLI: `python -m DhanHQ_src.Sensex_Scraping.update_2026_parquet`

Default `--year` is 2026 (matching the file name and NIFTY convention). The classification logic (missing/refresh/done/future) is identical and symbol-agnostic.

---

## 7. Parquet schema (identical to NIFTY)

16 columns, no SENSEX-specific additions:

```
timestamp, date, time, open, high, low, close,
volume, oi, iv, spot, strike, option_type,
atm_offset, expiry_date, expiry_flag
```

Strike values are SENSEX strikes (e.g. 75200, 76800), spot is SENSEX index level (~76,000–80,000 range). `atm_offset` range is -10..+10, same as NIFTY.

Note: `scripts/verify_parquet_*.py` in the repo are NIFTY-specific (their range checks are hardcoded `>=18000, <=30000`). A SENSEX verifier is **out of scope** for this design.

---

## 8. Tests

| File | Status | What it covers |
|---|---|---|
| `tests/test_loop_scraper.py` | EDIT | Update direct calls at lines 173 and 190 to pass `NIFTY_SYMBOL_CFG`; verify `symbol_cfg` propagates into `fetch_with_retry` payload |
| `tests/sensex/__init__.py` | NEW | empty |
| `tests/sensex/test_sensex_expiry_fetcher.py` | NEW | Unit tests with bundled BSE bhavcopy CSV fixture (3-5 SENSEX rows + non-SENSEX rows). Verify filter matches `IDO/SENSEX` only, returns sorted dates, classify_expiry_dates routes them correctly. |
| `tests/sensex/test_sensex_run_parquet.py` | NEW | Mock-based test mirroring `test_loop_scraper.py` patterns: mocks `scrape_single_expiry`, `fetch_all_expiry_dates`, `get_access_token`; asserts parquet write happens, progress is written, etc. |
| `tests/sensex/fixtures/bse_fo_sample.csv` | NEW | Trimmed real bhavcopy with mix of IDO/SENSEX, IDO/BANKEX, STO/etc. rows |

Live API tests are not in scope (no NIFTY parquet test hits the live API either).

---

## 9. Risks and mitigations

| Risk | Mitigation |
|---|---|
| **BSE_FNO subscription not active on Dhan account** (DH-902 401) | Run a single SENSEX scrape manually before kicking off a full year. Failure mode is an obvious 401 block at the auth/fetch layer, not silent data corruption. |
| **BSE bhavcopy returns HTML 200 for missing dates** | Header-sniff `TradDt`+`TckrSymb` in `_download_bse_bhavcopy` before caching. |
| **BSE rate limits / anti-bot** | Same 0.25s `API_DELAY_SECONDS` as NIFTY; one bhavcopy per month is small footprint. |
| **NIFTY pipeline breakage** | Refactor is signature-only; `NIFTY_SYMBOL_CFG` derives from existing constants. All callers updated in one PR. Tests cover both NIFTY and SENSEX paths. |
| **NIFTY parquet path change breaks running cron** | NIFTY parquet scripts run **manually** (no cron found in `.github/workflows/`); user has already moved files. Workflow files are unaffected. |
| **SENSEX strike spacing (100 vs 50) wrong server-side** | DhanHQ rolling-option resolves `ATM±N` server-side; verified empirically by the user's research. First SENSEX scrape will surface any issue immediately. |
| **`data/bhavcopy/` filename collision (NSE vs BSE)** | NSE files use `fo_bhavcopy_*` / `fo_bhavcopy_new_*`; BSE will use `bse_fo_*`. No collision. |

---

## 10. Files touched (final tally)

**New (8 files):**
- `DhanHQ_src/Sensex_Scraping/__init__.py`
- `DhanHQ_src/Sensex_Scraping/config.py`
- `DhanHQ_src/Sensex_Scraping/expiry_fetcher.py`
- `DhanHQ_src/Sensex_Scraping/run_parquet.py`
- `DhanHQ_src/Sensex_Scraping/update_2026_parquet.py`
- `tests/sensex/__init__.py`
- `tests/sensex/test_sensex_expiry_fetcher.py`
- `tests/sensex/fixtures/bse_fo_sample.csv`

**Modified (5 files):**
- `DhanHQ_src/loop_expiries/scraper.py` — `scrape_single_expiry` signature + internal NIFTY caller in `run_loop`
- `DhanHQ_src/loop_expiries/run_parquet.py` — `GDRIVE_BASE` + symbol_cfg passed
- `DhanHQ_src/loop_expiries/update_2026_parquet.py` — `GDRIVE_BASE` + symbol_cfg passed
- `DhanHQ_src/loop_expiries/config.py` — adds `NIFTY_SYMBOL_CFG`
- `tests/test_loop_scraper.py` — direct calls updated to pass symbol_cfg

**Optionally modified (1):**
- `tests/sensex/test_sensex_run_parquet.py` (NEW; can be deferred if time-pressed)

**Untouched (no behaviour change):**
- `DhanHQ_src/main.py`, `DhanHQ_src/fetcher.py`, `DhanHQ_src/auth.py`, `DhanHQ_src/calculator.py`, `DhanHQ_src/verifier.py`, `DhanHQ_src/db.py`, `DhanHQ_src/supabase_db.py`, `DhanHQ_src/config.py`
- `DhanHQ_src/loop_expiries/runner.py`, `DhanHQ_src/loop_expiries/db.py`, `DhanHQ_src/loop_expiries/expiry_fetcher.py`
- All `migrations/`, all `.github/workflows/`, all NIFTY-specific tests except `test_loop_scraper.py`

---

## 11. Acceptance criteria

1. `python -m DhanHQ_src.loop_expiries.run_parquet --year 2025` continues to read/write `G:/My Drive/OptionsData/NIFTY/nifty_options_2025.parquet` (post file-move).
2. `python -m DhanHQ_src.loop_expiries.update_2026_parquet` continues to update `G:/My Drive/OptionsData/NIFTY/nifty_options_2026.parquet` correctly.
3. `python -m DhanHQ_src.Sensex_Scraping.run_parquet --year 2025` produces `G:/My Drive/OptionsData/SENSEX/sensex_options_2025.parquet` with the 16-column schema, valid SENSEX strikes (~75k–80k), CE/PE balanced, atm_offset in `-10..+10`.
4. `python -m DhanHQ_src.Sensex_Scraping.update_2026_parquet` updates `G:/My Drive/OptionsData/SENSEX/sensex_options_2026.parquet` with the same missing/refresh classification semantics as NIFTY.
5. `python -m pytest tests/ -v --ignore=tests/supabase` passes (existing tests + new SENSEX tests).
6. `--dry-run` on the SENSEX updater lists work items without making API calls.
