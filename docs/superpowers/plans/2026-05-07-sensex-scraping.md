# SENSEX Scraping Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a SENSEX parquet scraping pipeline mirroring the NIFTY parquet flow (`run_parquet.py` + `update_2026_parquet.py`); refactor `scrape_single_expiry` to be symbol-agnostic; migrate NIFTY parquet output paths under `G:/My Drive/OptionsData/NIFTY/`.

**Architecture:** Refactor the existing `loop_expiries.scraper.scrape_single_expiry` to accept a `symbol_cfg` dict (`security_id`, `exchange_segment`, `instrument`) so both NIFTY and SENSEX callers reuse it. Add a new `DhanHQ_src/Sensex_Scraping/` module with SENSEX config, BSE Bhavcopy expiry fetcher, and structural copies of the parquet runner/updater. NIFTY behaviour is unchanged; only the GDrive output base path moves under a `NIFTY/` subfolder (files already moved by the user).

**Tech Stack:** Python 3.11, pandas/pyarrow (parquet), requests (BSE Bhavcopy), pytest. DhanHQ rolling-option REST API. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-05-07-sensex-scraping-design.md`

---

## File Structure

**New files (8):**
- `DhanHQ_src/Sensex_Scraping/__init__.py`
- `DhanHQ_src/Sensex_Scraping/config.py` — SENSEX symbol_cfg, paths, BSE Bhavcopy URL
- `DhanHQ_src/Sensex_Scraping/expiry_fetcher.py` — BSE Bhavcopy fetcher (IDO/SENSEX filter)
- `DhanHQ_src/Sensex_Scraping/run_parquet.py` — full-year backfill
- `DhanHQ_src/Sensex_Scraping/update_2026_parquet.py` — current-year incremental
- `tests/sensex/__init__.py`
- `tests/sensex/test_sensex_expiry_fetcher.py`
- `tests/sensex/fixtures/bse_fo_sample.csv`

**Modified files (5):**
- `DhanHQ_src/loop_expiries/config.py` — add `NIFTY_SYMBOL_CFG`
- `DhanHQ_src/loop_expiries/scraper.py` — `scrape_single_expiry` signature + internal call site
- `DhanHQ_src/loop_expiries/run_parquet.py` — `GDRIVE_BASE` → `.../NIFTY`; pass `NIFTY_SYMBOL_CFG`
- `DhanHQ_src/loop_expiries/update_2026_parquet.py` — `GDRIVE_BASE` → `.../NIFTY`; pass `NIFTY_SYMBOL_CFG`
- `tests/test_loop_scraper.py` — pass `NIFTY_SYMBOL_CFG` in two direct calls

---

## Task 1: Add `NIFTY_SYMBOL_CFG` constant

**Files:**
- Modify: `DhanHQ_src/loop_expiries/config.py`

- [ ] **Step 1: Add NIFTY_SYMBOL_CFG to loop_expiries config**

Append to `DhanHQ_src/loop_expiries/config.py` (after the existing `_LOT_SIZE_FALLBACK = 75` line):

```python

# Symbol configuration for the parameterized scraper.
# Keep all NIFTY constants here so the existing loop pipeline keeps working.
from DhanHQ_src.config import (
    NIFTY_SECURITY_ID,
    EXCHANGE_SEGMENT,
    INSTRUMENT_TYPE,
)

NIFTY_SYMBOL_CFG = {
    "security_id": NIFTY_SECURITY_ID,
    "exchange_segment": EXCHANGE_SEGMENT,
    "instrument": INSTRUMENT_TYPE,
}
```

- [ ] **Step 2: Verify import works**

Run: `python -c "from DhanHQ_src.loop_expiries.config import NIFTY_SYMBOL_CFG; print(NIFTY_SYMBOL_CFG)"`
Expected: `{'security_id': 13, 'exchange_segment': 'NSE_FNO', 'instrument': 'INDEX'}`

- [ ] **Step 3: Run existing tests to confirm no regression**

Run: `python -m pytest tests/test_loop_config.py -v`
Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add DhanHQ_src/loop_expiries/config.py
git commit -m "feat: NIFTY_SYMBOL_CFG constant"
```

---

## Task 2: Refactor `scrape_single_expiry` signature (TDD)

**Files:**
- Modify: `DhanHQ_src/loop_expiries/scraper.py`
- Test: `tests/test_loop_scraper.py`

- [ ] **Step 1: Write the failing test (symbol_cfg propagates)**

Replace the body of `TestScrapeSingleExpiry::test_returns_rows_and_counts` in `tests/test_loop_scraper.py` (currently lines 162–177) with:

```python
class TestScrapeSingleExpiry:
    @patch("DhanHQ_src.loop_expiries.scraper.fetch_with_retry")
    @patch("DhanHQ_src.loop_expiries.scraper.time")
    def test_returns_rows_and_counts(self, mock_time, mock_fetch):
        mock_fetch.return_value = _make_api_response(3)
        expiry = {
            "expiry_date": "2026-01-01",
            "expiry_flag": "WEEK",
            "from_date": "2025-12-18",
            "to_date": "2026-01-01",
        }
        symbol_cfg = {"security_id": 13, "exchange_segment": "NSE_FNO", "instrument": "INDEX"}
        dhan = MagicMock()
        rows, api_calls, empty_count = scrape_single_expiry(dhan, expiry, symbol_cfg)

        assert api_calls == 42  # 21 strikes x 2 types
        assert empty_count == 0
        assert len(rows) == 3 * 42  # 3 candles x 42 calls
        # First fetch_with_retry call payload should include the symbol_cfg values
        first_call_kwargs = mock_fetch.call_args_list[0].kwargs
        assert first_call_kwargs["security_id"] == 13
        assert first_call_kwargs["exchange_segment"] == "NSE_FNO"

    @patch("DhanHQ_src.loop_expiries.scraper.fetch_with_retry")
    @patch("DhanHQ_src.loop_expiries.scraper.time")
    def test_empty_response_counted(self, mock_time, mock_fetch):
        mock_fetch.return_value = {}  # empty
        expiry = {
            "expiry_date": "2026-01-01",
            "expiry_flag": "WEEK",
            "from_date": "2025-12-18",
            "to_date": "2026-01-01",
        }
        symbol_cfg = {"security_id": 13, "exchange_segment": "NSE_FNO", "instrument": "INDEX"}
        dhan = MagicMock()
        rows, api_calls, empty_count = scrape_single_expiry(dhan, expiry, symbol_cfg)

        assert api_calls == 42
        assert empty_count == 42
        assert len(rows) == 0

    @patch("DhanHQ_src.loop_expiries.scraper.fetch_with_retry")
    @patch("DhanHQ_src.loop_expiries.scraper.time")
    def test_sensex_symbol_cfg_propagates(self, mock_time, mock_fetch):
        """SENSEX symbol_cfg must be passed verbatim to fetch_with_retry."""
        mock_fetch.return_value = _make_api_response(1)
        expiry = {
            "expiry_date": "2026-05-07",
            "expiry_flag": "WEEK",
            "from_date": "2026-04-23",
            "to_date": "2026-05-07",
        }
        symbol_cfg = {"security_id": 51, "exchange_segment": "BSE_FNO", "instrument": "OPTIDX"}
        dhan = MagicMock()
        scrape_single_expiry(dhan, expiry, symbol_cfg)
        first_kwargs = mock_fetch.call_args_list[0].kwargs
        assert first_kwargs["security_id"] == 51
        assert first_kwargs["exchange_segment"] == "BSE_FNO"
```

- [ ] **Step 2: Run tests — expect failure**

Run: `python -m pytest tests/test_loop_scraper.py::TestScrapeSingleExpiry -v`
Expected: tests fail with `TypeError: scrape_single_expiry() takes 2 positional arguments but 3 were given` (and `KeyError`/`AssertionError` on the symbol_cfg keys).

- [ ] **Step 3: Refactor `scrape_single_expiry` signature**

In `DhanHQ_src/loop_expiries/scraper.py`:

- Replace the current import block (lines 9–26) with:

```python
from DhanHQ_src.auth import get_access_token
from DhanHQ_src.fetcher import (
    create_dhan_client,
    fetch_with_retry,
    parse_api_response,
    build_raw_rows,
)
from DhanHQ_src.config import (
    INSTRUMENT_TYPE,
    REQUIRED_DATA,
    API_DELAY_SECONDS,
)
from DhanHQ_src.loop_expiries.config import (
    LOOP_STRIKES,
    LOOP_OPTION_TYPES,
    NIFTY_SYMBOL_CFG,
)
from DhanHQ_src.loop_expiries.expiry_fetcher import fetch_all_expiry_dates
from DhanHQ_src.loop_expiries.db import LoopExpiriesDB
```

- Replace `scrape_single_expiry` (lines 77–118) with:

```python
def scrape_single_expiry(dhan, expiry: dict, symbol_cfg: dict, strikes=None):
    """Fetch all strikes x option types for one expiry.

    Args:
        dhan: DhanClient instance.
        expiry: dict with keys expiry_date, expiry_flag, from_date, to_date.
        symbol_cfg: dict with keys security_id, exchange_segment, instrument.
        strikes: optional list of ATM-relative strike strings; defaults to LOOP_STRIKES.

    Returns (rows, api_calls, empty_count).
    """
    expiry_date = expiry["expiry_date"]
    expiry_flag = expiry["expiry_flag"]
    from_date = expiry["from_date"]
    to_date = expiry["to_date"]

    _strikes = strikes if strikes is not None else LOOP_STRIKES
    strike_offsets = {s: i - len(_strikes) // 2 for i, s in enumerate(_strikes)}

    all_rows = []
    api_calls = 0
    empty_count = 0

    for strike in _strikes:
        for option_type in LOOP_OPTION_TYPES:
            api_calls += 1
            response = fetch_with_retry(
                dhan,
                security_id=symbol_cfg["security_id"],
                exchange_segment=symbol_cfg["exchange_segment"],
                instrument_type=symbol_cfg.get("instrument", INSTRUMENT_TYPE),
                expiry_flag=expiry_flag,
                expiry_code=1,
                strike=strike,
                drv_option_type=option_type,
                required_data=REQUIRED_DATA,
                from_date=from_date,
                to_date=to_date,
            )
            parsed = parse_api_response(response)
            if not parsed:
                empty_count += 1
            else:
                rows = build_raw_rows(parsed, option_type, strike_offsets[strike], expiry_date)
                all_rows.extend(rows)

            time.sleep(API_DELAY_SECONDS)

    return all_rows, api_calls, empty_count
```

- Update the internal `run_loop` call site (currently around line 187) — find:

```python
            rows, api_calls, empty_count = scrape_single_expiry(dhan, expiry)
```

Replace with:

```python
            rows, api_calls, empty_count = scrape_single_expiry(dhan, expiry, NIFTY_SYMBOL_CFG)
```

- [ ] **Step 4: Run tests — expect pass**

Run: `python -m pytest tests/test_loop_scraper.py -v`
Expected: all `TestScrapeSingleExpiry` and `TestRunLoop` tests pass (the `@patch`-style `TestRunLoop` mocks aren't affected by the signature change).

- [ ] **Step 5: Commit**

```bash
git add DhanHQ_src/loop_expiries/scraper.py tests/test_loop_scraper.py
git commit -m "refactor: symbol_cfg arg in scrape_single_expiry"
```

---

## Task 3: NIFTY parquet path migration (`run_parquet.py`)

**Files:**
- Modify: `DhanHQ_src/loop_expiries/run_parquet.py`

- [ ] **Step 1: Update GDRIVE_BASE and pass symbol_cfg**

In `DhanHQ_src/loop_expiries/run_parquet.py`:

- Line 45 — change:
```python
GDRIVE_BASE = "G:/My Drive/OptionsData"
```
to:
```python
GDRIVE_BASE = "G:/My Drive/OptionsData/NIFTY"
```

- After the existing `from DhanHQ_src.loop_expiries.scraper import (...)` block (around line 35–39), add:
```python
from DhanHQ_src.loop_expiries.config import NIFTY_SYMBOL_CFG
```

- Find the `scrape_single_expiry(dhan, expiry)` call (around line 165) and replace with:
```python
            rows, api_calls, empty_count = scrape_single_expiry(dhan, expiry, NIFTY_SYMBOL_CFG)
```

- Line 237 (help text) — change:
```python
                        help=f"Output path (default: {GDRIVE_BASE}/nifty_options_{{year}}.parquet)")
```
The expression already references `GDRIVE_BASE` so the help text auto-updates; no edit needed beyond Step 1's `GDRIVE_BASE` change. Verify by reading the file post-change.

- [ ] **Step 2: Smoke-import the module**

Run: `python -c "from DhanHQ_src.loop_expiries import run_parquet; print(run_parquet.GDRIVE_BASE)"`
Expected: `G:/My Drive/OptionsData/NIFTY`

- [ ] **Step 3: Verify no test regression**

Run: `python -m pytest tests/test_loop_scraper.py -v`
Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add DhanHQ_src/loop_expiries/run_parquet.py
git commit -m "feat: NIFTY parquet path under NIFTY/ subfolder"
```

---

## Task 4: NIFTY parquet path migration (`update_2026_parquet.py`)

**Files:**
- Modify: `DhanHQ_src/loop_expiries/update_2026_parquet.py`

- [ ] **Step 1: Update GDRIVE_BASE and pass symbol_cfg**

In `DhanHQ_src/loop_expiries/update_2026_parquet.py`:

- Line 77 — change:
```python
GDRIVE_BASE = "G:/My Drive/OptionsData"
```
to:
```python
GDRIVE_BASE = "G:/My Drive/OptionsData/NIFTY"
```

- After the existing `from DhanHQ_src.loop_expiries.scraper import (...)` block (around line 65–70), add:
```python
from DhanHQ_src.loop_expiries.config import NIFTY_SYMBOL_CFG
```

- Find the `scrape_single_expiry(dhan, expiry)` call (around line 293) and replace with:
```python
            rows, api_calls, empty_count = scrape_single_expiry(dhan, expiry, NIFTY_SYMBOL_CFG)
```

- [ ] **Step 2: Smoke-import the module**

Run: `python -c "from DhanHQ_src.loop_expiries import update_2026_parquet as u; print(u.GDRIVE_BASE, u._parquet_path(2026))"`
Expected: `G:/My Drive/OptionsData/NIFTY G:/My Drive/OptionsData/NIFTY/nifty_options_2026.parquet`

- [ ] **Step 3: Dry-run to confirm wiring**

This requires `pandas` and the existing 2026 NIFTY parquet to be at the new location. Run only if both are in place locally:

Run: `python -m DhanHQ_src.loop_expiries.update_2026_parquet --dry-run`
Expected: prints "Existing: N rows | M (date,flag) combos | latest expiry: ..." then "Classification: ... missing, ... refresh, ..." then "Dry-run mode -- exiting without scraping."

If parquet is not present locally, skip this step; it will be exercised in Task 8's acceptance.

- [ ] **Step 4: Commit**

```bash
git add DhanHQ_src/loop_expiries/update_2026_parquet.py
git commit -m "feat: NIFTY updater uses NIFTY/ path"
```

---

## Task 5: SENSEX module scaffold + config

**Files:**
- Create: `DhanHQ_src/Sensex_Scraping/__init__.py`
- Create: `DhanHQ_src/Sensex_Scraping/config.py`

- [ ] **Step 1: Create empty package init**

Create `DhanHQ_src/Sensex_Scraping/__init__.py` with content:

```python
"""SENSEX parquet scraping pipeline.

Mirrors the NIFTY parquet flow under DhanHQ_src/loop_expiries/.
Output: G:/My Drive/OptionsData/SENSEX/sensex_options_{year}.parquet
"""
```

- [ ] **Step 2: Create SENSEX config**

Create `DhanHQ_src/Sensex_Scraping/config.py`:

```python
"""Configuration for SENSEX parquet scraping."""

from DhanHQ_src.loop_expiries.config import LOOP_STRIKES, LOOP_OPTION_TYPES

# DhanHQ rolling-option API parameters for SENSEX index options.
# Verified against Dhan scrip master (BSE,I,51,INDEX,...,SENSEX) and
# 2026-04-30 BSE FO bhavcopy (FinInstrmTp=IDO, NewBrdLotQty=20).
SENSEX_SYMBOL_CFG = {
    "security_id": 51,
    "exchange_segment": "BSE_FNO",
    "instrument": "OPTIDX",
}

# Same strike grid as NIFTY: ATM-10 through ATM+10 (21 strikes).
# DhanHQ resolves the 100-pt SENSEX strike spacing server-side.
SENSEX_STRIKES = LOOP_STRIKES
SENSEX_OPTION_TYPES = LOOP_OPTION_TYPES

# Output paths
GDRIVE_BASE = "G:/My Drive/OptionsData/SENSEX"
PARQUET_FILENAME_TEMPLATE = "sensex_options_{year}.parquet"
PROGRESS_FILENAME_TEMPLATE = "sensex_options_{year}_progress.json"

# BSE Bhavcopy (per-day CSV — not zipped, unlike NSE's new format).
BSE_BHAVCOPY_URL = (
    "https://www.bseindia.com/download/Bhavcopy/Derivative/"
    "BhavCopy_BSE_FO_0_0_0_{yyyymmdd}_F_0000.CSV"
)
BSE_BHAVCOPY_CACHE_PREFIX = "bse_fo_"
BSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    ),
    "Referer": "https://www.bseindia.com/",
}
```

- [ ] **Step 3: Smoke-import**

Run: `python -c "from DhanHQ_src.Sensex_Scraping.config import SENSEX_SYMBOL_CFG, GDRIVE_BASE; print(SENSEX_SYMBOL_CFG, GDRIVE_BASE)"`
Expected: `{'security_id': 51, 'exchange_segment': 'BSE_FNO', 'instrument': 'OPTIDX'} G:/My Drive/OptionsData/SENSEX`

- [ ] **Step 4: Commit**

```bash
git add DhanHQ_src/Sensex_Scraping/__init__.py DhanHQ_src/Sensex_Scraping/config.py
git commit -m "feat: SENSEX scaffold and config"
```

---

## Task 6: SENSEX expiry fetcher (TDD)

**Files:**
- Create: `tests/sensex/__init__.py`
- Create: `tests/sensex/fixtures/bse_fo_sample.csv`
- Create: `tests/sensex/test_sensex_expiry_fetcher.py`
- Create: `DhanHQ_src/Sensex_Scraping/expiry_fetcher.py`

- [ ] **Step 1: Create test fixture (real BSE bhavcopy schema)**

Create `tests/sensex/__init__.py` with empty content.

Create `tests/sensex/fixtures/bse_fo_sample.csv` with content (header + 6 representative rows: 3 SENSEX options, 1 SENSEX future, 1 BANKEX option, 1 stock option):

```csv
TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb,SctySrs,XpryDt,FininstrmActlXpryDt,StrkPric,OptnTp,FinInstrmNm,OpnPric,HghPric,LwPric,ClsPric,LastPric,PrvsClsgPric,UndrlygPric,SttlmPric,OpnIntrst,ChngInOpnIntrst,TtlTradgVol,TtlTrfVal,TtlNbOfTxsExctd,SsnId,NewBrdLotQty,Rmks,Rsvd1,Rsvd2,Rsvd3,Rsvd4
2026-04-30,2026-04-30,FO,BSE,IDO,842625,,SENSEX,,2026-06-25,2026-06-25,78000.00,CE,SENSEX26JUN78000CE,1500.00,1982.60,1500.00,1969.30,1963.40,2300.00,76913.50,1969.30,2320,1860,2940,234310908.00,143,F1,20,,,,,
2026-04-30,2026-04-30,FO,BSE,IDO,848544,,SENSEX,,2026-04-30,2026-04-30,75200.00,CE,SENSEX26APR75200CE,1545.00,2082.50,916.00,76913.50,1710.10,2043.90,76913.50,76913.50,2340,220,44500,3404461699.00,935,F1,20,,,,,
2026-04-30,2026-04-30,FO,BSE,IDO,850320,,SENSEX,,2026-05-07,2026-05-07,75200.00,PE,SENSEX26MAY75200PE,2.95,5.35,0.05,76913.50,0.05,5.55,76913.50,76913.50,504200,136520,28416860,2137016640133.00,173582,F1,20,,,,,
2026-04-30,2026-04-30,FO,BSE,IDF,842600,,SENSEX,,2026-05-27,2026-05-27,0.00,XX,SENSEX26MAYFUT,76900.00,77000.00,76800.00,76913.50,76913.50,76900.00,76913.50,76913.50,1000,200,500,38456750000.00,50,F1,20,,,,,
2026-04-30,2026-04-30,FO,BSE,IDO,849153,,BANKEX,,2026-04-30,2026-04-30,62400.00,CE,BANKEX26APR62400CE,199.30,199.30,0.05,61706.88,0.05,287.55,61706.88,61706.88,30990,17310,2059860,128553277747.50,23910,F1,30,,,,,
2026-04-30,2026-04-30,FO,BSE,STO,860001,,RELIANCE,,2026-05-29,2026-05-29,2900.00,CE,RELIANCE26MAY2900CE,12.00,18.00,11.00,2862.00,15.20,14.50,2862.00,2862.00,500,200,1000,12810000.00,30,F1,250,,,,,
```

This fixture covers: 3 SENSEX index options across 3 different XpryDt values (2026-06-25, 2026-04-30, 2026-05-07), 1 SENSEX future (must be ignored: `IDF`), 1 BANKEX option (must be ignored: wrong TckrSymb), 1 stock option (must be ignored: wrong FinInstrmTp).

- [ ] **Step 2: Write failing tests**

Create `tests/sensex/test_sensex_expiry_fetcher.py`:

```python
import os
from pathlib import Path

import pytest
from unittest.mock import patch, MagicMock

from DhanHQ_src.Sensex_Scraping.expiry_fetcher import (
    _extract_sensex_expiry_dates,
    _download_bse_bhavcopy,
    fetch_all_expiry_dates,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "bse_fo_sample.csv"


class TestExtractSensexExpiryDates:
    def test_extracts_only_sensex_index_options(self):
        result = _extract_sensex_expiry_dates(str(FIXTURE_PATH))
        # Three unique SENSEX IDO XpryDt values; futures, BANKEX, stock options excluded.
        assert result == ["2026-04-30", "2026-05-07", "2026-06-25"]

    def test_returns_sorted_unique(self):
        result = _extract_sensex_expiry_dates(str(FIXTURE_PATH))
        assert result == sorted(set(result))

    def test_missing_file_returns_empty(self, tmp_path):
        missing = tmp_path / "does_not_exist.csv"
        result = _extract_sensex_expiry_dates(str(missing))
        assert result == []


class TestDownloadBseBhavcopy:
    def test_returns_cached_path_when_exists(self, tmp_path, monkeypatch):
        # Pre-create a cached file in a temp BHAVCOPY_DIR.
        monkeypatch.setattr(
            "DhanHQ_src.Sensex_Scraping.expiry_fetcher.BHAVCOPY_DIR",
            str(tmp_path),
        )
        cached = tmp_path / "bse_fo_2026-04-30.csv"
        cached.write_text("TradDt,...,TckrSymb,...\nrow", encoding="utf-8")

        result = _download_bse_bhavcopy("2026-04-30")
        assert result == str(cached)

    @patch("DhanHQ_src.Sensex_Scraping.expiry_fetcher.requests.get")
    def test_html_404_response_returns_none(self, mock_get, tmp_path, monkeypatch):
        """BSE returns HTTP 200 with HTML body for missing dates — must be detected."""
        monkeypatch.setattr(
            "DhanHQ_src.Sensex_Scraping.expiry_fetcher.BHAVCOPY_DIR",
            str(tmp_path),
        )
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"<html><body>page not found</body></html>"
        mock_get.return_value = mock_resp

        result = _download_bse_bhavcopy("2020-01-01")
        assert result is None
        # Must NOT cache the HTML body.
        assert not (tmp_path / "bse_fo_2020-01-01.csv").exists()

    @patch("DhanHQ_src.Sensex_Scraping.expiry_fetcher.requests.get")
    def test_valid_csv_response_caches_and_returns_path(self, mock_get, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "DhanHQ_src.Sensex_Scraping.expiry_fetcher.BHAVCOPY_DIR",
            str(tmp_path),
        )
        # Real CSV starts with the bhavcopy header.
        csv_body = FIXTURE_PATH.read_bytes()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = csv_body
        mock_get.return_value = mock_resp

        result = _download_bse_bhavcopy("2026-04-30")
        assert result == str(tmp_path / "bse_fo_2026-04-30.csv")
        assert (tmp_path / "bse_fo_2026-04-30.csv").read_bytes() == csv_body


class TestFetchAllExpiryDates:
    @patch("DhanHQ_src.Sensex_Scraping.expiry_fetcher._download_bse_bhavcopy")
    def test_returns_classified_expiries(self, mock_download):
        # Make every probed date hit the same fixture, only succeed for 2026 dates
        # and only on the first attempt per month so the inner break fires.
        def _side(date_str):
            if date_str.startswith("2026-"):
                return str(FIXTURE_PATH)
            return None
        mock_download.side_effect = _side

        result = fetch_all_expiry_dates(2026)

        # Must contain WEEK entries for each 2026 SENSEX expiry from the fixture
        # and MONTH entries for the last date in each month present (4-30, 5-07, 6-25
        # — each is the last in its month within this fixture, so all become monthly).
        dates = sorted({(e["expiry_date"], e["expiry_flag"]) for e in result})
        assert ("2026-04-30", "WEEK") in dates
        assert ("2026-04-30", "MONTH") in dates
        assert ("2026-05-07", "WEEK") in dates
        assert ("2026-06-25", "WEEK") in dates
        # Schema check on one entry
        first = result[0]
        assert set(first.keys()) == {"expiry_date", "expiry_flag", "from_date", "to_date"}

    @patch("DhanHQ_src.Sensex_Scraping.expiry_fetcher._download_bse_bhavcopy", return_value=None)
    def test_no_bhavcopy_returns_empty(self, mock_download):
        result = fetch_all_expiry_dates(2022)
        assert result == []
```

- [ ] **Step 3: Run tests — expect import failure**

Run: `python -m pytest tests/sensex/test_sensex_expiry_fetcher.py -v`
Expected: tests fail at collection with `ModuleNotFoundError: No module named 'DhanHQ_src.Sensex_Scraping.expiry_fetcher'`.

- [ ] **Step 4: Implement the expiry fetcher**

Create `DhanHQ_src/Sensex_Scraping/expiry_fetcher.py`:

```python
"""Fetch SENSEX option expiry dates from BSE F&O Bhavcopy.

Single source of truth: BSE exchange data. No calendar math.
Walks one bhavcopy per month for the year, filters IDO/SENSEX rows,
collects unique XpryDt values, then classifies into WEEK/MONTH using
the symbol-agnostic classify_expiry_dates from loop_expiries.

Note: BSE returns HTTP 200 with an HTML 'page not found' body for
missing dates. We sniff the first bytes for the bhavcopy header to
distinguish real CSV from an HTML redirect.
"""

import csv
import logging
import os
import time
from datetime import date

import requests

from DhanHQ_src.config import BHAVCOPY_DIR
from DhanHQ_src.loop_expiries.expiry_fetcher import classify_expiry_dates
from DhanHQ_src.Sensex_Scraping.config import (
    BSE_BHAVCOPY_URL,
    BSE_BHAVCOPY_CACHE_PREFIX,
    BSE_HEADERS,
)

logger = logging.getLogger(__name__)


def _download_bse_bhavcopy(date_str: str) -> str | None:
    """Download BSE F&O bhavcopy for one date.

    Returns the cached CSV path or None if the date has no bhavcopy
    (BSE returns HTTP 200 with HTML for missing dates — we header-sniff).
    """
    os.makedirs(BHAVCOPY_DIR, exist_ok=True)
    cached = os.path.join(BHAVCOPY_DIR, f"{BSE_BHAVCOPY_CACHE_PREFIX}{date_str}.csv")
    if os.path.exists(cached):
        return cached

    try:
        dt = date.fromisoformat(date_str)
    except ValueError:
        return None
    yyyymmdd = dt.strftime("%Y%m%d")
    url = BSE_BHAVCOPY_URL.format(yyyymmdd=yyyymmdd)

    try:
        resp = requests.get(url, headers=BSE_HEADERS, timeout=30)
        if resp.status_code != 200 or not resp.content:
            return None
        # Sniff header — real bhavcopy CSV starts with these column names.
        head = resp.content[:200].decode("utf-8", errors="replace")
        if "TradDt" not in head or "TckrSymb" not in head:
            logger.debug("BSE returned non-bhavcopy body for %s", date_str)
            return None
        with open(cached, "wb") as f:
            f.write(resp.content)
        logger.info("Downloaded BSE bhavcopy: %s", cached)
        return cached
    except Exception as e:
        logger.debug("BSE bhavcopy download failed %s: %s", url, e)
        return None


def _extract_sensex_expiry_dates(csv_path: str) -> list[str]:
    """Filter IDO + SENSEX rows from a bhavcopy CSV; return sorted unique XpryDt."""
    out: set[str] = set()
    if not os.path.exists(csv_path):
        return []
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if (
                    row.get("FinInstrmTp", "").strip() == "IDO"
                    and row.get("TckrSymb", "").strip() == "SENSEX"
                ):
                    xpry = row.get("XpryDt", "").strip()
                    if xpry:
                        out.add(xpry[:10])
    except Exception as e:
        logger.error("Failed to parse BSE bhavcopy %s: %s", csv_path, e)
    return sorted(out)


def fetch_expiry_dates_from_bhavcopy(year: int) -> list[str]:
    """Walk one bhavcopy per month for the year; merge SENSEX expiries."""
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
                year_dates = [
                    d
                    for d in _extract_sensex_expiry_dates(csv_path)
                    if d.startswith(str(year))
                ]
                all_dates.update(year_dates)
                logger.info(
                    "BSE bhavcopy %s: found %d SENSEX expiry dates for %d",
                    target.isoformat(), len(year_dates), year,
                )
                break
            time.sleep(1)

    result = sorted(all_dates)
    logger.info("BSE Bhavcopy: %d unique SENSEX expiry dates for %d", len(result), year)
    return result


def fetch_all_expiry_dates(year: int) -> list[dict]:
    """Fetch + classify all SENSEX expiry dates for a given year.

    Returns a list of dicts with keys expiry_date, expiry_flag, from_date,
    to_date — same shape as the NIFTY loop_expiries fetcher.
    """
    raw_dates = fetch_expiry_dates_from_bhavcopy(year)
    return classify_expiry_dates(raw_dates, year)
```

- [ ] **Step 5: Run tests — expect pass**

Run: `python -m pytest tests/sensex/test_sensex_expiry_fetcher.py -v`
Expected: all 8 tests pass.

- [ ] **Step 6: Commit**

```bash
git add tests/sensex/__init__.py tests/sensex/fixtures/bse_fo_sample.csv tests/sensex/test_sensex_expiry_fetcher.py DhanHQ_src/Sensex_Scraping/expiry_fetcher.py
git commit -m "feat: SENSEX expiry fetcher (BSE bhavcopy)"
```

---

## Task 7: SENSEX `run_parquet.py` (full-year backfill)

**Files:**
- Create: `DhanHQ_src/Sensex_Scraping/run_parquet.py`

- [ ] **Step 1: Create the runner**

Create `DhanHQ_src/Sensex_Scraping/run_parquet.py`:

```python
"""Run SENSEX loop expiries for a year, output to a single parquet file.

Resilient design (mirrors NIFTY's loop_expiries/run_parquet.py):
  - Saves progress to a local staging parquet after EVERY expiry
  - Tracks completed expiries in a JSON sidecar for resume support
  - Final output copied to GDrive only after all expiries are done
  - If the process dies, re-run the same command to resume

Usage:
    python -m DhanHQ_src.Sensex_Scraping.run_parquet --year 2025
    python -m DhanHQ_src.Sensex_Scraping.run_parquet --year 2025 --reset
    python -m DhanHQ_src.Sensex_Scraping.run_parquet --year 2025 --test
"""

import argparse
import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timezone, timedelta

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from DhanHQ_src.auth import get_access_token
from DhanHQ_src.fetcher import create_dhan_client
from DhanHQ_src.loop_expiries.scraper import (
    scrape_single_expiry,
    _fmt_duration,
    _progress_bar,
)
from DhanHQ_src.Sensex_Scraping.expiry_fetcher import fetch_all_expiry_dates
from DhanHQ_src.Sensex_Scraping.config import (
    SENSEX_SYMBOL_CFG,
    GDRIVE_BASE,
    PARQUET_FILENAME_TEMPLATE,
    PROGRESS_FILENAME_TEMPLATE,
)

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
_TOKEN_MAX_AGE_S = 20 * 3600
STAGING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "staging")


def _staging_paths(year: int):
    base = os.path.abspath(STAGING_DIR)
    return (
        os.path.join(base, PARQUET_FILENAME_TEMPLATE.format(year=year)),
        os.path.join(base, PROGRESS_FILENAME_TEMPLATE.format(year=year)),
    )


def _load_progress(progress_path: str) -> dict:
    if os.path.exists(progress_path):
        with open(progress_path, "r") as f:
            return json.load(f)
    return {
        "completed_keys": [],
        "stats": {"completed": 0, "failed": 0, "skipped": 0, "total_rows": 0},
    }


def _save_progress(progress_path: str, progress: dict):
    with open(progress_path, "w") as f:
        json.dump(progress, f)


def _append_to_parquet(staging_parquet: str, rows: list[dict]):
    new_df = pd.DataFrame(rows)
    if os.path.exists(staging_parquet):
        existing_df = pd.read_parquet(staging_parquet)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined = new_df
    combined.to_parquet(staging_parquet, index=False, engine="pyarrow")


def _print_progress(i, total, exp_date, exp_flag, status, rows, dur, eta_s):
    pct = i / total * 100 if total > 0 else 0
    bar = _progress_bar(i, total)
    eta = _fmt_duration(eta_s) if eta_s > 0 else "--"
    flag = "W" if exp_flag == "WEEK" else "M"
    if status == "skipped":
        detail = "SKIP"
    elif status == "failed":
        detail = "FAIL"
    elif status == "resumed":
        detail = "DONE"
    else:
        detail = f"{rows:,}r"
    line = (
        f"  {pct:5.1f}% {bar} {i}/{total} | {exp_date} {flag} | "
        f"{detail} | {_fmt_duration(dur)} | ETA {eta}"
    )
    print(f"\r{line}", end="", flush=True)


def run_to_parquet(year: int, output_path: str, test_mode: bool = False, reset: bool = False):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    staging_parquet, progress_path = _staging_paths(year)
    os.makedirs(os.path.dirname(staging_parquet), exist_ok=True)

    if reset:
        for f in (staging_parquet, progress_path):
            if os.path.exists(f):
                os.remove(f)
        print("  Reset: cleared staging data.", flush=True)

    token = get_access_token()
    token_time = time.time()
    dhan = create_dhan_client(token)

    expiries = fetch_all_expiry_dates(year)
    if not expiries:
        print(f"ERROR: No SENSEX expiry dates found for {year}.")
        sys.exit(1)

    if test_mode:
        expiries = expiries[:2]
        print("  TEST MODE: scraping first 2 expiries only", flush=True)

    total = len(expiries)
    weeks = sum(1 for e in expiries if e["expiry_flag"] == "WEEK")
    months = sum(1 for e in expiries if e["expiry_flag"] == "MONTH")

    progress = _load_progress(progress_path)
    done_keys = set(tuple(k) for k in progress["completed_keys"])
    stats = progress["stats"]
    resumed_count = len(done_keys)

    if resumed_count > 0:
        print(
            f"  Resuming: {resumed_count} expiries already done, "
            f"{total - resumed_count} remaining",
            flush=True,
        )

    print(
        f"  SENSEX Loop Expiries -> Parquet -- {year} | "
        f"{total} expiries ({weeks}W + {months}M)",
        flush=True,
    )

    start_time = time.time()

    for idx, expiry in enumerate(expiries, 1):
        exp_date = expiry["expiry_date"]
        exp_flag = expiry["expiry_flag"]
        key = (exp_date, exp_flag)

        if key in done_keys:
            _print_progress(idx, total, exp_date, exp_flag, "resumed", 0, 0, 0)
            continue

        if time.time() - token_time > _TOKEN_MAX_AGE_S:
            token = get_access_token()
            token_time = time.time()
            dhan = create_dhan_client(token)

        expiry_start = time.time()
        try:
            rows, api_calls, empty_count = scrape_single_expiry(
                dhan, expiry, SENSEX_SYMBOL_CFG
            )
            dur = time.time() - expiry_start

            pending = total - idx
            done_in_session = idx - resumed_count
            elapsed = time.time() - start_time
            avg = elapsed / max(done_in_session, 1)
            eta_s = avg * pending

            if empty_count == api_calls:
                stats["skipped"] += 1
                _print_progress(idx, total, exp_date, exp_flag, "skipped", 0, dur, eta_s)
            else:
                for r in rows:
                    r["expiry_flag"] = exp_flag
                _append_to_parquet(staging_parquet, rows)
                stats["completed"] += 1
                stats["total_rows"] += len(rows)
                _print_progress(idx, total, exp_date, exp_flag, "ok", len(rows), dur, eta_s)

            done_keys.add(key)
            progress["completed_keys"] = [list(k) for k in done_keys]
            progress["stats"] = stats
            _save_progress(progress_path, progress)

        except Exception as e:
            dur = time.time() - expiry_start
            pending = total - idx
            done_in_session = idx - resumed_count
            elapsed = time.time() - start_time
            avg = elapsed / max(done_in_session, 1)
            eta_s = avg * pending
            stats["failed"] += 1
            _print_progress(idx, total, exp_date, exp_flag, "failed", 0, dur, eta_s)
            logger.error("FAILED %s %s: %s", exp_date, exp_flag, e)
            progress["stats"] = stats
            _save_progress(progress_path, progress)

    print()
    elapsed_total = time.time() - start_time

    if not os.path.exists(staging_parquet):
        print("No data collected -- nothing to write.")
        sys.exit(1)

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    shutil.copy2(staging_parquet, output_path)

    size_mb = os.path.getsize(output_path) / 1024 / 1024
    df = pd.read_parquet(output_path)

    print(
        f"  DONE: {stats['completed']}ok {stats['failed']}fail {stats['skipped']}skip | "
        f"{stats['total_rows']:,} rows | {_fmt_duration(elapsed_total)}"
    )
    print(f"  Output: {output_path} ({size_mb:.1f} MB)")
    print(
        f"  Unique expiry dates: {df['expiry_date'].nunique()} | "
        f"Columns: {list(df.columns)}"
    )

    os.remove(staging_parquet)
    os.remove(progress_path)
    print("  Staging files cleaned up.", flush=True)


def main():
    parser = argparse.ArgumentParser(description="SENSEX Loop Expiries -> Parquet")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help=f"Output path (default: {GDRIVE_BASE}/sensex_options_{{year}}.parquet)",
    )
    parser.add_argument(
        "--test", action="store_true", help="Test mode: scrape first 2 expiries only"
    )
    parser.add_argument(
        "--reset", action="store_true", help="Wipe staging progress and start fresh"
    )
    args = parser.parse_args()

    output = args.output or os.path.join(
        GDRIVE_BASE, PARQUET_FILENAME_TEMPLATE.format(year=args.year)
    )
    run_to_parquet(args.year, output, test_mode=args.test, reset=args.reset)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Smoke-import**

Run: `python -c "from DhanHQ_src.Sensex_Scraping import run_parquet; print(run_parquet.GDRIVE_BASE)"`
Expected: `G:/My Drive/OptionsData/SENSEX`

- [ ] **Step 3: Help text smoke test**

Run: `python -m DhanHQ_src.Sensex_Scraping.run_parquet --help`
Expected: argparse output mentioning `--year`, `--test`, `--reset`, default output path under `G:/My Drive/OptionsData/SENSEX/sensex_options_{year}.parquet`.

- [ ] **Step 4: Commit**

```bash
git add DhanHQ_src/Sensex_Scraping/run_parquet.py
git commit -m "feat: SENSEX run_parquet runner"
```

---

## Task 8: SENSEX `update_2026_parquet.py` (current-year incremental)

**Files:**
- Create: `DhanHQ_src/Sensex_Scraping/update_2026_parquet.py`

- [ ] **Step 1: Create the updater**

Create `DhanHQ_src/Sensex_Scraping/update_2026_parquet.py`:

```python
"""Update the SENSEX year parquet with missing and incomplete expiries.

Mirrors DhanHQ_src/loop_expiries/update_2026_parquet.py with SENSEX
paths and symbol_cfg. Algorithm and resume semantics are identical:
  1. Load existing parquet (or staging if a prior run crashed).
  2. Build set of (expiry_date, expiry_flag) already present and
     per-key actual_end_date from the stored rows.
  3. Fetch all SENSEX expiries for the target year from BSE Bhavcopy.
  4. Classify into missing / refresh / done / future.
  5. For each refresh expiry, drop existing rows then re-scrape.
  6. For each missing expiry, scrape and append.
  7. After every successful expiry, flush staging parquet (crash-safe).
  8. On completion, copy staging -> GDrive and clean up staging.

Usage:
    python -m DhanHQ_src.Sensex_Scraping.update_2026_parquet
    python -m DhanHQ_src.Sensex_Scraping.update_2026_parquet --dry-run
    python -m DhanHQ_src.Sensex_Scraping.update_2026_parquet --year 2026
    python -m DhanHQ_src.Sensex_Scraping.update_2026_parquet --no-refresh
"""

import argparse
import logging
import os
import shutil
import sys
import time
from datetime import date, timezone, timedelta

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# Optional: pick up DhanHQ creds from project-local .env when invoked
# without env vars pre-set in the shell. Same pattern as NIFTY updater.
try:
    from dotenv import load_dotenv

    _env_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", ".env")
    )
    if os.path.exists(_env_path):
        load_dotenv(_env_path)
except ImportError:
    pass

from DhanHQ_src.auth import get_access_token
from DhanHQ_src.fetcher import create_dhan_client
from DhanHQ_src.loop_expiries.scraper import (
    scrape_single_expiry,
    _fmt_duration,
    _progress_bar,
)
from DhanHQ_src.Sensex_Scraping.expiry_fetcher import fetch_all_expiry_dates
from DhanHQ_src.Sensex_Scraping.config import (
    SENSEX_SYMBOL_CFG,
    GDRIVE_BASE,
    PARQUET_FILENAME_TEMPLATE,
)

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
_TOKEN_MAX_AGE_S = 20 * 3600
_SCRAPE_LOOKBACK_DAYS = 14
STAGING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "staging")


def _parquet_path(year: int) -> str:
    return os.path.join(GDRIVE_BASE, PARQUET_FILENAME_TEMPLATE.format(year=year))


def _staging_path(year: int) -> str:
    base = os.path.abspath(STAGING_DIR)
    return os.path.join(
        base, PARQUET_FILENAME_TEMPLATE.format(year=year).replace(".parquet", "_update.parquet")
    )


def _print_progress(i, total, exp_date, exp_flag, mode, status, rows, dur, eta_s):
    pct = i / total * 100 if total > 0 else 0
    bar = _progress_bar(i, total)
    eta = _fmt_duration(eta_s) if eta_s > 0 else "--"
    flag = "W" if exp_flag == "WEEK" else "M"
    tag = "NEW" if mode == "missing" else "RFR"
    if status == "skipped":
        detail = "SKIP"
    elif status == "failed":
        detail = "FAIL"
    elif status == "no-change":
        detail = "SAME"
    else:
        detail = f"{rows:,}r"
    line = (
        f"  {pct:5.1f}% {bar} {i}/{total} | {exp_date} {flag} {tag} | "
        f"{detail} | {_fmt_duration(dur)} | ETA {eta}"
    )
    print(f"\r{line}", end="", flush=True)


def _classify_expiries(all_expiries, present_keys, today, actual_end_by_key=None):
    """Same logic as NIFTY's updater — split into missing / refresh / done / future."""
    missing, refresh, done, future = [], [], [], []
    window = timedelta(days=_SCRAPE_LOOKBACK_DAYS)
    actual_end_by_key = actual_end_by_key or {}
    for e in all_expiries:
        key = (e["expiry_date"], e["expiry_flag"])
        exp_dt = date.fromisoformat(e["expiry_date"])
        if key in present_keys:
            expected_end = min(exp_dt, today)
            actual_end_str = actual_end_by_key.get(key)
            actual_end = (
                date.fromisoformat(actual_end_str) if actual_end_str else None
            )
            if exp_dt >= today or (
                actual_end is not None and actual_end < expected_end
            ):
                refresh.append(e)
            else:
                done.append(e)
        else:
            if exp_dt - window <= today:
                missing.append(e)
            else:
                future.append(e)
    return missing, refresh, done, future


def update_parquet(year: int, dry_run: bool = False, no_refresh: bool = False):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parquet_path = _parquet_path(year)
    staging_path = _staging_path(year)
    os.makedirs(os.path.dirname(staging_path), exist_ok=True)

    if not os.path.exists(parquet_path):
        print(f"ERROR: {parquet_path} does not exist. Run run_parquet.py first.")
        sys.exit(1)

    if os.path.exists(staging_path):
        print(f"  Resuming from existing staging file: {staging_path}", flush=True)
        source_path = staging_path
        staging_is_fresh = False
    else:
        source_path = parquet_path
        staging_is_fresh = True

    print(f"  Loading existing data from: {source_path}", flush=True)
    existing_df = pd.read_parquet(source_path)

    present_keys = set(
        zip(
            existing_df["expiry_date"].astype(str),
            existing_df["expiry_flag"].astype(str),
        )
    )
    _end_series = (
        existing_df.assign(
            _k=list(
                zip(
                    existing_df["expiry_date"].astype(str),
                    existing_df["expiry_flag"].astype(str),
                )
            ),
            _d=existing_df["date"].astype(str),
        )
        .groupby("_k")["_d"]
        .max()
    )
    actual_end_by_key = _end_series.to_dict()

    latest_date = existing_df["expiry_date"].astype(str).max()
    print(
        f"  Existing: {len(existing_df):,} rows | {len(present_keys)} (date,flag) combos "
        f"| latest expiry: {latest_date}",
        flush=True,
    )

    print(f"  Fetching SENSEX expiry dates for {year} from BSE Bhavcopy...", flush=True)
    all_expiries = fetch_all_expiry_dates(year)
    if not all_expiries:
        print(f"ERROR: No SENSEX expiry dates found for {year}.")
        sys.exit(1)

    today = date.today()
    missing, refresh, done, future = _classify_expiries(
        all_expiries, present_keys, today, actual_end_by_key=actual_end_by_key
    )
    if no_refresh:
        refresh = []

    print(
        f"  Classification: {len(missing)} missing, {len(refresh)} refresh, "
        f"{len(done)} done, {len(future)} future (window not open)",
        flush=True,
    )

    work = [("refresh", e) for e in refresh] + [("missing", e) for e in missing]

    if not work:
        print("  Parquet already up to date -- nothing to scrape.", flush=True)
        if os.path.exists(staging_path) and not staging_is_fresh:
            os.remove(staging_path)
        return

    total = len(work)
    w_miss = sum(1 for e in missing if e["expiry_flag"] == "WEEK")
    m_miss = sum(1 for e in missing if e["expiry_flag"] == "MONTH")
    w_ref = sum(1 for e in refresh if e["expiry_flag"] == "WEEK")
    m_ref = sum(1 for e in refresh if e["expiry_flag"] == "MONTH")
    print(
        f"  {total} expiries to process "
        f"(NEW: {w_miss}W+{m_miss}M | RFR: {w_ref}W+{m_ref}M):",
        flush=True,
    )
    for mode, e in work:
        tag = "NEW" if mode == "missing" else "RFR"
        print(f"    - {e['expiry_date']} {e['expiry_flag']} [{tag}]", flush=True)

    if dry_run:
        print("  Dry-run mode -- exiting without scraping.", flush=True)
        return

    if staging_is_fresh:
        print(f"  Copying baseline parquet to staging: {staging_path}", flush=True)
        shutil.copy2(parquet_path, staging_path)

    token = get_access_token()
    token_time = time.time()
    dhan = create_dhan_client(token)

    combined_df = existing_df
    stats = {
        "new_ok": 0,
        "refresh_ok": 0,
        "refresh_unchanged": 0,
        "failed": 0,
        "skipped": 0,
        "rows_added": 0,
        "rows_refreshed_delta": 0,
    }
    start_time = time.time()

    for idx, (mode, expiry) in enumerate(work, 1):
        exp_date = expiry["expiry_date"]
        exp_flag = expiry["expiry_flag"]

        if time.time() - token_time > _TOKEN_MAX_AGE_S:
            token = get_access_token()
            token_time = time.time()
            dhan = create_dhan_client(token)

        expiry_start = time.time()
        try:
            rows, api_calls, empty_count = scrape_single_expiry(
                dhan, expiry, SENSEX_SYMBOL_CFG
            )
            dur = time.time() - expiry_start

            elapsed = time.time() - start_time
            avg = elapsed / max(idx, 1)
            eta_s = avg * (total - idx)

            if empty_count == api_calls:
                stats["skipped"] += 1
                _print_progress(idx, total, exp_date, exp_flag, mode, "skipped", 0, dur, eta_s)
            else:
                for r in rows:
                    r["expiry_flag"] = exp_flag
                new_df = pd.DataFrame(rows)

                if mode == "refresh":
                    mask = ~(
                        (combined_df["expiry_date"].astype(str) == exp_date)
                        & (combined_df["expiry_flag"].astype(str) == exp_flag)
                    )
                    old_count = (~mask).sum()
                    combined_df = combined_df[mask].reset_index(drop=True)
                    combined_df = pd.concat(
                        [combined_df, new_df], ignore_index=True
                    )
                    delta = len(new_df) - int(old_count)
                    if delta == 0:
                        stats["refresh_unchanged"] += 1
                        status = "no-change"
                    else:
                        stats["refresh_ok"] += 1
                        stats["rows_refreshed_delta"] += delta
                        status = "ok"
                    combined_df.to_parquet(staging_path, index=False, engine="pyarrow")
                    _print_progress(
                        idx, total, exp_date, exp_flag, mode, status,
                        len(new_df), dur, eta_s,
                    )
                else:  # missing
                    combined_df = pd.concat(
                        [combined_df, new_df], ignore_index=True
                    )
                    combined_df.to_parquet(staging_path, index=False, engine="pyarrow")
                    stats["new_ok"] += 1
                    stats["rows_added"] += len(new_df)
                    _print_progress(
                        idx, total, exp_date, exp_flag, mode, "ok",
                        len(new_df), dur, eta_s,
                    )

        except Exception as e:
            dur = time.time() - expiry_start
            elapsed = time.time() - start_time
            avg = elapsed / max(idx, 1)
            eta_s = avg * (total - idx)
            stats["failed"] += 1
            _print_progress(idx, total, exp_date, exp_flag, mode, "failed", 0, dur, eta_s)
            logger.error("FAILED %s %s: %s", exp_date, exp_flag, e)

    print()
    elapsed_total = time.time() - start_time

    any_change = (
        stats["new_ok"] > 0
        or stats["refresh_ok"] > 0
        or stats["rows_refreshed_delta"] != 0
    )
    if not any_change:
        print("  No changes to parquet (all skipped / unchanged).", flush=True)
        if staging_is_fresh and os.path.exists(staging_path):
            os.remove(staging_path)
        return

    print(f"  Copying updated staging -> {parquet_path}", flush=True)
    shutil.copy2(staging_path, parquet_path)

    size_mb = os.path.getsize(parquet_path) / 1024 / 1024
    final_df = pd.read_parquet(parquet_path)

    print(
        f"  DONE: NEW {stats['new_ok']}ok (+{stats['rows_added']:,} rows) | "
        f"RFR {stats['refresh_ok']}ok {stats['refresh_unchanged']}same "
        f"(delta {stats['rows_refreshed_delta']:+,} rows) | "
        f"{stats['failed']}fail {stats['skipped']}skip | "
        f"{_fmt_duration(elapsed_total)}"
    )
    print(
        f"  Output: {parquet_path} ({size_mb:.1f} MB, {len(final_df):,} total rows)"
    )
    print(
        f"  Unique expiry dates: {final_df['expiry_date'].nunique()} | "
        f"Latest: {final_df['expiry_date'].astype(str).max()}"
    )

    os.remove(staging_path)
    print("  Staging file cleaned up.", flush=True)


def main():
    parser = argparse.ArgumentParser(
        description="Update SENSEX year parquet with missing expiries (default: 2026)"
    )
    parser.add_argument(
        "--year", type=int, default=2026, help="Year to update (default: 2026)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="List work items without scraping"
    )
    parser.add_argument(
        "--no-refresh",
        action="store_true",
        help="Skip re-scraping in-parquet future expiries (only add missing ones)",
    )
    args = parser.parse_args()
    update_parquet(args.year, dry_run=args.dry_run, no_refresh=args.no_refresh)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Smoke-import**

Run: `python -c "from DhanHQ_src.Sensex_Scraping import update_2026_parquet as u; print(u.GDRIVE_BASE, u._parquet_path(2026))"`
Expected: `G:/My Drive/OptionsData/SENSEX G:/My Drive/OptionsData/SENSEX/sensex_options_2026.parquet`

- [ ] **Step 3: Help text smoke test**

Run: `python -m DhanHQ_src.Sensex_Scraping.update_2026_parquet --help`
Expected: argparse output mentioning `--year`, `--dry-run`, `--no-refresh`.

- [ ] **Step 4: Commit**

```bash
git add DhanHQ_src/Sensex_Scraping/update_2026_parquet.py
git commit -m "feat: SENSEX update_2026_parquet"
```

---

## Task 9: End-to-end verification

**Files:** none modified — purely verification.

- [ ] **Step 1: Full unit-test suite**

Run: `python -m pytest tests/ -v --ignore=tests/supabase`
Expected: all tests pass (existing NIFTY tests + new SENSEX tests). No skipped tests except those that require live API keys.

- [ ] **Step 2: Verify NIFTY scripts still importable and paths correct**

Run:
```bash
python -c "from DhanHQ_src.loop_expiries import run_parquet, update_2026_parquet; \
print('NIFTY run_parquet:', run_parquet.GDRIVE_BASE); \
print('NIFTY update_2026:', update_2026_parquet.GDRIVE_BASE)"
```
Expected: both print `G:/My Drive/OptionsData/NIFTY`.

- [ ] **Step 3: Verify SENSEX scripts importable and paths correct**

Run:
```bash
python -c "from DhanHQ_src.Sensex_Scraping import run_parquet, update_2026_parquet; \
print('SENSEX run_parquet:', run_parquet.GDRIVE_BASE); \
print('SENSEX update_2026:', update_2026_parquet.GDRIVE_BASE)"
```
Expected: both print `G:/My Drive/OptionsData/SENSEX`.

- [ ] **Step 4: Existing main pipeline still importable**

Run: `python -c "from DhanHQ_src import main; from DhanHQ_src.fetcher import fetch_all_options_data, fetch_iv_baseline; from DhanHQ_src.loop_expiries.runner import main as runner_main"`
Expected: no errors.

- [ ] **Step 5: Optional live SENSEX test (manual, requires Dhan BSE_FNO subscription)**

Skip this step in CI / agentic execution. Run only as a manual sanity check after local merge:

```bash
python -m DhanHQ_src.Sensex_Scraping.run_parquet --year 2025 --test
```
Expected: scrapes 2 SENSEX expiries from 2025; produces a parquet under `G:/My Drive/OptionsData/SENSEX/sensex_options_2025.parquet` with the 16-column NIFTY-equivalent schema, SENSEX strike values (~75–80k), CE/PE balanced, atm_offset in `-10..+10`. If you see HTTP 401 / DH-902, your Dhan subscription does not include BSE_FNO — request the entitlement from Dhan support.

- [ ] **Step 6: Final commit (if anything was tweaked during verification)**

If Steps 1-4 surfaced anything that needs fixing, fix and commit; otherwise no-op.

```bash
git status
# If clean, no commit. If dirty:
# git add <files> && git commit -m "fix: <what>"
```

---

## Self-Review

**Spec coverage check:**

| Spec section | Task |
|---|---|
| §5.2 Refactor `scrape_single_expiry` | Task 2 |
| §5.3 All callers updated (5 callers in audit table) | Task 1 (config), Task 2 (scraper internal + tests), Task 3 (NIFTY run_parquet), Task 4 (NIFTY updater) |
| §5.4 NIFTY parquet path migration | Tasks 3, 4 |
| §6.1 SENSEX `config.py` | Task 5 |
| §6.2 SENSEX `expiry_fetcher.py` | Task 6 (TDD) |
| §6.3 SENSEX `run_parquet.py` | Task 7 |
| §6.4 SENSEX `update_2026_parquet.py` | Task 8 |
| §7 Parquet schema (16 cols) | Inherited automatically — same `scrape_single_expiry` output |
| §8 Tests | Task 2 (loop scraper tests), Task 6 (sensex fetcher tests) |
| §9 Risks: BSE HTML 200 sniff | Task 6 Step 4 implementation + Task 6 Step 2 test `test_html_404_response_returns_none` |
| §11 Acceptance criteria 1-4 | Task 9 Steps 2, 3, 5 |
| §11 Acceptance criterion 5 (pytest) | Task 9 Step 1 |
| §11 Acceptance criterion 6 (--dry-run) | Task 8 (built into the updater) |

**Placeholder scan:** No "TBD"/"TODO"/"add appropriate"/"similar to Task N"/"write tests for the above" present. All code blocks are complete.

**Type / signature consistency:**
- `scrape_single_expiry(dhan, expiry, symbol_cfg, strikes=None)` — signature consistent across Tasks 2 (definition), 2 (run_loop call), 3 (NIFTY run_parquet call), 4 (NIFTY updater call), 7 (SENSEX run_parquet call), 8 (SENSEX updater call), and tests in Task 2.
- `symbol_cfg` keys (`security_id`, `exchange_segment`, `instrument`) consistent in `NIFTY_SYMBOL_CFG` (Task 1), `SENSEX_SYMBOL_CFG` (Task 5), and the function body access (Task 2).
- Helper imports `_fmt_duration`, `_progress_bar` from `DhanHQ_src.loop_expiries.scraper` consistent in Tasks 7 and 8.
- `fetch_all_expiry_dates(year) -> list[dict]` shape consistent: each dict has keys `expiry_date`, `expiry_flag`, `from_date`, `to_date` (Task 6 implementation matches NIFTY's contract).
- `_parquet_path(year)` and `_staging_path(year)` helpers in Task 8 use `PARQUET_FILENAME_TEMPLATE` from Task 5 — names match.
- `BSE_BHAVCOPY_URL`, `BSE_BHAVCOPY_CACHE_PREFIX`, `BSE_HEADERS` defined in Task 5 and used in Task 6 — names match.
