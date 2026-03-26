# NIFTY Options Historical Data Scraper — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scrape NIFTY 30MAR2026 expiry options data (8 ATM±4 strikes, 1-min intraday, March 16-30) from DhanHQ's Expired Options API, store in SQLite, compute derived metrics, and verify against NSE Bhavcopy.

**Architecture:** Pipeline of 5 modules — config, fetcher (DhanHQ API client), db (SQLite schema/CRUD), calculator (derived/aggregate metrics), verifier (NSE Bhavcopy comparison). Orchestrated by main.py in sequence: fetch → store → calculate → verify.

**Tech Stack:** Python 3.11+, dhanhq SDK, pandas, scipy, requests, python-dotenv, SQLite3

**Spec:** `docs/superpowers/specs/2026-03-26-nifty-options-scraper-design.md`

---

## File Map

| File | Responsibility |
|------|---------------|
| `DhanHQ_src/config.py` | API credentials (from .env), date range, strike list, constants |
| `DhanHQ_src/db.py` | SQLite connection, schema creation (4 tables + iv_history), insert/query helpers |
| `DhanHQ_src/fetcher.py` | DhanHQ API client — fetch expired options data & IV baseline |
| `DhanHQ_src/calculator.py` | Compute derived_metrics, aggregate_metrics, IVR/IVP, Max Pain, Fair Price |
| `DhanHQ_src/verifier.py` | Download NSE Bhavcopy, compare EOD values, log to verification_log |
| `DhanHQ_src/main.py` | CLI orchestrator — run full pipeline |
| `DhanHQ_src/requirements.txt` | Python dependencies |
| `.env` | DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN |
| `.gitignore` | Ignore .env, data/*.db, __pycache__ |
| `tests/test_db.py` | DB schema and CRUD tests |
| `tests/test_calculator.py` | Derived/aggregate metric calculation tests |
| `tests/test_fetcher.py` | API response parsing tests (mocked) |
| `tests/test_verifier.py` | Bhavcopy parsing and comparison tests |

---

### Task 1: Project Setup & Config

**Files:**
- Create: `DhanHQ_src/requirements.txt`
- Create: `DhanHQ_src/config.py`
- Create: `.env`
- Create: `.gitignore`

- [ ] **Step 1: Create .gitignore**

```
.env
data/*.db
data/bhavcopy/
__pycache__/
*.pyc
.pytest_cache/
```

- [ ] **Step 2: Create requirements.txt**

```
dhanhq>=2.0.0
pandas>=2.0.0
scipy>=1.11.0
requests>=2.31.0
python-dotenv>=1.0.0
pytest>=7.4.0
```

- [ ] **Step 3: Create .env template**

```
DHAN_CLIENT_ID=your_client_id_here
DHAN_ACCESS_TOKEN=your_access_token_here
```

- [ ] **Step 4: Create config.py**

```python
import os
from dotenv import load_dotenv

load_dotenv()

# DhanHQ API credentials
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

# NIFTY underlying security ID
NIFTY_SECURITY_ID = 13

# Scrape parameters
EXPIRY_DATE = "2026-03-30"
EXPIRY_FLAG = "MONTH"
FROM_DATE = "2026-03-16"
TO_DATE = "2026-03-31"  # non-inclusive per API docs
EXCHANGE_SEGMENT = "NSE_FNO"
INSTRUMENT_TYPE = "INDEX"
INTERVAL = 1  # 1-minute candles

# 8 strikes: ATM-4 through ATM+4 (ATM-4, ATM-3, ATM-2, ATM-1, ATM, ATM+1, ATM+2, ATM+3, ATM+4)
# The API uses ATM-relative strings
STRIKES = ["ATM-4", "ATM-3", "ATM-2", "ATM-1", "ATM", "ATM+1", "ATM+2", "ATM+3", "ATM+4"]
OPTION_TYPES = ["CALL", "PUT"]

# Data fields to request from API
REQUIRED_DATA = ["open", "high", "low", "close", "iv", "volume", "strike", "oi", "spot"]

# IVR/IVP baseline: 52 weeks back from scrape start
IV_BASELINE_FROM = "2025-03-16"
IV_BASELINE_TO = "2026-03-16"

# Rate limiting
API_DELAY_SECONDS = 0.25  # 4 req/sec to stay safely under 5/sec limit
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # exponential backoff base in seconds

# NIFTY lot size
LOT_SIZE = 65

# Risk-free rate for Black-Scholes (RBI repo rate approx)
RISK_FREE_RATE = 0.065

# Database path
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "nifty_options.db")
BHAVCOPY_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "bhavcopy")
```

- [ ] **Step 5: Create data directories**

Run: `mkdir -p data/bhavcopy`

- [ ] **Step 6: Install dependencies**

Run: `cd DhanHQ_src && pip install -r requirements.txt`

- [ ] **Step 7: Commit**

```bash
git add .gitignore DhanHQ_src/requirements.txt DhanHQ_src/config.py .env
git commit -m "feat: project setup with config, dependencies, and .env template"
```

---

### Task 2: Database Schema & CRUD (db.py)

**Files:**
- Create: `DhanHQ_src/db.py`
- Create: `tests/test_db.py`

- [ ] **Step 1: Write failing tests for DB schema creation**

```python
# tests/test_db.py
import os
import sqlite3
import tempfile
import pytest
from DhanHQ_src.db import Database


@pytest.fixture
def db():
    """Create a temporary database for testing."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    database = Database(path)
    database.create_tables()
    yield database
    database.close()
    os.unlink(path)


def test_tables_exist(db):
    cursor = db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]
    assert "raw_option_data" in tables
    assert "derived_metrics" in tables
    assert "aggregate_metrics" in tables
    assert "verification_log" in tables
    assert "iv_history" in tables


def test_insert_raw_option_data(db):
    row = {
        "timestamp": "2026-03-16 09:15:00",
        "date": "2026-03-16",
        "time": "09:15:00",
        "expiry_date": "2026-03-30",
        "strike": 23200,
        "option_type": "CE",
        "open": 500.0,
        "high": 510.0,
        "low": 495.0,
        "close": 505.0,
        "volume": 1000,
        "oi": 50000,
        "iv": 25.5,
        "spot": 23250.0,
        "atm_offset": 0,
    }
    db.insert_raw_option_data([row])
    result = db.conn.execute("SELECT * FROM raw_option_data").fetchall()
    assert len(result) == 1
    assert result[0][5] == 23200  # strike
    assert result[0][6] == "CE"  # option_type


def test_unique_constraint_raw_option_data(db):
    row = {
        "timestamp": "2026-03-16 09:15:00",
        "date": "2026-03-16",
        "time": "09:15:00",
        "expiry_date": "2026-03-30",
        "strike": 23200,
        "option_type": "CE",
        "open": 500.0,
        "high": 510.0,
        "low": 495.0,
        "close": 505.0,
        "volume": 1000,
        "oi": 50000,
        "iv": 25.5,
        "spot": 23250.0,
        "atm_offset": 0,
    }
    db.insert_raw_option_data([row])
    # Inserting duplicate should not raise, should be ignored
    db.insert_raw_option_data([row])
    result = db.conn.execute("SELECT COUNT(*) FROM raw_option_data").fetchone()
    assert result[0] == 1


def test_insert_derived_metrics(db):
    row = {
        "timestamp": "2026-03-16 09:15:00",
        "strike": 23200,
        "ce_ltp": 505.0,
        "pe_ltp": 400.0,
        "ce_ltp_chg": -10.0,
        "pe_ltp_chg": -30.0,
        "pe_ltp_chg_pct": -6.98,
        "ce_volume": 1000,
        "pe_volume": 800,
        "ce_oi": 50000,
        "pe_oi": 60000,
        "ce_oi_chg": 500,
        "pe_oi_chg": -200,
        "ce_iv": 25.5,
        "pe_iv": 24.8,
        "ce_iv_chg": 0.3,
        "pe_iv_chg": -0.1,
        "pe_ce_oi": 10000,
        "pe_ce_oi_chg": -700,
        "pcr_oi": 1.2,
        "pcr_oi_chg": -0.05,
        "pcr_vol": 0.8,
    }
    db.insert_derived_metrics([row])
    result = db.conn.execute("SELECT * FROM derived_metrics").fetchall()
    assert len(result) == 1


def test_insert_aggregate_metrics(db):
    row = {
        "timestamp": "2026-03-16 09:15:00",
        "spot": 23250.0,
        "spot_chg": 42.5,
        "spot_chg_pct": 0.18,
        "fair_price": 23260.0,
        "fair_price_chg": 10.0,
        "atm_iv": 25.0,
        "ivr": 89.39,
        "ivp": 97.98,
        "max_pain": 24500,
        "overall_pcr": 1.03,
        "lot_size": 65,
        "total_ce_oi": 36786,
        "total_pe_oi": 66534,
        "total_oi_net": 29748,
        "total_ce_oi_chg": 0,
        "total_pe_oi_chg": 0,
        "total_oi_chg_net": 0,
        "total_bullish_oi": 0,
        "total_bearish_oi": 0,
        "otm_ce_oi": 22773,
        "otm_pe_oi": 41452,
        "otm_oi_net": 18679,
        "otm_ce_oi_chg": 0,
        "otm_pe_oi_chg": 0,
        "otm_oi_chg_net": 0,
        "itm_ce_oi": 14013,
        "itm_pe_oi": 25082,
        "itm_oi_net": 11069,
        "itm_ce_oi_chg": 0,
        "itm_pe_oi_chg": 0,
        "itm_oi_chg_net": 0,
    }
    db.insert_aggregate_metrics([row])
    result = db.conn.execute("SELECT * FROM aggregate_metrics").fetchall()
    assert len(result) == 1


def test_insert_iv_history(db):
    row = {
        "date": "2025-06-15",
        "atm_iv": 18.5,
        "spot": 22100.0,
        "atm_strike": 22100,
    }
    db.insert_iv_history([row])
    result = db.conn.execute("SELECT * FROM iv_history").fetchall()
    assert len(result) == 1


def test_get_raw_data_by_date(db):
    rows = [
        {
            "timestamp": "2026-03-16 09:15:00",
            "date": "2026-03-16",
            "time": "09:15:00",
            "expiry_date": "2026-03-30",
            "strike": 23200,
            "option_type": "CE",
            "open": 500.0,
            "high": 510.0,
            "low": 495.0,
            "close": 505.0,
            "volume": 1000,
            "oi": 50000,
            "iv": 25.5,
            "spot": 23250.0,
            "atm_offset": 0,
        },
        {
            "timestamp": "2026-03-17 09:15:00",
            "date": "2026-03-17",
            "time": "09:15:00",
            "expiry_date": "2026-03-30",
            "strike": 23200,
            "option_type": "CE",
            "open": 510.0,
            "high": 520.0,
            "low": 505.0,
            "close": 515.0,
            "volume": 1200,
            "oi": 52000,
            "iv": 26.0,
            "spot": 23300.0,
            "atm_offset": 0,
        },
    ]
    db.insert_raw_option_data(rows)
    result = db.get_raw_data_by_date("2026-03-16")
    assert len(result) == 1
    assert result[0]["close"] == 505.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd C:/Users/LENOVO/Documents/Claude_Code/Projects/OptionsScraper && python -m pytest tests/test_db.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'DhanHQ_src'`

- [ ] **Step 3: Implement db.py**

```python
# DhanHQ_src/db.py
import sqlite3
import os


class Database:
    def __init__(self, db_path):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")

    def create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS raw_option_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                date DATE NOT NULL,
                time TIME NOT NULL,
                expiry_date DATE NOT NULL,
                strike INTEGER NOT NULL,
                option_type TEXT NOT NULL CHECK(option_type IN ('CE', 'PE')),
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                oi INTEGER,
                iv REAL,
                spot REAL,
                atm_offset INTEGER,
                UNIQUE(timestamp, strike, option_type)
            );

            CREATE INDEX IF NOT EXISTS idx_raw_date ON raw_option_data(date);
            CREATE INDEX IF NOT EXISTS idx_raw_strike_type ON raw_option_data(strike, option_type);
            CREATE INDEX IF NOT EXISTS idx_raw_timestamp ON raw_option_data(timestamp);

            CREATE TABLE IF NOT EXISTS derived_metrics (
                timestamp DATETIME NOT NULL,
                strike INTEGER NOT NULL,
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
                PRIMARY KEY (timestamp, strike)
            );

            CREATE TABLE IF NOT EXISTS aggregate_metrics (
                timestamp DATETIME PRIMARY KEY,
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
                itm_oi_chg_net INTEGER
            );

            CREATE TABLE IF NOT EXISTS verification_log (
                date DATE NOT NULL,
                strike INTEGER NOT NULL,
                option_type TEXT NOT NULL,
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
                PRIMARY KEY (date, strike, option_type)
            );

            CREATE TABLE IF NOT EXISTS iv_history (
                date DATE PRIMARY KEY,
                atm_iv REAL,
                spot REAL,
                atm_strike INTEGER
            );
        """)
        self.conn.commit()

    def insert_raw_option_data(self, rows):
        self.conn.executemany(
            """INSERT OR IGNORE INTO raw_option_data
            (timestamp, date, time, expiry_date, strike, option_type,
             open, high, low, close, volume, oi, iv, spot, atm_offset)
            VALUES (:timestamp, :date, :time, :expiry_date, :strike, :option_type,
                    :open, :high, :low, :close, :volume, :oi, :iv, :spot, :atm_offset)""",
            rows,
        )
        self.conn.commit()

    def insert_derived_metrics(self, rows):
        self.conn.executemany(
            """INSERT OR REPLACE INTO derived_metrics
            (timestamp, strike, ce_ltp, pe_ltp, ce_ltp_chg, pe_ltp_chg, pe_ltp_chg_pct,
             ce_volume, pe_volume, ce_oi, pe_oi, ce_oi_chg, pe_oi_chg,
             ce_iv, pe_iv, ce_iv_chg, pe_iv_chg,
             pe_ce_oi, pe_ce_oi_chg, pcr_oi, pcr_oi_chg, pcr_vol)
            VALUES (:timestamp, :strike, :ce_ltp, :pe_ltp, :ce_ltp_chg, :pe_ltp_chg, :pe_ltp_chg_pct,
                    :ce_volume, :pe_volume, :ce_oi, :pe_oi, :ce_oi_chg, :pe_oi_chg,
                    :ce_iv, :pe_iv, :ce_iv_chg, :pe_iv_chg,
                    :pe_ce_oi, :pe_ce_oi_chg, :pcr_oi, :pcr_oi_chg, :pcr_vol)""",
            rows,
        )
        self.conn.commit()

    def insert_aggregate_metrics(self, rows):
        self.conn.executemany(
            """INSERT OR REPLACE INTO aggregate_metrics
            (timestamp, spot, spot_chg, spot_chg_pct, fair_price, fair_price_chg,
             atm_iv, ivr, ivp, max_pain, overall_pcr, lot_size,
             total_ce_oi, total_pe_oi, total_oi_net,
             total_ce_oi_chg, total_pe_oi_chg, total_oi_chg_net,
             total_bullish_oi, total_bearish_oi,
             otm_ce_oi, otm_pe_oi, otm_oi_net,
             otm_ce_oi_chg, otm_pe_oi_chg, otm_oi_chg_net,
             itm_ce_oi, itm_pe_oi, itm_oi_net,
             itm_ce_oi_chg, itm_pe_oi_chg, itm_oi_chg_net)
            VALUES (:timestamp, :spot, :spot_chg, :spot_chg_pct, :fair_price, :fair_price_chg,
                    :atm_iv, :ivr, :ivp, :max_pain, :overall_pcr, :lot_size,
                    :total_ce_oi, :total_pe_oi, :total_oi_net,
                    :total_ce_oi_chg, :total_pe_oi_chg, :total_oi_chg_net,
                    :total_bullish_oi, :total_bearish_oi,
                    :otm_ce_oi, :otm_pe_oi, :otm_oi_net,
                    :otm_ce_oi_chg, :otm_pe_oi_chg, :otm_oi_chg_net,
                    :itm_ce_oi, :itm_pe_oi, :itm_oi_net,
                    :itm_ce_oi_chg, :itm_pe_oi_chg, :itm_oi_chg_net)""",
            rows,
        )
        self.conn.commit()

    def insert_verification_log(self, rows):
        self.conn.executemany(
            """INSERT OR REPLACE INTO verification_log
            (date, strike, option_type, dhan_close, nse_close,
             dhan_oi, nse_oi, dhan_volume, nse_volume,
             close_match, oi_match, volume_match, notes)
            VALUES (:date, :strike, :option_type, :dhan_close, :nse_close,
                    :dhan_oi, :nse_oi, :dhan_volume, :nse_volume,
                    :close_match, :oi_match, :volume_match, :notes)""",
            rows,
        )
        self.conn.commit()

    def insert_iv_history(self, rows):
        self.conn.executemany(
            """INSERT OR REPLACE INTO iv_history (date, atm_iv, spot, atm_strike)
            VALUES (:date, :atm_iv, :spot, :atm_strike)""",
            rows,
        )
        self.conn.commit()

    def get_raw_data_by_date(self, date):
        cursor = self.conn.execute(
            "SELECT * FROM raw_option_data WHERE date = ? ORDER BY timestamp, strike, option_type",
            (date,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_raw_data_ordered(self):
        cursor = self.conn.execute(
            "SELECT * FROM raw_option_data ORDER BY timestamp, strike, option_type"
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_iv_history(self):
        cursor = self.conn.execute(
            "SELECT * FROM iv_history ORDER BY date"
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_eod_data(self, date):
        """Get the last candle of the day for each strike/option_type."""
        cursor = self.conn.execute(
            """SELECT strike, option_type, close, oi, volume
            FROM raw_option_data
            WHERE date = ?
            AND timestamp = (
                SELECT MAX(timestamp) FROM raw_option_data r2
                WHERE r2.date = raw_option_data.date
                AND r2.strike = raw_option_data.strike
                AND r2.option_type = raw_option_data.option_type
            )
            ORDER BY strike, option_type""",
            (date,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_distinct_dates(self):
        cursor = self.conn.execute(
            "SELECT DISTINCT date FROM raw_option_data ORDER BY date"
        )
        return [row[0] for row in cursor.fetchall()]

    def close(self):
        self.conn.close()
```

- [ ] **Step 4: Create `DhanHQ_src/__init__.py` and `tests/__init__.py`**

Create empty `DhanHQ_src/__init__.py` and `tests/__init__.py` files so Python can find the modules.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd C:/Users/LENOVO/Documents/Claude_Code/Projects/OptionsScraper && python -m pytest tests/test_db.py -v`
Expected: All 7 tests PASS

- [ ] **Step 6: Commit**

```bash
git add DhanHQ_src/db.py DhanHQ_src/__init__.py tests/test_db.py tests/__init__.py
git commit -m "feat: SQLite database schema with 5 tables and CRUD operations"
```

---

### Task 3: DhanHQ API Fetcher (fetcher.py)

**Files:**
- Create: `DhanHQ_src/fetcher.py`
- Create: `tests/test_fetcher.py`

- [ ] **Step 1: Write failing tests for response parsing**

```python
# tests/test_fetcher.py
import pytest
from unittest.mock import MagicMock, patch
from DhanHQ_src.fetcher import parse_api_response, build_raw_rows


def make_mock_response():
    """Simulates DhanHQ API response format (parallel arrays)."""
    return {
        "open": [500.0, 505.0, 502.0],
        "high": [510.0, 512.0, 508.0],
        "low": [495.0, 500.0, 498.0],
        "close": [505.0, 508.0, 504.0],
        "volume": [1000, 1200, 800],
        "oi": [50000, 50500, 50200],
        "iv": [25.5, 25.8, 25.3],
        "spot": [23250.0, 23260.0, 23255.0],
        "strike": [23200, 23200, 23200],
        "timestamp": [1742108100, 1742108160, 1742108220],  # epoch seconds
    }


def test_parse_api_response_row_count():
    response = make_mock_response()
    rows = parse_api_response(response)
    assert len(rows) == 3


def test_parse_api_response_row_fields():
    response = make_mock_response()
    rows = parse_api_response(response)
    row = rows[0]
    assert row["open"] == 500.0
    assert row["close"] == 505.0
    assert row["volume"] == 1000
    assert row["oi"] == 50000
    assert row["iv"] == 25.5
    assert row["spot"] == 23250.0
    assert row["strike"] == 23200


def test_parse_api_response_timestamp_conversion():
    response = make_mock_response()
    rows = parse_api_response(response)
    # Epoch 1742108100 = 2025-03-16 09:15:00 IST (approx — exact depends on TZ)
    assert "timestamp" in rows[0]
    assert "date" in rows[0]
    assert "time" in rows[0]


def test_build_raw_rows_adds_metadata():
    response = make_mock_response()
    parsed = parse_api_response(response)
    rows = build_raw_rows(parsed, option_type="CE", atm_offset=0, expiry_date="2026-03-30")
    assert rows[0]["option_type"] == "CE"
    assert rows[0]["atm_offset"] == 0
    assert rows[0]["expiry_date"] == "2026-03-30"


def test_parse_empty_response():
    response = {
        "open": [], "high": [], "low": [], "close": [],
        "volume": [], "oi": [], "iv": [], "spot": [],
        "strike": [], "timestamp": [],
    }
    rows = parse_api_response(response)
    assert len(rows) == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_fetcher.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement fetcher.py**

```python
# DhanHQ_src/fetcher.py
import time
import logging
from datetime import datetime, timezone, timedelta

from dhanhq import DhanContext, dhanhq

from DhanHQ_src.config import (
    DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN,
    NIFTY_SECURITY_ID, EXCHANGE_SEGMENT, INSTRUMENT_TYPE,
    EXPIRY_FLAG, REQUIRED_DATA, INTERVAL,
    STRIKES, OPTION_TYPES,
    FROM_DATE, TO_DATE, EXPIRY_DATE,
    IV_BASELINE_FROM, IV_BASELINE_TO,
    API_DELAY_SECONDS, MAX_RETRIES, RETRY_BACKOFF_BASE,
)

logger = logging.getLogger(__name__)

# IST offset
IST = timezone(timedelta(hours=5, minutes=30))


def create_dhan_client():
    context = DhanContext(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
    return dhanhq(context)


def parse_api_response(response_data):
    """Convert DhanHQ parallel-array response into list of row dicts."""
    if not response_data or not response_data.get("timestamp"):
        return []

    timestamps = response_data["timestamp"]
    count = len(timestamps)
    rows = []

    for i in range(count):
        # Convert epoch to IST datetime
        dt = datetime.fromtimestamp(timestamps[i], tz=IST)
        rows.append({
            "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "date": dt.strftime("%Y-%m-%d"),
            "time": dt.strftime("%H:%M:%S"),
            "open": response_data["open"][i] if "open" in response_data else None,
            "high": response_data["high"][i] if "high" in response_data else None,
            "low": response_data["low"][i] if "low" in response_data else None,
            "close": response_data["close"][i] if "close" in response_data else None,
            "volume": response_data["volume"][i] if "volume" in response_data else None,
            "oi": response_data["oi"][i] if "oi" in response_data else None,
            "iv": response_data["iv"][i] if "iv" in response_data else None,
            "spot": response_data["spot"][i] if "spot" in response_data else None,
            "strike": response_data["strike"][i] if "strike" in response_data else None,
        })
    return rows


def build_raw_rows(parsed_rows, option_type, atm_offset, expiry_date):
    """Add metadata fields to parsed rows for DB insertion."""
    db_option_type = "CE" if option_type == "CALL" else "PE"
    for row in parsed_rows:
        row["option_type"] = db_option_type
        row["atm_offset"] = atm_offset
        row["expiry_date"] = expiry_date
    return parsed_rows


def fetch_with_retry(dhan, **kwargs):
    """Call expired_options_data with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            response = dhan.expired_options_data(**kwargs)
            if response and response.get("status") == "success":
                return response.get("data", {})
            if response and response.get("status") == "failure":
                logger.error("API error: %s (params: %s)", response, kwargs)
                return {}
            return response if isinstance(response, dict) else {}
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF_BASE ** (attempt + 1)
                logger.warning("Retry %d/%d after %.1fs: %s", attempt + 1, MAX_RETRIES, wait, e)
                time.sleep(wait)
            else:
                logger.error("All retries failed for %s: %s", kwargs, e)
                raise


def fetch_all_options_data(dhan=None):
    """Fetch 1-min data for all strikes × option types. Returns list of row dicts."""
    if dhan is None:
        dhan = create_dhan_client()

    all_rows = []
    strike_offsets = {s: i - len(STRIKES) // 2 for i, s in enumerate(STRIKES)}

    for strike in STRIKES:
        for option_type in OPTION_TYPES:
            logger.info("Fetching %s %s ...", strike, option_type)
            response_data = fetch_with_retry(
                dhan,
                security_id=NIFTY_SECURITY_ID,
                exchange_segment=EXCHANGE_SEGMENT,
                instrument_type=INSTRUMENT_TYPE,
                expiry_flag=EXPIRY_FLAG,
                expiry_code=1,
                strike=strike,
                drv_option_type=option_type,
                required_data=REQUIRED_DATA,
                from_date=FROM_DATE,
                to_date=TO_DATE,
            )
            parsed = parse_api_response(response_data)
            rows = build_raw_rows(parsed, option_type, strike_offsets[strike], EXPIRY_DATE)
            all_rows.extend(rows)
            logger.info("  Got %d candles for %s %s", len(parsed), strike, option_type)
            time.sleep(API_DELAY_SECONDS)

    logger.info("Total raw rows fetched: %d", len(all_rows))
    return all_rows


def fetch_iv_baseline(dhan=None):
    """Fetch daily ATM IV for past 52 weeks for IVR/IVP calculation.

    Fetches ATM CALL data with daily interval to get IV + spot + strike.
    Returns list of dicts with {date, atm_iv, spot, atm_strike}.
    """
    if dhan is None:
        dhan = create_dhan_client()

    logger.info("Fetching 52-week IV baseline (ATM CALL daily) ...")

    # API allows max 30 days per call, so we need to chunk the date range
    from datetime import date as dt_date
    start = dt_date.fromisoformat(IV_BASELINE_FROM)
    end = dt_date.fromisoformat(IV_BASELINE_TO)
    chunk_days = 30
    all_rows = []

    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=chunk_days), end)
        response_data = fetch_with_retry(
            dhan,
            security_id=NIFTY_SECURITY_ID,
            exchange_segment=EXCHANGE_SEGMENT,
            instrument_type=INSTRUMENT_TYPE,
            expiry_flag=EXPIRY_FLAG,
            expiry_code=1,
            strike="ATM",
            drv_option_type="CALL",
            required_data=["iv", "spot", "strike"],
            from_date=current.isoformat(),
            to_date=chunk_end.isoformat(),
        )

        if response_data and response_data.get("timestamp"):
            timestamps = response_data["timestamp"]
            for i in range(len(timestamps)):
                dt = datetime.fromtimestamp(timestamps[i], tz=IST)
                all_rows.append({
                    "date": dt.strftime("%Y-%m-%d"),
                    "atm_iv": response_data["iv"][i] if "iv" in response_data else None,
                    "spot": response_data["spot"][i] if "spot" in response_data else None,
                    "atm_strike": response_data["strike"][i] if "strike" in response_data else None,
                })

        logger.info("  IV baseline chunk %s to %s: %d rows",
                     current.isoformat(), chunk_end.isoformat(),
                     len(response_data.get("timestamp", [])) if response_data else 0)
        current = chunk_end
        time.sleep(API_DELAY_SECONDS)

    # Deduplicate to daily (take last entry per date for EOD IV)
    daily = {}
    for row in all_rows:
        daily[row["date"]] = row
    result = sorted(daily.values(), key=lambda x: x["date"])
    logger.info("IV baseline: %d daily entries", len(result))
    return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_fetcher.py -v`
Expected: All 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add DhanHQ_src/fetcher.py tests/test_fetcher.py
git commit -m "feat: DhanHQ API fetcher with response parsing and retry logic"
```

---

### Task 4: Derived Metrics Calculator (calculator.py)

**Files:**
- Create: `DhanHQ_src/calculator.py`
- Create: `tests/test_calculator.py`

- [ ] **Step 1: Write failing tests for per-strike derived metrics**

```python
# tests/test_calculator.py
import pytest
from DhanHQ_src.calculator import (
    compute_derived_metrics,
    compute_aggregate_metrics,
    compute_max_pain,
    compute_ivr_ivp,
    compute_fair_price,
)


def make_raw_data():
    """Two timestamps, one strike (23200), CE and PE each."""
    return [
        # t=0, CE
        {"timestamp": "2026-03-16 09:15:00", "date": "2026-03-16", "strike": 23200,
         "option_type": "CE", "close": 550.0, "volume": 357, "oi": 3594, "iv": 25.73, "spot": 23241.0},
        # t=0, PE
        {"timestamp": "2026-03-16 09:15:00", "date": "2026-03-16", "strike": 23200,
         "option_type": "PE", "close": 441.0, "volume": 1139, "oi": 20428, "iv": 25.50, "spot": 23241.0},
        # t=1, CE
        {"timestamp": "2026-03-16 09:16:00", "date": "2026-03-16", "strike": 23200,
         "option_type": "CE", "close": 540.0, "volume": 400, "oi": 3800, "iv": 25.50, "spot": 23230.0},
        # t=1, PE
        {"timestamp": "2026-03-16 09:16:00", "date": "2026-03-16", "strike": 23200,
         "option_type": "PE", "close": 450.0, "volume": 1200, "oi": 20600, "iv": 25.80, "spot": 23230.0},
    ]


def test_compute_derived_metrics_count():
    raw = make_raw_data()
    result = compute_derived_metrics(raw)
    # 2 timestamps × 1 strike = 2 derived rows
    assert len(result) == 2


def test_compute_derived_metrics_first_row_no_change():
    raw = make_raw_data()
    result = compute_derived_metrics(raw)
    first = result[0]
    assert first["ce_ltp"] == 550.0
    assert first["pe_ltp"] == 441.0
    # First candle has no previous, so changes should be 0 or None
    assert first["ce_ltp_chg"] == 0.0
    assert first["pe_ltp_chg"] == 0.0


def test_compute_derived_metrics_second_row_changes():
    raw = make_raw_data()
    result = compute_derived_metrics(raw)
    second = result[1]
    assert second["ce_ltp"] == 540.0
    assert second["ce_ltp_chg"] == pytest.approx(-10.0)
    assert second["pe_ltp_chg"] == pytest.approx(9.0)
    assert second["ce_oi_chg"] == 206  # 3800 - 3594
    assert second["pe_oi_chg"] == 172  # 20600 - 20428
    assert second["ce_iv_chg"] == pytest.approx(-0.23)
    assert second["pe_iv_chg"] == pytest.approx(0.30)


def test_compute_derived_metrics_pe_ce_oi():
    raw = make_raw_data()
    result = compute_derived_metrics(raw)
    first = result[0]
    assert first["pe_ce_oi"] == 20428 - 3594  # 16834


def test_compute_derived_metrics_pcr():
    raw = make_raw_data()
    result = compute_derived_metrics(raw)
    first = result[0]
    assert first["pcr_oi"] == pytest.approx(20428 / 3594, rel=1e-3)
    assert first["pcr_vol"] == pytest.approx(1139 / 357, rel=1e-3)


def test_compute_max_pain():
    # Simple scenario: 3 strikes with known OI
    strike_oi = [
        {"strike": 23100, "call_oi": 5000, "put_oi": 15000},
        {"strike": 23200, "call_oi": 10000, "put_oi": 10000},
        {"strike": 23300, "call_oi": 15000, "put_oi": 5000},
    ]
    result = compute_max_pain(strike_oi)
    assert result == 23200  # symmetric OI → max pain at center


def test_compute_ivr_ivp():
    historical_iv = [15.0, 18.0, 20.0, 22.0, 25.0, 28.0, 30.0, 16.0, 19.0, 21.0]
    current_iv = 25.0
    ivr, ivp = compute_ivr_ivp(current_iv, historical_iv)
    # IVR = (25-15)/(30-15)*100 = 66.67
    assert ivr == pytest.approx(66.67, rel=1e-2)
    # IVP = 7 out of 10 values < 25 = 70.0
    assert ivp == pytest.approx(70.0, rel=1e-2)


def test_compute_fair_price():
    # Black-Scholes call price for ATM option
    price = compute_fair_price(
        spot=23250.0, strike=23250, days_to_expiry=14,
        iv=25.0, risk_free_rate=0.065, option_type="CE"
    )
    assert price > 0
    assert price < 23250  # call price always less than spot
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_calculator.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement calculator.py**

```python
# DhanHQ_src/calculator.py
import math
import logging
from collections import defaultdict
from scipy.stats import norm

from DhanHQ_src.config import LOT_SIZE, RISK_FREE_RATE

logger = logging.getLogger(__name__)


def compute_derived_metrics(raw_rows):
    """Compute per-strike derived metrics from raw option data.

    Groups by (timestamp, strike), pairs CE+PE, computes changes from previous candle.
    Returns list of derived_metrics row dicts.
    """
    # Group by (timestamp, strike) then by option_type
    grouped = defaultdict(dict)
    for row in raw_rows:
        key = (row["timestamp"], row["strike"])
        grouped[key][row["option_type"]] = row

    # Sort by timestamp then strike
    sorted_keys = sorted(grouped.keys())

    # Track previous values per strike for change calculations
    prev_by_strike = {}
    results = []

    for ts, strike in sorted_keys:
        data = grouped[(ts, strike)]
        ce = data.get("CE", {})
        pe = data.get("PE", {})

        ce_ltp = ce.get("close", 0) or 0
        pe_ltp = pe.get("close", 0) or 0
        ce_vol = ce.get("volume", 0) or 0
        pe_vol = pe.get("volume", 0) or 0
        ce_oi = ce.get("oi", 0) or 0
        pe_oi = pe.get("oi", 0) or 0
        ce_iv = ce.get("iv", 0) or 0
        pe_iv = pe.get("iv", 0) or 0

        prev = prev_by_strike.get(strike)

        ce_ltp_chg = (ce_ltp - prev["ce_ltp"]) if prev else 0.0
        pe_ltp_chg = (pe_ltp - prev["pe_ltp"]) if prev else 0.0
        pe_ltp_chg_pct = (pe_ltp_chg / prev["pe_ltp"] * 100) if prev and prev["pe_ltp"] else 0.0
        ce_oi_chg = (ce_oi - prev["ce_oi"]) if prev else 0
        pe_oi_chg = (pe_oi - prev["pe_oi"]) if prev else 0
        ce_iv_chg = (ce_iv - prev["ce_iv"]) if prev else 0.0
        pe_iv_chg = (pe_iv - prev["pe_iv"]) if prev else 0.0

        pe_ce_oi = pe_oi - ce_oi
        prev_pe_ce_oi = (prev["pe_oi"] - prev["ce_oi"]) if prev else pe_ce_oi
        pe_ce_oi_chg = pe_ce_oi - prev_pe_ce_oi if prev else 0

        pcr_oi = pe_oi / ce_oi if ce_oi > 0 else None
        prev_pcr_oi = prev.get("pcr_oi") if prev else pcr_oi
        pcr_oi_chg = (pcr_oi - prev_pcr_oi) if pcr_oi is not None and prev_pcr_oi is not None else 0.0
        pcr_vol = pe_vol / ce_vol if ce_vol > 0 else None

        row = {
            "timestamp": ts,
            "strike": strike,
            "ce_ltp": ce_ltp,
            "pe_ltp": pe_ltp,
            "ce_ltp_chg": round(ce_ltp_chg, 2),
            "pe_ltp_chg": round(pe_ltp_chg, 2),
            "pe_ltp_chg_pct": round(pe_ltp_chg_pct, 2),
            "ce_volume": ce_vol,
            "pe_volume": pe_vol,
            "ce_oi": ce_oi,
            "pe_oi": pe_oi,
            "ce_oi_chg": ce_oi_chg,
            "pe_oi_chg": pe_oi_chg,
            "ce_iv": ce_iv,
            "pe_iv": pe_iv,
            "ce_iv_chg": round(ce_iv_chg, 2),
            "pe_iv_chg": round(pe_iv_chg, 2),
            "pe_ce_oi": pe_ce_oi,
            "pe_ce_oi_chg": pe_ce_oi_chg,
            "pcr_oi": round(pcr_oi, 4) if pcr_oi is not None else None,
            "pcr_oi_chg": round(pcr_oi_chg, 4) if pcr_oi_chg is not None else None,
            "pcr_vol": round(pcr_vol, 4) if pcr_vol is not None else None,
        }
        results.append(row)

        prev_by_strike[strike] = {
            "ce_ltp": ce_ltp, "pe_ltp": pe_ltp,
            "ce_oi": ce_oi, "pe_oi": pe_oi,
            "ce_iv": ce_iv, "pe_iv": pe_iv,
            "pcr_oi": pcr_oi,
        }

    return results


def compute_max_pain(strike_oi_list):
    """Compute Max Pain strike.

    Args:
        strike_oi_list: list of dicts with {strike, call_oi, put_oi}
    Returns:
        The strike price where total option buyer payout is minimized.
    """
    strikes = [s["strike"] for s in strike_oi_list]
    min_pain = float("inf")
    max_pain_strike = strikes[0] if strikes else 0

    for expiry_price in strikes:
        total_pain = 0
        for item in strike_oi_list:
            call_intrinsic = max(0, expiry_price - item["strike"])
            total_pain += call_intrinsic * item["call_oi"]
            put_intrinsic = max(0, item["strike"] - expiry_price)
            total_pain += put_intrinsic * item["put_oi"]
        if total_pain < min_pain:
            min_pain = total_pain
            max_pain_strike = expiry_price

    return max_pain_strike


def compute_ivr_ivp(current_iv, historical_iv_list):
    """Compute IV Rank and IV Percentile.

    Args:
        current_iv: current ATM IV value
        historical_iv_list: list of historical IV floats (e.g., 252 daily values)
    Returns:
        (ivr, ivp) tuple
    """
    if not historical_iv_list:
        return 0.0, 0.0

    iv_high = max(historical_iv_list)
    iv_low = min(historical_iv_list)

    if iv_high == iv_low:
        ivr = 50.0
    else:
        ivr = ((current_iv - iv_low) / (iv_high - iv_low)) * 100

    days_below = sum(1 for iv in historical_iv_list if iv < current_iv)
    ivp = (days_below / len(historical_iv_list)) * 100

    return round(ivr, 2), round(ivp, 2)


def compute_fair_price(spot, strike, days_to_expiry, iv, risk_free_rate=RISK_FREE_RATE, option_type="CE"):
    """Black-Scholes option price.

    Args:
        spot: underlying price
        strike: strike price
        days_to_expiry: calendar days to expiry
        iv: implied volatility as percentage (e.g., 25.0 for 25%)
        risk_free_rate: annual risk-free rate (e.g., 0.065)
        option_type: "CE" for call, "PE" for put
    Returns:
        Theoretical option price
    """
    if days_to_expiry <= 0 or iv <= 0 or spot <= 0 or strike <= 0:
        return 0.0

    T = days_to_expiry / 365.0
    sigma = iv / 100.0  # convert percentage to decimal
    S, K, r = spot, strike, risk_free_rate

    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)

    if option_type == "CE":
        price = S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    else:
        price = K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

    return round(price, 2)


def compute_aggregate_metrics(derived_rows, spot, prev_spot_close, iv_history, expiry_date_str):
    """Compute aggregate metrics for a single timestamp.

    Args:
        derived_rows: list of derived_metrics dicts for ONE timestamp (all strikes)
        spot: current spot price
        prev_spot_close: previous day's closing spot (for spot_chg)
        iv_history: list of historical IV floats for IVR/IVP
        expiry_date_str: expiry date string "YYYY-MM-DD"
    Returns:
        Single aggregate_metrics row dict
    """
    from datetime import date as dt_date

    ts = derived_rows[0]["timestamp"] if derived_rows else ""
    total_ce_oi = sum(r["ce_oi"] for r in derived_rows)
    total_pe_oi = sum(r["pe_oi"] for r in derived_rows)
    total_ce_vol = sum(r["ce_volume"] for r in derived_rows)
    total_pe_vol = sum(r["pe_volume"] for r in derived_rows)
    total_ce_oi_chg = sum(r["ce_oi_chg"] for r in derived_rows)
    total_pe_oi_chg = sum(r["pe_oi_chg"] for r in derived_rows)

    # OTM/ITM split: CE is OTM when strike > spot, ITM when strike < spot
    otm_ce_oi = sum(r["ce_oi"] for r in derived_rows if r["strike"] > spot)
    itm_ce_oi = sum(r["ce_oi"] for r in derived_rows if r["strike"] <= spot)
    otm_pe_oi = sum(r["pe_oi"] for r in derived_rows if r["strike"] < spot)
    itm_pe_oi = sum(r["pe_oi"] for r in derived_rows if r["strike"] >= spot)

    otm_ce_oi_chg = sum(r["ce_oi_chg"] for r in derived_rows if r["strike"] > spot)
    itm_ce_oi_chg = sum(r["ce_oi_chg"] for r in derived_rows if r["strike"] <= spot)
    otm_pe_oi_chg = sum(r["pe_oi_chg"] for r in derived_rows if r["strike"] < spot)
    itm_pe_oi_chg = sum(r["pe_oi_chg"] for r in derived_rows if r["strike"] >= spot)

    # Bullish/Bearish OI
    bullish_oi = 0
    bearish_oi = 0
    for r in derived_rows:
        if r["ce_oi_chg"] > 0:
            bullish_oi += r["ce_oi_chg"]
        if r["ce_oi_chg"] < 0:
            bearish_oi += abs(r["ce_oi_chg"])
        if r["pe_oi_chg"] > 0:
            bearish_oi += r["pe_oi_chg"]
        if r["pe_oi_chg"] < 0:
            bullish_oi += abs(r["pe_oi_chg"])

    # Max Pain
    strike_oi = [
        {"strike": r["strike"], "call_oi": r["ce_oi"], "put_oi": r["pe_oi"]}
        for r in derived_rows
    ]
    max_pain = compute_max_pain(strike_oi) if strike_oi else 0

    # ATM IV (closest strike to spot)
    atm_row = min(derived_rows, key=lambda r: abs(r["strike"] - spot)) if derived_rows else None
    atm_iv = atm_row["ce_iv"] if atm_row else 0

    # IVR/IVP
    ivr, ivp = compute_ivr_ivp(atm_iv, iv_history) if iv_history else (0.0, 0.0)

    # Fair Price (using ATM call)
    expiry = dt_date.fromisoformat(expiry_date_str)
    current_date_str = ts[:10] if ts else expiry_date_str
    current = dt_date.fromisoformat(current_date_str)
    days_to_expiry = (expiry - current).days
    atm_strike = atm_row["strike"] if atm_row else int(spot)
    fair_price = compute_fair_price(spot, atm_strike, days_to_expiry, atm_iv) if atm_iv > 0 else 0

    overall_pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0
    spot_chg = spot - prev_spot_close if prev_spot_close else 0
    spot_chg_pct = (spot_chg / prev_spot_close * 100) if prev_spot_close else 0

    return {
        "timestamp": ts,
        "spot": spot,
        "spot_chg": round(spot_chg, 2),
        "spot_chg_pct": round(spot_chg_pct, 2),
        "fair_price": fair_price,
        "fair_price_chg": 0.0,  # computed in batch after all timestamps processed
        "atm_iv": atm_iv,
        "ivr": ivr,
        "ivp": ivp,
        "max_pain": max_pain,
        "overall_pcr": round(overall_pcr, 4),
        "lot_size": LOT_SIZE,
        "total_ce_oi": total_ce_oi,
        "total_pe_oi": total_pe_oi,
        "total_oi_net": total_pe_oi - total_ce_oi,
        "total_ce_oi_chg": total_ce_oi_chg,
        "total_pe_oi_chg": total_pe_oi_chg,
        "total_oi_chg_net": total_pe_oi_chg - total_ce_oi_chg,
        "total_bullish_oi": bullish_oi,
        "total_bearish_oi": bearish_oi,
        "otm_ce_oi": otm_ce_oi,
        "otm_pe_oi": otm_pe_oi,
        "otm_oi_net": otm_pe_oi - otm_ce_oi,
        "otm_ce_oi_chg": otm_ce_oi_chg,
        "otm_pe_oi_chg": otm_pe_oi_chg,
        "otm_oi_chg_net": otm_pe_oi_chg - otm_ce_oi_chg,
        "itm_ce_oi": itm_ce_oi,
        "itm_pe_oi": itm_pe_oi,
        "itm_oi_net": itm_pe_oi - itm_ce_oi,
        "itm_ce_oi_chg": itm_ce_oi_chg,
        "itm_pe_oi_chg": itm_pe_oi_chg,
        "itm_oi_chg_net": itm_pe_oi_chg - itm_ce_oi_chg,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_calculator.py -v`
Expected: All 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add DhanHQ_src/calculator.py tests/test_calculator.py
git commit -m "feat: derived and aggregate metrics calculator with Max Pain, IVR/IVP, Fair Price"
```

---

### Task 5: NSE Bhavcopy Verifier (verifier.py)

**Files:**
- Create: `DhanHQ_src/verifier.py`
- Create: `tests/test_verifier.py`

- [ ] **Step 1: Write failing tests for bhavcopy parsing and comparison**

```python
# tests/test_verifier.py
import os
import tempfile
import pytest
from DhanHQ_src.verifier import parse_bhavcopy_csv, compare_values


SAMPLE_BHAVCOPY_CSV = """INSTRUMENT,SYMBOL,EXPIRY_DT,STRIKE_PR,OPTION_TYP,OPEN,HIGH,LOW,CLOSE,SETTLE_PR,CONTRACTS,VAL_INLAKH,OPEN_INT,CHG_IN_OI,TIMESTAMP
OPTIDX,NIFTY,30-Mar-2026,23100.00,CE,560.00,570.00,540.00,550.60,550.60,5000,75000.00,3594,500,16-Mar-2026
OPTIDX,NIFTY,30-Mar-2026,23100.00,PE,390.00,405.00,385.00,399.80,399.80,4000,60000.00,15920,200,16-Mar-2026
OPTIDX,NIFTY,30-Mar-2026,23200.00,CE,500.00,515.00,490.00,491.90,491.90,6000,90000.00,11552,800,16-Mar-2026
OPTIDX,NIFTY,30-Mar-2026,23200.00,PE,430.00,450.00,425.00,441.00,441.00,3500,52500.00,20428,300,16-Mar-2026
"""


def test_parse_bhavcopy_csv():
    fd, path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    with open(path, "w") as f:
        f.write(SAMPLE_BHAVCOPY_CSV)

    result = parse_bhavcopy_csv(path, symbol="NIFTY", expiry="30-Mar-2026")
    os.unlink(path)

    assert len(result) == 4
    assert result[0]["strike"] == 23100
    assert result[0]["option_type"] == "CE"
    assert result[0]["close"] == 550.60
    assert result[0]["oi"] == 3594


def test_compare_values_match():
    dhan = {"close": 550.60, "oi": 3594, "volume": 5000}
    nse = {"close": 550.60, "oi": 3594, "volume": 5000}
    result = compare_values(dhan, nse, close_tolerance=0.05)
    assert result["close_match"] is True
    assert result["oi_match"] is True
    assert result["volume_match"] is True


def test_compare_values_close_within_tolerance():
    dhan = {"close": 550.60, "oi": 3594, "volume": 5000}
    nse = {"close": 550.63, "oi": 3594, "volume": 5000}
    result = compare_values(dhan, nse, close_tolerance=0.05)
    assert result["close_match"] is True  # within 0.05


def test_compare_values_mismatch():
    dhan = {"close": 550.60, "oi": 3594, "volume": 5000}
    nse = {"close": 555.00, "oi": 3600, "volume": 5100}
    result = compare_values(dhan, nse, close_tolerance=0.05)
    assert result["close_match"] is False
    assert result["oi_match"] is False
    assert result["volume_match"] is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_verifier.py -v`
Expected: FAIL

- [ ] **Step 3: Implement verifier.py**

```python
# DhanHQ_src/verifier.py
import os
import csv
import logging
import requests
from datetime import datetime, timedelta

from DhanHQ_src.config import BHAVCOPY_DIR, EXPIRY_DATE

logger = logging.getLogger(__name__)

# NSE Bhavcopy URL pattern
BHAVCOPY_URL_TEMPLATE = (
    "https://nsearchives.nseindia.com/content/historical/DERIVATIVES/"
    "{year}/{month}/fo{ddmmmyyyy}bhav.csv.zip"
)

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Referer": "https://www.nseindia.com/",
}


def download_bhavcopy(date_str, output_dir=None):
    """Download NSE F&O Bhavcopy for a given date.

    Args:
        date_str: date in YYYY-MM-DD format
        output_dir: directory to save the CSV
    Returns:
        Path to the downloaded/extracted CSV, or None on failure
    """
    if output_dir is None:
        output_dir = BHAVCOPY_DIR
    os.makedirs(output_dir, exist_ok=True)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%b").upper()
    ddmmmyyyy = dt.strftime("%d%b%Y").upper()

    csv_path = os.path.join(output_dir, f"fo_bhavcopy_{date_str}.csv")
    if os.path.exists(csv_path):
        logger.info("Bhavcopy already downloaded: %s", csv_path)
        return csv_path

    url = BHAVCOPY_URL_TEMPLATE.format(year=year, month=month, ddmmmyyyy=ddmmmyyyy)
    logger.info("Downloading bhavcopy from %s", url)

    try:
        session = requests.Session()
        # First hit NSE homepage to get cookies
        session.get("https://www.nseindia.com/", headers=NSE_HEADERS, timeout=10)

        response = session.get(url, headers=NSE_HEADERS, timeout=30)
        if response.status_code != 200:
            logger.warning("Bhavcopy download failed (HTTP %d) for %s", response.status_code, date_str)
            return None

        # Save and extract zip
        import zipfile
        import io
        z = zipfile.ZipFile(io.BytesIO(response.content))
        for name in z.namelist():
            if name.endswith(".csv"):
                with open(csv_path, "wb") as f:
                    f.write(z.read(name))
                logger.info("Saved bhavcopy to %s", csv_path)
                return csv_path

    except Exception as e:
        logger.error("Failed to download bhavcopy for %s: %s", date_str, e)
    return None


def parse_bhavcopy_csv(csv_path, symbol="NIFTY", expiry=None):
    """Parse NSE Bhavcopy CSV and filter for NIFTY options.

    Args:
        csv_path: path to the CSV file
        symbol: filter by symbol name
        expiry: filter by expiry date string (e.g., "30-Mar-2026")
    Returns:
        List of dicts with {strike, option_type, close, oi, volume}
    """
    results = []
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("INSTRUMENT", "").strip() != "OPTIDX":
                continue
            if row.get("SYMBOL", "").strip() != symbol:
                continue
            if expiry and row.get("EXPIRY_DT", "").strip() != expiry:
                continue

            results.append({
                "strike": int(float(row["STRIKE_PR"].strip())),
                "option_type": row["OPTION_TYP"].strip(),
                "close": float(row["CLOSE"].strip()),
                "oi": int(row["OPEN_INT"].strip()),
                "volume": int(row["CONTRACTS"].strip()),
            })
    return results


def compare_values(dhan_data, nse_data, close_tolerance=0.05):
    """Compare DhanHQ EOD values with NSE Bhavcopy values.

    Args:
        dhan_data: dict with {close, oi, volume}
        nse_data: dict with {close, oi, volume}
        close_tolerance: acceptable difference in close price
    Returns:
        Dict with match flags and notes
    """
    close_diff = abs(dhan_data["close"] - nse_data["close"])
    close_match = close_diff <= close_tolerance
    oi_match = dhan_data["oi"] == nse_data["oi"]
    volume_match = dhan_data["volume"] == nse_data["volume"]

    notes_parts = []
    if not close_match:
        notes_parts.append(f"close diff={close_diff:.2f}")
    if not oi_match:
        notes_parts.append(f"oi diff={dhan_data['oi'] - nse_data['oi']}")
    if not volume_match:
        notes_parts.append(f"vol diff={dhan_data['volume'] - nse_data['volume']}")

    return {
        "close_match": close_match,
        "oi_match": oi_match,
        "volume_match": volume_match,
        "notes": "; ".join(notes_parts) if notes_parts else "OK",
    }


def verify_against_bhavcopy(db, dates=None):
    """Run full verification for all dates.

    Args:
        db: Database instance
        dates: list of date strings to verify (default: all dates in DB)
    Returns:
        Summary dict with match counts
    """
    if dates is None:
        dates = db.get_distinct_dates()

    expiry_formatted = datetime.strptime(EXPIRY_DATE, "%Y-%m-%d").strftime("%d-%b-%Y")
    total_checks = 0
    total_matches = 0
    verification_rows = []

    for date_str in dates:
        csv_path = download_bhavcopy(date_str)
        if csv_path is None:
            logger.warning("Skipping verification for %s (no bhavcopy)", date_str)
            continue

        nse_data = parse_bhavcopy_csv(csv_path, symbol="NIFTY", expiry=expiry_formatted)
        nse_lookup = {(r["strike"], r["option_type"]): r for r in nse_data}

        eod_data = db.get_eod_data(date_str)

        for dhan_row in eod_data:
            key = (dhan_row["strike"], dhan_row["option_type"])
            nse_row = nse_lookup.get(key)
            if nse_row is None:
                logger.warning("No NSE data for %s %s %s", date_str, key[0], key[1])
                continue

            comparison = compare_values(
                {"close": dhan_row["close"], "oi": dhan_row["oi"], "volume": dhan_row["volume"]},
                {"close": nse_row["close"], "oi": nse_row["oi"], "volume": nse_row["volume"]},
            )

            all_match = comparison["close_match"] and comparison["oi_match"] and comparison["volume_match"]
            total_checks += 1
            if all_match:
                total_matches += 1

            verification_rows.append({
                "date": date_str,
                "strike": dhan_row["strike"],
                "option_type": dhan_row["option_type"],
                "dhan_close": dhan_row["close"],
                "nse_close": nse_row["close"],
                "dhan_oi": dhan_row["oi"],
                "nse_oi": nse_row["oi"],
                "dhan_volume": dhan_row["volume"],
                "nse_volume": nse_row["volume"],
                **comparison,
            })

    if verification_rows:
        db.insert_verification_log(verification_rows)

    summary = {
        "total_checks": total_checks,
        "total_matches": total_matches,
        "match_rate": (total_matches / total_checks * 100) if total_checks > 0 else 0,
        "mismatches": total_checks - total_matches,
    }

    logger.info("Verification complete: %d/%d matched (%.1f%%)",
                total_matches, total_checks, summary["match_rate"])
    return summary
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_verifier.py -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add DhanHQ_src/verifier.py tests/test_verifier.py
git commit -m "feat: NSE Bhavcopy verifier with download, parse, and comparison"
```

---

### Task 6: Main Orchestrator (main.py)

**Files:**
- Create: `DhanHQ_src/main.py`

- [ ] **Step 1: Implement main.py**

```python
# DhanHQ_src/main.py
import os
import sys
import logging
from collections import defaultdict

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from DhanHQ_src.config import DB_PATH, EXPIRY_DATE
from DhanHQ_src.db import Database
from DhanHQ_src.fetcher import create_dhan_client, fetch_all_options_data, fetch_iv_baseline
from DhanHQ_src.calculator import compute_derived_metrics, compute_aggregate_metrics
from DhanHQ_src.verifier import verify_against_bhavcopy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_pipeline():
    """Run the full scrape → store → calculate → verify pipeline."""
    logger.info("=" * 60)
    logger.info("NIFTY Options Scraper Pipeline")
    logger.info("=" * 60)

    # Step 1: Initialize DB
    logger.info("Step 1: Initializing database at %s", DB_PATH)
    db = Database(DB_PATH)
    db.create_tables()

    # Step 2: Create DhanHQ client
    logger.info("Step 2: Connecting to DhanHQ API")
    dhan = create_dhan_client()

    # Step 3: Fetch IV baseline (52-week history for IVR/IVP)
    logger.info("Step 3: Fetching 52-week IV baseline")
    iv_baseline_rows = fetch_iv_baseline(dhan)
    if iv_baseline_rows:
        db.insert_iv_history(iv_baseline_rows)
        logger.info("  Stored %d IV history entries", len(iv_baseline_rows))

    # Step 4: Fetch options data
    logger.info("Step 4: Fetching options data (8 strikes × CE/PE)")
    raw_rows = fetch_all_options_data(dhan)
    if not raw_rows:
        logger.error("No data fetched! Check API credentials and parameters.")
        db.close()
        return
    db.insert_raw_option_data(raw_rows)
    logger.info("  Stored %d raw option data rows", len(raw_rows))

    # Step 5: Compute derived metrics
    logger.info("Step 5: Computing derived metrics")
    derived_rows = compute_derived_metrics(raw_rows)
    db.insert_derived_metrics(derived_rows)
    logger.info("  Stored %d derived metric rows", len(derived_rows))

    # Step 6: Compute aggregate metrics
    logger.info("Step 6: Computing aggregate metrics")
    iv_history = db.get_iv_history()
    iv_values = [row["atm_iv"] for row in iv_history if row["atm_iv"]]

    # Group derived rows by timestamp
    by_timestamp = defaultdict(list)
    for row in derived_rows:
        by_timestamp[row["timestamp"]].append(row)

    # Get previous day's spot close for each date
    prev_spot_close = None
    aggregate_rows = []
    prev_fair_price = None

    for ts in sorted(by_timestamp.keys()):
        ts_rows = by_timestamp[ts]
        spot = raw_rows[0]["spot"]  # get spot from any raw row at this timestamp
        for r in raw_rows:
            if r["timestamp"] == ts:
                spot = r["spot"]
                break

        agg = compute_aggregate_metrics(ts_rows, spot, prev_spot_close, iv_values, EXPIRY_DATE)

        if prev_fair_price is not None:
            agg["fair_price_chg"] = round(agg["fair_price"] - prev_fair_price, 2)
        prev_fair_price = agg["fair_price"]

        aggregate_rows.append(agg)

        # Update prev_spot_close at end of each day
        current_date = ts[:10]
        if aggregate_rows and aggregate_rows[-1]["timestamp"][:10] != current_date:
            prev_spot_close = spot

    db.insert_aggregate_metrics(aggregate_rows)
    logger.info("  Stored %d aggregate metric rows", len(aggregate_rows))

    # Step 7: Verify against NSE Bhavcopy
    logger.info("Step 7: Verifying against NSE Bhavcopy")
    summary = verify_against_bhavcopy(db)
    logger.info("  Verification: %d/%d matched (%.1f%%)",
                summary["total_matches"], summary["total_checks"], summary["match_rate"])

    if summary["mismatches"] > 0:
        logger.warning("  %d mismatches found! Check verification_log table.", summary["mismatches"])

    # Done
    db.close()
    logger.info("=" * 60)
    logger.info("Pipeline complete!")
    logger.info("  Database: %s", os.path.abspath(DB_PATH))
    logger.info("  Raw rows: %d", len(raw_rows))
    logger.info("  Derived rows: %d", len(derived_rows))
    logger.info("  Aggregate rows: %d", len(aggregate_rows))
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()
```

- [ ] **Step 2: Smoke test (dry run without API creds)**

Run: `cd C:/Users/LENOVO/Documents/Claude_Code/Projects/OptionsScraper && python -c "from DhanHQ_src.main import run_pipeline; print('Import OK')"`
Expected: `Import OK` (verifies all imports resolve)

- [ ] **Step 3: Commit**

```bash
git add DhanHQ_src/main.py
git commit -m "feat: main orchestrator pipeline - fetch, store, calculate, verify"
```

---

### Task 7: Integration Test & Final Wiring

**Files:**
- Modify: `DhanHQ_src/main.py` (if needed)
- All existing files

- [ ] **Step 1: Run all unit tests**

Run: `cd C:/Users/LENOVO/Documents/Claude_Code/Projects/OptionsScraper && python -m pytest tests/ -v`
Expected: All tests PASS

- [ ] **Step 2: Verify project structure**

Run: `ls -la DhanHQ_src/ && ls -la tests/ && ls -la data/`
Expected: All files present per the file map

- [ ] **Step 3: Verify imports end-to-end**

Run: `python -c "from DhanHQ_src.config import *; from DhanHQ_src.db import Database; from DhanHQ_src.fetcher import fetch_all_options_data; from DhanHQ_src.calculator import compute_derived_metrics; from DhanHQ_src.verifier import verify_against_bhavcopy; print('All imports OK')"`
Expected: `All imports OK`

- [ ] **Step 4: Create a test with mock API response to verify full pipeline**

```python
# tests/test_integration.py
import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock
from DhanHQ_src.db import Database
from DhanHQ_src.fetcher import parse_api_response, build_raw_rows
from DhanHQ_src.calculator import compute_derived_metrics, compute_aggregate_metrics


@pytest.fixture
def db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    database = Database(path)
    database.create_tables()
    yield database
    database.close()
    os.unlink(path)


def test_full_pipeline_mock(db):
    """Test the full pipeline with mock data: fetch → parse → store → calculate."""
    # Simulate API response for one strike CE
    ce_response = {
        "open": [500.0, 495.0],
        "high": [510.0, 505.0],
        "low": [495.0, 490.0],
        "close": [505.0, 498.0],
        "volume": [1000, 1200],
        "oi": [50000, 50500],
        "iv": [25.5, 25.8],
        "spot": [23250.0, 23260.0],
        "strike": [23200, 23200],
        "timestamp": [1742108100, 1742108160],
    }
    # Simulate API response for one strike PE
    pe_response = {
        "open": [400.0, 410.0],
        "high": [415.0, 420.0],
        "low": [395.0, 405.0],
        "close": [410.0, 415.0],
        "volume": [800, 900],
        "oi": [60000, 60200],
        "iv": [24.8, 25.0],
        "spot": [23250.0, 23260.0],
        "strike": [23200, 23200],
        "timestamp": [1742108100, 1742108160],
    }

    # Parse and build rows
    ce_parsed = parse_api_response(ce_response)
    ce_rows = build_raw_rows(ce_parsed, "CALL", 0, "2026-03-30")

    pe_parsed = parse_api_response(pe_response)
    pe_rows = build_raw_rows(pe_parsed, "PUT", 0, "2026-03-30")

    all_rows = ce_rows + pe_rows

    # Store raw data
    db.insert_raw_option_data(all_rows)
    stored = db.get_raw_data_ordered()
    assert len(stored) == 4  # 2 timestamps × 2 option types

    # Compute derived metrics
    derived = compute_derived_metrics(all_rows)
    assert len(derived) == 2  # 2 timestamps × 1 strike
    db.insert_derived_metrics(derived)

    # Compute aggregate metrics
    agg = compute_aggregate_metrics(derived[:1], 23250.0, None, [20.0, 22.0, 25.0], "2026-03-30")
    assert agg["total_ce_oi"] == 50000
    assert agg["total_pe_oi"] == 60000
    assert agg["overall_pcr"] > 0
    db.insert_aggregate_metrics([agg])

    # Verify data was stored
    result = db.conn.execute("SELECT COUNT(*) FROM aggregate_metrics").fetchone()
    assert result[0] == 1
```

- [ ] **Step 5: Run all tests including integration**

Run: `python -m pytest tests/ -v`
Expected: All tests PASS

- [ ] **Step 6: Final commit**

```bash
git add tests/test_integration.py
git commit -m "feat: integration test verifying full mock pipeline"
```

---

## Post-Implementation Notes

**To run the actual pipeline** (after DhanHQ API is set up):

1. Update `.env` with real `DHAN_CLIENT_ID` and `DHAN_ACCESS_TOKEN`
2. Ensure Data API subscription is active
3. Run: `cd C:/Users/LENOVO/Documents/Claude_Code/Projects/OptionsScraper && python -m DhanHQ_src.main`

**Expected data volume:**
- ~375 candles/day × 11 trading days × 9 strikes × 2 types = ~74,250 raw rows
- ~375 × 11 × 9 = ~37,125 derived metric rows
- ~375 × 11 = ~4,125 aggregate metric rows

**API call budget:**
- Main data: 9 strikes × 2 types = 18 calls
- IV baseline: ~13 calls (365 days / 30 days per chunk)
- Total: ~31 calls (well within 100K/day limit)
