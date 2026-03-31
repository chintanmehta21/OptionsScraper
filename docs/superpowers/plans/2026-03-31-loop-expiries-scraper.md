# Loop Expiries Scraper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a year-parameterized batch scraper that fetches raw 1-min candle data for all NIFTY expiries (ATM-10 to ATM+10) and stores them in a flat `full_expiries_{year}` Supabase table, with resume support, TQDM-like progress, and GitHub Actions automation.

**Architecture:** New `DhanHQ_src/loop_expiries/` package with 4 modules (config, db, scraper, runner). Reuses existing `auth.py` and `fetcher.py` unchanged. Dynamic Supabase tables per year created via a Postgres function. Two GitHub Actions workflows: main scraper + hourly health monitor.

**Tech Stack:** Python 3.11, supabase-py, DhanHQ REST API, GitHub Actions, psycopg2 (migration only)

**Spec:** `docs/superpowers/specs/2026-03-31-loop-expiries-scraper-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `migrations/002_loop_expiries.sql` | Create | Postgres function `create_loop_tables(p_year)` |
| `DhanHQ_src/loop_expiries/__init__.py` | Create | Package init |
| `DhanHQ_src/loop_expiries/config.py` | Create | Expiry date generation, ATM±10 strikes, lot sizes |
| `DhanHQ_src/loop_expiries/db.py` | Create | Supabase client for dynamic year tables |
| `DhanHQ_src/loop_expiries/scraper.py` | Create | Core loop: fetch per-expiry, progress tracking |
| `DhanHQ_src/loop_expiries/runner.py` | Create | CLI entry point with --year, --reset, --status |
| `tests/test_loop_config.py` | Create | Unit tests for config module |
| `tests/test_loop_scraper.py` | Create | Unit tests for db + scraper (mocked) |
| `.github/workflows/loop_expiries.yml` | Create | Main scraper workflow |
| `.github/workflows/loop_expiries_monitor.yml` | Create | Hourly health-check workflow |

No existing files are modified.

---

### Task 1: Migration — `create_loop_tables` Postgres function

**Files:**
- Create: `migrations/002_loop_expiries.sql`

- [ ] **Step 1: Write the migration SQL**

Create `migrations/002_loop_expiries.sql`:

```sql
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
```

- [ ] **Step 2: Commit**

```bash
git add migrations/002_loop_expiries.sql
git commit -m "migration: create_loop_tables function"
```

---

### Task 2: Config module — expiry dates, strikes, lot sizes (TDD)

**Files:**
- Create: `tests/test_loop_config.py`
- Create: `DhanHQ_src/loop_expiries/__init__.py`
- Create: `DhanHQ_src/loop_expiries/config.py`

- [ ] **Step 1: Create package init**

Create `DhanHQ_src/loop_expiries/__init__.py`:

```python
```

(Empty file — just marks the package.)

- [ ] **Step 2: Write failing tests**

Create `tests/test_loop_config.py`:

```python
import pytest
from datetime import date
from DhanHQ_src.loop_expiries.config import (
    generate_expiry_dates,
    get_lot_size,
    LOOP_STRIKES,
    LOOP_OPTION_TYPES,
)


class TestLoopStrikes:
    def test_strike_count(self):
        assert len(LOOP_STRIKES) == 21

    def test_first_strike(self):
        assert LOOP_STRIKES[0] == "ATM-10"

    def test_last_strike(self):
        assert LOOP_STRIKES[-1] == "ATM+10"

    def test_atm_in_middle(self):
        assert LOOP_STRIKES[10] == "ATM"

    def test_option_types(self):
        assert LOOP_OPTION_TYPES == ["CALL", "PUT"]


class TestGenerateExpiryDates:
    def test_returns_list(self):
        result = generate_expiry_dates(2026)
        assert isinstance(result, list)
        assert len(result) > 0

    def test_all_thursdays(self):
        result = generate_expiry_dates(2026)
        for entry in result:
            d = date.fromisoformat(entry["expiry_date"])
            assert d.weekday() == 3, f"{entry['expiry_date']} is not Thursday"

    def test_weekly_count_roughly_52(self):
        result = generate_expiry_dates(2026)
        weekly = [e for e in result if e["expiry_flag"] == "WEEK"]
        assert 50 <= len(weekly) <= 53

    def test_monthly_count_is_12(self):
        result = generate_expiry_dates(2026)
        monthly = [e for e in result if e["expiry_flag"] == "MONTH"]
        assert len(monthly) == 12

    def test_from_date_is_14_days_before(self):
        result = generate_expiry_dates(2026)
        for entry in result:
            exp = date.fromisoformat(entry["expiry_date"])
            frm = date.fromisoformat(entry["from_date"])
            assert (exp - frm).days == 14

    def test_to_date_equals_expiry(self):
        result = generate_expiry_dates(2026)
        for entry in result:
            assert entry["to_date"] == entry["expiry_date"]

    def test_all_dates_in_year(self):
        result = generate_expiry_dates(2026)
        for entry in result:
            d = date.fromisoformat(entry["expiry_date"])
            assert d.year == 2026

    def test_monthly_is_last_thursday(self):
        """Each MONTH entry must be the last Thursday of its month."""
        import calendar
        result = generate_expiry_dates(2026)
        monthly = [e for e in result if e["expiry_flag"] == "MONTH"]
        for entry in monthly:
            d = date.fromisoformat(entry["expiry_date"])
            _, last_day = calendar.monthrange(d.year, d.month)
            last_date = date(d.year, d.month, last_day)
            while last_date.weekday() != 3:
                last_date -= __import__("datetime").timedelta(days=1)
            assert d == last_date, f"{d} is not last Thursday of {d.month}"

    def test_works_for_other_years(self):
        for year in [2024, 2025]:
            result = generate_expiry_dates(year)
            weekly = [e for e in result if e["expiry_flag"] == "WEEK"]
            assert 50 <= len(weekly) <= 53


class TestGetLotSize:
    def test_known_year_2026(self):
        assert get_lot_size(2026) == 65

    def test_known_year_2025(self):
        assert get_lot_size(2025) == 75

    def test_known_year_2024(self):
        assert get_lot_size(2024) == 25

    def test_unknown_year_fallback(self):
        assert get_lot_size(2020) == 75
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `python -m pytest tests/test_loop_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'DhanHQ_src.loop_expiries.config'`

- [ ] **Step 4: Implement config module**

Create `DhanHQ_src/loop_expiries/config.py`:

```python
"""Configuration for the loop expiries scraper."""

import calendar
from datetime import date, timedelta

# ATM-10 through ATM+10 = 21 strikes
LOOP_STRIKES = (
    [f"ATM{i}" for i in range(-10, 0)]
    + ["ATM"]
    + [f"ATM+{i}" for i in range(1, 11)]
)

LOOP_OPTION_TYPES = ["CALL", "PUT"]

# Historical NIFTY lot sizes by year
NIFTY_LOT_SIZES = {
    2024: 25,
    2025: 75,
    2026: 65,
}
_LOT_SIZE_FALLBACK = 75


def get_lot_size(year: int) -> int:
    """Return NIFTY lot size for a given year."""
    return NIFTY_LOT_SIZES.get(year, _LOT_SIZE_FALLBACK)


def _is_last_thursday(d: date) -> bool:
    """Check if a date is the last Thursday of its month."""
    _, last_day = calendar.monthrange(d.year, d.month)
    last_date = date(d.year, d.month, last_day)
    while last_date.weekday() != 3:
        last_date -= timedelta(days=1)
    return d == last_date


def generate_expiry_dates(year: int) -> list[dict]:
    """Generate all NIFTY expiry dates for a given year.

    Every Thursday → WEEK expiry.
    Last Thursday of each month → also a MONTH expiry (separate contract).
    Returns ~64 entries per year.
    """
    expiries = []
    d = date(year, 1, 1)
    while d.weekday() != 3:
        d += timedelta(days=1)

    while d.year == year:
        from_date = (d - timedelta(days=14)).isoformat()
        to_date = d.isoformat()

        expiries.append({
            "expiry_date": d.isoformat(),
            "expiry_flag": "WEEK",
            "from_date": from_date,
            "to_date": to_date,
        })

        if _is_last_thursday(d):
            expiries.append({
                "expiry_date": d.isoformat(),
                "expiry_flag": "MONTH",
                "from_date": from_date,
                "to_date": to_date,
            })

        d += timedelta(weeks=1)

    return expiries
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_loop_config.py -v`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add DhanHQ_src/loop_expiries/__init__.py DhanHQ_src/loop_expiries/config.py tests/test_loop_config.py
git commit -m "feat: loop_expiries config module"
```

---

### Task 3: DB module — Supabase client for dynamic tables (TDD)

**Files:**
- Create: `DhanHQ_src/loop_expiries/db.py`
- Append to: `tests/test_loop_scraper.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_loop_scraper.py`:

```python
import pytest
from unittest.mock import MagicMock, patch, call
from DhanHQ_src.loop_expiries.db import LoopExpiriesDB


@pytest.fixture
def mock_supabase():
    """Create a mock supabase client with chained method support."""
    client = MagicMock()
    # .rpc().execute() chain
    client.rpc.return_value.execute.return_value = MagicMock(data=None)
    # .table().upsert().execute() chain
    table_mock = MagicMock()
    table_mock.upsert.return_value.execute.return_value = MagicMock(data=[])
    table_mock.select.return_value.execute.return_value = MagicMock(data=[])
    table_mock.update.return_value.eq.return_value.eq.return_value.execute.return_value = (
        MagicMock(data=[])
    )
    client.table.return_value = table_mock
    return client


@pytest.fixture
def db(mock_supabase):
    with patch("DhanHQ_src.loop_expiries.db.create_client", return_value=mock_supabase):
        return LoopExpiriesDB(2026, url="http://fake", key="fake-key")


class TestLoopExpiriesDBSetup:
    def test_setup_tables_calls_rpc(self, db, mock_supabase):
        db.setup_tables()
        mock_supabase.rpc.assert_called_once_with(
            "create_loop_tables", {"p_year": 2026}
        )

    def test_table_names(self, db):
        assert db.data_table == "full_expiries_2026"
        assert db.progress_table == "scrape_progress_2026"

    def test_different_year(self, mock_supabase):
        with patch("DhanHQ_src.loop_expiries.db.create_client", return_value=mock_supabase):
            db = LoopExpiriesDB(2025, url="http://fake", key="fake-key")
        assert db.data_table == "full_expiries_2025"
        assert db.progress_table == "scrape_progress_2025"


class TestSeedProgress:
    def test_seed_inserts_pending(self, db, mock_supabase):
        expiries = [
            {"expiry_date": "2026-01-01", "expiry_flag": "WEEK"},
            {"expiry_date": "2026-01-08", "expiry_flag": "WEEK"},
        ]
        # No existing progress
        mock_supabase.table.return_value.select.return_value.execute.return_value.data = []
        db.seed_progress(expiries)
        mock_supabase.table.assert_any_call("scrape_progress_2026")

    def test_seed_skips_completed(self, db, mock_supabase):
        expiries = [
            {"expiry_date": "2026-01-01", "expiry_flag": "WEEK"},
            {"expiry_date": "2026-01-08", "expiry_flag": "WEEK"},
        ]
        # First one already completed
        mock_supabase.table.return_value.select.return_value.execute.return_value.data = [
            {"expiry_date": "2026-01-01", "expiry_flag": "WEEK", "status": "completed"},
        ]
        count = db.seed_progress(expiries)
        assert count == 1  # Only the second one seeded


class TestUpdateProgress:
    def test_update_sets_status(self, db, mock_supabase):
        db.update_progress("2026-01-01", "WEEK", status="completed", rows_inserted=5000)
        mock_supabase.table.assert_any_call("scrape_progress_2026")


class TestInsertCandles:
    def test_insert_maps_rows(self, db, mock_supabase):
        rows = [
            {
                "timestamp": "2026-03-16 09:15:00",
                "date": "2026-03-16",
                "time": "09:15:00",
                "expiry_date": "2026-03-30",
                "strike": 23200,
                "atm_offset": -2,
                "option_type": "CE",
                "open": 500.0,
                "high": 510.0,
                "low": 495.0,
                "close": 505.0,
                "volume": 1000,
                "oi": 50000,
                "iv": 25.5,
                "spot": 23250.0,
            }
        ]
        count = db.insert_candles(rows, "WEEK")
        assert count == 1
        mock_supabase.table.assert_any_call("full_expiries_2026")

    def test_insert_adds_timezone(self, db, mock_supabase):
        rows = [
            {
                "timestamp": "2026-03-16 09:15:00",
                "date": "2026-03-16",
                "time": "09:15:00",
                "expiry_date": "2026-03-30",
                "strike": 23200.0,  # float from API
                "atm_offset": 0,
                "option_type": "CE",
                "open": 500.0, "high": 510.0, "low": 495.0, "close": 505.0,
                "volume": 1000, "oi": 50000, "iv": 25.5, "spot": 23250.0,
            }
        ]
        db.insert_candles(rows, "WEEK")
        upsert_call = mock_supabase.table.return_value.upsert
        actual_rows = upsert_call.call_args_list[-1][0][0]
        assert actual_rows[0]["timestamp"] == "2026-03-16T09:15:00+05:30"
        assert actual_rows[0]["strike"] == 23200  # coerced to int


class TestGetProgressSummary:
    def test_summary_counts(self, db, mock_supabase):
        mock_supabase.table.return_value.select.return_value.execute.return_value.data = [
            {"expiry_date": "2026-01-01", "expiry_flag": "WEEK", "status": "completed",
             "rows_inserted": 5000, "error_message": None},
            {"expiry_date": "2026-01-08", "expiry_flag": "WEEK", "status": "failed",
             "rows_inserted": 0, "error_message": "timeout"},
            {"expiry_date": "2026-01-15", "expiry_flag": "WEEK", "status": "skipped",
             "rows_inserted": 0, "error_message": None},
            {"expiry_date": "2026-01-22", "expiry_flag": "WEEK", "status": "pending",
             "rows_inserted": 0, "error_message": None},
        ]
        summary = db.get_progress_summary()
        assert summary["total"] == 4
        assert summary["completed"] == 1
        assert summary["failed"] == 1
        assert summary["skipped"] == 1
        assert summary["pending"] == 1
        assert summary["in_progress"] == 0
        assert summary["total_rows"] == 5000
        assert len(summary["failed_details"]) == 1
        assert summary["failed_details"][0]["error_message"] == "timeout"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_loop_scraper.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'DhanHQ_src.loop_expiries.db'`

- [ ] **Step 3: Implement DB module**

Create `DhanHQ_src/loop_expiries/db.py`:

```python
"""Supabase client for loop_expiries dynamic year tables."""

import logging
from supabase import create_client

from DhanHQ_src.config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

logger = logging.getLogger(__name__)

BATCH_SIZE = 500

_INT_COLUMNS = {"strike", "atm_offset", "volume", "oi"}


class LoopExpiriesDB:
    def __init__(self, year: int, url=None, key=None):
        self.year = year
        self.data_table = f"full_expiries_{year}"
        self.progress_table = f"scrape_progress_{year}"
        self.client = create_client(
            url or SUPABASE_URL,
            key or SUPABASE_SERVICE_ROLE_KEY,
        )

    def setup_tables(self):
        """Create year-specific tables via Postgres function."""
        self.client.rpc("create_loop_tables", {"p_year": self.year}).execute()
        logger.info("Tables ready: %s, %s", self.data_table, self.progress_table)

    def seed_progress(self, expiries: list[dict]) -> int:
        """Seed progress table with pending entries, skip already completed/skipped."""
        existing = self._get_all_progress()
        done = {
            (e["expiry_date"], e["expiry_flag"])
            for e in existing
            if e["status"] in ("completed", "skipped")
        }

        rows = [
            {"expiry_date": e["expiry_date"], "expiry_flag": e["expiry_flag"], "status": "pending"}
            for e in expiries
            if (e["expiry_date"], e["expiry_flag"]) not in done
        ]

        if rows:
            for i in range(0, len(rows), BATCH_SIZE):
                chunk = rows[i : i + BATCH_SIZE]
                self.client.table(self.progress_table).upsert(
                    chunk, on_conflict="expiry_date,expiry_flag"
                ).execute()

        logger.info("Seeded %d pending expiries (%d already done)", len(rows), len(done))
        return len(rows)

    def get_pending_expiries(self) -> list[dict]:
        """Get expiries that need processing, ordered by date."""
        resp = (
            self.client.table(self.progress_table)
            .select("*")
            .in_("status", ["pending", "failed", "in_progress"])
            .order("expiry_date")
            .order("expiry_flag")
            .execute()
        )
        return resp.data

    def update_progress(self, expiry_date: str, expiry_flag: str, **fields):
        """Update progress for a single expiry."""
        (
            self.client.table(self.progress_table)
            .update(fields)
            .eq("expiry_date", expiry_date)
            .eq("expiry_flag", expiry_flag)
            .execute()
        )

    def reset_progress(self):
        """Reset all entries to pending (for --reset flag)."""
        all_rows = self._get_all_progress()
        for row in all_rows:
            self.update_progress(
                row["expiry_date"],
                row["expiry_flag"],
                status="pending",
                rows_inserted=0,
                api_calls_made=0,
                started_at=None,
                completed_at=None,
                error_message=None,
            )
        logger.info("Reset %d progress entries to pending", len(all_rows))

    def insert_candles(self, rows: list[dict], expiry_flag: str) -> int:
        """Batch upsert raw candle rows into full_expiries_{year}."""
        mapped = []
        for r in rows:
            ts = r["timestamp"]
            if "+" not in ts and "Z" not in ts:
                ts = ts.replace(" ", "T") + "+05:30"
            row = {
                "expiry_date": r["expiry_date"],
                "expiry_flag": expiry_flag,
                "timestamp": ts,
                "date": r["date"],
                "time": r["time"],
                "strike": int(r["strike"]) if r.get("strike") is not None else 0,
                "atm_offset": int(r["atm_offset"]),
                "option_type": r["option_type"],
                "open": r.get("open"),
                "high": r.get("high"),
                "low": r.get("low"),
                "close": r.get("close"),
                "volume": int(r["volume"]) if r.get("volume") is not None else None,
                "oi": int(r["oi"]) if r.get("oi") is not None else None,
                "iv": r.get("iv"),
                "spot": r.get("spot"),
            }
            mapped.append(row)

        for i in range(0, len(mapped), BATCH_SIZE):
            chunk = mapped[i : i + BATCH_SIZE]
            self.client.table(self.data_table).upsert(
                chunk,
                on_conflict="expiry_date,expiry_flag,timestamp,strike,option_type",
                returning="minimal",
            ).execute()

        logger.info("Upserted %d candle rows into %s", len(mapped), self.data_table)
        return len(mapped)

    def get_progress_summary(self) -> dict:
        """Get summary of scraping progress for reporting."""
        all_rows = self._get_all_progress()
        summary = {
            "total": len(all_rows),
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "pending": 0,
            "in_progress": 0,
            "total_rows": 0,
            "failed_details": [],
        }
        for row in all_rows:
            status = row["status"]
            summary[status] = summary.get(status, 0) + 1
            summary["total_rows"] += row.get("rows_inserted", 0) or 0
            if status == "failed":
                summary["failed_details"].append({
                    "expiry_date": row["expiry_date"],
                    "expiry_flag": row["expiry_flag"],
                    "error_message": row.get("error_message"),
                })
        return summary

    def close(self):
        """No-op — HTTP client is stateless."""
        pass

    def _get_all_progress(self) -> list[dict]:
        resp = (
            self.client.table(self.progress_table)
            .select("*")
            .order("expiry_date")
            .order("expiry_flag")
            .execute()
        )
        return resp.data
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_loop_scraper.py -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add DhanHQ_src/loop_expiries/db.py tests/test_loop_scraper.py
git commit -m "feat: loop_expiries db module"
```

---

### Task 4: Scraper module — single expiry fetch + main loop (TDD)

**Files:**
- Create: `DhanHQ_src/loop_expiries/scraper.py`
- Append to: `tests/test_loop_scraper.py`

- [ ] **Step 1: Write failing tests — append to `tests/test_loop_scraper.py`**

Add these classes at the end of `tests/test_loop_scraper.py`:

```python
from unittest.mock import patch, MagicMock
from DhanHQ_src.loop_expiries.scraper import scrape_single_expiry, run_loop


def _make_api_response(n_candles=3):
    """Build a fake DhanHQ parallel-array response."""
    return {
        "timestamp": [1742108100 + i * 60 for i in range(n_candles)],
        "open": [500.0] * n_candles,
        "high": [510.0] * n_candles,
        "low": [495.0] * n_candles,
        "close": [505.0] * n_candles,
        "volume": [1000] * n_candles,
        "oi": [50000] * n_candles,
        "iv": [25.5] * n_candles,
        "spot": [23250.0] * n_candles,
        "strike": [23200] * n_candles,
    }


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
        dhan = MagicMock()
        rows, api_calls, empty_count = scrape_single_expiry(dhan, expiry)

        assert api_calls == 42  # 21 strikes x 2 types
        assert empty_count == 0
        assert len(rows) == 3 * 42  # 3 candles x 42 calls

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
        dhan = MagicMock()
        rows, api_calls, empty_count = scrape_single_expiry(dhan, expiry)

        assert api_calls == 42
        assert empty_count == 42
        assert len(rows) == 0


class TestRunLoop:
    @patch("DhanHQ_src.loop_expiries.scraper.LoopExpiriesDB")
    @patch("DhanHQ_src.loop_expiries.scraper.create_dhan_client")
    @patch("DhanHQ_src.loop_expiries.scraper.get_access_token", return_value="fake-token")
    @patch("DhanHQ_src.loop_expiries.scraper.scrape_single_expiry")
    def test_skips_completed_expiries(self, mock_scrape, mock_auth, mock_dhan, mock_db_cls):
        mock_db = MagicMock()
        mock_db_cls.return_value = mock_db
        # All completed — nothing pending
        mock_db.get_pending_expiries.return_value = []
        mock_db.get_progress_summary.return_value = {
            "completed": 2, "failed": 0, "skipped": 0,
            "pending": 0, "in_progress": 0, "total": 2, "total_rows": 10000,
            "failed_details": [],
        }

        stats = run_loop(2026)
        mock_scrape.assert_not_called()
        assert stats["completed"] == 0  # nothing new to process

    @patch("DhanHQ_src.loop_expiries.scraper.LoopExpiriesDB")
    @patch("DhanHQ_src.loop_expiries.scraper.create_dhan_client")
    @patch("DhanHQ_src.loop_expiries.scraper.get_access_token", return_value="fake-token")
    @patch("DhanHQ_src.loop_expiries.scraper.scrape_single_expiry")
    def test_marks_skipped_on_all_empty(self, mock_scrape, mock_auth, mock_dhan, mock_db_cls):
        mock_db = MagicMock()
        mock_db_cls.return_value = mock_db
        # DB returns rows WITHOUT from_date/to_date — scraper computes them
        mock_db.get_pending_expiries.return_value = [
            {"expiry_date": "2026-01-01", "expiry_flag": "WEEK"},
        ]
        mock_db.get_progress_summary.return_value = {
            "completed": 0, "failed": 0, "skipped": 0,
            "pending": 1, "in_progress": 0, "total": 1, "total_rows": 0,
            "failed_details": [],
        }
        # All 42 calls empty
        mock_scrape.return_value = ([], 42, 42)

        stats = run_loop(2026)
        # Should have called update_progress with status="skipped"
        calls = mock_db.update_progress.call_args_list
        assert "skipped" in [c.kwargs["status"] for c in calls if "status" in c.kwargs]

    @patch("DhanHQ_src.loop_expiries.scraper.LoopExpiriesDB")
    @patch("DhanHQ_src.loop_expiries.scraper.create_dhan_client")
    @patch("DhanHQ_src.loop_expiries.scraper.get_access_token", return_value="fake-token")
    @patch("DhanHQ_src.loop_expiries.scraper.scrape_single_expiry")
    def test_marks_failed_on_error(self, mock_scrape, mock_auth, mock_dhan, mock_db_cls):
        mock_db = MagicMock()
        mock_db_cls.return_value = mock_db
        # DB returns rows WITHOUT from_date/to_date — scraper computes them
        mock_db.get_pending_expiries.return_value = [
            {"expiry_date": "2026-01-01", "expiry_flag": "WEEK"},
        ]
        mock_db.get_progress_summary.return_value = {
            "completed": 0, "failed": 0, "skipped": 0,
            "pending": 1, "in_progress": 0, "total": 1, "total_rows": 0,
            "failed_details": [],
        }
        mock_scrape.side_effect = RuntimeError("API timeout")

        stats = run_loop(2026)
        assert stats["failed"] == 1
        # Continues despite error — no exception raised
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_loop_scraper.py::TestScrapeSingleExpiry -v`
Expected: FAIL — `ImportError: cannot import name 'scrape_single_expiry'`

- [ ] **Step 3: Implement scraper module**

Create `DhanHQ_src/loop_expiries/scraper.py`:

```python
"""Core loop: iterate expiries, fetch raw candles, track progress."""

import os
import time
import logging
from datetime import datetime, timezone, timedelta

from DhanHQ_src.auth import get_access_token
from datetime import date as dt_date

from DhanHQ_src.fetcher import (
    create_dhan_client,
    fetch_with_retry,
    parse_api_response,
    build_raw_rows,
)
from DhanHQ_src.config import (
    NIFTY_SECURITY_ID,
    EXCHANGE_SEGMENT,
    INSTRUMENT_TYPE,
    REQUIRED_DATA,
    API_DELAY_SECONDS,
)
from DhanHQ_src.loop_expiries.config import (
    LOOP_STRIKES,
    LOOP_OPTION_TYPES,
    generate_expiry_dates,
)
from DhanHQ_src.loop_expiries.db import LoopExpiriesDB

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
_CI = os.environ.get("GITHUB_ACTIONS") == "true"
_TOKEN_MAX_AGE_S = 20 * 3600  # refresh after 20h


def _group(title):
    if _CI:
        print(f"::group::{title}", flush=True)
    logger.info(title)


def _endgroup():
    if _CI:
        print("::endgroup::", flush=True)


def scrape_single_expiry(dhan, expiry: dict):
    """Fetch all strikes x option types for one expiry.

    Returns (rows, api_calls, empty_count).
    Rows are flat dicts from build_raw_rows (ready for DB insert).
    """
    expiry_date = expiry["expiry_date"]
    expiry_flag = expiry["expiry_flag"]
    from_date = expiry["from_date"]
    to_date = expiry["to_date"]

    strike_offsets = {s: i - len(LOOP_STRIKES) // 2 for i, s in enumerate(LOOP_STRIKES)}

    all_rows = []
    api_calls = 0
    empty_count = 0

    for strike in LOOP_STRIKES:
        for option_type in LOOP_OPTION_TYPES:
            api_calls += 1
            response = fetch_with_retry(
                dhan,
                security_id=NIFTY_SECURITY_ID,
                exchange_segment=EXCHANGE_SEGMENT,
                instrument_type=INSTRUMENT_TYPE,
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


def run_loop(year: int, reset: bool = False) -> dict:
    """Main loop: scrape all expiries for a year with resume support.

    Returns stats dict: {completed, failed, skipped, total_rows}.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    db = LoopExpiriesDB(year)
    db.setup_tables()

    expiries = generate_expiry_dates(year)
    total = len(expiries)
    logger.info("=" * 60)
    logger.info("Loop Expiries Scraper — year %d (%d expiries)", year, total)
    logger.info("=" * 60)

    if reset:
        db.reset_progress()

    db.seed_progress(expiries)

    # Auth
    token = get_access_token()
    token_time = time.time()
    dhan = create_dhan_client(token)

    # Get pending work
    pending = db.get_pending_expiries()
    already_done = total - len(pending)
    logger.info("Resuming: %d done, %d to process", already_done, len(pending))

    start_time = time.time()
    stats = {"completed": 0, "failed": 0, "skipped": 0, "total_rows": 0}

    for i, progress_row in enumerate(pending, already_done + 1):
        exp_date = progress_row["expiry_date"]
        exp_flag = progress_row["expiry_flag"]
        # Reconstruct from_date/to_date (progress table only stores PK fields)
        exp_dt = dt_date.fromisoformat(exp_date)
        expiry = {
            "expiry_date": exp_date,
            "expiry_flag": exp_flag,
            "from_date": (exp_dt - timedelta(days=14)).isoformat(),
            "to_date": exp_date,
        }
        label = f"[{i}/{total}] {i / total * 100:.1f}% | {exp_date} {exp_flag}"

        # Token refresh if old
        if time.time() - token_time > _TOKEN_MAX_AGE_S:
            logger.info("Refreshing access token (>20h old)")
            token = get_access_token()
            token_time = time.time()
            dhan = create_dhan_client(token)

        _group(label)
        db.update_progress(
            exp_date, exp_flag,
            status="in_progress",
            started_at=datetime.now(IST).isoformat(),
        )

        try:
            rows, api_calls, empty_count = scrape_single_expiry(dhan, expiry)
            now_str = datetime.now(IST).isoformat()

            if empty_count == api_calls:
                db.update_progress(
                    exp_date, exp_flag,
                    status="skipped",
                    api_calls_made=api_calls,
                    completed_at=now_str,
                )
                stats["skipped"] += 1
                logger.info("  SKIPPED (holiday): all %d calls empty", api_calls)
            else:
                db.insert_candles(rows, exp_flag)
                db.update_progress(
                    exp_date, exp_flag,
                    status="completed",
                    rows_inserted=len(rows),
                    api_calls_made=api_calls,
                    completed_at=now_str,
                )
                stats["completed"] += 1
                stats["total_rows"] += len(rows)

                elapsed = time.time() - start_time
                done_in_session = i - already_done
                avg = elapsed / done_in_session if done_in_session > 0 else 0
                eta = avg * (total - i) / 60
                logger.info(
                    "  %s | %d rows | ETA: %.0fm", label, len(rows), eta
                )

        except Exception as e:
            db.update_progress(
                exp_date, exp_flag,
                status="failed",
                error_message=str(e)[:500],
                completed_at=datetime.now(IST).isoformat(),
            )
            stats["failed"] += 1
            logger.error("  FAILED: %s", e)

        _endgroup()

    # Final summary
    elapsed_total = time.time() - start_time
    logger.info("=" * 60)
    logger.info(
        "DONE: %d completed, %d failed, %d skipped | %d rows | %.1fm",
        stats["completed"],
        stats["failed"],
        stats["skipped"],
        stats["total_rows"],
        elapsed_total / 60,
    )
    logger.info("=" * 60)

    _write_job_summary(year, db, elapsed_total)

    return stats


def _write_job_summary(year: int, db: LoopExpiriesDB, elapsed: float):
    """Write markdown summary to $GITHUB_STEP_SUMMARY."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    s = db.get_progress_summary()
    lines = [
        f"## Loop Expiries Scraper — {year}",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Completed | {s['completed']}/{s['total']} |",
        f"| Failed | {s['failed']} |",
        f"| Skipped | {s['skipped']} |",
        f"| Pending | {s['pending']} |",
        f"| Total Rows | {s['total_rows']:,} |",
        f"| Duration | {elapsed / 60:.1f}m |",
        "",
    ]

    if s["failed_details"]:
        lines.extend(["### Failed Expiries", ""])
        for f in s["failed_details"]:
            lines.append(f"- **{f['expiry_date']} {f['expiry_flag']}**: {f['error_message']}")
        lines.append("")

    try:
        with open(summary_path, "a") as f:
            f.write("\n".join(lines) + "\n")
    except Exception as e:
        logger.debug("Could not write job summary: %s", e)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_loop_scraper.py -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add DhanHQ_src/loop_expiries/scraper.py tests/test_loop_scraper.py
git commit -m "feat: loop_expiries scraper module"
```

---

### Task 5: Runner CLI entry point

**Files:**
- Create: `DhanHQ_src/loop_expiries/runner.py`

- [ ] **Step 1: Implement runner**

Create `DhanHQ_src/loop_expiries/runner.py`:

```python
"""CLI entry point: python -m DhanHQ_src.loop_expiries.runner --year 2026"""

import argparse
import os
import sys
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from DhanHQ_src.loop_expiries.scraper import run_loop
from DhanHQ_src.loop_expiries.db import LoopExpiriesDB


def print_status(year: int):
    """Query Supabase and print progress summary."""
    db = LoopExpiriesDB(year)
    s = db.get_progress_summary()

    print(f"Loop Expiries Progress — {year}")
    print(f"  Total:       {s['total']}")
    print(f"  Completed:   {s['completed']}")
    print(f"  Failed:      {s['failed']}")
    print(f"  Skipped:     {s['skipped']}")
    print(f"  Pending:     {s['pending']}")
    print(f"  In Progress: {s['in_progress']}")
    print(f"  Total Rows:  {s['total_rows']:,}")

    if s["failed_details"]:
        print("  Failed expiries:")
        for f in s["failed_details"]:
            print(f"    {f['expiry_date']} {f['expiry_flag']}: {f['error_message']}")

    # Exit code: 0 if everything is completed/skipped, 1 if failures exist
    if s["failed"] > 0:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Loop Expiries Scraper")
    parser.add_argument("--year", type=int, required=True, help="Year to scrape (e.g. 2026)")
    parser.add_argument("--reset", action="store_true", help="Reset progress and start fresh")
    parser.add_argument("--status", action="store_true", help="Print progress summary and exit")
    args = parser.parse_args()

    if args.status:
        print_status(args.year)
        return

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stats = run_loop(args.year, reset=args.reset)

    if stats["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify CLI help works**

Run: `python -m DhanHQ_src.loop_expiries.runner --help`
Expected: Help text showing `--year`, `--reset`, `--status` flags.

- [ ] **Step 3: Commit**

```bash
git add DhanHQ_src/loop_expiries/runner.py
git commit -m "feat: loop_expiries runner CLI"
```

---

### Task 6: GitHub Actions — main scraper workflow

**Files:**
- Create: `.github/workflows/loop_expiries.yml`

- [ ] **Step 1: Write the workflow**

Create `.github/workflows/loop_expiries.yml`:

```yaml
name: Loop Expiries Scraper

on:
  workflow_dispatch:
    inputs:
      year:
        description: 'Year to scrape (e.g. 2026, 2025)'
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
      - name: Checkout repository
        uses: actions/checkout@v5

      - name: Set up Python
        uses: actions/setup-python@v5.6.0
        with:
          python-version: '3.11'
          cache: 'pip'
          cache-dependency-path: DhanHQ_src/requirements.txt

      - name: Install dependencies
        run: pip install -r DhanHQ_src/requirements.txt

      - name: Run loop expiries scraper
        env:
          DHAN_CLIENT_ID: ${{ secrets.DHAN_CLIENT_ID }}
          DHAN_PIN: ${{ secrets.DHAN_PIN }}
          DHAN_TOTP_SECRET: ${{ secrets.DHAN_TOTP_SECRET }}
          DHAN_ACCESS_TOKEN: ${{ secrets.DHAN_ACCESS_TOKEN }}
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_ROLE_KEY: ${{ secrets.SUPABASE_SERVICE_ROLE_KEY }}
        run: |
          ARGS="--year ${{ inputs.year }}"
          if [ "${{ inputs.reset }}" = "true" ]; then
            ARGS="$ARGS --reset"
          fi
          python -m DhanHQ_src.loop_expiries.runner $ARGS
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/loop_expiries.yml
git commit -m "ci: loop expiries scraper workflow"
```

---

### Task 7: GitHub Actions — health monitor workflow

**Files:**
- Create: `.github/workflows/loop_expiries_monitor.yml`

- [ ] **Step 1: Write the monitor workflow**

Create `.github/workflows/loop_expiries_monitor.yml`:

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
        type: string

jobs:
  check:
    runs-on: ubuntu-latest
    timeout-minutes: 5

    steps:
      - name: Checkout repository
        uses: actions/checkout@v5

      - name: Check if scraper is running
        id: running_check
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          RUNNING=$(gh run list \
            --workflow=loop_expiries.yml \
            --status=in_progress \
            --json status \
            --jq 'length')
          echo "running=$RUNNING" >> "$GITHUB_OUTPUT"
          if [ "$RUNNING" -gt 0 ]; then
            echo "## Loop Expiries Monitor" >> "$GITHUB_STEP_SUMMARY"
            echo "" >> "$GITHUB_STEP_SUMMARY"
            echo ":hourglass: **Scraper is still running.** Nothing to report." >> "$GITHUB_STEP_SUMMARY"
            echo "still_running=true" >> "$GITHUB_OUTPUT"
          else
            echo "still_running=false" >> "$GITHUB_OUTPUT"
          fi

      - name: Set up Python
        if: steps.running_check.outputs.still_running == 'false'
        uses: actions/setup-python@v5.6.0
        with:
          python-version: '3.11'
          cache: 'pip'
          cache-dependency-path: DhanHQ_src/requirements.txt

      - name: Install dependencies
        if: steps.running_check.outputs.still_running == 'false'
        run: pip install -r DhanHQ_src/requirements.txt

      - name: Query Supabase progress
        if: steps.running_check.outputs.still_running == 'false'
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_ROLE_KEY: ${{ secrets.SUPABASE_SERVICE_ROLE_KEY }}
        run: |
          YEAR="${{ inputs.year || '2026' }}"
          python -m DhanHQ_src.loop_expiries.runner --year "$YEAR" --status
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/loop_expiries_monitor.yml
git commit -m "ci: loop expiries monitor workflow"
```

---

### Task 8: Apply migration and run all tests

**Files:**
- No new files

- [ ] **Step 1: Run all unit tests**

Run: `python -m pytest tests/test_loop_config.py tests/test_loop_scraper.py -v`
Expected: ALL PASS

- [ ] **Step 2: Run existing tests (no regressions)**

Run: `python -m pytest tests/ -v --ignore=tests/supabase`
Expected: ALL PASS — existing tests unaffected

- [ ] **Step 3: Apply migration to Supabase**

Run: `python migrations/apply_migration.py` (with modified `__main__` to apply 002 instead of 001)

Alternative: Apply manually via Supabase SQL Editor — paste the content of `migrations/002_loop_expiries.sql`.

Verify: After applying, run `SELECT create_loop_tables(2026);` in the SQL editor. Then confirm tables exist:
```sql
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public' AND table_name LIKE '%2026%';
```
Expected: `full_expiries_2026`, `scrape_progress_2026`

- [ ] **Step 4: Trigger workflow via GitHub CLI**

Run: `gh workflow run loop_expiries.yml -f year=2026`

Monitor: `gh run watch` or check the Actions tab.

- [ ] **Step 5: Final commit (if any fixes needed)**

```bash
git add -A
git commit -m "fix: loop expiries post-integration fixes"
```
