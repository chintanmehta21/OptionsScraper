# Supabase Database Testing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Comprehensive pre- and post-migration test suite for Supabase Postgres backend (~45 tests), plus a project-specific best practices skill.

**Approach:** pytest + local Supabase stack (Docker). psycopg2 for schema validation, supabase-py for SDK tests.

**Spec:** `docs/superpowers/specs/2026-03-27-supabase-tests-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `.claude/skills/supabase-db/SKILL.md` | Create | Project-specific Supabase best practices skill |
| `supabase/config.toml` | Create | Local Supabase stack config (via `supabase init`) |
| `supabase/migrations/` | Create/Move | Postgres migration SQL files |
| `supabase/seed.sql` | Create | Test seed data for local stack |
| `DhanHQ_src/requirements.txt` | Update | Add psycopg2-binary |
| `tests/supabase/__init__.py` | Create | Package init |
| `tests/supabase/conftest.py` | Create | Supabase fixtures (supa_service, supa_anon, pg_conn, seed_expiry) |
| `tests/supabase/test_schema.py` | Create | Pre-migration: schema, FK, RLS, views (20 tests) |
| `tests/supabase/test_db.py` | Create | Post-migration: CRUD, upsert, queries, edge cases (25 tests) |

---

### Task 1: Environment Setup (Docker + Supabase CLI + Skill)

**Files:** `.claude/skills/supabase-db/SKILL.md`, `DhanHQ_src/requirements.txt`

- [ ] **Step 1: Install Docker Engine (via WSL2)**

Install Docker CLI via WSL2 (no Docker Desktop needed). In WSL2:
```bash
sudo apt-get update
sudo apt-get install docker.io docker-compose-v2
sudo service docker start
docker --version
docker compose version
```
Or install Docker Desktop if preferred. Supabase CLI just needs a running Docker daemon.

- [ ] **Step 2: Install Supabase CLI**

```bash
npm install -g supabase
supabase --version
```

- [ ] **Step 3: Initialize Supabase in project**

```bash
cd /c/Users/LENOVO/Documents/Claude_Code/Projects/OptionsScraper
supabase init
```

This creates `supabase/config.toml`. Edit it to set the project name.

- [ ] **Step 4: Create migration files**

Move or create the migration SQL from `supabase-init.md` plan into `supabase/migrations/`:
- `supabase/migrations/20260327000001_reference_tables.sql` — expiries, strikes
- `supabase/migrations/20260327000002_data_tables.sql` — raw_candles, derived_metrics, aggregate_metrics, iv_history, verification_log
- `supabase/migrations/20260327000003_views.sql` — v_eod_snapshot, v_pcr_trend, v_oi_buildup
- `supabase/migrations/20260327000004_rls_policies.sql` — RLS enable + service/anon policies

Each migration file must contain the exact SQL from the `supabase-init.md` plan (Task 2).

- [ ] **Step 5: Create seed data**

Create `supabase/seed.sql` with test data:
```sql
-- Test expiry
insert into expiries (symbol, expiry_date, expiry_flag, lot_size)
values ('NIFTY', '2026-03-30', 'MONTH', 65);

-- Test strikes (ATM-4 through ATM+4 at 50-point spacing around 23000)
insert into strikes (expiry_id, strike, atm_offset)
values
    (1, 22800, -4), (1, 22850, -3), (1, 22900, -2), (1, 22950, -1),
    (1, 23000, 0), (1, 23050, 1), (1, 23100, 2), (1, 23150, 3), (1, 23200, 4);

-- Test raw candles (2 timestamps, 1 strike, CE+PE)
insert into raw_candles (strike_id, option_type, timestamp, open, high, low, close, volume, oi, iv, spot)
values
    (5, 'CE', '2026-03-16 09:15:00+05:30', 150.0, 155.0, 148.0, 153.0, 1000, 50000, 18.5, 23000.0),
    (5, 'PE', '2026-03-16 09:15:00+05:30', 140.0, 145.0, 138.0, 142.0, 800, 45000, 19.0, 23000.0),
    (5, 'CE', '2026-03-16 09:16:00+05:30', 153.0, 158.0, 151.0, 156.0, 1200, 51000, 18.8, 23010.0),
    (5, 'PE', '2026-03-16 09:16:00+05:30', 142.0, 143.0, 139.0, 140.0, 900, 45500, 19.2, 23010.0);
```

- [ ] **Step 6: Start local Supabase stack**

```bash
supabase start
```

Note the output: API URL, anon key, service_role key, DB URL. These are needed for test fixtures.

- [ ] **Step 7: Verify stack and apply migrations**

```bash
supabase db reset
```

This drops DB, replays all migrations, runs seed.sql.

- [ ] **Step 8: Add psycopg2-binary to requirements**

Add `psycopg2-binary>=2.9.0` to `DhanHQ_src/requirements.txt`. Install:
```bash
pip install psycopg2-binary>=2.9.0
```

- [ ] **Step 9: Create the Supabase best practices skill**

Create `.claude/skills/supabase-db/SKILL.md` with content from the design spec (schema conventions, SDK patterns, RLS rules, migration conventions, batch upsert, testing patterns, project specifics).

- [ ] **Step 10: Commit**

```bash
git add supabase/ .claude/skills/ DhanHQ_src/requirements.txt
git commit -m "setup: supabase local dev + skill"
```

---

### Task 2: Test Fixtures

**Files:** `tests/conftest.py`

- [ ] **Step 1: Add Supabase fixtures to conftest.py**

```python
import subprocess
import psycopg2
import pytest
from supabase import create_client

# Local Supabase defaults (from `supabase start` output)
LOCAL_SUPABASE_URL = "http://localhost:54321"
LOCAL_DB_URL = "postgresql://postgres:postgres@localhost:54322/postgres"
# Keys from supabase start output — update after first run
LOCAL_ANON_KEY = "<paste-from-supabase-start>"
LOCAL_SERVICE_ROLE_KEY = "<paste-from-supabase-start>"


@pytest.fixture(scope="session", autouse=True)
def supabase_reset():
    """Reset local Supabase DB before test session."""
    result = subprocess.run(
        ["supabase", "db", "reset"],
        capture_output=True, text=True, timeout=120,
        cwd=r"C:\Users\LENOVO\Documents\Claude_Code\Projects\OptionsScraper",
    )
    assert result.returncode == 0, f"supabase db reset failed: {result.stderr}"
    yield


@pytest.fixture(scope="session")
def supa_service(supabase_reset):
    """Supabase client with service_role key (bypasses RLS)."""
    return create_client(LOCAL_SUPABASE_URL, LOCAL_SERVICE_ROLE_KEY)


@pytest.fixture(scope="session")
def supa_anon(supabase_reset):
    """Supabase client with anon key (respects RLS)."""
    return create_client(LOCAL_SUPABASE_URL, LOCAL_ANON_KEY)


@pytest.fixture
def pg_conn(supabase_reset):
    """Direct Postgres connection for schema introspection."""
    conn = psycopg2.connect(LOCAL_DB_URL)
    yield conn
    conn.close()


@pytest.fixture
def seed_expiry(supa_service):
    """Insert a test expiry + 9 strikes, return (expiry_id, strike_map)."""
    resp = supa_service.table("expiries").upsert({
        "symbol": "NIFTY",
        "expiry_date": "2026-03-30",
        "expiry_flag": "MONTH",
        "lot_size": 65,
    }, on_conflict="symbol,expiry_date").execute()
    expiry_id = resp.data[0]["id"]

    strikes = []
    for offset in range(-4, 5):
        strikes.append({
            "expiry_id": expiry_id,
            "strike": 23000 + (offset * 50),
            "atm_offset": offset,
        })
    resp = supa_service.table("strikes").upsert(
        strikes, on_conflict="expiry_id,strike"
    ).execute()
    strike_map = {row["strike"]: row["id"] for row in resp.data}

    yield expiry_id, strike_map

    # Cleanup: cascade delete removes strikes + downstream
    supa_service.table("expiries").delete().eq("id", expiry_id).execute()
```

- [ ] **Step 2: Verify fixtures work**

```bash
python -m pytest tests/ -v -k "not test_" --co
```

Confirm no import errors.

- [ ] **Step 3: Commit**

```bash
git add tests/conftest.py
git commit -m "test: supabase fixtures"
```

---

### Task 3: Pre-Migration Validation Tests (20 tests)

**Files:** `tests/test_supabase_schema.py`

- [ ] **Step 1: Schema structure tests (7 tests)**

```python
# test_tables_exist — all 7 tables present in public schema
# test_column_types — spot-check key columns (strike=integer, timestamp=timestamptz, etc.)
# test_primary_keys — verify PK on each table
# test_unique_constraints — expiries(symbol,expiry_date), strikes(expiry_id,strike), raw_candles(strike_id,option_type,timestamp)
# test_check_constraints — option_type IN ('CE','PE'), expiry_flag IN ('MONTH','WEEK')
# test_indexes_exist — idx_raw_candles_timestamp, idx_raw_candles_strike_type_ts
# test_views_exist — v_eod_snapshot, v_pcr_trend, v_oi_buildup
```

All use `pg_conn` fixture with `information_schema` / `pg_catalog` queries.

- [ ] **Step 2: Foreign key & cascade tests (5 tests)**

```python
# test_strikes_fk_to_expiries — FK exists in pg_constraint
# test_raw_candles_fk_to_strikes — FK exists
# test_fk_violation_raises — INSERT into raw_candles with bad strike_id → IntegrityError
# test_cascade_delete_expiry — delete expiry → strikes + candles gone
# test_cascade_propagates_all_levels — delete expiry → all downstream tables cleared
```

- [ ] **Step 3: RLS policy tests (5 tests)**

```python
# test_service_role_full_access — INSERT+SELECT+UPDATE+DELETE on all tables via supa_service
# test_anon_can_select — SELECT on all tables via supa_anon succeeds
# test_anon_cannot_insert — INSERT on each table via supa_anon raises APIError
# test_anon_cannot_update — UPDATE via supa_anon returns 0 affected rows
# test_anon_cannot_delete — DELETE via supa_anon returns 0 affected rows
```

- [ ] **Step 4: View tests (3 tests)**

```python
# test_v_eod_snapshot_distinct_on — insert 2 candles at different times, view returns only the latest
# test_v_pcr_trend_joins — verify view contains expiry symbol and date alongside PCR
# test_v_oi_buildup_joins — verify view contains strike, atm_offset, symbol
```

- [ ] **Step 5: Run and verify all 20 pass**

```bash
python -m pytest tests/test_supabase_schema.py -v
```

- [ ] **Step 6: Commit**

```bash
git add tests/test_supabase_schema.py
git commit -m "test: pre-migration schema validation"
```

---

### Task 4: Post-Migration Tests (25 tests)

**Files:** `tests/test_supabase_db.py`

- [ ] **Step 1: CRUD operation tests (8 tests)**

```python
# test_ensure_expiry_creates — new expiry returns ID
# test_ensure_expiry_idempotent — same data returns same ID
# test_ensure_strikes_returns_mapping — returns {strike: id} dict with 9 entries
# test_insert_raw_candles — verify data in table via select
# test_insert_derived_metrics — verify with strike_id FK
# test_insert_aggregate_metrics — verify with expiry_id FK
# test_insert_iv_history — verify with expiry_id FK
# test_get_raw_data_by_date — filter by date, ordered by timestamp/strike/option_type
```

These tests will use the `SupabaseDB` class once implemented. Initially, write them against the SDK directly to validate the operations, then refactor to use `SupabaseDB` when Task 3 of `supabase-init.md` is done.

- [ ] **Step 2: Upsert & conflict resolution tests (5 tests)**

```python
# test_raw_candles_upsert_updates — same key → value changes, no duplicate
# test_derived_metrics_upsert_replaces — same (strike_id, timestamp) → new values
# test_aggregate_metrics_upsert_replaces — same (expiry_id, timestamp) → new values
# test_batch_upsert_chunking — 1500 rows split into 3x500 batches
# test_batch_atomic_rollback — one bad row rolls back entire batch
```

- [ ] **Step 3: Query method tests (5 tests)**

```python
# test_get_eod_data — last candle per strike/type/day from v_eod_snapshot
# test_get_iv_history_by_expiry — filtered by expiry_id, ordered by date
# test_get_distinct_dates — unique dates for given expiry
# test_get_raw_data_ordered — all data, ordered
# test_empty_query_returns_list — no data → empty list, not error
```

- [ ] **Step 4: Edge case tests (7 tests)**

```python
# test_null_iv_accepted — IV can be NULL
# test_boundary_strike_values — strike=0 and negative values
# test_timezone_epoch_to_timestamptz — epoch seconds → IST stored correctly
# test_pagination_over_1000_rows — insert 1500, select all via .range()
# test_ensure_expiry_idempotent_concurrent — multiple rapid calls, same result
# test_on_conflict_composite_key — on_conflict="strike_id,option_type,timestamp" works
# test_empty_batch_insert — 0 rows → no error
```

- [ ] **Step 5: Run and verify all 25 pass**

```bash
python -m pytest tests/test_supabase_db.py -v
```

- [ ] **Step 6: Commit**

```bash
git add tests/test_supabase_db.py
git commit -m "test: post-migration SupabaseDB tests"
```

---

### Task 5: Skill Updates & Final Verification

**Files:** `.claude/skills/supabase-db/SKILL.md`

- [ ] **Step 1: Update skill with discovered patterns**

After running all tests, update the skill file with any new patterns, gotchas, or best practices discovered during implementation.

- [ ] **Step 2: Run full test suite**

```bash
python -m pytest tests/ -v
```

Verify all tests pass (existing SQLite tests + new Supabase tests).

- [ ] **Step 3: Verify test count**

```bash
python -m pytest tests/test_supabase_schema.py tests/test_supabase_db.py -v --co | grep "test_" | wc -l
```

Should show ~45 tests.

- [ ] **Step 4: Final commit**

```bash
git add .claude/skills/
git commit -m "update: supabase skill with findings"
```

- [ ] **Step 5: Push**

```bash
git push origin master
```
