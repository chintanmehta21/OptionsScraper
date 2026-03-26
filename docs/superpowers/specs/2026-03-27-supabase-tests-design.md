# Supabase Database Testing — Design Spec

**Date:** 2026-03-27
**Goal:** Comprehensive pre- and post-migration test suite for the Supabase Postgres backend, plus a project-specific Supabase best practices skill.

**Approach:** pytest with local Supabase stack (Docker). Direct Postgres via `psycopg2` for schema/RLS validation. supabase-py SDK for application-layer tests. Single test framework, real database.

---

## Prerequisites

- **Docker Desktop** on Windows 11
- **Supabase CLI** via npm (`npx supabase`)
- **psycopg2-binary** added to requirements
- Local Supabase stack: `supabase init` + `supabase start`

## Test Infrastructure

### Fixtures (`tests/conftest.py`)

| Fixture | Scope | Purpose |
|---------|-------|---------|
| `supabase_reset` | session | Runs `supabase db reset` once before all tests |
| `supa_service` | session | supabase-py client with service_role key (bypasses RLS) |
| `supa_anon` | session | supabase-py client with anon key (respects RLS) |
| `pg_conn` | function | Direct psycopg2 connection for SQL introspection |
| `seed_expiry` | function | Inserts a test expiry + strikes, returns IDs |

### Local endpoints (after `supabase start`)

- PostgREST API: `http://localhost:54321`
- Direct Postgres: `postgresql://postgres:postgres@localhost:54322/postgres`
- Studio: `http://localhost:54323`

---

## Pre-Migration Validation (~20 tests)

Tests run against local Supabase to verify schema correctness before writing SupabaseDB class.

### Schema Structure (7 tests)

- 7 tables exist: expiries, strikes, raw_candles, derived_metrics, aggregate_metrics, iv_history, verification_log
- Column types match migration SQL (strike=INTEGER, timestamp=TIMESTAMPTZ, etc.)
- Primary keys correct on all tables
- Unique constraints: expiries(symbol, expiry_date), strikes(expiry_id, strike), raw_candles(strike_id, option_type, timestamp)
- CHECK constraints: option_type IN ('CE', 'PE'), expiry_flag IN ('MONTH', 'WEEK')
- Indexes exist: idx_raw_candles_timestamp, idx_raw_candles_strike_type_ts
- 3 views exist: v_eod_snapshot, v_pcr_trend, v_oi_buildup

### Foreign Keys & Cascades (5 tests)

- strikes.expiry_id -> expiries.id FK exists
- raw_candles.strike_id -> strikes.id FK exists
- Insert with nonexistent FK raises constraint violation (error code 23503)
- CASCADE DELETE: deleting expiry removes strikes, which removes candles
- CASCADE propagates through all levels (expiries -> strikes -> raw_candles + derived_metrics + iv_history + verification_log)

### RLS Policies (5 tests)

- Service role: INSERT, SELECT, UPDATE, DELETE on all 7 tables
- Anon role: SELECT on all 7 tables succeeds
- Anon role: INSERT blocked on all tables
- Anon role: UPDATE blocked on all tables
- Anon role: DELETE blocked on all tables

### Views (3 tests)

- v_eod_snapshot: returns one row per strike/option_type/day (DISTINCT ON)
- v_pcr_trend: joins aggregate_metrics with expiries correctly
- v_oi_buildup: joins derived_metrics with strikes and expiries correctly

---

## Post-Migration Tests (~25 tests)

Tests validate the SupabaseDB class via Python SDK against local Supabase.

### CRUD Operations (8 tests)

- ensure_expiry() upserts and returns ID; idempotent on duplicate
- ensure_strikes() upserts 9 strikes, returns {strike: strike_id} mapping
- insert_raw_candles() inserts with correct strike_id FK
- insert_derived_metrics() inserts with strike_id FK
- insert_aggregate_metrics() inserts with expiry_id FK
- insert_iv_history() inserts with expiry_id FK
- insert_verification_log() inserts with strike_id FK
- get_raw_data_by_date() filters and orders correctly

### Upsert & Conflict Resolution (5 tests)

- raw_candles upsert on (strike_id, option_type, timestamp) updates, no duplicate
- derived_metrics upsert on (strike_id, timestamp) replaces old row
- aggregate_metrics upsert on (expiry_id, timestamp) replaces old row
- Batch upsert of 1500 rows chunks into 3x500-row batches
- Single row failure in batch rolls back entire batch (atomic)

### Query Methods (5 tests)

- get_eod_data() returns last candle per strike/option_type/day
- get_iv_history() filtered by expiry_id, ordered by date
- get_distinct_dates() returns unique dates for an expiry
- get_raw_data_ordered() returns all data ordered by timestamp, strike, option_type
- Query with no matching data returns empty list

### Edge Cases (7 tests)

- NULL IV values accepted
- Boundary strike values (0, negative)
- Timezone: epoch -> TIMESTAMPTZ stored/queried correctly
- Pagination: >1000 rows handled via .range()
- Duplicate ensure_expiry() is idempotent
- on_conflict string format with composite keys
- Empty batch insert (0 rows) no error

---

## Custom Skill

**File:** `.claude/skills/supabase-db/SKILL.md`

Project-specific Supabase best practices covering:
1. Schema conventions (snake_case, identity IDs, FK naming)
2. supabase-py SDK patterns (on_conflict string, in_(), .range(), returning="minimal")
3. RLS policy rules (separate per operation, auth.uid() subquery)
4. Migration conventions (timestamped, RLS on every table)
5. Batch upsert (500-row chunks, atomic, PK in every row)
6. Testing patterns (local stack, psycopg2 introspection, anon vs service_role)
7. Project specifics (7-table schema, FK chain, DISTINCT ON views)

Updated on the fly as patterns are discovered during testing.

---

## File Map

| File | Action |
|------|--------|
| `supabase/config.toml` | Create (supabase init) |
| `supabase/migrations/001_initial_schema.sql` | Exists (move/verify) |
| `supabase/seed.sql` | Create (test seed data) |
| `tests/conftest.py` | Update (add Supabase fixtures) |
| `tests/test_supabase_schema.py` | Create (pre-migration: schema, FK, RLS, views) |
| `tests/test_supabase_db.py` | Create (post-migration: CRUD, upsert, queries, edge cases) |
| `DhanHQ_src/requirements.txt` | Update (add psycopg2-binary) |
| `.claude/skills/supabase-db/SKILL.md` | Create (best practices skill) |
