---
name: supabase-db
description: Project-specific Supabase Postgres best practices for the NIFTY OptionsScraper. Covers schema conventions, supabase-py SDK patterns, RLS, migrations, batch upsert, and testing. Use when working with Supabase tables, writing migrations, or testing database operations.
---

# Supabase Database Best Practices — OptionsScraper

## 1. Schema Conventions

- Use **lowercase SQL** for all reserved words
- Use **snake_case** for tables and columns
- Plural table names (`expiries`, `strikes`, `raw_candles`)
- Singular column names (`strike`, `expiry_date`, `option_type`)
- Primary keys: `id bigint generated always as identity primary key`
- Foreign key columns: `{referenced_table_singular}_id` (e.g., `expiry_id`, `strike_id`)
- Add `comment on table` for every new table (max 1024 chars)
- Store timestamps as `TIMESTAMPTZ` (not `TIMESTAMP`)
- Store dates as `DATE` in ISO 8601 format

### This project's schema

```
expiries (id, symbol, expiry_date, expiry_flag, lot_size)
  └─ strikes (id, expiry_id, strike, atm_offset)
       ├─ raw_candles (id, strike_id, option_type, timestamp, OHLCIV, spot)
       ├─ derived_metrics (strike_id, timestamp, CE/PE pairs, PCR, OI diffs)
       ├─ iv_history (expiry_id, date, atm_iv, spot, atm_strike)
       └─ verification_log (strike_id, date, dhan vs nse comparisons)
  └─ aggregate_metrics (expiry_id, timestamp, spot, fair_price, IVR, max_pain, OI)
```

CASCADE DELETE flows: `expiries → strikes → raw_candles/derived_metrics/verification_log`.

---

## 2. supabase-py SDK Patterns

### Client creation
```python
from supabase import create_client
client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
```

### Critical gotchas

| Pattern | Correct | Wrong |
|---------|---------|-------|
| `on_conflict` | `"col1,col2"` (string) | `["col1", "col2"]` (list) |
| `in` filter | `.in_("col", [1,2,3])` (underscore) | `.in("col", [1,2,3])` |
| Row count | `response.count` with `count="exact"` | `len(response.data)` (capped at 1000) |
| Pagination | `.select("*").range(0, 1999)` | `.select("*")` (returns max 1000) |
| Bulk insert speed | `returning="minimal"` | Default returns all data |
| Upsert requires | PK columns in every row dict | Missing PK silently inserts |
| Missing fields | `default_to_null=False` uses column DEFAULT | Default: missing = NULL |

### Upsert pattern
```python
client.table("raw_candles").upsert(
    rows,
    on_conflict="strike_id,option_type,timestamp",
    returning="minimal",
).execute()
```

### Error handling
```python
from postgrest.exceptions import APIError

try:
    client.table("t").insert(row).execute()
except APIError as e:
    if "23503" in str(e):  # FK violation
        ...
    elif "23505" in str(e):  # unique violation
        ...
```

---

## 3. RLS Policies

### Rules
- **Never** use `FOR ALL` — create separate policies for SELECT, INSERT, UPDATE, DELETE
- Always specify role: `TO authenticated`, `TO anon`, `TO service_role`
- Wrap auth calls: `(select auth.uid())` not `auth.uid()` (avoids per-row function call)
- SELECT: `USING` only (no `WITH CHECK`)
- INSERT: `WITH CHECK` only (no `USING`)
- UPDATE: both `USING` and `WITH CHECK`
- DELETE: `USING` only
- Prefer `PERMISSIVE` policies (default) over `RESTRICTIVE`

### This project's pattern
```sql
-- Service role: full access
create policy "service_all" on raw_candles for all using (auth.role() = 'service_role');

-- Anon: read-only
create policy "anon_read" on raw_candles for select to anon using (true);
```

### Testing RLS
- Service role client (`SUPABASE_SERVICE_ROLE_KEY`): bypasses RLS
- Anon client (`SUPABASE_ANON_KEY`): respects RLS
- INSERT violations **throw errors** (use `pytest.raises`)
- UPDATE/DELETE violations **silently return 0 rows** (check response)

---

## 4. Migrations

### File naming
```
supabase/migrations/YYYYMMDDHHmmss_short_description.sql
```
Example: `20260327000001_reference_tables.sql`

### Rules
- Always enable RLS on new tables: `alter table X enable row level security;`
- Add comments for destructive SQL (DROP, TRUNCATE, ALTER COLUMN)
- One logical unit per migration file
- Use `supabase db diff -f name` for declarative schema changes
- Use `supabase db reset` to test migrations from scratch

### Testing migrations
```bash
supabase db reset  # Drops DB, replays all migrations + seed.sql
```

---

## 5. Batch Upsert

### Pattern
```python
BATCH_SIZE = 500

def batch_upsert(client, table, rows, on_conflict):
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i:i + BATCH_SIZE]
        client.table(table).upsert(
            chunk,
            on_conflict=on_conflict,
            returning="minimal",
        ).execute()
```

### Key facts
- PostgREST wraps each HTTP request in a transaction
- One constraint violation rolls back the **entire batch** (all-or-nothing)
- No hard limit on batch size, but 500-1000 rows recommended
- PK columns must be present in every row for upsert to match correctly
- Empty batch (0 rows) is a no-op, not an error

---

## 6. Testing Patterns

### Local Supabase stack
```bash
supabase start   # Starts Postgres + PostgREST + Auth in Docker
supabase db reset # Drops DB, replays migrations + seed.sql
supabase stop    # Shuts down
```

### Endpoints
- API: `http://localhost:54321`
- DB: `postgresql://postgres:postgres@localhost:54322/postgres`
- Studio: `http://localhost:54323`

### Fixtures
- `supa_service` — service_role client (bypasses RLS, for writes)
- `supa_anon` — anon client (respects RLS, for read-only validation)
- `pg_conn` — direct psycopg2 for schema introspection via `information_schema`
- `seed_expiry` — inserts test expiry + strikes, returns IDs, cleans up after

### Schema introspection via psycopg2
```python
cursor.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
""")
tables = [row[0] for row in cursor.fetchall()]
```

### Mocking for unit tests (when local stack unavailable)
```python
from unittest.mock import MagicMock

mock_client = MagicMock()
mock_response = MagicMock()
mock_response.data = [{"id": 1}]
mock_client.table.return_value.upsert.return_value.execute.return_value = mock_response
```

---

## 7. Project-Specific Notes

- **DhanHQ returns CALL/PUT**, DB stores CE/PE — `build_raw_rows()` handles conversion
- **Timestamps**: DhanHQ returns epoch seconds, convert to IST (UTC+5:30) TIMESTAMPTZ
- **ATM-relative strikes**: API uses `"ATM-4"` to `"ATM+4"`, mapped to `atm_offset` column
- **Views are read-only** via PostgREST — no INSERT/UPDATE/DELETE
- **`v_eod_snapshot`** uses `DISTINCT ON` — order matters for correctness
- **Calculator is DB-agnostic** — works with list-of-dicts, no FK awareness needed
- **`ensure_expiry()` + `ensure_strikes()`** must be called before any inserts (FK dependency)
