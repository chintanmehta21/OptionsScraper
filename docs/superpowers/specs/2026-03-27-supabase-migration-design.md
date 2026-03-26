# Supabase Migration Design — SQLite to Supabase Postgres

> **Goal:** Completely replace SQLite with Supabase Postgres. Normalized schema, `supabase-py` SDK, MCP integration, multi-expiry support. Zero traces of SQLite remain.

**Date:** 2026-03-27
**Parent spec:** `2026-03-26-nifty-options-scraper-design.md`

---

## 1. Motivation

- **Cloud persistence + remote access** — query data from anywhere, enable dashboards
- **Multi-user collaboration** — team members can read/query the same dataset
- **Proper relational design** — normalize the flat SQLite schema into FK-linked Postgres tables

---

## 2. Supabase MCP Configuration

### `.mcp.json` (repo root)

```json
{
  "mcpServers": {
    "supabase": {
      "type": "http",
      "url": "https://mcp.supabase.com/mcp?project_ref=${SUPABASE_PROJECT_REF}"
    }
  }
}
```

### Environment Variables (added to `.env`)

```
SUPABASE_URL=https://<project_ref>.supabase.co
SUPABASE_SERVICE_ROLE_KEY=<service_role_key>
SUPABASE_ANON_KEY=<anon_key>
SUPABASE_PROJECT_REF=<project_ref>
```

- **Service role key** — used by the pipeline (full read/write, bypasses RLS)
- **Anon key** — for read-only consumers (respects RLS)

---

## 3. Normalized Postgres Schema (7 tables + 3 views)

### 3.1 Reference Tables

#### `expiries`

| Column | Type | Constraints |
|--------|------|-------------|
| id | SERIAL | PRIMARY KEY |
| symbol | TEXT | NOT NULL (e.g. 'NIFTY') |
| expiry_date | DATE | NOT NULL |
| expiry_flag | TEXT | NOT NULL ('MONTH', 'WEEK') |
| lot_size | INTEGER | NOT NULL |
| created_at | TIMESTAMPTZ | DEFAULT now() |

- UNIQUE(symbol, expiry_date)

#### `strikes`

| Column | Type | Constraints |
|--------|------|-------------|
| id | SERIAL | PRIMARY KEY |
| expiry_id | INTEGER | REFERENCES expiries(id) |
| strike | INTEGER | NOT NULL |
| atm_offset | INTEGER | NOT NULL (-4 to +4) |

- UNIQUE(expiry_id, strike)

### 3.2 Data Tables

#### `raw_candles` (replaces `raw_option_data`)

| Column | Type | Constraints |
|--------|------|-------------|
| id | BIGSERIAL | PRIMARY KEY |
| strike_id | INTEGER | REFERENCES strikes(id) |
| option_type | TEXT | NOT NULL CHECK(IN ('CE','PE')) |
| timestamp | TIMESTAMPTZ | NOT NULL |
| open | REAL | |
| high | REAL | |
| low | REAL | |
| close | REAL | |
| volume | INTEGER | |
| oi | INTEGER | |
| iv | REAL | |
| spot | REAL | |

- UNIQUE(strike_id, option_type, timestamp)
- Indexes: `(timestamp)`, `(strike_id, option_type, timestamp)`
- No redundant `date`, `time`, `expiry_date` columns — derivable from `timestamp` and FK chain

#### `derived_metrics`

| Column | Type |
|--------|------|
| strike_id | INTEGER REFERENCES strikes(id) |
| timestamp | TIMESTAMPTZ NOT NULL |
| ce_ltp, pe_ltp | REAL |
| ce_ltp_chg, pe_ltp_chg, pe_ltp_chg_pct | REAL |
| ce_volume, pe_volume | INTEGER |
| ce_oi, pe_oi, ce_oi_chg, pe_oi_chg | INTEGER |
| ce_iv, pe_iv, ce_iv_chg, pe_iv_chg | REAL |
| pe_ce_oi, pe_ce_oi_chg | INTEGER |
| pcr_oi, pcr_oi_chg, pcr_vol | REAL |

- PRIMARY KEY (strike_id, timestamp)

#### `aggregate_metrics`

| Column | Type |
|--------|------|
| expiry_id | INTEGER REFERENCES expiries(id) |
| timestamp | TIMESTAMPTZ NOT NULL |
| spot, spot_chg, spot_chg_pct | REAL |
| fair_price, fair_price_chg | REAL |
| atm_iv, ivr, ivp | REAL |
| max_pain | INTEGER |
| overall_pcr | REAL |
| lot_size | INTEGER |
| total_ce_oi, total_pe_oi, total_oi_net | INTEGER |
| total_ce_oi_chg, total_pe_oi_chg, total_oi_chg_net | INTEGER |
| total_bullish_oi, total_bearish_oi | INTEGER |
| otm_ce_oi, otm_pe_oi, otm_oi_net | INTEGER |
| otm_ce_oi_chg, otm_pe_oi_chg, otm_oi_chg_net | INTEGER |
| itm_ce_oi, itm_pe_oi, itm_oi_net | INTEGER |
| itm_ce_oi_chg, itm_pe_oi_chg, itm_oi_chg_net | INTEGER |

- PRIMARY KEY (expiry_id, timestamp)

#### `iv_history`

| Column | Type |
|--------|------|
| expiry_id | INTEGER REFERENCES expiries(id) |
| date | DATE NOT NULL |
| atm_iv | REAL |
| spot | REAL |
| atm_strike | INTEGER |

- PRIMARY KEY (expiry_id, date)

#### `verification_log`

| Column | Type |
|--------|------|
| strike_id | INTEGER REFERENCES strikes(id) |
| date | DATE NOT NULL |
| dhan_close, nse_close | REAL |
| dhan_oi, nse_oi | INTEGER |
| dhan_volume, nse_volume | INTEGER |
| close_match, oi_match, volume_match | BOOLEAN |
| notes | TEXT |

- PRIMARY KEY (strike_id, date)

### 3.3 Views

#### `v_eod_snapshot`
Last candle per strike/option_type/day. Replaces `get_eod_data()` DB method.

```sql
SELECT DISTINCT ON (rc.strike_id, rc.option_type, rc.timestamp::date)
    rc.strike_id, s.strike, rc.option_type,
    rc.timestamp::date AS date,
    rc.close, rc.oi, rc.volume, rc.spot,
    e.symbol, e.expiry_date
FROM raw_candles rc
JOIN strikes s ON s.id = rc.strike_id
JOIN expiries e ON e.id = s.expiry_id
ORDER BY rc.strike_id, rc.option_type, rc.timestamp::date, rc.timestamp DESC;
```

#### `v_pcr_trend`
PCR (OI-based) over time per expiry.

```sql
SELECT am.timestamp, am.overall_pcr, am.spot, e.symbol, e.expiry_date
FROM aggregate_metrics am
JOIN expiries e ON e.id = am.expiry_id
ORDER BY am.timestamp;
```

#### `v_oi_buildup`
OI change trends by strike.

```sql
SELECT dm.timestamp, s.strike, s.atm_offset,
    dm.ce_oi, dm.pe_oi, dm.ce_oi_chg, dm.pe_oi_chg,
    e.symbol, e.expiry_date
FROM derived_metrics dm
JOIN strikes s ON s.id = dm.strike_id
JOIN expiries e ON e.id = s.expiry_id
ORDER BY dm.timestamp, s.strike;
```

---

## 4. Row Level Security (RLS)

All 7 tables:

```sql
ALTER TABLE <table> ENABLE ROW LEVEL SECURITY;

-- Pipeline (service role) can do everything
CREATE POLICY "service_all" ON <table>
    FOR ALL USING (auth.role() = 'service_role');

-- Anonymous users can only read
CREATE POLICY "anon_read" ON <table>
    FOR SELECT USING (auth.role() = 'anon');
```

Views inherit RLS from underlying tables.

---

## 5. Python Module Changes

### `config.py`
- Remove: `DB_PATH`
- Add: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` from env
- Keep: all DhanHQ config unchanged

### `db.py` — Complete Rewrite
Replace `Database` class with `SupabaseDB`:

- `__init__()`: creates `supabase.Client` using service role key
- `ensure_expiry(symbol, expiry_date, expiry_flag, lot_size) -> int`: upsert into `expiries`, return `id`
- `ensure_strikes(expiry_id, strike_list) -> dict[int, int]`: upsert into `strikes`, return `{strike: strike_id}`
- `insert_raw_candles(rows)`: batch upsert (500-row chunks) via `.upsert()`
- `insert_derived_metrics(rows)`: batch upsert using `strike_id` FK
- `insert_aggregate_metrics(rows)`: batch upsert using `expiry_id` FK
- `insert_iv_history(rows)`: batch upsert using `expiry_id` FK
- `insert_verification_log(rows)`: batch upsert using `strike_id` FK
- `get_raw_data_by_date(date)`: `.select()` with date filter
- `get_raw_data_ordered()`: `.select()` with ordering
- `get_iv_history(expiry_id)`: `.select()` filtered by expiry
- `get_eod_data(date)`: query `v_eod_snapshot` view
- `get_distinct_dates(expiry_id)`: distinct dates from `raw_candles` via FK chain
- No `close()` method — HTTP client is stateless

### `fetcher.py` — No Changes
DhanHQ API logic is storage-independent.

### `calculator.py` — Minor Changes
- Input/output format stays the same (list of dicts)
- `SupabaseDB` handles strike→strike_id mapping so calculator remains DB-agnostic

### `verifier.py` — Minor Changes
- Works with `SupabaseDB` interface instead of `Database`
- Bhavcopy download/parse logic unchanged
- `SupabaseDB` maps strike_id ↔ strike for comparison results

### `main.py` — Small Changes
- `Database(DB_PATH)` → `SupabaseDB()`
- Remove `db.create_tables()` (schema via migrations)
- Add `expiry_id = db.ensure_expiry(...)` at pipeline start
- Add `strike_map = db.ensure_strikes(expiry_id, ...)` at pipeline start
- Pass `expiry_id`/`strike_map` through pipeline
- Remove `db.close()`

---

## 6. Dependencies

### `requirements.txt` changes
- Add: `supabase>=2.0.0`
- Keep: `dhanhq`, `pandas`, `scipy`, `requests`, `python-dotenv`, `pytest`

---

## 7. Batch Insert Strategy

- SQLite used `executemany()` for bulk inserts
- Supabase REST API: use `.upsert()` with 500-row batch chunks
- `ON CONFLICT` maps to `upsert(..., on_conflict="column1,column2")`
- `SupabaseDB` handles chunking internally

---

## 8. Test Strategy

- `test_db.py` — rewrite: mock `supabase.Client`, test `SupabaseDB` methods
- `test_integration.py` — mock `SupabaseDB` instead of `Database`
- `test_calculator.py` — unchanged (DB-agnostic)
- `test_fetcher.py` — unchanged
- `test_verifier.py` — minor: mock `SupabaseDB`

---

## 9. SQLite Cleanup Checklist

### Remove entirely
- `data/` directory (no local DB)
- `data/*.db` gitignore entry
- All `sqlite3` imports
- `DB_PATH` config
- `PRAGMA` statements
- `Database` class
- `conn.execute`, `executemany`, `executescript`, `row_factory`

### Words that must not appear in final codebase
`sqlite`, `sqlite3`, `nifty_options.db`, `DB_PATH`, `PRAGMA`, `executemany`, `executescript`, `row_factory`, `conn.execute`

---

## 10. Migration Order

1. Set up `.mcp.json` + env vars
2. Apply Postgres schema migration via Supabase MCP
3. Rewrite `config.py` (add Supabase vars, remove DB_PATH)
4. Rewrite `db.py` (SupabaseDB class)
5. Update `main.py` (new DB init + expiry/strike setup)
6. Update `calculator.py` (minor FK-aware changes)
7. Update `verifier.py` (minor interface changes)
8. Rewrite tests
9. Update docs + old plan references
10. Delete `data/` directory, clean gitignore
