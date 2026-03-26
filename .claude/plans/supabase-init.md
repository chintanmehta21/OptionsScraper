# Supabase Migration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Completely replace SQLite with Supabase Postgres. Normalized 7-table schema, `supabase-py` SDK, Supabase MCP integration, RLS policies. Zero traces of SQLite remain.

**Spec:** `docs/superpowers/specs/2026-03-27-supabase-migration-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `.mcp.json` | Create | Supabase MCP server config |
| `.env` | Update | Add Supabase env vars |
| `.gitignore` | Update | Remove `data/*.db`, add nothing |
| `DhanHQ_src/config.py` | Update | Remove DB_PATH, add Supabase vars |
| `DhanHQ_src/db.py` | Rewrite | SupabaseDB class replacing Database |
| `DhanHQ_src/main.py` | Update | New DB init, expiry/strike setup |
| `DhanHQ_src/calculator.py` | Update | Minor FK-aware changes |
| `DhanHQ_src/verifier.py` | Update | Use SupabaseDB interface |
| `DhanHQ_src/requirements.txt` | Update | Add supabase>=2.0.0 |
| `tests/test_db.py` | Rewrite | Mock supabase.Client tests |
| `tests/test_integration.py` | Update | Mock SupabaseDB |
| `tests/test_verifier.py` | Update | Mock SupabaseDB |
| `docs/superpowers/specs/2026-03-26-nifty-options-scraper-design.md` | Update | Replace SQLite references |
| `.claude/plans/2026-03-26-nifty-options-scraper.md` | Update | Replace SQLite references |

---

### Task 1: Supabase MCP + Project Config

**Files:** `.mcp.json`, `.env`, `DhanHQ_src/config.py`, `DhanHQ_src/requirements.txt`, `.gitignore`

- [ ] **Step 1: Create `.mcp.json`**

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

- [ ] **Step 2: Update `.env` with Supabase variables**

Add to existing `.env`:
```
SUPABASE_URL=https://<project_ref>.supabase.co
SUPABASE_SERVICE_ROLE_KEY=<service_role_key>
SUPABASE_ANON_KEY=<anon_key>
SUPABASE_PROJECT_REF=<project_ref>
```

- [ ] **Step 3: Update `config.py`**

Remove:
- `DB_PATH`
- `BHAVCOPY_DIR` using `os.path.dirname`

Add:
```python
# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Bhavcopy temp dir
BHAVCOPY_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "bhavcopy")
```

Keep: all DhanHQ config, rate limits, strikes, risk-free rate, lot size.

- [ ] **Step 4: Update `requirements.txt`**

Add `supabase>=2.0.0` to the dependencies list.

- [ ] **Step 5: Update `.gitignore`**

Remove `data/*.db` line. Keep `data/bhavcopy/` (still used for temp bhavcopy downloads).

- [ ] **Step 6: Install new dependencies**

```bash
pip install supabase>=2.0.0
```

- [ ] **Step 7: Commit**

```bash
git add .mcp.json .gitignore DhanHQ_src/config.py DhanHQ_src/requirements.txt
git commit -m "feat: supabase config and MCP setup"
```

---

### Task 2: Postgres Schema Migration

**Method:** Use Supabase MCP `apply_migration` or `execute_sql` tool to run the migration SQL.

- [ ] **Step 1: Create and apply migration — reference tables**

```sql
-- expiries: tracks expiry cycles
CREATE TABLE expiries (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    expiry_date DATE NOT NULL,
    expiry_flag TEXT NOT NULL CHECK (expiry_flag IN ('MONTH', 'WEEK')),
    lot_size INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(symbol, expiry_date)
);

-- strikes: per-expiry strike prices
CREATE TABLE strikes (
    id SERIAL PRIMARY KEY,
    expiry_id INTEGER NOT NULL REFERENCES expiries(id) ON DELETE CASCADE,
    strike INTEGER NOT NULL,
    atm_offset INTEGER NOT NULL,
    UNIQUE(expiry_id, strike)
);
```

- [ ] **Step 2: Create and apply migration — data tables**

```sql
-- raw_candles: 1-min OHLCIV candle data
CREATE TABLE raw_candles (
    id BIGSERIAL PRIMARY KEY,
    strike_id INTEGER NOT NULL REFERENCES strikes(id) ON DELETE CASCADE,
    option_type TEXT NOT NULL CHECK (option_type IN ('CE', 'PE')),
    timestamp TIMESTAMPTZ NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    oi INTEGER,
    iv REAL,
    spot REAL,
    UNIQUE(strike_id, option_type, timestamp)
);

CREATE INDEX idx_raw_candles_timestamp ON raw_candles(timestamp);
CREATE INDEX idx_raw_candles_strike_type_ts ON raw_candles(strike_id, option_type, timestamp);

-- derived_metrics: per-strike-pair computed metrics
CREATE TABLE derived_metrics (
    strike_id INTEGER NOT NULL REFERENCES strikes(id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL,
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
    PRIMARY KEY (strike_id, timestamp)
);

-- aggregate_metrics: cross-strike aggregates per expiry per timestamp
CREATE TABLE aggregate_metrics (
    expiry_id INTEGER NOT NULL REFERENCES expiries(id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL,
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
    itm_oi_chg_net INTEGER,
    PRIMARY KEY (expiry_id, timestamp)
);

-- iv_history: 52-week IV baseline per expiry
CREATE TABLE iv_history (
    expiry_id INTEGER NOT NULL REFERENCES expiries(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    atm_iv REAL,
    spot REAL,
    atm_strike INTEGER,
    PRIMARY KEY (expiry_id, date)
);

-- verification_log: NSE Bhavcopy comparison results
CREATE TABLE verification_log (
    strike_id INTEGER NOT NULL REFERENCES strikes(id) ON DELETE CASCADE,
    date DATE NOT NULL,
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
    PRIMARY KEY (strike_id, date)
);
```

- [ ] **Step 3: Create and apply migration — views**

```sql
-- v_eod_snapshot: last candle per strike/type/day
CREATE VIEW v_eod_snapshot AS
SELECT DISTINCT ON (rc.strike_id, rc.option_type, rc.timestamp::date)
    rc.strike_id, s.strike, s.atm_offset, rc.option_type,
    rc.timestamp::date AS date,
    rc.close, rc.oi, rc.volume, rc.spot,
    e.symbol, e.expiry_date
FROM raw_candles rc
JOIN strikes s ON s.id = rc.strike_id
JOIN expiries e ON e.id = s.expiry_id
ORDER BY rc.strike_id, rc.option_type, rc.timestamp::date, rc.timestamp DESC;

-- v_pcr_trend: PCR over time per expiry
CREATE VIEW v_pcr_trend AS
SELECT am.timestamp, am.overall_pcr, am.spot, e.symbol, e.expiry_date
FROM aggregate_metrics am
JOIN expiries e ON e.id = am.expiry_id
ORDER BY am.timestamp;

-- v_oi_buildup: OI change trends by strike
CREATE VIEW v_oi_buildup AS
SELECT dm.timestamp, s.strike, s.atm_offset,
    dm.ce_oi, dm.pe_oi, dm.ce_oi_chg, dm.pe_oi_chg,
    e.symbol, e.expiry_date
FROM derived_metrics dm
JOIN strikes s ON s.id = dm.strike_id
JOIN expiries e ON e.id = s.expiry_id
ORDER BY dm.timestamp, s.strike;
```

- [ ] **Step 4: Create and apply migration — RLS policies**

```sql
-- Enable RLS on all tables
ALTER TABLE expiries ENABLE ROW LEVEL SECURITY;
ALTER TABLE strikes ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw_candles ENABLE ROW LEVEL SECURITY;
ALTER TABLE derived_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE aggregate_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE iv_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE verification_log ENABLE ROW LEVEL SECURITY;

-- Service role: full access
CREATE POLICY "service_all" ON expiries FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "service_all" ON strikes FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "service_all" ON raw_candles FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "service_all" ON derived_metrics FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "service_all" ON aggregate_metrics FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "service_all" ON iv_history FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "service_all" ON verification_log FOR ALL USING (auth.role() = 'service_role');

-- Anon role: read-only
CREATE POLICY "anon_read" ON expiries FOR SELECT USING (auth.role() = 'anon');
CREATE POLICY "anon_read" ON strikes FOR SELECT USING (auth.role() = 'anon');
CREATE POLICY "anon_read" ON raw_candles FOR SELECT USING (auth.role() = 'anon');
CREATE POLICY "anon_read" ON derived_metrics FOR SELECT USING (auth.role() = 'anon');
CREATE POLICY "anon_read" ON aggregate_metrics FOR SELECT USING (auth.role() = 'anon');
CREATE POLICY "anon_read" ON iv_history FOR SELECT USING (auth.role() = 'anon');
CREATE POLICY "anon_read" ON verification_log FOR SELECT USING (auth.role() = 'anon');
```

- [ ] **Step 5: Verify schema via MCP**

Use `list_tables` to confirm all 7 tables exist. Run a test `execute_sql` query.

---

### Task 3: Rewrite `db.py` — SupabaseDB Class

**Files:** `DhanHQ_src/db.py`

- [ ] **Step 1: Write failing tests for SupabaseDB**

Create `tests/test_db.py` (complete rewrite):
- Mock `supabase.Client` using `unittest.mock`
- Test `ensure_expiry()` — verifies upsert call and return of id
- Test `ensure_strikes()` — verifies upsert and return of `{strike: id}` mapping
- Test `insert_raw_candles()` — verifies batching (500-row chunks) and upsert calls
- Test `insert_derived_metrics()` — verifies upsert with strike_id FK
- Test `insert_aggregate_metrics()` — verifies upsert with expiry_id FK
- Test `insert_iv_history()` — verifies upsert with expiry_id FK
- Test `insert_verification_log()` — verifies upsert with strike_id FK
- Test `get_raw_data_by_date()` — verifies select with date filter
- Test `get_eod_data()` — verifies query against v_eod_snapshot view
- Test `get_iv_history()` — verifies select filtered by expiry_id
- Test `get_distinct_dates()` — verifies distinct date extraction

- [ ] **Step 2: Implement SupabaseDB class**

```python
from supabase import create_client, Client
from DhanHQ_src.config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

BATCH_SIZE = 500

class SupabaseDB:
    def __init__(self, url=None, key=None):
        self.client: Client = create_client(
            url or SUPABASE_URL,
            key or SUPABASE_SERVICE_ROLE_KEY,
        )

    def ensure_expiry(self, symbol, expiry_date, expiry_flag, lot_size) -> int:
        """Upsert expiry, return id."""
        ...

    def ensure_strikes(self, expiry_id, strike_offsets) -> dict:
        """Upsert strikes for an expiry, return {strike: strike_id} mapping."""
        ...

    def _batch_upsert(self, table, rows, on_conflict):
        """Upsert rows in BATCH_SIZE chunks."""
        ...

    def insert_raw_candles(self, rows):
        ...

    def insert_derived_metrics(self, rows):
        ...

    def insert_aggregate_metrics(self, rows):
        ...

    def insert_iv_history(self, rows):
        ...

    def insert_verification_log(self, rows):
        ...

    def get_raw_data_by_date(self, date):
        ...

    def get_raw_data_ordered(self):
        ...

    def get_iv_history(self, expiry_id):
        ...

    def get_eod_data(self, date):
        ...

    def get_distinct_dates(self, expiry_id):
        ...
```

- [ ] **Step 3: Run tests, iterate until green**

- [ ] **Step 4: Commit**

```bash
git add DhanHQ_src/db.py tests/test_db.py
git commit -m "feat: SupabaseDB replacing SQLite"
```

---

### Task 4: Update Pipeline Modules

**Files:** `DhanHQ_src/main.py`, `DhanHQ_src/calculator.py`, `DhanHQ_src/verifier.py`

- [ ] **Step 1: Update `main.py`**

Changes:
- Import `SupabaseDB` instead of `Database`
- Remove `DB_PATH` import
- Replace `db = Database(DB_PATH)` with `db = SupabaseDB()`
- Remove `db.create_tables()`
- Add at pipeline start:
  ```python
  expiry_id = db.ensure_expiry("NIFTY", EXPIRY_DATE, EXPIRY_FLAG, LOT_SIZE)
  strike_map = db.ensure_strikes(expiry_id, STRIKES)
  ```
- Pass `expiry_id` and `strike_map` to insert methods
- Remove `db.close()`

- [ ] **Step 2: Update `calculator.py`**

Minimal changes:
- Input/output format stays as list of dicts
- If any method references `strike` directly, ensure it works with `strike_id` mapping
- The `SupabaseDB` class handles mapping, so calculator should remain DB-agnostic
- Verify all metric computation functions still work with the same dict structure

- [ ] **Step 3: Update `verifier.py`**

Changes:
- `verify_against_bhavcopy(db, ...)` still takes db param
- Use `db.get_eod_data()` and `db.get_distinct_dates()` (same interface)
- Result rows: `SupabaseDB.insert_verification_log()` handles `strike_id` mapping
- Bhavcopy download/parse logic: unchanged

- [ ] **Step 4: Run full test suite**

```bash
pytest tests/ -v
```

- [ ] **Step 5: Commit**

```bash
git add DhanHQ_src/main.py DhanHQ_src/calculator.py DhanHQ_src/verifier.py
git commit -m "feat: pipeline modules use SupabaseDB"
```

---

### Task 5: Update Tests

**Files:** `tests/test_integration.py`, `tests/test_verifier.py`

- [ ] **Step 1: Rewrite `test_integration.py`**

- Mock `SupabaseDB` instead of creating temp SQLite files
- Same test flow: fetch mock data → insert → calculate → verify
- Assert the mock was called with correct arguments

- [ ] **Step 2: Update `test_verifier.py`**

- Mock `SupabaseDB` methods instead of `Database`
- Same assertion logic for bhavcopy comparison

- [ ] **Step 3: Verify `test_calculator.py` and `test_fetcher.py` still pass**

These should be unchanged — calculator is DB-agnostic, fetcher is API-only.

- [ ] **Step 4: Run full suite**

```bash
pytest tests/ -v
```

- [ ] **Step 5: Commit**

```bash
git add tests/
git commit -m "feat: tests updated for Supabase"
```

---

### Task 6: SQLite Cleanup & Doc Updates

**Files:** `.gitignore`, docs, old plan

- [ ] **Step 1: Delete `data/` directory**

```bash
rm -rf data/
mkdir -p data/bhavcopy  # Keep bhavcopy temp dir only
```

- [ ] **Step 2: Update `.gitignore`**

Remove `data/*.db` line. Ensure `data/bhavcopy/` is still ignored.

- [ ] **Step 3: Update original design spec**

In `docs/superpowers/specs/2026-03-26-nifty-options-scraper-design.md`:
- Replace all SQLite references with Supabase/Postgres
- Update architecture diagram
- Update schema section
- Add note: "Migrated to Supabase Postgres per `2026-03-27-supabase-migration-design.md`"

- [ ] **Step 4: Update original implementation plan**

In `.claude/plans/2026-03-26-nifty-options-scraper.md`:
- Update Task 2 (DB Schema) references from SQLite to Supabase
- Update file map (db.py responsibility)

- [ ] **Step 5: Final grep for SQLite remnants**

```bash
grep -ri "sqlite\|nifty_options\.db\|DB_PATH\|PRAGMA\|executemany\|executescript\|row_factory\|conn\.execute" DhanHQ_src/ tests/ docs/
```

Must return zero matches.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore: remove all SQLite traces"
```

---

### Task 7: End-to-End Verification

- [ ] **Step 1: Verify Supabase MCP connection**

Restart Claude Code session, verify Supabase MCP tools are available (`list_tables`, `execute_sql`, etc.)

- [ ] **Step 2: Verify schema via MCP**

Run `list_tables` — should show all 7 tables. Run `execute_sql` with `SELECT * FROM expiries LIMIT 1`.

- [ ] **Step 3: Dry run pipeline** (optional, requires DhanHQ credentials)

```bash
cd DhanHQ_src && python main.py
```

Verify data flows: DhanHQ API → SupabaseDB → Supabase Postgres.

- [ ] **Step 4: Verify RLS**

Test with anon key: can SELECT, cannot INSERT/UPDATE/DELETE.
Test with service key: can do everything.

- [ ] **Step 5: Final commit and summary**

```bash
git log --oneline -10
```

Verify clean commit history for the migration.
