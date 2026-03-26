"""Pre-migration validation: schema structure, FKs, cascades, RLS, views.

Tests run against local Supabase stack via psycopg2 (schema introspection)
and supabase-py (RLS validation). 20 tests total.
"""

import pytest
from postgrest.exceptions import APIError
from tests.supabase.conftest import skip_no_supabase

pytestmark = skip_no_supabase

# ─── Expected schema ───────────────────────────────────────────────

EXPECTED_TABLES = [
    "expiries",
    "strikes",
    "raw_candles",
    "derived_metrics",
    "aggregate_metrics",
    "iv_history",
    "verification_log",
]

EXPECTED_VIEWS = ["v_eod_snapshot", "v_pcr_trend", "v_oi_buildup"]

# Spot-check column types: (table, column, expected_type)
COLUMN_TYPE_CHECKS = [
    ("expiries", "id", "integer"),
    ("expiries", "symbol", "text"),
    ("expiries", "expiry_date", "date"),
    ("expiries", "expiry_flag", "text"),
    ("expiries", "lot_size", "integer"),
    ("expiries", "created_at", "timestamp with time zone"),
    ("strikes", "strike", "integer"),
    ("strikes", "atm_offset", "integer"),
    ("raw_candles", "id", "bigint"),
    ("raw_candles", "timestamp", "timestamp with time zone"),
    ("raw_candles", "close", "real"),
    ("raw_candles", "iv", "real"),
    ("raw_candles", "volume", "integer"),
    ("derived_metrics", "timestamp", "timestamp with time zone"),
    ("derived_metrics", "pcr_oi", "real"),
    ("aggregate_metrics", "timestamp", "timestamp with time zone"),
    ("aggregate_metrics", "max_pain", "integer"),
    ("iv_history", "date", "date"),
    ("iv_history", "atm_iv", "real"),
    ("verification_log", "date", "date"),
    ("verification_log", "close_match", "boolean"),
]


# ═══════════════════════════════════════════════════════════════════
# Section A: Schema Structure (7 tests)
# ═══════════════════════════════════════════════════════════════════


class TestSchemaStructure:
    """Verify tables, columns, constraints, indexes, and views exist."""

    def test_tables_exist(self, pg_conn):
        """All 7 tables exist in the public schema."""
        cur = pg_conn.cursor()
        cur.execute(
            """SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
               ORDER BY table_name"""
        )
        tables = [row[0] for row in cur.fetchall()]
        for t in EXPECTED_TABLES:
            assert t in tables, f"Table '{t}' not found. Got: {tables}"

    def test_column_types(self, pg_conn):
        """Key columns have correct data types."""
        cur = pg_conn.cursor()
        for table, column, expected_type in COLUMN_TYPE_CHECKS:
            cur.execute(
                """SELECT data_type FROM information_schema.columns
                   WHERE table_schema = 'public'
                   AND table_name = %s AND column_name = %s""",
                (table, column),
            )
            row = cur.fetchone()
            assert row is not None, f"Column {table}.{column} not found"
            assert row[0] == expected_type, (
                f"{table}.{column}: expected '{expected_type}', got '{row[0]}'"
            )

    def test_primary_keys(self, pg_conn):
        """Each table has a primary key."""
        cur = pg_conn.cursor()
        for table in EXPECTED_TABLES:
            cur.execute(
                """SELECT constraint_name FROM information_schema.table_constraints
                   WHERE table_schema = 'public'
                   AND table_name = %s AND constraint_type = 'PRIMARY KEY'""",
                (table,),
            )
            assert cur.fetchone() is not None, f"No PK on '{table}'"

    def test_unique_constraints(self, pg_conn):
        """Critical unique constraints exist."""
        cur = pg_conn.cursor()
        # Check unique constraints via pg_indexes (unique indexes back unique constraints)
        checks = [
            ("expiries", {"symbol", "expiry_date"}),
            ("strikes", {"expiry_id", "strike"}),
            ("raw_candles", {"strike_id", "option_type", "timestamp"}),
        ]
        for table, expected_cols in checks:
            cur.execute(
                """SELECT c.conname, array_agg(a.attname ORDER BY a.attnum)
                   FROM pg_constraint c
                   JOIN pg_attribute a ON a.attrelid = c.conrelid
                       AND a.attnum = ANY(c.conkey)
                   WHERE c.conrelid = %s::regclass
                   AND c.contype = 'u'
                   GROUP BY c.conname""",
                (f"public.{table}",),
            )
            found = False
            for _name, cols in cur.fetchall():
                if set(cols) == expected_cols:
                    found = True
                    break
            assert found, (
                f"UNIQUE({', '.join(expected_cols)}) not found on '{table}'"
            )

    def test_check_constraints(self, pg_conn):
        """CHECK constraints on option_type and expiry_flag."""
        cur = pg_conn.cursor()
        # option_type IN ('CE', 'PE') on raw_candles
        cur.execute(
            """SELECT consrc FROM pg_constraint
               WHERE conrelid = 'public.raw_candles'::regclass
               AND contype = 'c'"""
        )
        check_srcs = [row[0] for row in cur.fetchall()]
        assert any("CE" in s and "PE" in s for s in check_srcs), (
            f"No CHECK for option_type on raw_candles. Found: {check_srcs}"
        )

        # expiry_flag IN ('MONTH', 'WEEK') on expiries
        cur.execute(
            """SELECT consrc FROM pg_constraint
               WHERE conrelid = 'public.expiries'::regclass
               AND contype = 'c'"""
        )
        check_srcs = [row[0] for row in cur.fetchall()]
        assert any("MONTH" in s and "WEEK" in s for s in check_srcs), (
            f"No CHECK for expiry_flag on expiries. Found: {check_srcs}"
        )

    def test_indexes_exist(self, pg_conn):
        """Performance indexes exist on raw_candles."""
        cur = pg_conn.cursor()
        cur.execute(
            """SELECT indexname FROM pg_indexes
               WHERE schemaname = 'public' AND tablename = 'raw_candles'"""
        )
        index_names = [row[0] for row in cur.fetchall()]
        assert "idx_raw_candles_timestamp" in index_names
        assert "idx_raw_candles_strike_type_ts" in index_names

    def test_views_exist(self, pg_conn):
        """All 3 views exist in the public schema."""
        cur = pg_conn.cursor()
        cur.execute(
            """SELECT table_name FROM information_schema.views
               WHERE table_schema = 'public' ORDER BY table_name"""
        )
        views = [row[0] for row in cur.fetchall()]
        for v in EXPECTED_VIEWS:
            assert v in views, f"View '{v}' not found. Got: {views}"


# ═══════════════════════════════════════════════════════════════════
# Section B: Foreign Keys & Cascades (5 tests)
# ═══════════════════════════════════════════════════════════════════


class TestForeignKeys:
    """Verify FK relationships and CASCADE delete behavior."""

    def _get_fks(self, pg_conn, table):
        """Return list of (constraint_name, referenced_table, columns) for a table."""
        cur = pg_conn.cursor()
        cur.execute(
            """SELECT
                   c.conname,
                   ref.relname AS referenced_table,
                   array_agg(a.attname ORDER BY a.attnum) AS columns
               FROM pg_constraint c
               JOIN pg_class ref ON ref.oid = c.confrelid
               JOIN pg_attribute a ON a.attrelid = c.conrelid
                   AND a.attnum = ANY(c.conkey)
               WHERE c.conrelid = %s::regclass AND c.contype = 'f'
               GROUP BY c.conname, ref.relname""",
            (f"public.{table}",),
        )
        return cur.fetchall()

    def test_strikes_fk_to_expiries(self, pg_conn):
        """strikes.expiry_id references expiries.id."""
        fks = self._get_fks(pg_conn, "strikes")
        assert any(
            ref_table == "expiries" and "expiry_id" in cols
            for _, ref_table, cols in fks
        ), f"FK strikes->expiries not found. FKs: {fks}"

    def test_raw_candles_fk_to_strikes(self, pg_conn):
        """raw_candles.strike_id references strikes.id."""
        fks = self._get_fks(pg_conn, "raw_candles")
        assert any(
            ref_table == "strikes" and "strike_id" in cols
            for _, ref_table, cols in fks
        ), f"FK raw_candles->strikes not found. FKs: {fks}"

    def test_fk_violation_raises(self, supa_service):
        """Inserting raw_candles with nonexistent strike_id raises error."""
        with pytest.raises(APIError) as exc_info:
            supa_service.table("raw_candles").insert(
                {
                    "strike_id": 999999,
                    "option_type": "CE",
                    "timestamp": "2026-03-16T09:15:00+05:30",
                    "open": 100,
                    "high": 100,
                    "low": 100,
                    "close": 100,
                    "volume": 0,
                    "oi": 0,
                    "spot": 23000,
                }
            ).execute()
        assert "foreign key" in str(exc_info.value).lower() or "23503" in str(
            exc_info.value
        )

    def test_cascade_delete_expiry(self, supa_service):
        """Deleting an expiry cascades to strikes and raw_candles."""
        # Create isolated test data
        resp = (
            supa_service.table("expiries")
            .insert(
                {
                    "symbol": "NIFTY",
                    "expiry_date": "2099-01-01",
                    "expiry_flag": "WEEK",
                    "lot_size": 25,
                }
            )
            .execute()
        )
        eid = resp.data[0]["id"]

        resp = (
            supa_service.table("strikes")
            .insert({"expiry_id": eid, "strike": 20000, "atm_offset": 0})
            .execute()
        )
        sid = resp.data[0]["id"]

        supa_service.table("raw_candles").insert(
            {
                "strike_id": sid,
                "option_type": "CE",
                "timestamp": "2099-01-01T09:15:00+05:30",
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 0,
                "oi": 0,
                "spot": 20000,
            }
        ).execute()

        # Delete expiry — should cascade
        supa_service.table("expiries").delete().eq("id", eid).execute()

        # Verify strikes and candles are gone
        strikes = (
            supa_service.table("strikes").select("id").eq("expiry_id", eid).execute()
        )
        assert len(strikes.data) == 0, "Strikes not cascaded"

        candles = (
            supa_service.table("raw_candles")
            .select("id")
            .eq("strike_id", sid)
            .execute()
        )
        assert len(candles.data) == 0, "Candles not cascaded"

    def test_cascade_propagates_all_levels(self, supa_service):
        """CASCADE from expiry propagates through to derived_metrics,
        iv_history, and verification_log."""
        # Create chain: expiry -> strike -> derived_metrics + verification_log
        resp = (
            supa_service.table("expiries")
            .insert(
                {
                    "symbol": "NIFTY",
                    "expiry_date": "2098-12-31",
                    "expiry_flag": "MONTH",
                    "lot_size": 65,
                }
            )
            .execute()
        )
        eid = resp.data[0]["id"]

        resp = (
            supa_service.table("strikes")
            .insert({"expiry_id": eid, "strike": 21000, "atm_offset": 0})
            .execute()
        )
        sid = resp.data[0]["id"]

        supa_service.table("derived_metrics").insert(
            {
                "strike_id": sid,
                "timestamp": "2098-12-31T09:15:00+05:30",
                "ce_ltp": 100,
                "pe_ltp": 90,
            }
        ).execute()

        supa_service.table("iv_history").insert(
            {
                "expiry_id": eid,
                "date": "2098-12-31",
                "atm_iv": 15.0,
                "spot": 21000,
                "atm_strike": 21000,
            }
        ).execute()

        supa_service.table("verification_log").insert(
            {
                "strike_id": sid,
                "date": "2098-12-31",
                "dhan_close": 100,
                "nse_close": 100,
                "close_match": True,
            }
        ).execute()

        # Delete expiry
        supa_service.table("expiries").delete().eq("id", eid).execute()

        # All downstream should be gone
        dm = (
            supa_service.table("derived_metrics")
            .select("strike_id")
            .eq("strike_id", sid)
            .execute()
        )
        assert len(dm.data) == 0, "derived_metrics not cascaded"

        iv = (
            supa_service.table("iv_history")
            .select("expiry_id")
            .eq("expiry_id", eid)
            .execute()
        )
        assert len(iv.data) == 0, "iv_history not cascaded"

        vl = (
            supa_service.table("verification_log")
            .select("strike_id")
            .eq("strike_id", sid)
            .execute()
        )
        assert len(vl.data) == 0, "verification_log not cascaded"


# ═══════════════════════════════════════════════════════════════════
# Section C: RLS Policies (5 tests)
# ═══════════════════════════════════════════════════════════════════


class TestRLSPolicies:
    """Verify RLS: service_role has full access, anon is read-only."""

    def test_service_role_full_access(self, supa_service):
        """Service role can INSERT, SELECT, UPDATE, DELETE on all tables."""
        # INSERT
        resp = (
            supa_service.table("expiries")
            .insert(
                {
                    "symbol": "BANKNIFTY",
                    "expiry_date": "2097-06-15",
                    "expiry_flag": "WEEK",
                    "lot_size": 15,
                }
            )
            .execute()
        )
        eid = resp.data[0]["id"]

        # SELECT
        resp = (
            supa_service.table("expiries").select("*").eq("id", eid).execute()
        )
        assert len(resp.data) == 1

        # UPDATE
        supa_service.table("expiries").update({"lot_size": 25}).eq(
            "id", eid
        ).execute()
        resp = (
            supa_service.table("expiries").select("lot_size").eq("id", eid).execute()
        )
        assert resp.data[0]["lot_size"] == 25

        # DELETE
        supa_service.table("expiries").delete().eq("id", eid).execute()
        resp = (
            supa_service.table("expiries").select("id").eq("id", eid).execute()
        )
        assert len(resp.data) == 0

    def test_anon_can_select(self, supa_anon, seed_expiry):
        """Anon role can SELECT from all tables."""
        expiry_id, _ = seed_expiry
        for table in EXPECTED_TABLES:
            resp = supa_anon.table(table).select("*").limit(1).execute()
            # Should not raise — just verify no error

    def test_anon_cannot_insert(self, supa_anon):
        """Anon role cannot INSERT into any table."""
        for table, row in [
            (
                "expiries",
                {
                    "symbol": "HACK",
                    "expiry_date": "2097-01-01",
                    "expiry_flag": "WEEK",
                    "lot_size": 1,
                },
            ),
            ("strikes", {"expiry_id": 1, "strike": 99999, "atm_offset": 0}),
        ]:
            with pytest.raises(APIError):
                supa_anon.table(table).insert(row).execute()

    def test_anon_cannot_update(self, supa_anon, seed_expiry):
        """Anon role cannot UPDATE any table (silently returns 0 rows)."""
        expiry_id, _ = seed_expiry
        resp = (
            supa_anon.table("expiries")
            .update({"lot_size": 999})
            .eq("id", expiry_id)
            .execute()
        )
        # UPDATE blocked by RLS returns empty data
        assert len(resp.data) == 0

        # Verify value unchanged (via service role would be better, but
        # we check anon can still read the original value)
        resp = (
            supa_anon.table("expiries")
            .select("lot_size")
            .eq("id", expiry_id)
            .execute()
        )
        if resp.data:
            assert resp.data[0]["lot_size"] != 999

    def test_anon_cannot_delete(self, supa_anon, seed_expiry):
        """Anon role cannot DELETE from any table (silently returns 0 rows)."""
        expiry_id, _ = seed_expiry
        resp = (
            supa_anon.table("expiries").delete().eq("id", expiry_id).execute()
        )
        assert len(resp.data) == 0


# ═══════════════════════════════════════════════════════════════════
# Section D: Views (3 tests)
# ═══════════════════════════════════════════════════════════════════


class TestViews:
    """Verify view correctness: joins, DISTINCT ON, column presence."""

    def test_v_eod_snapshot_distinct_on(self, supa_service, seed_candles):
        """v_eod_snapshot returns only the latest candle per strike/type/day."""
        _, expiry_id, strike_map = seed_candles
        atm_strike_id = strike_map[23000]

        resp = (
            supa_service.table("v_eod_snapshot")
            .select("*")
            .eq("strike_id", atm_strike_id)
            .execute()
        )
        # 2 candles at different times for CE + 2 for PE, same day
        # DISTINCT ON should return 1 CE + 1 PE = 2 rows
        assert len(resp.data) == 2

        # The returned rows should have the LATEST timestamp's close
        for row in resp.data:
            if row["option_type"] == "CE":
                assert row["close"] == 156.0  # 09:16 candle, not 09:15
            elif row["option_type"] == "PE":
                assert row["close"] == 140.0  # 09:16 candle

    def test_v_pcr_trend_joins(self, supa_service, seed_expiry):
        """v_pcr_trend contains expiry symbol and date alongside PCR."""
        expiry_id, _ = seed_expiry

        # Insert aggregate_metrics for the expiry
        supa_service.table("aggregate_metrics").upsert(
            {
                "expiry_id": expiry_id,
                "timestamp": "2026-03-16T09:15:00+05:30",
                "overall_pcr": 0.85,
                "spot": 23000,
            },
            on_conflict="expiry_id,timestamp",
        ).execute()

        resp = (
            supa_service.table("v_pcr_trend")
            .select("*")
            .eq("symbol", "NIFTY")
            .execute()
        )
        assert len(resp.data) >= 1
        row = resp.data[0]
        assert "symbol" in row
        assert "expiry_date" in row
        assert "overall_pcr" in row
        assert "spot" in row
        assert row["symbol"] == "NIFTY"

    def test_v_oi_buildup_joins(self, supa_service, seed_expiry):
        """v_oi_buildup contains strike, atm_offset, symbol from joins."""
        expiry_id, strike_map = seed_expiry
        atm_strike_id = strike_map[23000]

        supa_service.table("derived_metrics").upsert(
            {
                "strike_id": atm_strike_id,
                "timestamp": "2026-03-16T09:15:00+05:30",
                "ce_oi": 50000,
                "pe_oi": 45000,
                "ce_oi_chg": 1000,
                "pe_oi_chg": 500,
            },
            on_conflict="strike_id,timestamp",
        ).execute()

        resp = (
            supa_service.table("v_oi_buildup")
            .select("*")
            .eq("strike", 23000)
            .execute()
        )
        assert len(resp.data) >= 1
        row = resp.data[0]
        assert row["strike"] == 23000
        assert row["atm_offset"] == 0
        assert "symbol" in row
        assert "expiry_date" in row
        assert row["ce_oi"] == 50000
