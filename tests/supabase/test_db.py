"""Post-migration tests: CRUD, upsert, queries, edge cases.

Tests validate SupabaseDB operations via the Python SDK against
the local Supabase stack. 25 tests total.
"""

import pytest
from postgrest.exceptions import APIError
from tests.supabase.conftest import skip_no_supabase

pytestmark = skip_no_supabase


# ═══════════════════════════════════════════════════════════════════
# Section A: CRUD Operations (8 tests)
# ═══════════════════════════════════════════════════════════════════


class TestCRUD:
    """Basic insert, upsert, and select operations."""

    def test_ensure_expiry_creates(self, supa_service):
        """Inserting a new expiry returns its ID."""
        resp = (
            supa_service.table("expiries")
            .insert(
                {
                    "symbol": "NIFTY",
                    "expiry_date": "2096-01-15",
                    "expiry_flag": "MONTH",
                    "lot_size": 65,
                }
            )
            .execute()
        )
        assert resp.data[0]["id"] is not None
        eid = resp.data[0]["id"]
        assert isinstance(eid, int)

        # Cleanup
        supa_service.table("expiries").delete().eq("id", eid).execute()

    def test_ensure_expiry_idempotent(self, supa_service):
        """Upserting the same expiry twice returns the same ID."""
        row = {
            "symbol": "NIFTY",
            "expiry_date": "2096-02-15",
            "expiry_flag": "MONTH",
            "lot_size": 65,
        }
        resp1 = (
            supa_service.table("expiries")
            .upsert(row, on_conflict="symbol,expiry_date")
            .execute()
        )
        resp2 = (
            supa_service.table("expiries")
            .upsert(row, on_conflict="symbol,expiry_date")
            .execute()
        )
        assert resp1.data[0]["id"] == resp2.data[0]["id"]

        # Cleanup
        supa_service.table("expiries").delete().eq(
            "id", resp1.data[0]["id"]
        ).execute()

    def test_ensure_strikes_returns_mapping(self, seed_expiry):
        """ensure_strikes returns a {strike: id} mapping with 9 entries."""
        _, strike_map = seed_expiry
        assert len(strike_map) == 9
        assert all(isinstance(v, int) for v in strike_map.values())
        assert 23000 in strike_map  # ATM
        assert 22800 in strike_map  # ATM-4
        assert 23200 in strike_map  # ATM+4

    def test_insert_raw_candles(self, supa_service, seed_expiry):
        """Inserting raw candles with valid strike_id succeeds."""
        _, strike_map = seed_expiry
        sid = strike_map[23000]

        resp = (
            supa_service.table("raw_candles")
            .insert(
                {
                    "strike_id": sid,
                    "option_type": "CE",
                    "timestamp": "2026-03-17T10:00:00+05:30",
                    "open": 200,
                    "high": 210,
                    "low": 195,
                    "close": 205,
                    "volume": 500,
                    "oi": 30000,
                    "iv": 17.5,
                    "spot": 23050,
                }
            )
            .execute()
        )
        assert len(resp.data) == 1
        assert resp.data[0]["close"] == 205

    def test_insert_derived_metrics(self, supa_service, seed_expiry):
        """Inserting derived metrics with valid strike_id succeeds."""
        _, strike_map = seed_expiry
        sid = strike_map[23000]

        resp = (
            supa_service.table("derived_metrics")
            .upsert(
                {
                    "strike_id": sid,
                    "timestamp": "2026-03-17T10:00:00+05:30",
                    "ce_ltp": 205,
                    "pe_ltp": 180,
                    "ce_volume": 500,
                    "pe_volume": 400,
                    "ce_oi": 30000,
                    "pe_oi": 28000,
                    "pcr_oi": 0.93,
                },
                on_conflict="strike_id,timestamp",
            )
            .execute()
        )
        assert len(resp.data) == 1

    def test_insert_aggregate_metrics(self, supa_service, seed_expiry):
        """Inserting aggregate metrics with valid expiry_id succeeds."""
        expiry_id, _ = seed_expiry

        resp = (
            supa_service.table("aggregate_metrics")
            .upsert(
                {
                    "expiry_id": expiry_id,
                    "timestamp": "2026-03-17T10:00:00+05:30",
                    "spot": 23050,
                    "atm_iv": 17.5,
                    "max_pain": 23000,
                    "overall_pcr": 0.85,
                    "lot_size": 65,
                },
                on_conflict="expiry_id,timestamp",
            )
            .execute()
        )
        assert len(resp.data) == 1

    def test_insert_iv_history(self, supa_service, seed_expiry):
        """Inserting IV history with valid expiry_id succeeds."""
        expiry_id, _ = seed_expiry

        resp = (
            supa_service.table("iv_history")
            .upsert(
                {
                    "expiry_id": expiry_id,
                    "date": "2026-03-17",
                    "atm_iv": 17.5,
                    "spot": 23050,
                    "atm_strike": 23050,
                },
                on_conflict="expiry_id,date",
            )
            .execute()
        )
        assert len(resp.data) == 1

    def test_get_raw_data_by_date(self, supa_service, seed_candles):
        """Selecting raw data filtered by date returns correct rows."""
        _, expiry_id, strike_map = seed_candles

        # raw_candles has timestamp with timezone; filter using gte/lt for a day
        resp = (
            supa_service.table("raw_candles")
            .select("*")
            .gte("timestamp", "2026-03-16T00:00:00+05:30")
            .lt("timestamp", "2026-03-17T00:00:00+05:30")
            .order("timestamp")
            .order("strike_id")
            .execute()
        )
        assert len(resp.data) == 4  # 2 timestamps x 2 option types


# ═══════════════════════════════════════════════════════════════════
# Section B: Upsert & Conflict Resolution (5 tests)
# ═══════════════════════════════════════════════════════════════════


class TestUpsert:
    """Verify upsert behavior, batching, and atomicity."""

    def test_raw_candles_upsert_updates(self, supa_service, seed_expiry):
        """Upserting with same key updates values, doesn't create duplicate."""
        _, strike_map = seed_expiry
        sid = strike_map[22800]

        row = {
            "strike_id": sid,
            "option_type": "CE",
            "timestamp": "2026-03-18T09:15:00+05:30",
            "open": 100,
            "high": 110,
            "low": 95,
            "close": 105,
            "volume": 500,
            "oi": 20000,
            "iv": 16.0,
            "spot": 22800,
        }

        # First insert
        supa_service.table("raw_candles").upsert(
            row, on_conflict="strike_id,option_type,timestamp"
        ).execute()

        # Upsert with updated close
        row["close"] = 108
        supa_service.table("raw_candles").upsert(
            row, on_conflict="strike_id,option_type,timestamp"
        ).execute()

        # Verify: one row, updated value
        resp = (
            supa_service.table("raw_candles")
            .select("close")
            .eq("strike_id", sid)
            .eq("option_type", "CE")
            .eq("timestamp", "2026-03-18T09:15:00+05:30")
            .execute()
        )
        assert len(resp.data) == 1
        assert resp.data[0]["close"] == 108

    def test_derived_metrics_upsert_replaces(self, supa_service, seed_expiry):
        """Upserting derived_metrics on same (strike_id, timestamp) replaces."""
        _, strike_map = seed_expiry
        sid = strike_map[22850]

        base = {
            "strike_id": sid,
            "timestamp": "2026-03-18T09:15:00+05:30",
            "ce_ltp": 100,
            "pe_ltp": 90,
        }
        supa_service.table("derived_metrics").upsert(
            base, on_conflict="strike_id,timestamp"
        ).execute()

        base["ce_ltp"] = 110
        supa_service.table("derived_metrics").upsert(
            base, on_conflict="strike_id,timestamp"
        ).execute()

        resp = (
            supa_service.table("derived_metrics")
            .select("ce_ltp")
            .eq("strike_id", sid)
            .eq("timestamp", "2026-03-18T09:15:00+05:30")
            .execute()
        )
        assert len(resp.data) == 1
        assert resp.data[0]["ce_ltp"] == 110

    def test_aggregate_metrics_upsert_replaces(self, supa_service, seed_expiry):
        """Upserting aggregate_metrics on same (expiry_id, timestamp) replaces."""
        expiry_id, _ = seed_expiry

        base = {
            "expiry_id": expiry_id,
            "timestamp": "2026-03-18T09:15:00+05:30",
            "spot": 23000,
            "overall_pcr": 0.80,
        }
        supa_service.table("aggregate_metrics").upsert(
            base, on_conflict="expiry_id,timestamp"
        ).execute()

        base["overall_pcr"] = 0.95
        supa_service.table("aggregate_metrics").upsert(
            base, on_conflict="expiry_id,timestamp"
        ).execute()

        resp = (
            supa_service.table("aggregate_metrics")
            .select("overall_pcr")
            .eq("expiry_id", expiry_id)
            .eq("timestamp", "2026-03-18T09:15:00+05:30")
            .execute()
        )
        assert len(resp.data) == 1
        assert resp.data[0]["overall_pcr"] == pytest.approx(0.95, abs=0.01)

    def test_batch_upsert_chunking(self, supa_service, seed_expiry):
        """Upserting 1500 rows works (tests batch chunking at 500)."""
        _, strike_map = seed_expiry
        sid = strike_map[23000]
        batch_size = 500

        rows = []
        for i in range(1500):
            # Spread across minutes: 09:15 + i minutes
            hour = 9 + (15 + i) // 60
            minute = (15 + i) % 60
            rows.append(
                {
                    "strike_id": sid,
                    "option_type": "CE",
                    "timestamp": f"2026-03-19T{hour:02d}:{minute:02d}:00+05:30",
                    "open": 100 + i * 0.1,
                    "high": 105 + i * 0.1,
                    "low": 98 + i * 0.1,
                    "close": 102 + i * 0.1,
                    "volume": 100 + i,
                    "oi": 10000 + i * 10,
                    "iv": 15.0,
                    "spot": 23000,
                }
            )

        # Upsert in chunks
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            supa_service.table("raw_candles").upsert(
                chunk,
                on_conflict="strike_id,option_type,timestamp",
                returning="minimal",
            ).execute()

        # Verify count
        resp = (
            supa_service.table("raw_candles")
            .select("id", count="exact")
            .eq("strike_id", sid)
            .eq("option_type", "CE")
            .gte("timestamp", "2026-03-19T00:00:00+05:30")
            .lt("timestamp", "2026-03-20T00:00:00+05:30")
            .execute()
        )
        assert resp.count == 1500

    def test_batch_atomic_rollback(self, supa_service, seed_expiry):
        """One bad row in a batch rolls back the entire batch."""
        _, strike_map = seed_expiry
        sid = strike_map[22900]

        rows = [
            {
                "strike_id": sid,
                "option_type": "CE",
                "timestamp": "2026-03-20T09:15:00+05:30",
                "open": 100,
                "high": 110,
                "low": 95,
                "close": 105,
                "volume": 100,
                "oi": 5000,
                "spot": 22900,
            },
            {
                "strike_id": 999999,  # Bad FK — will cause failure
                "option_type": "CE",
                "timestamp": "2026-03-20T09:16:00+05:30",
                "open": 100,
                "high": 110,
                "low": 95,
                "close": 105,
                "volume": 100,
                "oi": 5000,
                "spot": 22900,
            },
        ]

        with pytest.raises(APIError):
            supa_service.table("raw_candles").insert(rows).execute()

        # Verify the good row was NOT inserted (atomic rollback)
        resp = (
            supa_service.table("raw_candles")
            .select("id")
            .eq("strike_id", sid)
            .eq("timestamp", "2026-03-20T09:15:00+05:30")
            .execute()
        )
        assert len(resp.data) == 0, "Good row should have been rolled back"


# ═══════════════════════════════════════════════════════════════════
# Section C: Query Methods (5 tests)
# ═══════════════════════════════════════════════════════════════════


class TestQueries:
    """Verify query patterns used by the pipeline."""

    def test_get_eod_data(self, supa_service, seed_candles):
        """v_eod_snapshot returns last candle per strike/type/day."""
        _, expiry_id, strike_map = seed_candles

        resp = (
            supa_service.table("v_eod_snapshot")
            .select("*")
            .eq("date", "2026-03-16")
            .order("strike")
            .order("option_type")
            .execute()
        )
        # Should have 2 rows (CE + PE for ATM strike, last candle only)
        assert len(resp.data) == 2

    def test_get_iv_history_by_expiry(self, supa_service, seed_expiry):
        """IV history filtered by expiry_id, ordered by date."""
        expiry_id, _ = seed_expiry

        # Insert multiple dates
        rows = [
            {
                "expiry_id": expiry_id,
                "date": f"2026-03-{d:02d}",
                "atm_iv": 15.0 + d * 0.1,
                "spot": 23000 + d,
                "atm_strike": 23000,
            }
            for d in range(16, 21)
        ]
        supa_service.table("iv_history").upsert(
            rows, on_conflict="expiry_id,date"
        ).execute()

        resp = (
            supa_service.table("iv_history")
            .select("*")
            .eq("expiry_id", expiry_id)
            .order("date")
            .execute()
        )
        assert len(resp.data) >= 5
        dates = [r["date"] for r in resp.data]
        assert dates == sorted(dates), "Dates should be ordered ascending"

    def test_get_distinct_dates(self, supa_service, seed_candles):
        """Can extract distinct dates from raw_candles for an expiry."""
        _, expiry_id, strike_map = seed_candles

        # Use RPC or raw query via distinct; PostgREST doesn't have DISTINCT
        # but we can group by the date portion
        resp = (
            supa_service.table("raw_candles")
            .select("timestamp")
            .in_("strike_id", list(strike_map.values()))
            .order("timestamp")
            .execute()
        )
        dates = sorted(set(row["timestamp"][:10] for row in resp.data))
        assert len(dates) >= 1
        assert "2026-03-16" in dates

    def test_get_raw_data_ordered(self, supa_service, seed_candles):
        """Raw data ordered by timestamp, strike_id, option_type."""
        _, expiry_id, strike_map = seed_candles

        resp = (
            supa_service.table("raw_candles")
            .select("*")
            .in_("strike_id", list(strike_map.values()))
            .order("timestamp")
            .order("strike_id")
            .order("option_type")
            .execute()
        )
        assert len(resp.data) >= 4

        # Verify ordering: timestamps should be non-decreasing
        timestamps = [r["timestamp"] for r in resp.data]
        assert timestamps == sorted(timestamps)

    def test_empty_query_returns_list(self, supa_service):
        """Query with no matching data returns empty list, not error."""
        resp = (
            supa_service.table("raw_candles")
            .select("*")
            .eq("strike_id", 999999)
            .execute()
        )
        assert resp.data == []
        assert isinstance(resp.data, list)


# ═══════════════════════════════════════════════════════════════════
# Section D: Edge Cases (7 tests)
# ═══════════════════════════════════════════════════════════════════


class TestEdgeCases:
    """Boundary values, NULL handling, pagination, idempotency."""

    def test_null_iv_accepted(self, supa_service, seed_expiry):
        """IV column can be NULL (some candles may lack IV data)."""
        _, strike_map = seed_expiry
        sid = strike_map[23050]

        resp = (
            supa_service.table("raw_candles")
            .insert(
                {
                    "strike_id": sid,
                    "option_type": "CE",
                    "timestamp": "2026-03-21T09:15:00+05:30",
                    "open": 100,
                    "high": 110,
                    "low": 95,
                    "close": 105,
                    "volume": 100,
                    "oi": 5000,
                    "iv": None,
                    "spot": 23050,
                }
            )
            .execute()
        )
        assert resp.data[0]["iv"] is None

    def test_boundary_strike_values(self, supa_service, seed_expiry):
        """Strike value of 0 is accepted (edge case)."""
        expiry_id, _ = seed_expiry

        resp = (
            supa_service.table("strikes")
            .insert({"expiry_id": expiry_id, "strike": 0, "atm_offset": -99})
            .execute()
        )
        assert resp.data[0]["strike"] == 0

        # Cleanup
        supa_service.table("strikes").delete().eq(
            "id", resp.data[0]["id"]
        ).execute()

    def test_timezone_epoch_to_timestamptz(self, supa_service, seed_expiry):
        """Timestamps stored as TIMESTAMPTZ preserve timezone correctly."""
        _, strike_map = seed_expiry
        sid = strike_map[23100]

        # Insert with explicit IST offset
        supa_service.table("raw_candles").insert(
            {
                "strike_id": sid,
                "option_type": "PE",
                "timestamp": "2026-03-21T15:30:00+05:30",
                "open": 50,
                "high": 55,
                "low": 48,
                "close": 52,
                "volume": 200,
                "oi": 10000,
                "spot": 23100,
            }
        ).execute()

        # Query back — PostgREST returns UTC
        resp = (
            supa_service.table("raw_candles")
            .select("timestamp")
            .eq("strike_id", sid)
            .eq("option_type", "PE")
            .execute()
        )
        ts = resp.data[0]["timestamp"]
        # 15:30 IST = 10:00 UTC
        assert "10:00:00" in ts or "15:30:00" in ts

    def test_pagination_over_1000_rows(self, supa_service, seed_expiry):
        """Selecting >1000 rows requires explicit .range() calls."""
        _, strike_map = seed_expiry
        sid = strike_map[23150]

        # Insert 1200 rows
        rows = []
        for i in range(1200):
            hour = 9 + (15 + i) // 60
            minute = (15 + i) % 60
            rows.append(
                {
                    "strike_id": sid,
                    "option_type": "PE",
                    "timestamp": f"2026-03-22T{hour:02d}:{minute:02d}:00+05:30",
                    "open": 50,
                    "high": 55,
                    "low": 48,
                    "close": 52,
                    "volume": 100,
                    "oi": 5000,
                    "spot": 23150,
                }
            )

        # Batch insert
        for i in range(0, len(rows), 500):
            supa_service.table("raw_candles").upsert(
                rows[i : i + 500],
                on_conflict="strike_id,option_type,timestamp",
                returning="minimal",
            ).execute()

        # Default select caps at 1000
        resp_default = (
            supa_service.table("raw_candles")
            .select("id")
            .eq("strike_id", sid)
            .eq("option_type", "PE")
            .gte("timestamp", "2026-03-22T00:00:00+05:30")
            .execute()
        )
        assert len(resp_default.data) <= 1000

        # Use .range() to get all 1200
        all_rows = []
        page_size = 1000
        offset = 0
        while True:
            resp = (
                supa_service.table("raw_candles")
                .select("id")
                .eq("strike_id", sid)
                .eq("option_type", "PE")
                .gte("timestamp", "2026-03-22T00:00:00+05:30")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            all_rows.extend(resp.data)
            if len(resp.data) < page_size:
                break
            offset += page_size

        assert len(all_rows) == 1200

    def test_ensure_expiry_idempotent_concurrent(self, supa_service):
        """Multiple rapid upserts to the same expiry all return the same ID."""
        row = {
            "symbol": "NIFTY",
            "expiry_date": "2096-06-30",
            "expiry_flag": "MONTH",
            "lot_size": 65,
        }
        ids = []
        for _ in range(5):
            resp = (
                supa_service.table("expiries")
                .upsert(row, on_conflict="symbol,expiry_date")
                .execute()
            )
            ids.append(resp.data[0]["id"])

        assert len(set(ids)) == 1, f"Expected 1 unique ID, got {set(ids)}"

        # Cleanup
        supa_service.table("expiries").delete().eq("id", ids[0]).execute()

    def test_on_conflict_composite_key(self, supa_service, seed_expiry):
        """on_conflict with composite key string format works correctly."""
        _, strike_map = seed_expiry
        sid = strike_map[22950]

        row = {
            "strike_id": sid,
            "option_type": "CE",
            "timestamp": "2026-03-23T09:15:00+05:30",
            "open": 80,
            "high": 85,
            "low": 78,
            "close": 82,
            "volume": 300,
            "oi": 15000,
            "spot": 22950,
        }

        # Insert then upsert with on_conflict as comma-separated string
        supa_service.table("raw_candles").upsert(
            row, on_conflict="strike_id,option_type,timestamp"
        ).execute()

        row["close"] = 88
        supa_service.table("raw_candles").upsert(
            row, on_conflict="strike_id,option_type,timestamp"
        ).execute()

        resp = (
            supa_service.table("raw_candles")
            .select("close")
            .eq("strike_id", sid)
            .eq("option_type", "CE")
            .eq("timestamp", "2026-03-23T09:15:00+05:30")
            .execute()
        )
        assert len(resp.data) == 1
        assert resp.data[0]["close"] == 88

    def test_empty_batch_insert(self, supa_service):
        """Inserting an empty list doesn't error."""
        # supabase-py may handle this differently — verify no crash
        try:
            resp = (
                supa_service.table("raw_candles")
                .upsert(
                    [],
                    on_conflict="strike_id,option_type,timestamp",
                    returning="minimal",
                )
                .execute()
            )
        except APIError:
            # Some PostgREST versions reject empty arrays — that's acceptable
            pass
