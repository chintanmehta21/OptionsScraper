import pytest
from unittest.mock import MagicMock, patch, call
from DhanHQ_src.loop_expiries.db import LoopExpiriesDB


@pytest.fixture
def mock_supabase():
    """Create a mock supabase client with chained method support."""
    client = MagicMock()
    client.rpc.return_value.execute.return_value = MagicMock(data=None)
    table_mock = MagicMock()
    table_mock.upsert.return_value.execute.return_value = MagicMock(data=[])
    table_mock.select.return_value.execute.return_value = MagicMock(data=[])
    table_mock.select.return_value.order.return_value.order.return_value.execute.return_value = MagicMock(data=[])
    table_mock.select.return_value.in_.return_value.order.return_value.order.return_value.execute.return_value = MagicMock(data=[])
    table_mock.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
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
        mock_supabase.table.return_value.select.return_value.order.return_value.order.return_value.execute.return_value.data = []
        db.seed_progress(expiries)
        mock_supabase.table.assert_any_call("scrape_progress_2026")

    def test_seed_skips_completed(self, db, mock_supabase):
        expiries = [
            {"expiry_date": "2026-01-01", "expiry_flag": "WEEK"},
            {"expiry_date": "2026-01-08", "expiry_flag": "WEEK"},
        ]
        mock_supabase.table.return_value.select.return_value.order.return_value.order.return_value.execute.return_value.data = [
            {"expiry_date": "2026-01-01", "expiry_flag": "WEEK", "status": "completed"},
        ]
        count = db.seed_progress(expiries)
        assert count == 1


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
                "open": 500.0, "high": 510.0, "low": 495.0, "close": 505.0,
                "volume": 1000, "oi": 50000, "iv": 25.5, "spot": 23250.0,
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
                "strike": 23200.0,
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
        assert actual_rows[0]["strike"] == 23200


class TestGetProgressSummary:
    def test_summary_counts(self, db, mock_supabase):
        mock_supabase.table.return_value.select.return_value.order.return_value.order.return_value.execute.return_value.data = [
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
