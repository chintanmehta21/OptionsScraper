import pytest
from unittest.mock import MagicMock, patch, call
from DhanHQ_src.loop_expiries.db import LoopExpiriesDB
from DhanHQ_src.loop_expiries.scraper import scrape_single_expiry, run_loop


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


# Shared fake expiry list returned by fetch_all_expiry_dates
FAKE_EXPIRIES = [
    {"expiry_date": "2026-01-06", "expiry_flag": "WEEK", "from_date": "2025-12-23", "to_date": "2026-01-06"},
    {"expiry_date": "2026-01-06", "expiry_flag": "MONTH", "from_date": "2025-12-23", "to_date": "2026-01-06"},
]


class TestLoopExpiriesDBSetup:
    def test_setup_tables_verifies_tables(self, db, mock_supabase):
        db.setup_tables()
        # Verifies both tables exist via select queries
        mock_supabase.table.assert_any_call("scrape_progress_2026")
        mock_supabase.table.assert_any_call("full_expiries_2026")

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
    @patch("DhanHQ_src.loop_expiries.scraper.fetch_all_expiry_dates", return_value=FAKE_EXPIRIES)
    @patch("DhanHQ_src.loop_expiries.scraper.LoopExpiriesDB")
    @patch("DhanHQ_src.loop_expiries.scraper.create_dhan_client")
    @patch("DhanHQ_src.loop_expiries.scraper.get_access_token", return_value="fake-token")
    @patch("DhanHQ_src.loop_expiries.scraper.scrape_single_expiry")
    def test_skips_completed_expiries(self, mock_scrape, mock_auth, mock_dhan, mock_db_cls, mock_fetch_dates):
        mock_db = MagicMock()
        mock_db_cls.return_value = mock_db
        mock_db.get_pending_expiries.return_value = []
        mock_db.get_progress_summary.return_value = {
            "completed": 2, "failed": 0, "skipped": 0,
            "pending": 0, "in_progress": 0, "total": 2, "total_rows": 10000,
            "failed_details": [],
        }

        stats = run_loop(2026)
        mock_scrape.assert_not_called()
        assert stats["completed"] == 0

    @patch("DhanHQ_src.loop_expiries.scraper.fetch_all_expiry_dates", return_value=FAKE_EXPIRIES)
    @patch("DhanHQ_src.loop_expiries.scraper.LoopExpiriesDB")
    @patch("DhanHQ_src.loop_expiries.scraper.create_dhan_client")
    @patch("DhanHQ_src.loop_expiries.scraper.get_access_token", return_value="fake-token")
    @patch("DhanHQ_src.loop_expiries.scraper.scrape_single_expiry")
    def test_marks_skipped_on_all_empty(self, mock_scrape, mock_auth, mock_dhan, mock_db_cls, mock_fetch_dates):
        mock_db = MagicMock()
        mock_db_cls.return_value = mock_db
        mock_db.get_pending_expiries.return_value = [
            {"expiry_date": "2026-01-06", "expiry_flag": "WEEK"},
        ]
        mock_db.get_progress_summary.return_value = {
            "completed": 0, "failed": 0, "skipped": 0,
            "pending": 1, "in_progress": 0, "total": 1, "total_rows": 0,
            "failed_details": [],
        }
        mock_scrape.return_value = ([], 42, 42)

        stats = run_loop(2026)
        calls = mock_db.update_progress.call_args_list
        assert "skipped" in [c.kwargs["status"] for c in calls if "status" in c.kwargs]

    @patch("DhanHQ_src.loop_expiries.scraper.fetch_all_expiry_dates", return_value=FAKE_EXPIRIES)
    @patch("DhanHQ_src.loop_expiries.scraper.LoopExpiriesDB")
    @patch("DhanHQ_src.loop_expiries.scraper.create_dhan_client")
    @patch("DhanHQ_src.loop_expiries.scraper.get_access_token", return_value="fake-token")
    @patch("DhanHQ_src.loop_expiries.scraper.scrape_single_expiry")
    def test_marks_failed_on_error(self, mock_scrape, mock_auth, mock_dhan, mock_db_cls, mock_fetch_dates):
        mock_db = MagicMock()
        mock_db_cls.return_value = mock_db
        mock_db.get_pending_expiries.return_value = [
            {"expiry_date": "2026-01-06", "expiry_flag": "WEEK"},
        ]
        mock_db.get_progress_summary.return_value = {
            "completed": 0, "failed": 0, "skipped": 0,
            "pending": 1, "in_progress": 0, "total": 1, "total_rows": 0,
            "failed_details": [],
        }
        mock_scrape.side_effect = RuntimeError("API timeout")

        stats = run_loop(2026)
        assert stats["failed"] == 1

    @patch("DhanHQ_src.loop_expiries.scraper.fetch_all_expiry_dates", return_value=[])
    @patch("DhanHQ_src.loop_expiries.scraper.create_dhan_client")
    @patch("DhanHQ_src.loop_expiries.scraper.get_access_token", return_value="fake-token")
    def test_empty_expiry_list_returns_early(self, mock_auth, mock_dhan, mock_fetch_dates):
        """If no expiry dates found, return early without crashing."""
        stats = run_loop(2026)
        assert stats["completed"] == 0
        assert stats["total_rows"] == 0
