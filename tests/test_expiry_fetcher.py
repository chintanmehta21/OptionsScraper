"""Tests for expiry_fetcher — NSE Bhavcopy-based expiry date discovery."""

import pytest
from unittest.mock import patch
from DhanHQ_src.loop_expiries.expiry_fetcher import (
    classify_expiry_dates,
    fetch_all_expiry_dates,
    _extract_nifty_expiry_dates_from_bhavcopy,
)


class TestClassifyExpiryDates:
    """Position-based: every date = WEEK, last of month = also MONTH."""

    def test_all_dates_get_week_entry(self):
        dates = ["2026-01-06", "2026-01-13", "2026-01-20", "2026-01-27"]
        result = classify_expiry_dates(dates, 2026)
        weekly = [e for e in result if e["expiry_flag"] == "WEEK"]
        assert len(weekly) == 4

    def test_last_of_month_also_gets_month_entry(self):
        dates = ["2026-01-06", "2026-01-13", "2026-01-20", "2026-01-27"]
        result = classify_expiry_dates(dates, 2026)
        monthly = [e for e in result if e["expiry_flag"] == "MONTH"]
        assert len(monthly) == 1
        assert monthly[0]["expiry_date"] == "2026-01-27"  # Last date in Jan

    def test_last_of_month_has_both_week_and_month(self):
        """The last date of each month should appear TWICE: as WEEK and MONTH."""
        dates = ["2026-01-06", "2026-01-27"]
        result = classify_expiry_dates(dates, 2026)
        jan27 = [e for e in result if e["expiry_date"] == "2026-01-27"]
        flags = {e["expiry_flag"] for e in jan27}
        assert flags == {"WEEK", "MONTH"}

    def test_multi_month(self):
        dates = ["2026-01-06", "2026-01-27", "2026-02-03", "2026-02-24"]
        result = classify_expiry_dates(dates, 2026)
        monthly = [e for e in result if e["expiry_flag"] == "MONTH"]
        assert len(monthly) == 2
        monthly_dates = {m["expiry_date"] for m in monthly}
        assert monthly_dates == {"2026-01-27", "2026-02-24"}

    def test_single_date_month_is_both(self):
        """If only one date in a month, it's both WEEK and MONTH."""
        dates = ["2026-06-30"]
        result = classify_expiry_dates(dates, 2026)
        assert len(result) == 2
        flags = {e["expiry_flag"] for e in result}
        assert flags == {"WEEK", "MONTH"}

    def test_filters_year(self):
        dates = ["2025-12-30", "2026-01-06", "2027-01-05"]
        result = classify_expiry_dates(dates, 2026)
        assert all(e["expiry_date"].startswith("2026") for e in result)

    def test_from_date_is_14_days_before(self):
        from datetime import date
        dates = ["2026-01-06", "2026-01-27"]
        result = classify_expiry_dates(dates, 2026)
        for entry in result:
            exp = date.fromisoformat(entry["expiry_date"])
            frm = date.fromisoformat(entry["from_date"])
            assert (exp - frm).days == 14

    def test_to_date_equals_expiry(self):
        dates = ["2026-01-06"]
        result = classify_expiry_dates(dates, 2026)
        for e in result:
            assert e["to_date"] == e["expiry_date"]

    def test_empty_returns_empty(self):
        assert classify_expiry_dates([], 2026) == []

    def test_deduplicates(self):
        dates = ["2026-01-06", "2026-01-06"]
        result = classify_expiry_dates(dates, 2026)
        weekly = [e for e in result if e["expiry_flag"] == "WEEK"]
        assert len(weekly) == 1

    def test_sorted_output(self):
        dates = ["2026-01-13", "2026-01-06", "2026-01-27"]
        result = classify_expiry_dates(dates, 2026)
        pairs = [(e["expiry_date"], e["expiry_flag"]) for e in result]
        assert pairs == sorted(pairs)

    def test_realistic_bhavcopy_data(self):
        """Test with the actual 22 dates returned by NSE Bhavcopy."""
        dates = [
            "2026-01-06", "2026-01-13", "2026-01-20", "2026-01-27",
            "2026-02-03", "2026-02-10", "2026-02-17", "2026-02-24",
            "2026-03-02", "2026-03-10", "2026-03-17", "2026-03-24", "2026-03-30",
            "2026-04-07", "2026-04-13", "2026-04-21", "2026-04-28",
            "2026-05-05", "2026-05-26",
            "2026-06-30", "2026-09-29", "2026-12-29",
        ]
        result = classify_expiry_dates(dates, 2026)
        weekly = [e for e in result if e["expiry_flag"] == "WEEK"]
        monthly = [e for e in result if e["expiry_flag"] == "MONTH"]

        assert len(weekly) == 22  # All dates get WEEK
        assert len(monthly) == 8  # 8 months with data (Jan-Jun, Sep, Dec)

        # Check specific monthly dates
        monthly_dates = {m["expiry_date"] for m in monthly}
        assert "2026-01-27" in monthly_dates  # Last Tue of Jan
        assert "2026-02-24" in monthly_dates  # Last Tue of Feb
        assert "2026-03-30" in monthly_dates  # Last Mon of Mar (holiday shift)
        assert "2026-04-28" in monthly_dates  # Last Tue of Apr
        assert "2026-12-29" in monthly_dates  # Last Tue of Dec


class TestFetchAllExpiryDates:
    @patch("DhanHQ_src.loop_expiries.expiry_fetcher.fetch_expiry_dates_from_bhavcopy")
    def test_returns_classified_dates(self, mock_bhav):
        mock_bhav.return_value = [
            "2026-01-06", "2026-01-13", "2026-01-20", "2026-01-27",
        ]
        result = fetch_all_expiry_dates(2026)
        weekly = [e for e in result if e["expiry_flag"] == "WEEK"]
        monthly = [e for e in result if e["expiry_flag"] == "MONTH"]
        assert len(weekly) == 4
        assert len(monthly) == 1  # Jan-27

    @patch("DhanHQ_src.loop_expiries.expiry_fetcher.fetch_expiry_dates_from_bhavcopy")
    def test_empty_returns_empty(self, mock_bhav):
        mock_bhav.return_value = []
        result = fetch_all_expiry_dates(2026)
        assert result == []


class TestExtractFromBhavcopy:
    def test_extracts_nifty_dates(self, tmp_path):
        csv_file = tmp_path / "test_bhav.csv"
        csv_file.write_text(
            "INSTRUMENT,SYMBOL,EXPIRY_DT,STRIKE_PR,OPTION_TYP,OPEN,HIGH,LOW,CLOSE\n"
            "OPTIDX,NIFTY,06-JAN-2026,23000,CE,100,110,95,105\n"
            "OPTIDX,NIFTY,13-JAN-2026,23000,PE,200,210,195,205\n"
            "OPTIDX,BANKNIFTY,06-JAN-2026,50000,CE,300,310,295,305\n"
            "FUTIDX,NIFTY,29-JAN-2026,0,XX,0,0,0,0\n"
        )
        dates = _extract_nifty_expiry_dates_from_bhavcopy(str(csv_file))
        assert dates == ["2026-01-06", "2026-01-13"]
