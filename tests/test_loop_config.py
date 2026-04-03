import pytest
from datetime import date

from DhanHQ_src.loop_expiries.config import (
    get_lot_size,
    LOOP_STRIKES,
    LOOP_OPTION_TYPES,
)
from DhanHQ_src.loop_expiries.expiry_fetcher import classify_expiry_dates


class TestLoopStrikes:
    def test_strike_count(self):
        assert len(LOOP_STRIKES) == 21

    def test_first_strike(self):
        assert LOOP_STRIKES[0] == "ATM-10"

    def test_last_strike(self):
        assert LOOP_STRIKES[-1] == "ATM+10"

    def test_atm_in_middle(self):
        assert LOOP_STRIKES[10] == "ATM"

    def test_option_types(self):
        assert LOOP_OPTION_TYPES == ["CALL", "PUT"]


class TestClassifyExpiryDates:
    """Position-based: every date = WEEK, last of each month = also MONTH."""

    # Realistic bhavcopy dates (Tuesdays + holiday-shifted Mondays)
    SAMPLE_DATES = [
        "2026-01-06", "2026-01-13", "2026-01-20", "2026-01-27",
        "2026-02-03", "2026-02-10", "2026-02-17", "2026-02-24",
        "2026-03-02", "2026-03-10", "2026-03-17", "2026-03-24", "2026-03-30",
    ]

    def test_returns_list(self):
        result = classify_expiry_dates(self.SAMPLE_DATES, 2026)
        assert isinstance(result, list)
        assert len(result) > 0

    def test_every_date_gets_week_entry(self):
        result = classify_expiry_dates(self.SAMPLE_DATES, 2026)
        weekly = [e for e in result if e["expiry_flag"] == "WEEK"]
        assert len(weekly) == len(self.SAMPLE_DATES)

    def test_last_per_month_also_gets_month_entry(self):
        result = classify_expiry_dates(self.SAMPLE_DATES, 2026)
        monthly = [e for e in result if e["expiry_flag"] == "MONTH"]
        assert len(monthly) == 3  # Jan, Feb, Mar
        monthly_dates = {m["expiry_date"] for m in monthly}
        assert monthly_dates == {"2026-01-27", "2026-02-24", "2026-03-30"}

    def test_from_date_is_14_days_before(self):
        result = classify_expiry_dates(self.SAMPLE_DATES, 2026)
        for entry in result:
            exp = date.fromisoformat(entry["expiry_date"])
            frm = date.fromisoformat(entry["from_date"])
            assert (exp - frm).days == 14

    def test_to_date_equals_expiry(self):
        result = classify_expiry_dates(self.SAMPLE_DATES, 2026)
        for entry in result:
            assert entry["to_date"] == entry["expiry_date"]

    def test_filters_to_target_year(self):
        mixed = ["2025-12-30", "2026-01-06", "2027-01-06"]
        result = classify_expiry_dates(mixed, 2026)
        for entry in result:
            assert entry["expiry_date"].startswith("2026")

    def test_deduplicates_dates(self):
        dupes = ["2026-01-06", "2026-01-06", "2026-01-13"]
        result = classify_expiry_dates(dupes, 2026)
        weekly = [e for e in result if e["expiry_flag"] == "WEEK"]
        assert len(weekly) == 2

    def test_empty_input(self):
        assert classify_expiry_dates([], 2026) == []

    def test_sorted_output(self):
        result = classify_expiry_dates(self.SAMPLE_DATES, 2026)
        pairs = [(e["expiry_date"], e["expiry_flag"]) for e in result]
        assert pairs == sorted(pairs)


class TestGetLotSize:
    def test_known_year_2026(self):
        assert get_lot_size(2026) == 65

    def test_known_year_2025(self):
        assert get_lot_size(2025) == 75

    def test_known_year_2024(self):
        assert get_lot_size(2024) == 25

    def test_unknown_year_fallback(self):
        assert get_lot_size(2020) == 75
