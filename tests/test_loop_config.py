import pytest
from datetime import date
from DhanHQ_src.loop_expiries.config import (
    generate_expiry_dates,
    get_lot_size,
    LOOP_STRIKES,
    LOOP_OPTION_TYPES,
)


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


class TestGenerateExpiryDates:
    def test_returns_list(self):
        result = generate_expiry_dates(2026)
        assert isinstance(result, list)
        assert len(result) > 0

    def test_all_thursdays(self):
        result = generate_expiry_dates(2026)
        for entry in result:
            d = date.fromisoformat(entry["expiry_date"])
            assert d.weekday() == 3, f"{entry['expiry_date']} is not Thursday"

    def test_weekly_count_roughly_52(self):
        result = generate_expiry_dates(2026)
        weekly = [e for e in result if e["expiry_flag"] == "WEEK"]
        assert 50 <= len(weekly) <= 53

    def test_monthly_count_is_12(self):
        result = generate_expiry_dates(2026)
        monthly = [e for e in result if e["expiry_flag"] == "MONTH"]
        assert len(monthly) == 12

    def test_from_date_is_14_days_before(self):
        result = generate_expiry_dates(2026)
        for entry in result:
            exp = date.fromisoformat(entry["expiry_date"])
            frm = date.fromisoformat(entry["from_date"])
            assert (exp - frm).days == 14

    def test_to_date_equals_expiry(self):
        result = generate_expiry_dates(2026)
        for entry in result:
            assert entry["to_date"] == entry["expiry_date"]

    def test_all_dates_in_year(self):
        result = generate_expiry_dates(2026)
        for entry in result:
            d = date.fromisoformat(entry["expiry_date"])
            assert d.year == 2026

    def test_monthly_is_last_thursday(self):
        import calendar
        from datetime import timedelta
        result = generate_expiry_dates(2026)
        monthly = [e for e in result if e["expiry_flag"] == "MONTH"]
        for entry in monthly:
            d = date.fromisoformat(entry["expiry_date"])
            _, last_day = calendar.monthrange(d.year, d.month)
            last_date = date(d.year, d.month, last_day)
            while last_date.weekday() != 3:
                last_date -= timedelta(days=1)
            assert d == last_date, f"{d} is not last Thursday of {d.month}"

    def test_works_for_other_years(self):
        for year in [2024, 2025]:
            result = generate_expiry_dates(year)
            weekly = [e for e in result if e["expiry_flag"] == "WEEK"]
            assert 50 <= len(weekly) <= 53


class TestGetLotSize:
    def test_known_year_2026(self):
        assert get_lot_size(2026) == 65

    def test_known_year_2025(self):
        assert get_lot_size(2025) == 75

    def test_known_year_2024(self):
        assert get_lot_size(2024) == 25

    def test_unknown_year_fallback(self):
        assert get_lot_size(2020) == 75
