"""Configuration for the loop expiries scraper."""

import calendar
from datetime import date, timedelta

# ATM-10 through ATM+10 = 21 strikes
LOOP_STRIKES = (
    [f"ATM{i}" for i in range(-10, 0)]
    + ["ATM"]
    + [f"ATM+{i}" for i in range(1, 11)]
)

LOOP_OPTION_TYPES = ["CALL", "PUT"]

# Historical NIFTY lot sizes by year
NIFTY_LOT_SIZES = {
    2024: 25,
    2025: 75,
    2026: 65,
}
_LOT_SIZE_FALLBACK = 75


def get_lot_size(year: int) -> int:
    """Return NIFTY lot size for a given year."""
    return NIFTY_LOT_SIZES.get(year, _LOT_SIZE_FALLBACK)


def _is_last_thursday(d: date) -> bool:
    """Check if a date is the last Thursday of its month."""
    _, last_day = calendar.monthrange(d.year, d.month)
    last_date = date(d.year, d.month, last_day)
    while last_date.weekday() != 3:
        last_date -= timedelta(days=1)
    return d == last_date


def generate_expiry_dates(year: int) -> list[dict]:
    """Generate all NIFTY expiry dates for a given year.

    Every Thursday → WEEK expiry.
    Last Thursday of each month → also a MONTH expiry (separate contract).
    Returns ~64 entries per year.
    """
    expiries = []
    d = date(year, 1, 1)
    while d.weekday() != 3:
        d += timedelta(days=1)

    while d.year == year:
        from_date = (d - timedelta(days=14)).isoformat()
        to_date = d.isoformat()

        expiries.append({
            "expiry_date": d.isoformat(),
            "expiry_flag": "WEEK",
            "from_date": from_date,
            "to_date": to_date,
        })

        if _is_last_thursday(d):
            expiries.append({
                "expiry_date": d.isoformat(),
                "expiry_flag": "MONTH",
                "from_date": from_date,
                "to_date": to_date,
            })

        d += timedelta(weeks=1)

    return expiries
