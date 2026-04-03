"""Configuration for the loop expiries scraper.

Expiry date generation has been moved to expiry_fetcher.py which
fetches actual dates from DhanHQ API + NSE Bhavcopy instead of
computing them from calendar rules.
"""

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
