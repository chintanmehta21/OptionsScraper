import os
from datetime import date as dt_date, timedelta

# DhanHQ API credentials (raw env vars only — auth happens at runtime)
DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")

# Supabase credentials
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")

# SQLite fallback path (used when Supabase is not configured)
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "nifty_options.db")

# NIFTY underlying security ID
NIFTY_SECURITY_ID = 13
SYMBOL = "NIFTY"

# ── Expiry Configurations ──────────────────────────────────────
# Add entries to scrape multiple expiries.  Each entry's data flows
# through the full pipeline independently and lands in the output table.
#
# All dates: YYYY-MM-DD (ISO 8601).
#
# Fields:
#   expiry_date  — contract expiry date
#   from_date    — first date to fetch (inclusive)
#   to_date      — last date to fetch (non-inclusive per DhanHQ API)
#   expiry_flag  — "MONTH" or "WEEK"
#   expiry_code  — 1 = current/nearest, 2 = next, etc.
#   lot_size     — contract lot size
EXPIRY_CONFIGS = [
    {
        "expiry_date": "2026-03-30",
        "from_date": "2026-03-16",
        "to_date": "2026-03-31",
        "expiry_flag": "MONTH",
        "expiry_code": 1,
        "lot_size": 65,
    },
]

# ── Backward-compatible single-expiry vars ─────────────────────
# Derived from first EXPIRY_CONFIGS entry so that calculator.py,
# db.py tests, and anything importing individual vars still works.
_default = EXPIRY_CONFIGS[0]
EXPIRY_DATE = _default["expiry_date"]
EXPIRY_FLAG = _default["expiry_flag"]
FROM_DATE = _default["from_date"]
TO_DATE = _default["to_date"]
LOT_SIZE = _default["lot_size"]

# IV baseline: 52 weeks before from_date
_from_dt = dt_date.fromisoformat(FROM_DATE)
IV_BASELINE_FROM = (_from_dt - timedelta(weeks=52)).isoformat()
IV_BASELINE_TO = FROM_DATE

# ── Shared / fixed settings ────────────────────────────────────
EXCHANGE_SEGMENT = "NSE_FNO"
INSTRUMENT_TYPE = "INDEX"
INTERVAL = 1  # 1-minute candles

# 9 strikes: ATM-4 through ATM+4
STRIKES = ["ATM-4", "ATM-3", "ATM-2", "ATM-1", "ATM", "ATM+1", "ATM+2", "ATM+3", "ATM+4"]
OPTION_TYPES = ["CALL", "PUT"]

# Data fields to request from API
REQUIRED_DATA = ["open", "high", "low", "close", "iv", "volume", "strike", "oi", "spot"]

# Rate limiting
API_DELAY_SECONDS = 0.25
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2

# Risk-free rate for Black-Scholes (RBI repo rate approx)
RISK_FREE_RATE = 0.065

# Bhavcopy temp directory
BHAVCOPY_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "bhavcopy")
