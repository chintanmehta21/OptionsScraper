import os

# DhanHQ API credentials (set via environment variables or GitHub Secrets)
DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")

# NIFTY underlying security ID
NIFTY_SECURITY_ID = 13

# Scrape parameters
EXPIRY_DATE = "2026-03-30"
EXPIRY_FLAG = "MONTH"
FROM_DATE = "2026-03-16"
TO_DATE = "2026-03-31"  # non-inclusive per API docs
EXCHANGE_SEGMENT = "NSE_FNO"
INSTRUMENT_TYPE = "INDEX"
INTERVAL = 1  # 1-minute candles

# 9 strikes: ATM-4 through ATM+4
STRIKES = ["ATM-4", "ATM-3", "ATM-2", "ATM-1", "ATM", "ATM+1", "ATM+2", "ATM+3", "ATM+4"]
OPTION_TYPES = ["CALL", "PUT"]

# Data fields to request from API
REQUIRED_DATA = ["open", "high", "low", "close", "iv", "volume", "strike", "oi", "spot"]

# IVR/IVP baseline: 52 weeks back from scrape start
IV_BASELINE_FROM = "2025-03-16"
IV_BASELINE_TO = "2026-03-16"

# Rate limiting
API_DELAY_SECONDS = 0.25
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2

# NIFTY lot size
LOT_SIZE = 65

# Risk-free rate for Black-Scholes (RBI repo rate approx)
RISK_FREE_RATE = 0.065

# Database path
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "nifty_options.db")
BHAVCOPY_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "bhavcopy")
