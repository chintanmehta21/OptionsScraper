"""Configuration for SENSEX parquet scraping."""

from DhanHQ_src.loop_expiries.config import LOOP_STRIKES, LOOP_OPTION_TYPES

# DhanHQ rolling-option API parameters for SENSEX index options.
# Verified against Dhan scrip master (BSE,I,51,INDEX,...,SENSEX) and
# 2026-04-30 BSE FO bhavcopy (FinInstrmTp=IDO, NewBrdLotQty=20).
SENSEX_SYMBOL_CFG = {
    "security_id": 51,
    "exchange_segment": "BSE_FNO",
    "instrument": "OPTIDX",
}

# Same strike grid as NIFTY: ATM-10 through ATM+10 (21 strikes).
# DhanHQ resolves the 100-pt SENSEX strike spacing server-side.
SENSEX_STRIKES = LOOP_STRIKES
SENSEX_OPTION_TYPES = LOOP_OPTION_TYPES

# Output paths
GDRIVE_BASE = "G:/My Drive/OptionsData/SENSEX"
PARQUET_FILENAME_TEMPLATE = "sensex_options_{year}.parquet"
PROGRESS_FILENAME_TEMPLATE = "sensex_options_{year}_progress.json"

# BSE Bhavcopy (per-day CSV — not zipped, unlike NSE's new format).
BSE_BHAVCOPY_URL = (
    "https://www.bseindia.com/download/Bhavcopy/Derivative/"
    "BhavCopy_BSE_FO_0_0_0_{yyyymmdd}_F_0000.CSV"
)
BSE_BHAVCOPY_CACHE_PREFIX = "bse_fo_"
BSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    ),
    "Referer": "https://www.bseindia.com/",
}
