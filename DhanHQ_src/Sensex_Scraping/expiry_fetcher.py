"""Fetch SENSEX option expiry dates from BSE F&O Bhavcopy.

Single source of truth: BSE exchange data. No calendar math.
Walks one bhavcopy per month for the year, filters IDO/SENSEX rows,
collects unique XpryDt values, then classifies into WEEK/MONTH using
the symbol-agnostic classify_expiry_dates from loop_expiries.

Note: BSE returns HTTP 200 with an HTML 'page not found' body for
missing dates. We sniff the first bytes for the bhavcopy header to
distinguish real CSV from an HTML redirect.
"""

import csv
import logging
import os
import time
from datetime import date

import requests

from DhanHQ_src.config import BHAVCOPY_DIR
from DhanHQ_src.loop_expiries.expiry_fetcher import classify_expiry_dates
from DhanHQ_src.Sensex_Scraping.config import (
    BSE_BHAVCOPY_URL,
    BSE_BHAVCOPY_CACHE_PREFIX,
    BSE_HEADERS,
)

logger = logging.getLogger(__name__)


def _download_bse_bhavcopy(date_str: str) -> str | None:
    """Download BSE F&O bhavcopy for one date.

    Returns the cached CSV path or None if the date has no bhavcopy
    (BSE returns HTTP 200 with HTML for missing dates — we header-sniff).
    """
    os.makedirs(BHAVCOPY_DIR, exist_ok=True)
    cached = os.path.join(BHAVCOPY_DIR, f"{BSE_BHAVCOPY_CACHE_PREFIX}{date_str}.csv")
    if os.path.exists(cached):
        return cached

    try:
        dt = date.fromisoformat(date_str)
    except ValueError:
        return None
    yyyymmdd = dt.strftime("%Y%m%d")
    url = BSE_BHAVCOPY_URL.format(yyyymmdd=yyyymmdd)

    try:
        resp = requests.get(url, headers=BSE_HEADERS, timeout=30)
        if resp.status_code != 200 or not resp.content:
            return None
        # Sniff header — real bhavcopy CSV starts with these column names.
        head = resp.content[:200].decode("utf-8", errors="replace")
        if "TradDt" not in head or "TckrSymb" not in head:
            logger.debug("BSE returned non-bhavcopy body for %s", date_str)
            return None
        with open(cached, "wb") as f:
            f.write(resp.content)
        logger.info("Downloaded BSE bhavcopy: %s", cached)
        return cached
    except Exception as e:
        logger.debug("BSE bhavcopy download failed %s: %s", url, e)
        return None


def _extract_sensex_expiry_dates(csv_path: str) -> list[str]:
    """Filter IDO + SENSEX rows from a bhavcopy CSV; return sorted unique XpryDt."""
    out: set[str] = set()
    if not os.path.exists(csv_path):
        return []
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if (
                    row.get("FinInstrmTp", "").strip() == "IDO"
                    and row.get("TckrSymb", "").strip() == "SENSEX"
                ):
                    xpry = row.get("XpryDt", "").strip()
                    if xpry:
                        out.add(xpry[:10])
    except Exception as e:
        logger.error("Failed to parse BSE bhavcopy %s: %s", csv_path, e)
    return sorted(out)


def fetch_expiry_dates_from_bhavcopy(year: int) -> list[str]:
    """Walk one bhavcopy per month for the year; merge SENSEX expiries."""
    all_dates: set[str] = set()
    today = date.today()

    for month in range(1, 13):
        for day_offset in range(2, 8):
            try:
                target = date(year, month, day_offset)
            except ValueError:
                continue
            if target >= today:
                break
            if target.weekday() >= 5:
                continue

            csv_path = _download_bse_bhavcopy(target.isoformat())
            if csv_path:
                year_dates = [
                    d
                    for d in _extract_sensex_expiry_dates(csv_path)
                    if d.startswith(str(year))
                ]
                all_dates.update(year_dates)
                logger.info(
                    "BSE bhavcopy %s: found %d SENSEX expiry dates for %d",
                    target.isoformat(), len(year_dates), year,
                )
                break
            time.sleep(1)

    result = sorted(all_dates)
    logger.info("BSE Bhavcopy: %d unique SENSEX expiry dates for %d", len(result), year)
    return result


def fetch_all_expiry_dates(year: int) -> list[dict]:
    """Fetch + classify all SENSEX expiry dates for a given year.

    Returns a list of dicts with keys expiry_date, expiry_flag, from_date,
    to_date — same shape as the NIFTY loop_expiries fetcher.
    """
    raw_dates = fetch_expiry_dates_from_bhavcopy(year)
    return classify_expiry_dates(raw_dates, year)
