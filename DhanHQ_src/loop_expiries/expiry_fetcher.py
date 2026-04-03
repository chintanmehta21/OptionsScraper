"""Fetch actual NIFTY option expiry dates from NSE F&O Bhavcopy.

Single source of truth: NSE exchange data. No calendar math.
Downloads one bhavcopy per month, extracts all NIFTY option expiry dates.

Key facts (verified from NSE Bhavcopy data):
  Post Sep 2025 (NIFTY on NSE):
    - ALL expiries (weekly AND monthly) are on TUESDAY (Mon if holiday).
    - Monthly = last Tuesday of each month (same day as weekly).
    - On that last Tuesday, both WEEK and MONTH contracts coexist.
  Pre Sep 2025:
    - ALL expiries were on THURSDAY.
    - Monthly = last Thursday of each month.
"""

import io
import os
import csv
import time
import logging
import zipfile
from datetime import date, datetime, timedelta
from collections import defaultdict

import requests

from DhanHQ_src.config import BHAVCOPY_DIR

logger = logging.getLogger(__name__)

# ── NSE Bhavcopy URL formats ────────────────────────────────────
# New format (2024+): YYYY-MM-DD columns, IDO instrument type
_NEW_URL = (
    "https://nsearchives.nseindia.com/content/fo/"
    "BhavCopy_NSE_FO_0_0_0_{yyyymmdd}_F_0000.csv.zip"
)
# Old format (legacy): DD-MMM-YYYY columns, OPTIDX instrument type
_OLD_URL = (
    "https://nsearchives.nseindia.com/content/historical/DERIVATIVES/"
    "{year}/{month}/fo{ddmmmyyyy}bhav.csv.zip"
)

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Referer": "https://www.nseindia.com/",
}


def _download_bhavcopy(date_str: str) -> str | None:
    """Download NSE FO Bhavcopy for a specific date.

    Tries the new URL format first, falls back to old format.
    Returns CSV path or None.
    """
    output_dir = BHAVCOPY_DIR
    os.makedirs(output_dir, exist_ok=True)

    # Check both naming conventions for cached files
    new_path = os.path.join(output_dir, f"fo_bhavcopy_new_{date_str}.csv")
    old_path = os.path.join(output_dir, f"fo_bhavcopy_{date_str}.csv")
    if os.path.exists(new_path):
        return new_path
    if os.path.exists(old_path):
        return old_path

    dt = date.fromisoformat(date_str)
    session = requests.Session()
    try:
        session.get("https://www.nseindia.com/", headers=NSE_HEADERS, timeout=10)
    except Exception:
        pass  # Cookie fetch failed — try download anyway

    # Try new URL format first
    yyyymmdd = dt.strftime("%Y%m%d")
    new_url = _NEW_URL.format(yyyymmdd=yyyymmdd)
    csv_path = _try_download(session, new_url, new_path)
    if csv_path:
        return csv_path

    # Fall back to old URL format
    year_str = dt.strftime("%Y")
    month_str = dt.strftime("%b").upper()
    ddmmmyyyy = dt.strftime("%d%b%Y").upper()
    old_url = _OLD_URL.format(year=year_str, month=month_str, ddmmmyyyy=ddmmmyyyy)
    csv_path = _try_download(session, old_url, old_path)
    if csv_path:
        return csv_path

    logger.debug("Bhavcopy not available for %s", date_str)
    return None


def _try_download(session, url: str, save_path: str) -> str | None:
    """Attempt to download and extract a bhavcopy zip. Returns path or None."""
    try:
        resp = session.get(url, headers=NSE_HEADERS, timeout=30)
        if resp.status_code != 200:
            return None
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        for name in z.namelist():
            if name.endswith(".csv"):
                with open(save_path, "wb") as f:
                    f.write(z.read(name))
                logger.info("Downloaded bhavcopy: %s", save_path)
                return save_path
    except Exception as e:
        logger.debug("Download failed %s: %s", url, e)
    return None


def _extract_nifty_expiry_dates_from_bhavcopy(csv_path: str) -> list[str]:
    """Extract unique NIFTY option expiry dates from a bhavcopy CSV.

    Handles both formats:
      New: TckrSymb=NIFTY, FinInstrmTp=IDO, XpryDt=YYYY-MM-DD
      Old: SYMBOL=NIFTY, INSTRUMENT=OPTIDX, EXPIRY_DT=DD-MMM-YYYY
    """
    expiry_dates: set[str] = set()
    try:
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []

            is_new_format = "TckrSymb" in headers

            for row in reader:
                if is_new_format:
                    symbol = row.get("TckrSymb", "").strip()
                    instr = row.get("FinInstrmTp", "").strip()
                    if instr == "IDO" and symbol == "NIFTY":
                        raw = row.get("XpryDt", "").strip()
                        if raw:
                            # Already YYYY-MM-DD
                            expiry_dates.add(raw[:10])
                else:
                    symbol = row.get("SYMBOL", "").strip()
                    instr = row.get("INSTRUMENT", "").strip()
                    if instr == "OPTIDX" and symbol == "NIFTY":
                        raw = row.get("EXPIRY_DT", "").strip()
                        if raw:
                            dt = datetime.strptime(raw, "%d-%b-%Y")
                            expiry_dates.add(dt.strftime("%Y-%m-%d"))
    except Exception as e:
        logger.error("Failed to parse bhavcopy %s: %s", csv_path, e)
    return sorted(expiry_dates)


def fetch_expiry_dates_from_bhavcopy(year: int) -> list[str]:
    """Fetch NIFTY expiry dates by downloading NSE bhavcopies for the year.

    Downloads bhavcopy for the 2nd of each month (or next weekday).
    Each bhavcopy lists all NIFTY option contracts active on that day,
    including their expiry dates for weeks/months ahead.
    Merging across months gives us all weekly + monthly expiry dates.

    Returns sorted unique list of YYYY-MM-DD date strings.
    """
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

            csv_path = _download_bhavcopy(target.isoformat())
            if csv_path:
                dates = _extract_nifty_expiry_dates_from_bhavcopy(csv_path)
                year_dates = [d for d in dates if d.startswith(str(year))]
                all_dates.update(year_dates)
                logger.info(
                    "Bhavcopy %s: found %d NIFTY expiry dates for %d",
                    target.isoformat(), len(year_dates), year,
                )
                break
            time.sleep(1)

    result = sorted(all_dates)
    logger.info(
        "NSE Bhavcopy: %d unique NIFTY expiry dates for %d", len(result), year,
    )
    return result


def classify_expiry_dates(raw_dates: list[str], year: int) -> list[dict]:
    """Classify fetched expiry dates into WEEK and MONTH entries.

    Classification is POSITION-BASED (not weekday-based):
      - Every date is a WEEK entry.
      - The LAST date in each calendar month is ALSO a MONTH entry
        (separate contract with different OI/liquidity).

    NSE Bhavcopy data confirms: for NIFTY in 2026, both weekly and
    monthly expiries fall on the same day (Tuesday). On the last
    Tuesday of each month, BOTH weekly and monthly contracts coexist.
    """
    year_dates = sorted(set(d for d in raw_dates if d.startswith(str(year))))

    if not year_dates:
        logger.warning("No expiry dates found for year %d", year)
        return []

    # Group dates by month
    by_month: dict[str, list[str]] = defaultdict(list)
    for d in year_dates:
        by_month[d[:7]].append(d)

    # Find the last date in each month → that's the monthly expiry
    monthly_dates = set()
    for month_key, month_list in by_month.items():
        monthly_dates.add(max(month_list))

    expiries: list[dict] = []

    for d in year_dates:
        exp = date.fromisoformat(d)
        from_date = (exp - timedelta(days=14)).isoformat()

        # Every date → WEEK entry
        expiries.append({
            "expiry_date": d,
            "expiry_flag": "WEEK",
            "from_date": from_date,
            "to_date": d,
        })

        # Last date of each month → also MONTH entry (separate contract)
        if d in monthly_dates:
            expiries.append({
                "expiry_date": d,
                "expiry_flag": "MONTH",
                "from_date": from_date,
                "to_date": d,
            })

    expiries.sort(key=lambda e: (e["expiry_date"], e["expiry_flag"]))

    weeks = sum(1 for e in expiries if e["expiry_flag"] == "WEEK")
    months = sum(1 for e in expiries if e["expiry_flag"] == "MONTH")
    logger.info("Classified: %d WEEK + %d MONTH = %d total", weeks, months, len(expiries))
    return expiries


def fetch_all_expiry_dates(year: int) -> list[dict]:
    """Fetch and classify all NIFTY expiry dates for a given year.

    Single source: NSE F&O Bhavcopy (actual exchange data).
    Downloads one bhavcopy per month, extracts NIFTY option expiry dates,
    classifies into WEEK and MONTH entries.

    Args:
        year: Target year (e.g. 2026).

    Returns:
        List of classified expiry dicts ready for the scraper.
    """
    raw_dates = fetch_expiry_dates_from_bhavcopy(year)

    result = classify_expiry_dates(raw_dates, year)
    weeks = sum(1 for e in result if e["expiry_flag"] == "WEEK")
    months = sum(1 for e in result if e["expiry_flag"] == "MONTH")
    logger.info(
        "Final expiry list for %d: %d entries (%d WEEK + %d MONTH)",
        year, len(result), weeks, months,
    )
    return result
