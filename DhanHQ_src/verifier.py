# DhanHQ_src/verifier.py
import os
import csv
import logging
import requests
from datetime import datetime, timedelta

from DhanHQ_src.config import BHAVCOPY_DIR, EXPIRY_DATE

logger = logging.getLogger(__name__)

# NSE Bhavcopy URL pattern
BHAVCOPY_URL_TEMPLATE = (
    "https://nsearchives.nseindia.com/content/historical/DERIVATIVES/"
    "{year}/{month}/fo{ddmmmyyyy}bhav.csv.zip"
)

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Referer": "https://www.nseindia.com/",
}


def download_bhavcopy(date_str, output_dir=None):
    """Download NSE F&O Bhavcopy for a given date."""
    if output_dir is None:
        output_dir = BHAVCOPY_DIR
    os.makedirs(output_dir, exist_ok=True)

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%b").upper()
    ddmmmyyyy = dt.strftime("%d%b%Y").upper()

    csv_path = os.path.join(output_dir, f"fo_bhavcopy_{date_str}.csv")
    if os.path.exists(csv_path):
        logger.info("Bhavcopy already downloaded: %s", csv_path)
        return csv_path

    url = BHAVCOPY_URL_TEMPLATE.format(year=year, month=month, ddmmmyyyy=ddmmmyyyy)
    logger.info("Downloading bhavcopy from %s", url)

    try:
        session = requests.Session()
        session.get("https://www.nseindia.com/", headers=NSE_HEADERS, timeout=10)
        response = session.get(url, headers=NSE_HEADERS, timeout=30)
        if response.status_code != 200:
            logger.warning("Bhavcopy download failed (HTTP %d) for %s", response.status_code, date_str)
            return None

        import zipfile
        import io
        z = zipfile.ZipFile(io.BytesIO(response.content))
        for name in z.namelist():
            if name.endswith(".csv"):
                with open(csv_path, "wb") as f:
                    f.write(z.read(name))
                logger.info("Saved bhavcopy to %s", csv_path)
                return csv_path

    except Exception as e:
        logger.error("Failed to download bhavcopy for %s: %s", date_str, e)
    return None


def parse_bhavcopy_csv(csv_path, symbol="NIFTY", expiry=None):
    """Parse NSE Bhavcopy CSV and filter for NIFTY options."""
    results = []
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("INSTRUMENT", "").strip() != "OPTIDX":
                continue
            if row.get("SYMBOL", "").strip() != symbol:
                continue
            if expiry and row.get("EXPIRY_DT", "").strip() != expiry:
                continue

            results.append({
                "strike": int(float(row["STRIKE_PR"].strip())),
                "option_type": row["OPTION_TYP"].strip(),
                "close": float(row["CLOSE"].strip()),
                "oi": int(row["OPEN_INT"].strip()),
                "volume": int(row["CONTRACTS"].strip()),
            })
    return results


def compare_values(dhan_data, nse_data, close_tolerance=0.05):
    """Compare DhanHQ EOD values with NSE Bhavcopy values."""
    close_diff = abs(dhan_data["close"] - nse_data["close"])
    close_match = close_diff <= close_tolerance
    oi_match = dhan_data["oi"] == nse_data["oi"]
    volume_match = dhan_data["volume"] == nse_data["volume"]

    notes_parts = []
    if not close_match:
        notes_parts.append(f"close diff={close_diff:.2f}")
    if not oi_match:
        notes_parts.append(f"oi diff={dhan_data['oi'] - nse_data['oi']}")
    if not volume_match:
        notes_parts.append(f"vol diff={dhan_data['volume'] - nse_data['volume']}")

    return {
        "close_match": close_match,
        "oi_match": oi_match,
        "volume_match": volume_match,
        "notes": "; ".join(notes_parts) if notes_parts else "OK",
    }


def verify_against_bhavcopy(db, dates=None):
    """Run full verification for all dates."""
    if dates is None:
        dates = db.get_distinct_dates()

    expiry_formatted = datetime.strptime(EXPIRY_DATE, "%Y-%m-%d").strftime("%d-%b-%Y")
    total_checks = 0
    total_matches = 0
    verification_rows = []

    for date_str in dates:
        csv_path = download_bhavcopy(date_str)
        if csv_path is None:
            logger.warning("Skipping verification for %s (no bhavcopy)", date_str)
            continue

        nse_data = parse_bhavcopy_csv(csv_path, symbol="NIFTY", expiry=expiry_formatted)
        nse_lookup = {(r["strike"], r["option_type"]): r for r in nse_data}

        eod_data = db.get_eod_data(date_str)

        for dhan_row in eod_data:
            key = (dhan_row["strike"], dhan_row["option_type"])
            nse_row = nse_lookup.get(key)
            if nse_row is None:
                logger.warning("No NSE data for %s %s %s", date_str, key[0], key[1])
                continue

            comparison = compare_values(
                {"close": dhan_row["close"], "oi": dhan_row["oi"], "volume": dhan_row["volume"]},
                {"close": nse_row["close"], "oi": nse_row["oi"], "volume": nse_row["volume"]},
            )

            all_match = comparison["close_match"] and comparison["oi_match"] and comparison["volume_match"]
            total_checks += 1
            if all_match:
                total_matches += 1

            verification_rows.append({
                "date": date_str,
                "strike": dhan_row["strike"],
                "option_type": dhan_row["option_type"],
                "dhan_close": dhan_row["close"],
                "nse_close": nse_row["close"],
                "dhan_oi": dhan_row["oi"],
                "nse_oi": nse_row["oi"],
                "dhan_volume": dhan_row["volume"],
                "nse_volume": nse_row["volume"],
                **comparison,
            })

    if verification_rows:
        db.insert_verification_log(verification_rows)

    summary = {
        "total_checks": total_checks,
        "total_matches": total_matches,
        "match_rate": (total_matches / total_checks * 100) if total_checks > 0 else 0,
        "mismatches": total_checks - total_matches,
    }

    logger.info("Verification complete: %d/%d matched (%.1f%%)",
                total_matches, total_checks, summary["match_rate"])
    return summary
