# DhanHQ_src/fetcher.py
import time
import logging
import requests as _requests
from datetime import datetime, timezone, timedelta

from DhanHQ_src.config import (
    DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN,
    NIFTY_SECURITY_ID, EXCHANGE_SEGMENT, INSTRUMENT_TYPE,
    EXPIRY_FLAG, REQUIRED_DATA, INTERVAL,
    STRIKES, OPTION_TYPES,
    FROM_DATE, TO_DATE, EXPIRY_DATE,
    IV_BASELINE_FROM, IV_BASELINE_TO,
    API_DELAY_SECONDS, MAX_RETRIES, RETRY_BACKOFF_BASE,
)

logger = logging.getLogger(__name__)

# IST offset
IST = timezone(timedelta(hours=5, minutes=30))

# DhanHQ Rolling Option API endpoint (for expired options data)
ROLLING_OPTION_URL = "https://api.dhan.co/v2/charts/rollingoption"


class DhanClient:
    """Wrapper for DhanHQ expired options API using direct REST calls."""

    def __init__(self, client_id, access_token):
        self.client_id = client_id
        self.access_token = access_token
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": access_token,
            "client-id": client_id,
        }

    def expired_options_data(self, **kwargs):
        """Fetch expired options data via POST to /v2/charts/rollingoption."""
        payload = {
            "securityId": kwargs.get("security_id"),
            "exchangeSegment": kwargs.get("exchange_segment"),
            "instrument": "OPTIDX",
            "interval": kwargs.get("interval", INTERVAL),
            "expiryFlag": kwargs.get("expiry_flag"),
            "expiryCode": kwargs.get("expiry_code", 1),
            "strike": kwargs.get("strike"),
            "drvOptionType": kwargs.get("drv_option_type"),
            "requiredData": kwargs.get("required_data", REQUIRED_DATA),
            "fromDate": kwargs.get("from_date"),
            "toDate": kwargs.get("to_date"),
        }
        resp = _requests.post(ROLLING_OPTION_URL, headers=self.headers, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()


def create_dhan_client():
    return DhanClient(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)


def parse_api_response(response_data):
    """Convert DhanHQ parallel-array response into list of row dicts."""
    if not response_data or not response_data.get("timestamp"):
        return []

    timestamps = response_data["timestamp"]
    count = len(timestamps)
    rows = []

    for i in range(count):
        # Convert epoch to IST datetime
        dt = datetime.fromtimestamp(timestamps[i], tz=IST)
        rows.append({
            "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "date": dt.strftime("%Y-%m-%d"),
            "time": dt.strftime("%H:%M:%S"),
            "open": response_data["open"][i] if "open" in response_data else None,
            "high": response_data["high"][i] if "high" in response_data else None,
            "low": response_data["low"][i] if "low" in response_data else None,
            "close": response_data["close"][i] if "close" in response_data else None,
            "volume": response_data["volume"][i] if "volume" in response_data else None,
            "oi": response_data["oi"][i] if "oi" in response_data else None,
            "iv": response_data["iv"][i] if "iv" in response_data else None,
            "spot": response_data["spot"][i] if "spot" in response_data else None,
            "strike": response_data["strike"][i] if "strike" in response_data else None,
        })
    return rows


def build_raw_rows(parsed_rows, option_type, atm_offset, expiry_date):
    """Add metadata fields to parsed rows for DB insertion."""
    if option_type == "CALL":
        db_option_type = "CE"
    elif option_type == "PUT":
        db_option_type = "PE"
    else:
        db_option_type = option_type  # pass through CE/PE as-is
    for row in parsed_rows:
        row["option_type"] = db_option_type
        row["atm_offset"] = atm_offset
        row["expiry_date"] = expiry_date
    return parsed_rows


def fetch_with_retry(dhan, **kwargs):
    """Call expired_options_data with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            response = dhan.expired_options_data(**kwargs)
            if not isinstance(response, dict):
                return {}
            # REST API may return data directly or wrapped in status/data
            if "status" in response:
                if response["status"] == "success":
                    return response.get("data", {})
                logger.error("API error: %s (params: %s)", response, kwargs)
                return {}
            # Direct response (has timestamp, open, close, etc.)
            if "timestamp" in response:
                return response
            return response
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF_BASE ** (attempt + 1)
                logger.warning("Retry %d/%d after %.1fs: %s", attempt + 1, MAX_RETRIES, wait, e)
                time.sleep(wait)
            else:
                logger.error("All retries failed for %s: %s", kwargs, e)
                raise


def fetch_all_options_data(dhan=None):
    """Fetch 1-min data for all strikes x option types. Returns list of row dicts."""
    if dhan is None:
        dhan = create_dhan_client()

    all_rows = []
    strike_offsets = {s: i - len(STRIKES) // 2 for i, s in enumerate(STRIKES)}

    total_calls = len(STRIKES) * len(OPTION_TYPES)
    call_num = 0
    for strike in STRIKES:
        for option_type in OPTION_TYPES:
            call_num += 1
            logger.info("[%d/%d] Fetching %s %s ...", call_num, total_calls, strike, option_type)
            response_data = fetch_with_retry(
                dhan,
                security_id=NIFTY_SECURITY_ID,
                exchange_segment=EXCHANGE_SEGMENT,
                instrument_type=INSTRUMENT_TYPE,
                expiry_flag=EXPIRY_FLAG,
                expiry_code=1,
                strike=strike,
                drv_option_type=option_type,
                required_data=REQUIRED_DATA,
                from_date=FROM_DATE,
                to_date=TO_DATE,
            )
            parsed = parse_api_response(response_data)
            rows = build_raw_rows(parsed, option_type, strike_offsets[strike], EXPIRY_DATE)
            all_rows.extend(rows)
            logger.info("  Got %d candles for %s %s", len(parsed), strike, option_type)
            time.sleep(API_DELAY_SECONDS)

    logger.info("Total raw rows fetched: %d", len(all_rows))
    return all_rows


def fetch_iv_baseline(dhan=None):
    """Fetch daily ATM IV for past 52 weeks for IVR/IVP calculation."""
    if dhan is None:
        dhan = create_dhan_client()

    logger.info("Fetching 52-week IV baseline (ATM CALL daily) ...")

    from datetime import date as dt_date
    start = dt_date.fromisoformat(IV_BASELINE_FROM)
    end = dt_date.fromisoformat(IV_BASELINE_TO)
    chunk_days = 30
    all_rows = []

    total_chunks = ((end - start).days + chunk_days - 1) // chunk_days
    chunk_num = 0
    current = start
    while current < end:
        chunk_num += 1
        chunk_end = min(current + timedelta(days=chunk_days), end)
        logger.info("[%d/%d] IV chunk %s to %s", chunk_num, total_chunks,
                     current.isoformat(), chunk_end.isoformat())
        response_data = fetch_with_retry(
            dhan,
            security_id=NIFTY_SECURITY_ID,
            exchange_segment=EXCHANGE_SEGMENT,
            instrument_type=INSTRUMENT_TYPE,
            expiry_flag=EXPIRY_FLAG,
            expiry_code=1,
            strike="ATM",
            drv_option_type="CALL",
            required_data=["iv", "spot", "strike"],
            from_date=current.isoformat(),
            to_date=chunk_end.isoformat(),
        )

        if response_data and response_data.get("timestamp"):
            timestamps = response_data["timestamp"]
            for i in range(len(timestamps)):
                dt = datetime.fromtimestamp(timestamps[i], tz=IST)
                all_rows.append({
                    "date": dt.strftime("%Y-%m-%d"),
                    "atm_iv": response_data["iv"][i] if "iv" in response_data else None,
                    "spot": response_data["spot"][i] if "spot" in response_data else None,
                    "atm_strike": response_data["strike"][i] if "strike" in response_data else None,
                })

        logger.info("  IV baseline chunk %s to %s: %d rows",
                     current.isoformat(), chunk_end.isoformat(),
                     len(response_data.get("timestamp", [])) if response_data else 0)
        current = chunk_end
        time.sleep(API_DELAY_SECONDS)

    # Deduplicate to daily (take last entry per date for EOD IV)
    daily = {}
    for row in all_rows:
        daily[row["date"]] = row
    result = sorted(daily.values(), key=lambda x: x["date"])
    logger.info("IV baseline: %d daily entries", len(result))
    return result
