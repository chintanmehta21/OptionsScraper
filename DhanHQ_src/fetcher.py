# DhanHQ_src/fetcher.py
import time
import logging
import requests as _requests
from datetime import datetime, timezone, timedelta

from DhanHQ_src.config import (
    DHAN_CLIENT_ID,
    NIFTY_SECURITY_ID, EXCHANGE_SEGMENT, INSTRUMENT_TYPE,
    EXPIRY_FLAG, REQUIRED_DATA, INTERVAL,
    STRIKES, OPTION_TYPES,
    FROM_DATE, TO_DATE, EXPIRY_DATE,
    IV_BASELINE_FROM, IV_BASELINE_TO,
    API_DELAY_SECONDS, MAX_RETRIES, RETRY_BACKOFF_BASE,
)

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
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


def create_dhan_client(token=None):
    if token is None:
        from DhanHQ_src.auth import get_access_token
        token = get_access_token()
    return DhanClient(DHAN_CLIENT_ID, token)


def parse_api_response(response_data):
    """Convert DhanHQ parallel-array response into list of row dicts."""
    if not response_data or not response_data.get("timestamp"):
        return []

    timestamps = response_data["timestamp"]
    count = len(timestamps)
    rows = []

    for i in range(count):
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


def _unwrap_nested_response(response, option_type):
    """Extract flat data from nested {"data": {"ce": {...}, "pe": {...}}} format.

    DhanHQ rolling option API returns ce/pe nested objects.
    Each inner object has parallel arrays: timestamp, open, high, low, close, etc.
    """
    data = response.get("data")
    if not isinstance(data, dict):
        return response

    key = "ce" if option_type in ("CALL", "CE") else "pe"
    inner = data.get(key)
    if isinstance(inner, dict) and inner:
        return inner
    # Try the other key as fallback
    other = data.get("pe" if key == "ce" else "ce")
    if isinstance(other, dict) and other:
        return other
    return response


def fetch_with_retry(dhan, **kwargs):
    """Call expired_options_data with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            response = dhan.expired_options_data(**kwargs)
            if not isinstance(response, dict):
                logger.warning("Non-dict API response: %s", type(response))
                return {}
            if "status" in response:
                if response["status"] == "success":
                    return response.get("data", {})
                logger.error("API error response: %s", str(response)[:500])
                return {}
            if "timestamp" in response:
                return response
            # Nested {"data": {"ce": {...}, "pe": {...}}} format
            if "data" in response:
                option_type = kwargs.get("drv_option_type", "CALL")
                unwrapped = _unwrap_nested_response(response, option_type)
                if "timestamp" in unwrapped:
                    return unwrapped
                logger.debug("Unwrapped but no timestamp: %s", list(unwrapped.keys())[:10])
                return unwrapped
            # Unknown response shape — log for diagnostics
            logger.debug("API response keys: %s", list(response.keys()))
            return response
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF_BASE ** (attempt + 1)
                logger.warning("Retry %d/%d after %.1fs: %s", attempt + 1, MAX_RETRIES, wait, e)
                time.sleep(wait)
            else:
                logger.error("All retries failed for %s: %s", kwargs.get("strike", "?"), e)
                raise


def fetch_all_options_data(dhan=None, from_date=None, to_date=None,
                           expiry_date=None, expiry_flag=None, expiry_code=1):
    """Fetch 1-min data for all strikes x option types.

    All params fall back to config.py globals when None.
    """
    if dhan is None:
        dhan = create_dhan_client()

    _from = from_date or FROM_DATE
    _to = to_date or TO_DATE
    _expiry = expiry_date or EXPIRY_DATE
    _flag = expiry_flag or EXPIRY_FLAG

    all_rows = []
    strike_offsets = {s: i - len(STRIKES) // 2 for i, s in enumerate(STRIKES)}
    total_calls = len(STRIKES) * len(OPTION_TYPES)
    call_num = 0
    empty_count = 0

    for strike in STRIKES:
        for option_type in OPTION_TYPES:
            call_num += 1
            if call_num == 1 or call_num % 6 == 0 or call_num == total_calls:
                logger.info("  Progress: %d/%d API calls ...", call_num, total_calls)
            logger.debug("[%d/%d] Fetching %s %s ...", call_num, total_calls, strike, option_type)
            response_data = fetch_with_retry(
                dhan,
                security_id=NIFTY_SECURITY_ID,
                exchange_segment=EXCHANGE_SEGMENT,
                instrument_type=INSTRUMENT_TYPE,
                expiry_flag=_flag,
                expiry_code=expiry_code,
                strike=strike,
                drv_option_type=option_type,
                required_data=REQUIRED_DATA,
                from_date=_from,
                to_date=_to,
            )
            parsed = parse_api_response(response_data)
            if not parsed:
                empty_count += 1
                if isinstance(response_data, dict):
                    logger.warning("  0 candles. keys=%s body=%s",
                                   list(response_data.keys()), str(response_data)[:200])
                else:
                    logger.warning("  0 candles. type=%s", type(response_data))

            rows = build_raw_rows(parsed, option_type, strike_offsets[strike], _expiry)
            all_rows.extend(rows)
            logger.debug("  Got %d candles for %s %s", len(parsed), strike, option_type)
            time.sleep(API_DELAY_SECONDS)

    if empty_count == total_calls:
        logger.error(
            "ALL %d API calls returned 0 candles. "
            "Likely causes: "
            "(1) DHAN_DYNAMIC_ACCESS expired (24h validity — TOTP will auto-regenerate), "
            "(2) expiryCode=%d does not match the target expiry %s, "
            "(3) date range %s to %s has no traded data for this contract.",
            total_calls, expiry_code, _expiry, _from, _to,
        )
    elif empty_count > 0:
        logger.warning("%d/%d API calls returned 0 candles", empty_count, total_calls)

    logger.info("Fetched %d strikes x %d types: %d candles (%d empty calls)",
                 len(STRIKES), len(OPTION_TYPES), len(all_rows), empty_count)
    return all_rows


def fetch_iv_baseline(dhan=None, baseline_from=None, baseline_to=None, expiry_flag=None):
    """Fetch daily ATM IV for IVR/IVP calculation.

    All params fall back to config.py globals when None.
    """
    if dhan is None:
        dhan = create_dhan_client()

    _from = baseline_from or IV_BASELINE_FROM
    _to = baseline_to or IV_BASELINE_TO
    _flag = expiry_flag or EXPIRY_FLAG

    logger.info("Fetching IV baseline (ATM CALL daily) %s to %s ...", _from, _to)

    from datetime import date as dt_date
    start = dt_date.fromisoformat(_from)
    end = dt_date.fromisoformat(_to)
    chunk_days = 30
    all_rows = []

    total_chunks = max(1, ((end - start).days + chunk_days - 1) // chunk_days)
    chunk_num = 0
    current = start
    while current < end:
        chunk_num += 1
        chunk_end = min(current + timedelta(days=chunk_days), end)
        if chunk_num == 1 or chunk_num % 4 == 0 or chunk_num == total_chunks:
            logger.info("  IV progress: %d/%d chunks ...", chunk_num, total_chunks)
        logger.debug("[%d/%d] IV chunk %s to %s", chunk_num, total_chunks,
                      current.isoformat(), chunk_end.isoformat())
        response_data = fetch_with_retry(
            dhan,
            security_id=NIFTY_SECURITY_ID,
            exchange_segment=EXCHANGE_SEGMENT,
            instrument_type=INSTRUMENT_TYPE,
            expiry_flag=_flag,
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

        logger.debug("  IV chunk %s to %s: %d rows",
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
