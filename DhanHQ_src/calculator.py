# DhanHQ_src/calculator.py
import math
import logging
from collections import defaultdict
from scipy.stats import norm

from DhanHQ_src.config import LOT_SIZE, RISK_FREE_RATE

logger = logging.getLogger(__name__)


def compute_derived_metrics(raw_rows):
    """Compute per-strike derived metrics from raw option data.

    Groups by (timestamp, strike), pairs CE+PE, computes changes from previous candle.
    Returns list of derived_metrics row dicts.
    """
    # Group by (timestamp, strike) then by option_type
    grouped = defaultdict(dict)
    for row in raw_rows:
        key = (row["timestamp"], row["strike"])
        grouped[key][row["option_type"]] = row

    # Sort by timestamp then strike
    sorted_keys = sorted(grouped.keys())

    # Track previous values per strike for change calculations
    prev_by_strike = {}
    results = []

    for ts, strike in sorted_keys:
        data = grouped[(ts, strike)]
        ce = data.get("CE", {})
        pe = data.get("PE", {})

        ce_ltp = ce.get("close", 0) or 0
        pe_ltp = pe.get("close", 0) or 0
        ce_vol = ce.get("volume", 0) or 0
        pe_vol = pe.get("volume", 0) or 0
        ce_oi = ce.get("oi", 0) or 0
        pe_oi = pe.get("oi", 0) or 0
        ce_iv = ce.get("iv", 0) or 0
        pe_iv = pe.get("iv", 0) or 0

        prev = prev_by_strike.get(strike)

        ce_ltp_chg = (ce_ltp - prev["ce_ltp"]) if prev else 0.0
        pe_ltp_chg = (pe_ltp - prev["pe_ltp"]) if prev else 0.0
        pe_ltp_chg_pct = (pe_ltp_chg / prev["pe_ltp"] * 100) if prev and prev["pe_ltp"] else 0.0
        ce_oi_chg = (ce_oi - prev["ce_oi"]) if prev else 0
        pe_oi_chg = (pe_oi - prev["pe_oi"]) if prev else 0
        ce_iv_chg = (ce_iv - prev["ce_iv"]) if prev else 0.0
        pe_iv_chg = (pe_iv - prev["pe_iv"]) if prev else 0.0

        pe_ce_oi = pe_oi - ce_oi
        prev_pe_ce_oi = (prev["pe_oi"] - prev["ce_oi"]) if prev else pe_ce_oi
        pe_ce_oi_chg = pe_ce_oi - prev_pe_ce_oi if prev else 0

        pcr_oi = pe_oi / ce_oi if ce_oi > 0 else None
        prev_pcr_oi = prev.get("pcr_oi") if prev else pcr_oi
        pcr_oi_chg = (pcr_oi - prev_pcr_oi) if pcr_oi is not None and prev_pcr_oi is not None else 0.0
        pcr_vol = pe_vol / ce_vol if ce_vol > 0 else None

        row = {
            "timestamp": ts,
            "strike": strike,
            "ce_ltp": ce_ltp,
            "pe_ltp": pe_ltp,
            "ce_ltp_chg": round(ce_ltp_chg, 2),
            "pe_ltp_chg": round(pe_ltp_chg, 2),
            "pe_ltp_chg_pct": round(pe_ltp_chg_pct, 2),
            "ce_volume": ce_vol,
            "pe_volume": pe_vol,
            "ce_oi": ce_oi,
            "pe_oi": pe_oi,
            "ce_oi_chg": ce_oi_chg,
            "pe_oi_chg": pe_oi_chg,
            "ce_iv": ce_iv,
            "pe_iv": pe_iv,
            "ce_iv_chg": round(ce_iv_chg, 2),
            "pe_iv_chg": round(pe_iv_chg, 2),
            "pe_ce_oi": pe_ce_oi,
            "pe_ce_oi_chg": pe_ce_oi_chg,
            "pcr_oi": round(pcr_oi, 4) if pcr_oi is not None else None,
            "pcr_oi_chg": round(pcr_oi_chg, 4) if pcr_oi_chg is not None else None,
            "pcr_vol": round(pcr_vol, 4) if pcr_vol is not None else None,
        }
        results.append(row)

        prev_by_strike[strike] = {
            "ce_ltp": ce_ltp, "pe_ltp": pe_ltp,
            "ce_oi": ce_oi, "pe_oi": pe_oi,
            "ce_iv": ce_iv, "pe_iv": pe_iv,
            "pcr_oi": pcr_oi,
        }

    return results


def compute_max_pain(strike_oi_list):
    """Compute Max Pain strike.

    Args:
        strike_oi_list: list of dicts with {strike, call_oi, put_oi}
    Returns:
        The strike price where total option buyer payout is minimized.
    """
    strikes = [s["strike"] for s in strike_oi_list]
    min_pain = float("inf")
    max_pain_strike = strikes[0] if strikes else 0

    for expiry_price in strikes:
        total_pain = 0
        for item in strike_oi_list:
            call_intrinsic = max(0, expiry_price - item["strike"])
            total_pain += call_intrinsic * item["call_oi"]
            put_intrinsic = max(0, item["strike"] - expiry_price)
            total_pain += put_intrinsic * item["put_oi"]
        if total_pain < min_pain:
            min_pain = total_pain
            max_pain_strike = expiry_price

    return max_pain_strike


def compute_ivr_ivp(current_iv, historical_iv_list):
    """Compute IV Rank and IV Percentile.

    Args:
        current_iv: current ATM IV value
        historical_iv_list: list of historical IV floats (e.g., 252 daily values)
    Returns:
        (ivr, ivp) tuple
    """
    if not historical_iv_list:
        return 0.0, 0.0

    iv_high = max(historical_iv_list)
    iv_low = min(historical_iv_list)

    if iv_high == iv_low:
        ivr = 50.0
    else:
        ivr = ((current_iv - iv_low) / (iv_high - iv_low)) * 100

    days_below = sum(1 for iv in historical_iv_list if iv < current_iv)
    ivp = (days_below / len(historical_iv_list)) * 100

    return round(ivr, 2), round(ivp, 2)


def compute_fair_price(spot, strike, days_to_expiry, iv, risk_free_rate=RISK_FREE_RATE, option_type="CE"):
    """Black-Scholes option price.

    Args:
        spot: underlying price
        strike: strike price
        days_to_expiry: calendar days to expiry
        iv: implied volatility as percentage (e.g., 25.0 for 25%)
        risk_free_rate: annual risk-free rate (e.g., 0.065)
        option_type: "CE" for call, "PE" for put
    Returns:
        Theoretical option price
    """
    if days_to_expiry <= 0 or iv <= 0 or spot <= 0 or strike <= 0:
        return 0.0

    T = days_to_expiry / 365.0
    sigma = iv / 100.0  # convert percentage to decimal
    S, K, r = spot, strike, risk_free_rate

    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)

    if option_type == "CE":
        price = S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    else:
        price = K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

    return round(price, 2)


def compute_aggregate_metrics(derived_rows, spot, prev_spot_close, iv_history, expiry_date_str,
                               lot_size=None):
    """Compute aggregate metrics for a single timestamp.

    Args:
        derived_rows: list of derived_metrics dicts for ONE timestamp (all strikes)
        spot: current spot price
        prev_spot_close: previous day's closing spot (for spot_chg)
        iv_history: list of historical IV floats for IVR/IVP
        expiry_date_str: expiry date string "YYYY-MM-DD"
    Returns:
        Single aggregate_metrics row dict
    """
    from datetime import date as dt_date

    ts = derived_rows[0]["timestamp"] if derived_rows else ""
    total_ce_oi = sum(r["ce_oi"] for r in derived_rows)
    total_pe_oi = sum(r["pe_oi"] for r in derived_rows)
    total_ce_vol = sum(r["ce_volume"] for r in derived_rows)
    total_pe_vol = sum(r["pe_volume"] for r in derived_rows)
    total_ce_oi_chg = sum(r["ce_oi_chg"] for r in derived_rows)
    total_pe_oi_chg = sum(r["pe_oi_chg"] for r in derived_rows)

    # OTM/ITM split: CE is OTM when strike > spot, ITM when strike < spot
    otm_ce_oi = sum(r["ce_oi"] for r in derived_rows if r["strike"] > spot)
    itm_ce_oi = sum(r["ce_oi"] for r in derived_rows if r["strike"] <= spot)
    otm_pe_oi = sum(r["pe_oi"] for r in derived_rows if r["strike"] < spot)
    itm_pe_oi = sum(r["pe_oi"] for r in derived_rows if r["strike"] >= spot)

    otm_ce_oi_chg = sum(r["ce_oi_chg"] for r in derived_rows if r["strike"] > spot)
    itm_ce_oi_chg = sum(r["ce_oi_chg"] for r in derived_rows if r["strike"] <= spot)
    otm_pe_oi_chg = sum(r["pe_oi_chg"] for r in derived_rows if r["strike"] < spot)
    itm_pe_oi_chg = sum(r["pe_oi_chg"] for r in derived_rows if r["strike"] >= spot)

    # Bullish/Bearish OI
    bullish_oi = 0
    bearish_oi = 0
    for r in derived_rows:
        if r["ce_oi_chg"] > 0:
            bullish_oi += r["ce_oi_chg"]
        if r["ce_oi_chg"] < 0:
            bearish_oi += abs(r["ce_oi_chg"])
        if r["pe_oi_chg"] > 0:
            bearish_oi += r["pe_oi_chg"]
        if r["pe_oi_chg"] < 0:
            bullish_oi += abs(r["pe_oi_chg"])

    # Max Pain
    strike_oi = [
        {"strike": r["strike"], "call_oi": r["ce_oi"], "put_oi": r["pe_oi"]}
        for r in derived_rows
    ]
    max_pain = compute_max_pain(strike_oi) if strike_oi else 0

    # ATM IV (closest strike to spot)
    atm_row = min(derived_rows, key=lambda r: abs(r["strike"] - spot)) if derived_rows else None
    atm_iv = atm_row["ce_iv"] if atm_row else 0

    # IVR/IVP
    ivr, ivp = compute_ivr_ivp(atm_iv, iv_history) if iv_history else (0.0, 0.0)

    # Fair Price (using ATM call)
    expiry = dt_date.fromisoformat(expiry_date_str)
    current_date_str = ts[:10] if ts else expiry_date_str
    current = dt_date.fromisoformat(current_date_str)
    days_to_expiry = (expiry - current).days
    atm_strike = atm_row["strike"] if atm_row else int(spot)
    fair_price = compute_fair_price(spot, atm_strike, days_to_expiry, atm_iv) if atm_iv > 0 else 0

    overall_pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0
    spot_chg = spot - prev_spot_close if prev_spot_close else 0
    spot_chg_pct = (spot_chg / prev_spot_close * 100) if prev_spot_close else 0

    return {
        "timestamp": ts,
        "spot": spot,
        "spot_chg": round(spot_chg, 2),
        "spot_chg_pct": round(spot_chg_pct, 2),
        "fair_price": fair_price,
        "fair_price_chg": 0.0,
        "atm_iv": atm_iv,
        "ivr": ivr,
        "ivp": ivp,
        "max_pain": max_pain,
        "overall_pcr": round(overall_pcr, 4),
        "lot_size": lot_size if lot_size is not None else LOT_SIZE,
        "total_ce_oi": total_ce_oi,
        "total_pe_oi": total_pe_oi,
        "total_oi_net": total_pe_oi - total_ce_oi,
        "total_ce_oi_chg": total_ce_oi_chg,
        "total_pe_oi_chg": total_pe_oi_chg,
        "total_oi_chg_net": total_pe_oi_chg - total_ce_oi_chg,
        "total_bullish_oi": bullish_oi,
        "total_bearish_oi": bearish_oi,
        "otm_ce_oi": otm_ce_oi,
        "otm_pe_oi": otm_pe_oi,
        "otm_oi_net": otm_pe_oi - otm_ce_oi,
        "otm_ce_oi_chg": otm_ce_oi_chg,
        "otm_pe_oi_chg": otm_pe_oi_chg,
        "otm_oi_chg_net": otm_pe_oi_chg - otm_ce_oi_chg,
        "itm_ce_oi": itm_ce_oi,
        "itm_pe_oi": itm_pe_oi,
        "itm_oi_net": itm_pe_oi - itm_ce_oi,
        "itm_ce_oi_chg": itm_ce_oi_chg,
        "itm_pe_oi_chg": itm_pe_oi_chg,
        "itm_oi_chg_net": itm_pe_oi_chg - itm_ce_oi_chg,
    }
