# tests/test_calculator.py
import pytest
from DhanHQ_src.calculator import (
    compute_derived_metrics,
    compute_aggregate_metrics,
    compute_max_pain,
    compute_ivr_ivp,
    compute_fair_price,
)


def make_raw_data():
    """Two timestamps, one strike (23200), CE and PE each."""
    return [
        # t=0, CE
        {"timestamp": "2026-03-16 09:15:00", "date": "2026-03-16", "strike": 23200,
         "option_type": "CE", "close": 550.0, "volume": 357, "oi": 3594, "iv": 25.73, "spot": 23241.0},
        # t=0, PE
        {"timestamp": "2026-03-16 09:15:00", "date": "2026-03-16", "strike": 23200,
         "option_type": "PE", "close": 441.0, "volume": 1139, "oi": 20428, "iv": 25.50, "spot": 23241.0},
        # t=1, CE
        {"timestamp": "2026-03-16 09:16:00", "date": "2026-03-16", "strike": 23200,
         "option_type": "CE", "close": 540.0, "volume": 400, "oi": 3800, "iv": 25.50, "spot": 23230.0},
        # t=1, PE
        {"timestamp": "2026-03-16 09:16:00", "date": "2026-03-16", "strike": 23200,
         "option_type": "PE", "close": 450.0, "volume": 1200, "oi": 20600, "iv": 25.80, "spot": 23230.0},
    ]


def test_compute_derived_metrics_count():
    raw = make_raw_data()
    result = compute_derived_metrics(raw)
    assert len(result) == 2


def test_compute_derived_metrics_first_row_no_change():
    raw = make_raw_data()
    result = compute_derived_metrics(raw)
    first = result[0]
    assert first["ce_ltp"] == 550.0
    assert first["pe_ltp"] == 441.0
    assert first["ce_ltp_chg"] == 0.0
    assert first["pe_ltp_chg"] == 0.0


def test_compute_derived_metrics_second_row_changes():
    raw = make_raw_data()
    result = compute_derived_metrics(raw)
    second = result[1]
    assert second["ce_ltp"] == 540.0
    assert second["ce_ltp_chg"] == pytest.approx(-10.0)
    assert second["pe_ltp_chg"] == pytest.approx(9.0)
    assert second["ce_oi_chg"] == 206
    assert second["pe_oi_chg"] == 172
    assert second["ce_iv_chg"] == pytest.approx(-0.23)
    assert second["pe_iv_chg"] == pytest.approx(0.30)


def test_compute_derived_metrics_pe_ce_oi():
    raw = make_raw_data()
    result = compute_derived_metrics(raw)
    first = result[0]
    assert first["pe_ce_oi"] == 20428 - 3594


def test_compute_derived_metrics_pcr():
    raw = make_raw_data()
    result = compute_derived_metrics(raw)
    first = result[0]
    assert first["pcr_oi"] == pytest.approx(20428 / 3594, rel=1e-3)
    assert first["pcr_vol"] == pytest.approx(1139 / 357, rel=1e-3)


def test_compute_max_pain():
    strike_oi = [
        {"strike": 23100, "call_oi": 5000, "put_oi": 15000},
        {"strike": 23200, "call_oi": 10000, "put_oi": 10000},
        {"strike": 23300, "call_oi": 15000, "put_oi": 5000},
    ]
    result = compute_max_pain(strike_oi)
    assert result == 23200


def test_compute_ivr_ivp():
    historical_iv = [15.0, 18.0, 20.0, 22.0, 25.0, 28.0, 30.0, 16.0, 19.0, 21.0]
    current_iv = 25.0
    ivr, ivp = compute_ivr_ivp(current_iv, historical_iv)
    assert ivr == pytest.approx(66.67, rel=1e-2)
    assert ivp == pytest.approx(70.0, rel=1e-2)


def test_compute_fair_price():
    price = compute_fair_price(
        spot=23250.0, strike=23250, days_to_expiry=14,
        iv=25.0, risk_free_rate=0.065, option_type="CE"
    )
    assert price > 0
    assert price < 23250
