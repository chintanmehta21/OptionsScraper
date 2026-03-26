"""Supabase test fixtures — connects to local Supabase stack (Docker)."""

import os
import subprocess
import pytest
import psycopg2
from supabase import create_client

# Local Supabase defaults (from `supabase start` output).
# Override with env vars if needed.
LOCAL_SUPABASE_URL = os.getenv("SUPABASE_TEST_URL", "http://localhost:54321")
LOCAL_DB_URL = os.getenv(
    "SUPABASE_TEST_DB_URL",
    "postgresql://postgres:postgres@localhost:54322/postgres",
)
LOCAL_ANON_KEY = os.getenv("SUPABASE_TEST_ANON_KEY", "")
LOCAL_SERVICE_ROLE_KEY = os.getenv("SUPABASE_TEST_SERVICE_ROLE_KEY", "")

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)


def _keys_configured():
    return bool(LOCAL_ANON_KEY) and bool(LOCAL_SERVICE_ROLE_KEY)


skip_no_supabase = pytest.mark.skipif(
    not _keys_configured(),
    reason="Supabase local keys not configured (set SUPABASE_TEST_ANON_KEY and SUPABASE_TEST_SERVICE_ROLE_KEY)",
)


@pytest.fixture(scope="session")
def supabase_reset():
    """Reset local Supabase DB before the test session."""
    if not _keys_configured():
        pytest.skip("Supabase local keys not configured")
    result = subprocess.run(
        ["npx", "supabase", "db", "reset"],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=PROJECT_ROOT,
        shell=True,
    )
    if result.returncode != 0:
        pytest.fail(f"supabase db reset failed:\n{result.stderr}\n{result.stdout}")
    yield


@pytest.fixture(scope="session")
def supa_service(supabase_reset):
    """Supabase client with service_role key — bypasses RLS."""
    return create_client(LOCAL_SUPABASE_URL, LOCAL_SERVICE_ROLE_KEY)


@pytest.fixture(scope="session")
def supa_anon(supabase_reset):
    """Supabase client with anon key — respects RLS."""
    return create_client(LOCAL_SUPABASE_URL, LOCAL_ANON_KEY)


@pytest.fixture
def pg_conn(supabase_reset):
    """Direct Postgres connection for schema introspection."""
    conn = psycopg2.connect(LOCAL_DB_URL)
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture
def seed_expiry(supa_service):
    """Insert a test expiry + 9 strikes. Returns (expiry_id, strike_map).

    strike_map is {strike_price: strike_id}, e.g. {22800: 1, 22850: 2, ...}.
    Cleans up via CASCADE delete after the test.
    """
    resp = (
        supa_service.table("expiries")
        .upsert(
            {
                "symbol": "NIFTY",
                "expiry_date": "2026-03-30",
                "expiry_flag": "MONTH",
                "lot_size": 65,
            },
            on_conflict="symbol,expiry_date",
        )
        .execute()
    )
    expiry_id = resp.data[0]["id"]

    strikes = []
    for offset in range(-4, 5):
        strikes.append(
            {
                "expiry_id": expiry_id,
                "strike": 23000 + (offset * 50),
                "atm_offset": offset,
            }
        )
    resp = (
        supa_service.table("strikes")
        .upsert(strikes, on_conflict="expiry_id,strike")
        .execute()
    )
    strike_map = {row["strike"]: row["id"] for row in resp.data}

    yield expiry_id, strike_map

    # Cleanup: CASCADE delete removes strikes + all downstream rows
    supa_service.table("expiries").delete().eq("id", expiry_id).execute()


@pytest.fixture
def seed_candles(supa_service, seed_expiry):
    """Insert sample raw candles for testing queries. Returns inserted rows."""
    expiry_id, strike_map = seed_expiry
    atm_strike_id = strike_map[23000]

    candles = [
        {
            "strike_id": atm_strike_id,
            "option_type": "CE",
            "timestamp": "2026-03-16T09:15:00+05:30",
            "open": 150.0,
            "high": 155.0,
            "low": 148.0,
            "close": 153.0,
            "volume": 1000,
            "oi": 50000,
            "iv": 18.5,
            "spot": 23000.0,
        },
        {
            "strike_id": atm_strike_id,
            "option_type": "PE",
            "timestamp": "2026-03-16T09:15:00+05:30",
            "open": 140.0,
            "high": 145.0,
            "low": 138.0,
            "close": 142.0,
            "volume": 800,
            "oi": 45000,
            "iv": 19.0,
            "spot": 23000.0,
        },
        {
            "strike_id": atm_strike_id,
            "option_type": "CE",
            "timestamp": "2026-03-16T09:16:00+05:30",
            "open": 153.0,
            "high": 158.0,
            "low": 151.0,
            "close": 156.0,
            "volume": 1200,
            "oi": 51000,
            "iv": 18.8,
            "spot": 23010.0,
        },
        {
            "strike_id": atm_strike_id,
            "option_type": "PE",
            "timestamp": "2026-03-16T09:16:00+05:30",
            "open": 142.0,
            "high": 143.0,
            "low": 139.0,
            "close": 140.0,
            "volume": 900,
            "oi": 45500,
            "iv": 19.2,
            "spot": 23010.0,
        },
    ]

    resp = (
        supa_service.table("raw_candles")
        .upsert(candles, on_conflict="strike_id,option_type,timestamp")
        .execute()
    )
    yield resp.data, expiry_id, strike_map
