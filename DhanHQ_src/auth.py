# DhanHQ_src/auth.py
"""TOTP-based access token generation for DhanHQ API.

Generates a fresh 24h access token using:
  - DHAN_CLIENT_ID
  - DHAN_PIN (6-digit account PIN)
  - DHAN_TOTP_SECRET (base32 secret from TOTP setup)

Falls back to static DHAN_ACCESS_TOKEN if TOTP vars are missing.
"""
import os
import logging

import pyotp
import requests

logger = logging.getLogger(__name__)

AUTH_URL = "https://auth.dhan.co/app/generateAccessToken"


def generate_totp(secret: str) -> str:
    """Generate current 6-digit TOTP code from base32 secret."""
    totp = pyotp.TOTP(secret)
    return totp.now()


def generate_access_token(client_id: str, pin: str, totp_secret: str) -> str:
    """Call DhanHQ auth endpoint to get a fresh 24h access token."""
    totp_code = generate_totp(totp_secret)
    payload = {
        "dhanClientId": client_id,
        "pin": pin,
        "totp": totp_code,
    }
    logger.info("Generating fresh access token via TOTP for client %s", client_id)
    resp = requests.post(AUTH_URL, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    token = data.get("accessToken") or data.get("access_token")
    if not token:
        raise RuntimeError(f"No access token in auth response: {list(data.keys())}")

    logger.info("Access token generated successfully (expires: %s)",
                data.get("tokenExpiry", "unknown"))
    return token


def get_access_token() -> str:
    """Get access token: TOTP auto-generation if available, else static env var.

    Priority:
      1. TOTP-based generation (DHAN_PIN + DHAN_TOTP_SECRET present)
      2. Static DHAN_ACCESS_TOKEN env var (legacy fallback)
    """
    client_id = os.environ.get("DHAN_CLIENT_ID")
    pin = os.environ.get("DHAN_PIN")
    totp_secret = os.environ.get("DHAN_TOTP_SECRET")

    if client_id and pin and totp_secret:
        return generate_access_token(client_id, pin, totp_secret)

    # Fallback to static token
    static_token = os.environ.get("DHAN_ACCESS_TOKEN")
    if static_token:
        logger.info("Using static DHAN_ACCESS_TOKEN (no TOTP configured)")
        return static_token

    raise RuntimeError(
        "No DhanHQ credentials found. Set either "
        "(DHAN_CLIENT_ID + DHAN_PIN + DHAN_TOTP_SECRET) for TOTP auth, "
        "or (DHAN_CLIENT_ID + DHAN_ACCESS_TOKEN) for static token."
    )
