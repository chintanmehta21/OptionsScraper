# DhanHQ_src/auth.py
"""TOTP-based access token generation for DhanHQ API.

Generates a fresh 24h DHAN_DYNAMIC_ACCESS token using:
  - DHAN_CLIENT_ID
  - DHAN_PIN (6-digit account PIN)
  - DHAN_TOTP_SECRET (base32 secret from TOTP setup)

Falls back to static DHAN_ACCESS_TOKEN only when TOTP vars are absent.
If TOTP vars are present but auth fails, raises immediately (no silent fallback).
"""
import os
import time
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
    """Call DhanHQ auth endpoint with TOTP retry on failure.

    Tries twice: if the first TOTP code is rejected (window boundary),
    waits 31 seconds for the next TOTP window and retries once.
    """
    max_attempts = 2
    last_error = None

    for attempt in range(1, max_attempts + 1):
        totp_code = generate_totp(totp_secret)
        payload = {
            "dhanClientId": client_id,
            "pin": pin,
            "totp": totp_code,
        }
        logger.info("TOTP auth attempt %d/%d for client %s",
                     attempt, max_attempts, client_id)
        try:
            resp = requests.post(AUTH_URL, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            token = data.get("accessToken") or data.get("access_token")
            if not token:
                raise RuntimeError(
                    f"No token in auth response keys: {list(data.keys())}"
                )

            logger.info("DHAN_DYNAMIC_ACCESS generated successfully (expires: %s)",
                        data.get("tokenExpiry", "unknown"))
            return token

        except Exception as e:
            last_error = e
            logger.warning("TOTP attempt %d/%d failed: %s", attempt, max_attempts, e)
            if attempt < max_attempts:
                logger.info("Waiting 31s for next TOTP window...")
                time.sleep(31)

    raise RuntimeError(
        f"TOTP auth failed after {max_attempts} attempts: {last_error}"
    )


def get_access_token() -> str:
    """Get access token: TOTP if configured (required), else static fallback.

    If TOTP vars (DHAN_PIN + DHAN_TOTP_SECRET) are present, TOTP is REQUIRED.
    Failure raises immediately — no silent fallback to DHAN_ACCESS_TOKEN.
    Static DHAN_ACCESS_TOKEN is only used when TOTP vars are absent.
    """
    client_id = os.environ.get("DHAN_CLIENT_ID")
    pin = os.environ.get("DHAN_PIN")
    totp_secret = os.environ.get("DHAN_TOTP_SECRET")

    if client_id and pin and totp_secret:
        # TOTP path — required, no fallback
        return generate_access_token(client_id, pin, totp_secret)

    # Fallback to static token (only when TOTP vars absent)
    static_token = os.environ.get("DHAN_ACCESS_TOKEN")
    if static_token:
        logger.info("Using static DHAN_ACCESS_TOKEN (no TOTP configured)")
        return static_token

    raise RuntimeError(
        "No DhanHQ credentials found. Set either "
        "(DHAN_CLIENT_ID + DHAN_PIN + DHAN_TOTP_SECRET) for TOTP auth, "
        "or (DHAN_CLIENT_ID + DHAN_ACCESS_TOKEN) for static token."
    )
