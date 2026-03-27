import os
import requests
from typing import Iterable

BASE_URL = "https://data.alpaca.markets"


def _auth_headers() -> dict:
    key = os.getenv("ALPACA_API_KEY_ID")
    secret = os.getenv("ALPACA_API_SECRET_KEY")
    if not key or not secret:
        raise RuntimeError("Missing ALPACA_API_KEY_ID / ALPACA_API_SECRET_KEY env vars")
    return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}


def fetch_stock_bars(
    symbols: Iterable[str],
    start: str,
    end: str,
    timeframe: str = "1Day",
    feed: str = "iex",
    limit: int = 1000,
) -> dict:
    url = f"{BASE_URL}/v2/stocks/bars"
    params = {
        "symbols": ",".join(symbols),
        "start": start,
        "end": end,
        "timeframe": timeframe,
        "limit": limit,
        "feed": feed,
    }
    resp = requests.get(url, headers=_auth_headers(), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_crypto_orderbooks(symbols: Iterable[str]) -> dict:
    url = f"{BASE_URL}/v1beta3/crypto/us/latest/orderbooks"
    params = {"symbols": ",".join(symbols)}

    headers = {}
    key = os.getenv("ALPACA_API_KEY_ID")
    secret = os.getenv("ALPACA_API_SECRET_KEY")
    if key and secret:
        headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()
