from __future__ import annotations

from typing import Optional

import requests

BASE_URL = "https://www.alphavantage.co/query"


def _require_key(api_key: Optional[str]) -> str:
    key = api_key or ""
    if not key:
        raise RuntimeError("Missing ALPHAVANTAGE_API_KEY")
    return key


def download_equity_intraday_csv(symbol: str, interval: str = "5min", month: str | None = None, api_key: str | None = None) -> str:
    key = _require_key(api_key)
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": interval,
        "datatype": "csv",
        "outputsize": "full",
        "apikey": key,
    }
    if month:
        params["month"] = month
    resp = requests.get(BASE_URL, params=params, timeout=60)
    resp.raise_for_status()
    return resp.text


def download_fx_intraday_csv(from_symbol: str, to_symbol: str, interval: str = "5min", api_key: str | None = None) -> str:
    key = _require_key(api_key)
    params = {
        "function": "FX_INTRADAY",
        "from_symbol": from_symbol,
        "to_symbol": to_symbol,
        "interval": interval,
        "datatype": "csv",
        "outputsize": "full",
        "apikey": key,
    }
    resp = requests.get(BASE_URL, params=params, timeout=60)
    resp.raise_for_status()
    return resp.text
