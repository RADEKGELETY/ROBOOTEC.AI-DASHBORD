from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Optional

import requests

BASE_URL = "https://data-api.binance.vision/api/v3/klines"


def _to_binance_symbol(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USD") and not s.endswith("USDT"):
        s = s[:-3] + "USDT"
    return s


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def download_klines(symbol: str, interval: str = "5m", start: Optional[datetime] = None, end: Optional[datetime] = None, limit: int = 1000) -> List[list]:
    params = {
        "symbol": _to_binance_symbol(symbol),
        "interval": interval,
        "limit": limit,
    }
    if start:
        params["startTime"] = _to_ms(start)
    if end:
        params["endTime"] = _to_ms(end)
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _interval_minutes(interval: str) -> int:
    if interval.endswith("m"):
        return int(interval[:-1])
    if interval.endswith("h"):
        return int(interval[:-1]) * 60
    if interval.endswith("d"):
        return int(interval[:-1]) * 60 * 24
    return 5


def download_klines_csv(symbol: str, interval: str = "5m", start: Optional[datetime] = None, end: Optional[datetime] = None) -> str:
    rows: List[List[str]] = [["date", "open", "high", "low", "close", "volume"]]

    if start is None or end is None:
        data = download_klines(symbol, interval=interval, start=start, end=end)
        for k in data:
            ts = datetime.utcfromtimestamp(k[0] / 1000).isoformat()
            rows.append([ts, k[1], k[2], k[3], k[4], k[5]])
    else:
        cur = start
        while cur < end:
            data = download_klines(symbol, interval=interval, start=cur, end=end)
            if not data:
                break
            for k in data:
                ts = datetime.utcfromtimestamp(k[0] / 1000).isoformat()
                rows.append([ts, k[1], k[2], k[3], k[4], k[5]])
            last_open = datetime.utcfromtimestamp(data[-1][0] / 1000)
            if last_open <= cur:
                break
            cur = last_open + timedelta(minutes=_interval_minutes(interval))

    return "\n".join([",".join(r) for r in rows])
