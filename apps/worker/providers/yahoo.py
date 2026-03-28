import csv
from datetime import datetime
from io import StringIO
from typing import Optional

import requests
import time

BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/"


def _to_unix(dt: datetime) -> int:
    return int(dt.timestamp())


def download_daily_csv(symbol: str, start: Optional[datetime] = None, end: Optional[datetime] = None) -> str:
    period1 = _to_unix(start) if start else 0
    period2 = _to_unix(end) if end else int(datetime.utcnow().timestamp())

    params = {
        "period1": period1,
        "period2": period2,
        "interval": "1d",
        "events": "div,splits",
        "includeAdjustedClose": "true",
    }

    headers = {"User-Agent": "Mozilla/5.0 (compatible; RoboOtec/1.0)"}
    resp = None
    for delay in [0, 2, 5]:
        if delay:
            time.sleep(delay)
        resp = requests.get(f"{BASE_URL}{symbol}", params=params, headers=headers, timeout=30)
        if resp.status_code != 429:
            break
    if resp is None:
        raise ValueError(f"No response for {symbol}")
    resp.raise_for_status()
    payload = resp.json()

    result = (payload.get("chart") or {}).get("result")
    if not result:
        raise ValueError(f"No data for {symbol}")

    result = result[0]
    timestamps = result.get("timestamp") or []
    quotes = (result.get("indicators") or {}).get("quote") or []
    if not quotes:
        raise ValueError(f"No quote data for {symbol}")

    quote = quotes[0]
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []

    out = StringIO()
    writer = csv.writer(out)
    writer.writerow(["Date", "Open", "High", "Low", "Close", "Volume"])

    for idx, ts in enumerate(timestamps):
        o = opens[idx] if idx < len(opens) else None
        h = highs[idx] if idx < len(highs) else None
        l = lows[idx] if idx < len(lows) else None
        c = closes[idx] if idx < len(closes) else None
        v = volumes[idx] if idx < len(volumes) else None
        if c is None:
            continue
        if o is None:
            o = c
        if h is None:
            h = c
        if l is None:
            l = c
        dt = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
        writer.writerow([dt, f"{o:.6f}", f"{h:.6f}", f"{l:.6f}", f"{c:.6f}", int(v or 0)])

    return out.getvalue()
