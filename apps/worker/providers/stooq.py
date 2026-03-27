import requests
from datetime import datetime
from typing import Optional

BASE_URL = "https://stooq.com/q/d/l/"


def _fmt_date(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def download_csv(
    symbol: str,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    interval: str = "d",
) -> str:
    params = {"s": symbol, "i": interval}
    if start:
        params["d1"] = _fmt_date(start)
    if end:
        params["d2"] = _fmt_date(end)

    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.text
