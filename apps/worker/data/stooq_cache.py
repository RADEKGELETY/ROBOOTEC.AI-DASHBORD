from datetime import datetime
from pathlib import Path

from apps.worker.providers.stooq import download_csv


def cache_dir() -> Path:
    base = Path(__file__).resolve().parents[3]
    d = base / "data" / "markets"
    d.mkdir(parents=True, exist_ok=True)
    return d


def save_symbol_csv(symbol: str, interval: str = "d", start: datetime | None = None, end: datetime | None = None) -> Path:
    csv_text = download_csv(symbol, start=start, end=end, interval=interval)
    out = cache_dir() / f"{symbol}_{interval}.csv"
    out.write_text(csv_text, encoding="utf-8")
    return out
