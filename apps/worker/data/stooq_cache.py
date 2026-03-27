from datetime import datetime
from pathlib import Path
import os

from apps.worker.providers.stooq import download_csv
from apps.worker.providers.alpha_vantage_intraday import download_equity_intraday_csv, download_fx_intraday_csv
from apps.worker.providers.binance import download_klines_csv


def cache_dir() -> Path:
    base = Path(__file__).resolve().parents[3]
    d = base / "data" / "markets"
    d.mkdir(parents=True, exist_ok=True)
    return d


def save_symbol_csv(
    symbol: str,
    interval: str = "d",
    start: datetime | None = None,
    end: datetime | None = None,
    provider: str = "stooq",
    asset: str = "stock",
    month: str | None = None,
    use_cache: bool = True,
) -> Path:
    out = cache_dir() / f"{symbol}_{interval}.csv"
    if use_cache and out.exists():
        return out

    if provider == "stooq":
        csv_text = download_csv(symbol, start=start, end=end, interval=interval)
    elif provider == "alpha_vantage":
        api_key = os.getenv("ALPHAVANTAGE_API_KEY")
        if asset == "fx":
            from_symbol, to_symbol = symbol[:3].upper(), symbol[3:].upper()
            csv_text = download_fx_intraday_csv(from_symbol, to_symbol, interval=interval, api_key=api_key)
        else:
            csv_text = download_equity_intraday_csv(symbol.upper(), interval=interval, month=month, api_key=api_key)
    elif provider == "binance":
        csv_text = download_klines_csv(symbol, interval=interval, start=start, end=end)
    else:
        raise ValueError(f"Unknown provider: {provider}")

    out.write_text(csv_text, encoding="utf-8")
    return out
