import csv
from dataclasses import dataclass
from datetime import datetime
from typing import List


@dataclass
class Candle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


def _parse_dt(value: str) -> datetime:
    # Supports YYYY-MM-DD or ISO
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return datetime.strptime(value, "%Y-%m-%d")


def load_ohlcv_csv(path: str) -> List[Candle]:
    candles: List[Candle] = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_l = {k.lower(): v for k, v in row.items()}
            candles.append(
                Candle(
                    timestamp=_parse_dt(row_l["date"]),
                    open=float(row_l["open"]),
                    high=float(row_l["high"]),
                    low=float(row_l["low"]),
                    close=float(row_l["close"]),
                    volume=float(row_l.get("volume", 0) or 0),
                )
            )
    return candles
