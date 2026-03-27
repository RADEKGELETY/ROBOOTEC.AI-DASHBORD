from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

from apps.worker.backtest.indicators import ema, rsi, roc
from apps.worker.data.csv_loader import Candle
from apps.worker.news.red_news import load_red_news


@dataclass
class FeatureBin:
    pos: int = 0
    total: int = 0


class HistoricalOutcomeFilter:
    def __init__(self, min_samples: int = 50, min_prob: float = 0.55, max_prob: float = 0.45, use_news: bool = True):
        self.min_samples = min_samples
        self.min_prob = min_prob
        self.max_prob = max_prob
        self.stats: Dict[Tuple[int, int, int, int], FeatureBin] = {}
        self.red_days = load_red_news() if use_news else set()

    def _bin(self, candles: List[Candle], idx: int) -> Tuple[int, int, int, int]:
        closes = [c.close for c in candles[: idx + 1]]
        r1 = roc(closes, 1)
        r5 = roc(closes, 5)
        rsi14 = rsi(closes, 14)
        trend = ema(closes, 12) - ema(closes, 26)

        def bucket(v: float, steps: List[float]) -> int:
            for i, s in enumerate(steps):
                if v < s:
                    return i
            return len(steps)

        b1 = bucket(r1, [-0.002, 0.0, 0.002])
        b2 = bucket(r5, [-0.005, 0.0, 0.005])
        b3 = bucket(rsi14, [35, 50, 65])
        b4 = bucket(trend, [-0.001, 0.0, 0.001])
        return b1, b2, b3, b4

    def update(self, candles: List[Candle], idx: int) -> None:
        if idx == 0 or idx >= len(candles):
            return
        prev_idx = idx - 1
        prev_close = candles[prev_idx].close
        next_close = candles[idx].close
        label_up = 1 if next_close > prev_close else 0

        key = self._bin(candles, prev_idx)
        entry = self.stats.get(key, FeatureBin())
        entry.total += 1
        entry.pos += label_up
        self.stats[key] = entry

    def allow(self, candles: List[Candle], idx: int, side: str) -> bool:
        if idx < 5:
            return True
        if self.red_days:
            bar_day = candles[idx].timestamp.date()
            if bar_day in self.red_days:
                return False
        key = self._bin(candles, idx)
        entry = self.stats.get(key)
        if not entry or entry.total < self.min_samples:
            return True
        prob_up = entry.pos / entry.total if entry.total else 0.5
        if side == "BUY" and prob_up >= self.min_prob:
            return True
        if side == "SELL" and prob_up <= self.max_prob:
            return True
        return False
