from dataclasses import dataclass
from typing import List

from apps.worker.data.csv_loader import Candle


@dataclass
class StrategySignal:
    side: str  # BUY/SELL/NO_TRADE
    confidence: float


class BaseStrategy:
    name = "base"

    def on_bar(self, idx: int, candles: List[Candle]) -> StrategySignal:
        raise NotImplementedError


class SmaCrossStrategy(BaseStrategy):
    name = "sma_cross"

    def __init__(self, fast: int = 10, slow: int = 30):
        self.fast = fast
        self.slow = slow

    def on_bar(self, idx: int, candles: List[Candle]) -> StrategySignal:
        if idx < self.slow:
            return StrategySignal("NO_TRADE", 0.0)

        closes = [c.close for c in candles[: idx + 1]]
        fast_ma = sum(closes[-self.fast :]) / self.fast
        slow_ma = sum(closes[-self.slow :]) / self.slow

        if fast_ma > slow_ma:
            return StrategySignal("BUY", 0.6)
        if fast_ma < slow_ma:
            return StrategySignal("SELL", 0.6)
        return StrategySignal("NO_TRADE", 0.0)


class MeanReversionStrategy(BaseStrategy):
    name = "mean_reversion"

    def __init__(self, window: int = 20, threshold: float = 1.5):
        self.window = window
        self.threshold = threshold

    def on_bar(self, idx: int, candles: List[Candle]) -> StrategySignal:
        if idx < self.window:
            return StrategySignal("NO_TRADE", 0.0)

        closes = [c.close for c in candles[: idx + 1]]
        window = closes[-self.window :]
        mean = sum(window) / self.window
        variance = sum((x - mean) ** 2 for x in window) / self.window
        std = variance ** 0.5
        if std == 0:
            return StrategySignal("NO_TRADE", 0.0)

        z = (closes[-1] - mean) / std
        if z <= -self.threshold:
            return StrategySignal("BUY", 0.55)
        if z >= self.threshold:
            return StrategySignal("SELL", 0.55)
        return StrategySignal("NO_TRADE", 0.0)


def get_strategy(name: str) -> BaseStrategy:
    if name == "sma_cross":
        return SmaCrossStrategy()
    if name == "mean_reversion":
        return MeanReversionStrategy()
    raise ValueError(f"Unknown strategy: {name}")
