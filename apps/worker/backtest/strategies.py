from dataclasses import dataclass
from typing import List, Dict

from apps.worker.data.csv_loader import Candle


@dataclass
class StrategySignal:
    side: str  # BUY/SELL/NO_TRADE
    confidence: float


class BaseStrategy:
    name = "base"

    def on_bar(self, idx: int, candles: List[Candle]) -> StrategySignal:
        raise NotImplementedError


def sma(values: List[float], window: int) -> float:
    if len(values) < window:
        return 0.0
    return sum(values[-window:]) / window


def ema(values: List[float], window: int) -> float:
    if len(values) < window:
        return 0.0
    k = 2 / (window + 1)
    e = values[-window]
    for v in values[-window + 1 :]:
        e = v * k + e * (1 - k)
    return e


def rsi(values: List[float], window: int) -> float:
    if len(values) < window + 1:
        return 50.0
    gains = 0.0
    losses = 0.0
    for i in range(-window, 0):
        diff = values[i] - values[i - 1]
        if diff >= 0:
            gains += diff
        else:
            losses -= diff
    if losses == 0:
        return 100.0
    rs = gains / losses if losses else 0.0
    return 100 - (100 / (1 + rs))


def bollinger(values: List[float], window: int, mult: float) -> tuple[float, float, float]:
    if len(values) < window:
        return 0.0, 0.0, 0.0
    w = values[-window:]
    mean = sum(w) / window
    variance = sum((x - mean) ** 2 for x in w) / window
    std = variance ** 0.5
    upper = mean + mult * std
    lower = mean - mult * std
    return lower, mean, upper


def donchian(values: List[float], window: int) -> tuple[float, float]:
    if len(values) < window:
        return 0.0, 0.0
    w = values[-window:]
    return min(w), max(w)


def roc(values: List[float], window: int) -> float:
    if len(values) < window + 1:
        return 0.0
    return (values[-1] - values[-window - 1]) / values[-window - 1]


class SmaCrossStrategy(BaseStrategy):
    def __init__(self, fast: int, slow: int):
        self.fast = fast
        self.slow = slow
        self.name = f"sma_cross_{fast}_{slow}"

    def on_bar(self, idx: int, candles: List[Candle]) -> StrategySignal:
        closes = [c.close for c in candles[: idx + 1]]
        if len(closes) < self.slow:
            return StrategySignal("NO_TRADE", 0.0)
        fast_ma = sma(closes, self.fast)
        slow_ma = sma(closes, self.slow)
        if fast_ma > slow_ma:
            return StrategySignal("BUY", 0.6)
        if fast_ma < slow_ma:
            return StrategySignal("SELL", 0.6)
        return StrategySignal("NO_TRADE", 0.0)


class EmaCrossStrategy(BaseStrategy):
    def __init__(self, fast: int, slow: int):
        self.fast = fast
        self.slow = slow
        self.name = f"ema_cross_{fast}_{slow}"

    def on_bar(self, idx: int, candles: List[Candle]) -> StrategySignal:
        closes = [c.close for c in candles[: idx + 1]]
        if len(closes) < self.slow:
            return StrategySignal("NO_TRADE", 0.0)
        fast_ma = ema(closes, self.fast)
        slow_ma = ema(closes, self.slow)
        if fast_ma > slow_ma:
            return StrategySignal("BUY", 0.6)
        if fast_ma < slow_ma:
            return StrategySignal("SELL", 0.6)
        return StrategySignal("NO_TRADE", 0.0)


class RsiReversionStrategy(BaseStrategy):
    def __init__(self, window: int, overbought: int, oversold: int):
        self.window = window
        self.overbought = overbought
        self.oversold = oversold
        self.name = f"rsi_revert_{window}_{overbought}_{oversold}"

    def on_bar(self, idx: int, candles: List[Candle]) -> StrategySignal:
        closes = [c.close for c in candles[: idx + 1]]
        val = rsi(closes, self.window)
        if val >= self.overbought:
            return StrategySignal("SELL", 0.55)
        if val <= self.oversold:
            return StrategySignal("BUY", 0.55)
        return StrategySignal("NO_TRADE", 0.0)


class BollingerReversionStrategy(BaseStrategy):
    def __init__(self, window: int, mult: float):
        self.window = window
        self.mult = mult
        self.name = f"bollinger_revert_{window}_{mult}"

    def on_bar(self, idx: int, candles: List[Candle]) -> StrategySignal:
        closes = [c.close for c in candles[: idx + 1]]
        lower, mean, upper = bollinger(closes, self.window, self.mult)
        if closes[-1] <= lower:
            return StrategySignal("BUY", 0.55)
        if closes[-1] >= upper:
            return StrategySignal("SELL", 0.55)
        return StrategySignal("NO_TRADE", 0.0)


class DonchianBreakoutStrategy(BaseStrategy):
    def __init__(self, window: int):
        self.window = window
        self.name = f"donchian_breakout_{window}"

    def on_bar(self, idx: int, candles: List[Candle]) -> StrategySignal:
        closes = [c.close for c in candles[: idx + 1]]
        low, high = donchian(closes, self.window)
        if closes[-1] >= high:
            return StrategySignal("BUY", 0.6)
        if closes[-1] <= low:
            return StrategySignal("SELL", 0.6)
        return StrategySignal("NO_TRADE", 0.0)


class MomentumStrategy(BaseStrategy):
    def __init__(self, window: int):
        self.window = window
        self.name = f"momentum_{window}"

    def on_bar(self, idx: int, candles: List[Candle]) -> StrategySignal:
        closes = [c.close for c in candles[: idx + 1]]
        v = roc(closes, self.window)
        if v > 0:
            return StrategySignal("BUY", 0.6)
        if v < 0:
            return StrategySignal("SELL", 0.6)
        return StrategySignal("NO_TRADE", 0.0)


def get_all_strategies() -> List[BaseStrategy]:
    return [
        SmaCrossStrategy(5, 20),
        SmaCrossStrategy(10, 30),
        SmaCrossStrategy(20, 50),
        EmaCrossStrategy(9, 21),
        EmaCrossStrategy(12, 26),
        RsiReversionStrategy(14, 70, 30),
        RsiReversionStrategy(7, 80, 20),
        BollingerReversionStrategy(20, 2.0),
        DonchianBreakoutStrategy(20),
        MomentumStrategy(10),
        MomentumStrategy(20),
        BollingerReversionStrategy(10, 2.5),
    ]


def get_strategy(name: str) -> BaseStrategy:
    for s in get_all_strategies():
        if s.name == name:
            return s
    raise ValueError(f"Unknown strategy: {name}")
