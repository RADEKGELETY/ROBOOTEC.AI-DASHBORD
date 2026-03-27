from dataclasses import dataclass
from typing import List, Dict

from apps.worker.data.csv_loader import Candle


@dataclass
class StrategySignal:
    side: str  # BUY/SELL/NO_TRADE
    confidence: float
    intent: str = "ENTER"  # ENTER/EXIT


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


class RgLiquiditySweepStrategy(BaseStrategy):
    def __init__(self, instrument: str):
        self.instrument = instrument
        self.name = f"rg_{instrument}"
        self.position = 0
        self.entry_price = 0.0
        self.entry_idx = None
        self.day = None
        self.day_start_idx = 0
        self.day_high = None
        self.day_low = None
        self.prev_day_high = None
        self.prev_day_low = None
        self.trades_today = 0
        self.losses_today = 0
        self.stop_price = None
        self.target_price = None
        self.break_even = False
        self.open_wait_bars = 12  # ~60 min on 5m
        self.max_hold_bars = 108  # ~9h on 5m

    def _in_ny_session(self, ts) -> bool:
        # NY session: 14:30-21:00 UTC (approx, no DST handling)
        if ts.hour == 0 and ts.minute == 0:
            return True
        minutes = ts.hour * 60 + ts.minute
        return 14 * 60 + 30 <= minutes <= 21 * 60

    def _update_day(self, idx: int, bar: Candle) -> None:
        bar_day = bar.timestamp.date()
        if self.day is None:
            self.day = bar_day
            self.day_start_idx = idx
            self.day_high = bar.high
            self.day_low = bar.low
            return
        if bar_day != self.day:
            self.prev_day_high = self.day_high
            self.prev_day_low = self.day_low
            self.day = bar_day
            self.day_start_idx = idx
            self.trades_today = 0
            self.losses_today = 0
            self.day_high = bar.high
            self.day_low = bar.low
        else:
            self.day_high = max(self.day_high or bar.high, bar.high)
            self.day_low = min(self.day_low or bar.low, bar.low)

    def _trend_dir(self, closes: List[float]) -> int:
        if len(closes) < 30:
            return 0
        fast = ema(closes, 12)
        slow = ema(closes, 26)
        if fast > slow:
            return 1
        if fast < slow:
            return -1
        return 0

    def on_bar(self, idx: int, candles: List[Candle]) -> StrategySignal:
        bar = candles[idx]
        self._update_day(idx, bar)

        intraday = not (bar.timestamp.hour == 0 and bar.timestamp.minute == 0)
        if intraday and not self._in_ny_session(bar.timestamp):
            return StrategySignal("NO_TRADE", 0.0)

        bars_since_open = idx - self.day_start_idx
        closes = [c.close for c in candles[: idx + 1]]
        trend = self._trend_dir(closes)

        # Manage open position
        if self.position != 0:
            hit_stop = False
            hit_target = False
            if self.position == 1:
                hit_stop = self.stop_price is not None and bar.low <= self.stop_price
                hit_target = self.target_price is not None and bar.high >= self.target_price
                if hit_stop and hit_target:
                    hit_target = False  # conservative
                if hit_stop or hit_target:
                    exit_price = self.stop_price if hit_stop else self.target_price
                    loss = exit_price <= self.entry_price
                    self.position = 0
                    self.entry_idx = None
                    self.stop_price = None
                    self.target_price = None
                    self.trades_today += 1
                    if loss:
                        self.losses_today += 1
                    return StrategySignal("SELL", 0.9, intent="EXIT")
            else:
                hit_stop = self.stop_price is not None and bar.high >= self.stop_price
                hit_target = self.target_price is not None and bar.low <= self.target_price
                if hit_stop and hit_target:
                    hit_target = False
                if hit_stop or hit_target:
                    exit_price = self.stop_price if hit_stop else self.target_price
                    loss = exit_price >= self.entry_price
                    self.position = 0
                    self.entry_idx = None
                    self.stop_price = None
                    self.target_price = None
                    self.trades_today += 1
                    if loss:
                        self.losses_today += 1
                    return StrategySignal("BUY", 0.9, intent="EXIT")

            if intraday and self.entry_idx is not None and (idx - self.entry_idx) >= self.max_hold_bars:
                if (self.position == 1 and bar.close > self.entry_price) or (
                    self.position == -1 and bar.close < self.entry_price
                ):
                    self.stop_price = self.entry_price
                    self.break_even = True
                else:
                    side = "SELL" if self.position == 1 else "BUY"
                    self.position = 0
                    self.entry_idx = None
                    self.stop_price = None
                    self.target_price = None
                    self.trades_today += 1
                    self.losses_today += 1
                    return StrategySignal(side, 0.7, intent="EXIT")

            if trend != 0 and ((self.position == 1 and trend < 0) or (self.position == -1 and trend > 0)):
                if (self.position == 1 and bar.close > self.entry_price) or (
                    self.position == -1 and bar.close < self.entry_price
                ):
                    self.stop_price = self.entry_price
                    self.break_even = True
                else:
                    side = "SELL" if self.position == 1 else "BUY"
                    self.position = 0
                    self.entry_idx = None
                    self.stop_price = None
                    self.target_price = None
                    self.trades_today += 1
                    self.losses_today += 1
                    return StrategySignal(side, 0.7, intent="EXIT")

            return StrategySignal("NO_TRADE", 0.0)

        # Entry conditions
        if self.trades_today >= 2 or self.losses_today >= 2:
            return StrategySignal("NO_TRADE", 0.0)
        if intraday and bars_since_open < self.open_wait_bars:
            return StrategySignal("NO_TRADE", 0.0)
        if self.prev_day_high is None or self.prev_day_low is None or trend == 0:
            return StrategySignal("NO_TRADE", 0.0)

        sweep_low = bar.low < self.prev_day_low and bar.close > self.prev_day_low
        sweep_high = bar.high > self.prev_day_high and bar.close < self.prev_day_high

        if trend > 0 and sweep_low:
            self.position = 1
            self.entry_price = bar.close
            self.entry_idx = idx
            self.stop_price = self.entry_price * (1 - 0.005)
            self.target_price = self.entry_price * (1 + 0.02)
            self.break_even = False
            return StrategySignal("BUY", 0.75)

        if trend < 0 and sweep_high:
            self.position = -1
            self.entry_price = bar.close
            self.entry_idx = idx
            self.stop_price = self.entry_price * (1 + 0.005)
            self.target_price = self.entry_price * (1 - 0.02)
            self.break_even = False
            return StrategySignal("SELL", 0.75)

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
        RgLiquiditySweepStrategy("nasdaq"),
        RgLiquiditySweepStrategy("gold"),
        RgLiquiditySweepStrategy("btc"),
        RgLiquiditySweepStrategy("eurusd"),
    ]


def get_strategy(name: str) -> BaseStrategy:
    for s in get_all_strategies():
        if s.name == name:
            return s
    raise ValueError(f"Unknown strategy: {name}")
