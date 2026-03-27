from typing import List, Tuple


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


def bollinger(values: List[float], window: int, mult: float) -> Tuple[float, float, float]:
    if len(values) < window:
        return 0.0, 0.0, 0.0
    w = values[-window:]
    mean = sum(w) / window
    variance = sum((x - mean) ** 2 for x in w) / window
    std = variance ** 0.5
    upper = mean + mult * std
    lower = mean - mult * std
    return lower, mean, upper


def donchian(values: List[float], window: int) -> Tuple[float, float]:
    if len(values) < window:
        return 0.0, 0.0
    w = values[-window:]
    return min(w), max(w)


def roc(values: List[float], window: int) -> float:
    if len(values) < window + 1:
        return 0.0
    return (values[-1] - values[-window - 1]) / values[-window - 1]


def atr(highs: List[float], lows: List[float], closes: List[float], window: int) -> float:
    if len(closes) < window + 1:
        return 0.0
    trs = []
    for i in range(-window, 0):
        high = highs[i]
        low = lows[i]
        prev_close = closes[i - 1]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    return sum(trs) / window


def macd(values: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[float, float, float]:
    if len(values) < slow + signal:
        return 0.0, 0.0, 0.0
    macd_line = ema(values, fast) - ema(values, slow)
    # Build a short series for signal EMA
    macd_series = []
    for i in range(-(signal + 1), 0):
        sub = values[: i] if i != 0 else values
        if len(sub) < slow:
            continue
        macd_series.append(ema(sub, fast) - ema(sub, slow))
    if len(macd_series) < signal:
        signal_line = 0.0
    else:
        signal_line = ema(macd_series, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


def vwap(prices: List[float], volumes: List[float], window: int) -> float:
    if len(prices) < window:
        return 0.0
    p = prices[-window:]
    v = volumes[-window:]
    total = sum(v)
    if total == 0:
        return 0.0
    return sum(px * vol for px, vol in zip(p, v)) / total
