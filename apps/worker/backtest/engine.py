from dataclasses import dataclass
from typing import List

from apps.worker.data.csv_loader import Candle
from apps.worker.backtest.strategies import BaseStrategy


@dataclass
class Trade:
    entry_price: float
    exit_price: float
    side: str
    pnl: float


@dataclass
class BacktestMetrics:
    win_rate: float
    profit_factor: float
    max_drawdown: float
    total_return: float


def run_backtest(
    candles: List[Candle],
    strategy: BaseStrategy,
    initial_cash: float = 100000.0,
) -> tuple[BacktestMetrics, List[Trade]]:
    position = 0
    entry = 0.0
    cash = initial_cash
    equity_curve = [initial_cash]
    trades: List[Trade] = []

    for i, bar in enumerate(candles):
        signal = strategy.on_bar(i, candles)

        if signal.side == "BUY" and position == 0:
            position = 1
            entry = bar.close
        elif signal.side == "SELL" and position == 1:
            pnl = bar.close - entry
            cash += pnl
            trades.append(Trade(entry_price=entry, exit_price=bar.close, side="LONG", pnl=pnl))
            position = 0
            entry = 0.0

        equity = cash + (bar.close - entry if position == 1 else 0)
        equity_curve.append(equity)

    wins = [t for t in trades if t.pnl > 0]
    losses = [t for t in trades if t.pnl < 0]
    win_rate = (len(wins) / len(trades)) if trades else 0.0
    gross_profit = sum(t.pnl for t in wins)
    gross_loss = abs(sum(t.pnl for t in losses))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else 0.0

    peak = equity_curve[0]
    max_dd = 0.0
    for e in equity_curve:
        if e > peak:
            peak = e
        dd = (peak - e) / peak if peak else 0
        if dd > max_dd:
            max_dd = dd

    total_return = (equity_curve[-1] - initial_cash) / initial_cash if initial_cash else 0.0

    return BacktestMetrics(
        win_rate=win_rate,
        profit_factor=profit_factor,
        max_drawdown=max_dd,
        total_return=total_return,
    ), trades
