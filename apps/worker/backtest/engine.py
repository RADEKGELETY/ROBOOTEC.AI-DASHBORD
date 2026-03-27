from dataclasses import dataclass
from typing import List

from apps.worker.data.csv_loader import Candle
from apps.worker.backtest.strategies import BaseStrategy


@dataclass
class Trade:
    entry_price: float
    exit_price: float
    side: str  # LONG/SHORT
    pnl: float


@dataclass
class BacktestMetrics:
    win_rate: float
    profit_factor: float
    max_drawdown: float
    total_return: float
    trades: int
    wins: int
    losses: int
    avg_win: float
    avg_loss: float
    expectancy: float
    long_trades: int
    short_trades: int
    final_equity: float
    return_usd: float


def run_backtest(
    candles: List[Candle],
    strategy: BaseStrategy,
    initial_cash: float = 100000.0,
) -> tuple[BacktestMetrics, List[Trade]]:
    position = 0  # 1 long, -1 short, 0 flat
    entry = 0.0
    cash = initial_cash
    equity_curve = [initial_cash]
    trades: List[Trade] = []

    for i, bar in enumerate(candles):
        signal = strategy.on_bar(i, candles)

        # Open/close logic
        if signal.side == "BUY":
            if position == -1:
                pnl = entry - bar.close
                cash += pnl
                trades.append(Trade(entry_price=entry, exit_price=bar.close, side="SHORT", pnl=pnl))
                position = 0
            if position == 0:
                position = 1
                entry = bar.close

        elif signal.side == "SELL":
            if position == 1:
                pnl = bar.close - entry
                cash += pnl
                trades.append(Trade(entry_price=entry, exit_price=bar.close, side="LONG", pnl=pnl))
                position = 0
            if position == 0:
                position = -1
                entry = bar.close

        # Mark-to-market
        if position == 1:
            equity = cash + (bar.close - entry)
        elif position == -1:
            equity = cash + (entry - bar.close)
        else:
            equity = cash
        equity_curve.append(equity)

    # Close any open position at last price
    if candles:
        last = candles[-1].close
        if position == 1:
            pnl = last - entry
            cash += pnl
            trades.append(Trade(entry_price=entry, exit_price=last, side="LONG", pnl=pnl))
        elif position == -1:
            pnl = entry - last
            cash += pnl
            trades.append(Trade(entry_price=entry, exit_price=last, side="SHORT", pnl=pnl))

    wins = [t for t in trades if t.pnl > 0]
    losses = [t for t in trades if t.pnl < 0]
    trades_count = len(trades)
    wins_count = len(wins)
    losses_count = len(losses)
    win_rate = (wins_count / trades_count) if trades_count else 0.0
    gross_profit = sum(t.pnl for t in wins)
    gross_loss = abs(sum(t.pnl for t in losses))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else 0.0
    avg_win = (gross_profit / wins_count) if wins_count else 0.0
    avg_loss = (gross_loss / losses_count) if losses_count else 0.0
    expectancy = win_rate * avg_win - (1 - win_rate) * avg_loss

    peak = equity_curve[0]
    max_dd = 0.0
    for e in equity_curve:
        if e > peak:
            peak = e
        dd = (peak - e) / peak if peak else 0
        if dd > max_dd:
            max_dd = dd

    final_equity = cash
    total_return = (final_equity - initial_cash) / initial_cash if initial_cash else 0.0

    long_trades = len([t for t in trades if t.side == "LONG"])
    short_trades = len([t for t in trades if t.side == "SHORT"])

    return BacktestMetrics(
        win_rate=win_rate,
        profit_factor=profit_factor,
        max_drawdown=max_dd,
        total_return=total_return,
        trades=trades_count,
        wins=wins_count,
        losses=losses_count,
        avg_win=avg_win,
        avg_loss=avg_loss,
        expectancy=expectancy,
        long_trades=long_trades,
        short_trades=short_trades,
        final_equity=final_equity,
        return_usd=final_equity - initial_cash,
    ), trades
