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
    fee_rate: float = 0.0005,
    slippage: float = 0.0005,
) -> tuple[BacktestMetrics, List[Trade]]:
    # Full capital deployment per trade, long/short, with simple fees and slippage
    position = 0  # 1 long, -1 short, 0 flat
    entry_price = 0.0
    equity = initial_cash
    entry_equity_before = initial_cash
    entry_equity_after = initial_cash

    equity_curve = [equity]
    trades: List[Trade] = []

    def px(price: float, side: str) -> float:
        if side == "BUY":
            return price * (1 + slippage)
        return price * (1 - slippage)

    for i, bar in enumerate(candles):
        signal = strategy.on_bar(i, candles)

        if signal.side == "BUY":
            if position == -1:
                exit_price = px(bar.close, "BUY")
                equity_after = entry_equity_after * (entry_price / exit_price)
                equity_after -= equity_after * fee_rate
                pnl = equity_after - entry_equity_before
                equity = equity_after
                trades.append(Trade(entry_price=entry_price, exit_price=exit_price, side="SHORT", pnl=pnl))
                position = 0

            if position == 0:
                entry_equity_before = equity
                equity -= equity * fee_rate
                entry_equity_after = equity
                entry_price = px(bar.close, "BUY")
                position = 1

        elif signal.side == "SELL":
            if position == 1:
                exit_price = px(bar.close, "SELL")
                equity_after = entry_equity_after * (exit_price / entry_price)
                equity_after -= equity_after * fee_rate
                pnl = equity_after - entry_equity_before
                equity = equity_after
                trades.append(Trade(entry_price=entry_price, exit_price=exit_price, side="LONG", pnl=pnl))
                position = 0

            if position == 0:
                entry_equity_before = equity
                equity -= equity * fee_rate
                entry_equity_after = equity
                entry_price = px(bar.close, "SELL")
                position = -1

        # Mark-to-market
        if position == 1:
            equity_mark = entry_equity_after * (bar.close / entry_price)
        elif position == -1:
            equity_mark = entry_equity_after * (entry_price / bar.close)
        else:
            equity_mark = equity
        equity_curve.append(equity_mark)

    # Close any open position at last price
    if candles:
        last = candles[-1].close
        if position == 1:
            exit_price = px(last, "SELL")
            equity_after = entry_equity_after * (exit_price / entry_price)
            equity_after -= equity_after * fee_rate
            pnl = equity_after - entry_equity_before
            equity = equity_after
            trades.append(Trade(entry_price=entry_price, exit_price=exit_price, side="LONG", pnl=pnl))
        elif position == -1:
            exit_price = px(last, "BUY")
            equity_after = entry_equity_after * (entry_price / exit_price)
            equity_after -= equity_after * fee_rate
            pnl = equity_after - entry_equity_before
            equity = equity_after
            trades.append(Trade(entry_price=entry_price, exit_price=exit_price, side="SHORT", pnl=pnl))

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

    final_equity = equity
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
