import json
from datetime import datetime, timedelta
from pathlib import Path

from apps.worker.backtest.engine import run_backtest
from apps.worker.backtest.predictor import HistoricalOutcomeFilter
from apps.worker.backtest.strategies import get_tuned_strategies
from apps.worker.data.csv_loader import load_ohlcv_csv
from apps.worker.data.stooq_cache import save_symbol_csv


def filter_last_months(candles, months=6):
    if not candles:
        return candles
    candles = sorted(candles, key=lambda c: c.timestamp)
    end = candles[-1].timestamp
    start = end - timedelta(days=30 * months)
    return [c for c in candles if c.timestamp >= start]


def period_info(candles):
    if not candles:
        return None, None, None
    candles = sorted(candles, key=lambda c: c.timestamp)
    start = candles[0].timestamp.date().isoformat()
    end = candles[-1].timestamp.date().isoformat()
    days = (candles[-1].timestamp - candles[0].timestamp).days
    return start, end, days


def load_universe() -> list[dict]:
    path = Path(__file__).resolve().parents[1] / "data" / "global_top100.json"
    if not path.exists():
        raise FileNotFoundError("global_top100.json not found. Run scripts/build_global_top100.py first.")
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload.get("items", [])


def main() -> None:
    items = load_universe()
    symbols = [i["stooq"] for i in items if i.get("stooq")]

    strategies = [s for s in get_tuned_strategies() if not s.name.startswith("rg_")]

    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=180)

    results = []
    skipped = 0

    for idx, sym in enumerate(symbols, start=1):
        try:
            csv_path = save_symbol_csv(sym, interval="d", start=start_dt, end=end_dt, use_cache=True)
            candles = filter_last_months(load_ohlcv_csv(str(csv_path)), months=6)
        except Exception as exc:  # noqa: BLE001
            print(f"[{idx}/{len(symbols)}] {sym}: download failed ({exc})")
            skipped += 1
            continue

        if len(candles) < 30:
            print(f"[{idx}/{len(symbols)}] {sym}: insufficient data ({len(candles)} bars)")
            skipped += 1
            continue

        start, end, days = period_info(candles)
        for strat in strategies:
            predictor = HistoricalOutcomeFilter()
            metrics, trades = run_backtest(
                candles,
                strat,
                initial_cash=100000.0,
                predictor=predictor,
            )
            results.append(
                {
                    "strategy": strat.name,
                    "symbol": sym,
                    "initial_cash": 100000.0,
                    "start": start,
                    "end": end,
                    "period_days": days,
                    "metrics": metrics.__dict__,
                    "trades": [t.__dict__ for t in trades],
                }
            )
        print(f"[{idx}/{len(symbols)}] {sym}: {len(strategies)} strategies done")

    out = Path(__file__).resolve().parents[1] / "data" / "global_backtests.json"
    out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"Saved global backtests: {len(results)} runs (skipped {skipped}) -> {out}")


if __name__ == "__main__":
    main()
