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
    symbols = []
    symbol_meta = {}
    for item in items:
        stooq = item.get("stooq")
        yahoo = item.get("yahoo")
        if not (stooq or yahoo):
            continue
        if stooq:
            key = stooq
            provider = "stooq"
        else:
            key = yahoo
            provider = "yahoo"
        symbols.append(key)
        meta = {
            "stooq": stooq,
            "yahoo": yahoo,
            "provider": provider,
            "ticker": item.get("ticker"),
        }
        symbol_meta[key] = meta
        if stooq:
            symbol_meta.setdefault(stooq, meta)
        if yahoo:
            symbol_meta.setdefault(yahoo, meta)

    strategies = [s for s in get_tuned_strategies() if not s.name.startswith("rg_")]

    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=180)

    results = []
    skipped = 0

    for idx, sym in enumerate(symbols, start=1):
        provider = (symbol_meta.get(sym) or {}).get("provider", "stooq")
        try:
            csv_path = save_symbol_csv(sym, interval="d", start=start_dt, end=end_dt, provider=provider, use_cache=True)
            candles = filter_last_months(load_ohlcv_csv(str(csv_path)), months=6)
        except Exception as exc:  # noqa: BLE001
            print(f"[{idx}/{len(symbols)}] {sym}: {provider} failed ({exc})")
            stooq = (symbol_meta.get(sym) or {}).get("stooq")
            yahoo = (symbol_meta.get(sym) or {}).get("yahoo")
            fallback = None
            if provider == "stooq" and yahoo:
                fallback = ("yahoo", yahoo)
            elif provider == "yahoo" and stooq:
                fallback = ("stooq", stooq)
            if fallback:
                try:
                    provider = fallback[0]
                    sym = fallback[1]
                    csv_path = save_symbol_csv(sym, interval="d", start=start_dt, end=end_dt, provider=provider, use_cache=True)
                    candles = filter_last_months(load_ohlcv_csv(str(csv_path)), months=6)
                except Exception as exc2:  # noqa: BLE001
                    print(f"[{idx}/{len(symbols)}] {sym}: {provider} failed ({exc2})")
                    skipped += 1
                    continue
            else:
                skipped += 1
                continue

        if len(candles) < 30 and provider == "stooq":
            yahoo = (symbol_meta.get(sym) or {}).get("yahoo")
            if yahoo:
                try:
                    provider = "yahoo"
                    sym = yahoo
                    csv_path = save_symbol_csv(sym, interval="d", start=start_dt, end=end_dt, provider="yahoo", use_cache=True)
                    candles = filter_last_months(load_ohlcv_csv(str(csv_path)), months=6)
                except Exception as exc:  # noqa: BLE001
                    print(f"[{idx}/{len(symbols)}] {sym}: yahoo failed ({exc})")
            if len(candles) < 30:
                print(f"[{idx}/{len(symbols)}] {sym}: insufficient data ({len(candles)} bars)")
                skipped += 1
                continue
        elif len(candles) < 30 and provider == "yahoo":
            stooq = (symbol_meta.get(sym) or {}).get("stooq")
            if stooq:
                try:
                    provider = "stooq"
                    sym = stooq
                    csv_path = save_symbol_csv(sym, interval="d", start=start_dt, end=end_dt, provider="stooq", use_cache=True)
                    candles = filter_last_months(load_ohlcv_csv(str(csv_path)), months=6)
                except Exception as exc:  # noqa: BLE001
                    print(f"[{idx}/{len(symbols)}] {sym}: stooq failed ({exc})")
            if len(candles) < 30:
                print(f"[{idx}/{len(symbols)}] {sym}: insufficient data ({len(candles)} bars)")
                skipped += 1
                continue
        elif len(candles) < 30:
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
                    "provider": provider,
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
