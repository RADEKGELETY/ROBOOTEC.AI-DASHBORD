import json
from datetime import datetime
from pathlib import Path

repo = Path(__file__).resolve().parents[1]
base_path = repo / "docs" / "data" / "dashboard.json"
global_backtests = repo / "data" / "global_backtests.json"
global_universe_path = repo / "data" / "global_top100.json"


def strat_desc(name: str) -> str:
    if name.startswith("sma_cross"):
        parts = name.split("_")
        return f"SMA crossover (fast={parts[-2]}, slow={parts[-1]})"
    if name.startswith("ema_cross"):
        parts = name.split("_")
        return f"EMA crossover (fast={parts[-2]}, slow={parts[-1]})"
    if name.startswith("rsi_revert"):
        parts = name.split("_")
        return f"RSI mean reversion (window={parts[-3]}, OB={parts[-2]}, OS={parts[-1]})"
    if name.startswith("bollinger_revert"):
        parts = name.split("_")
        return f"Bollinger mean reversion (window={parts[-2]}, mult={parts[-1]})"
    if name.startswith("donchian_breakout"):
        parts = name.split("_")
        return f"Donchian breakout (window={parts[-1]})"
    if name.startswith("momentum"):
        parts = name.split("_")
        return f"Momentum (window={parts[-1]})"
    if name.startswith("macd"):
        parts = name.split("_")
        return f"MACD trend (fast={parts[-3]}, slow={parts[-2]}, signal={parts[-1]})"
    if name.startswith("atr_breakout"):
        parts = name.split("_")
        return f"ATR breakout (window={parts[-2]}, mult={parts[-1]})"
    if name.startswith("rg_"):
        instrument = name.split("_", 1)[1]
        label_map = {
            "nasdaq": "Nasdaq futures (NY session)",
            "gold": "Gold (NY session)",
            "btc": "Bitcoin (NY session)",
            "eurusd": "EUR/USD (NY session)",
        }
        label = label_map.get(instrument, instrument)
        return f"RG liquidity sweep trend ({label}, SL 0.5% / TP 2%)"
    return name


def load_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    base = load_json(base_path)
    if not base:
        raise FileNotFoundError(f"Missing {base_path}")

    universe = load_json(global_universe_path) or {}
    items = universe.get("items", [])
    symbol_map = {}
    for item in items:
        for sym in [item.get("stooq"), item.get("yahoo")]:
            if sym:
                symbol_map[sym] = {
                    "ticker": item.get("ticker") or sym,
                    "name": item.get("name") or sym,
                }

    global_raw = load_json(global_backtests) or []

    by_symbol = {}
    for rec in global_raw:
        symbol = rec.get("symbol") or "unknown"
        metrics = rec.get("metrics") or {}
        by_symbol.setdefault(symbol, []).append(
            {
                "id": symbol_map.get(symbol, {}).get("ticker", symbol),
                "desc": strat_desc(rec.get("strategy") or "unknown"),
                "symbol": symbol,
                "win_rate": metrics.get("win_rate"),
                "profit_factor": metrics.get("profit_factor"),
                "max_drawdown": metrics.get("max_drawdown"),
                "total_return": metrics.get("total_return"),
                "return_usd": metrics.get("return_usd"),
                "final_equity": metrics.get("final_equity"),
                "initial_cash": rec.get("initial_cash"),
                "trades": metrics.get("trades"),
                "wins": metrics.get("wins"),
                "losses": metrics.get("losses"),
                "long_trades": metrics.get("long_trades"),
                "short_trades": metrics.get("short_trades"),
            }
        )

    global_rows = []
    for symbol, rows in by_symbol.items():
        rows.sort(key=lambda x: (x["total_return"], x["profit_factor"], x["win_rate"]), reverse=True)
        global_rows.append(rows[0])

    global_rows.sort(key=lambda x: (x["total_return"], x["profit_factor"], x["win_rate"]), reverse=True)
    global_top10 = global_rows[:10]

    base["global_top10"] = global_top10
    base["global_meta"] = {
        "universe_count": len(items),
        "tested_count": len({r.get("symbol") for r in global_raw}),
    }
    base["generated_at"] = datetime.utcnow().isoformat() + "Z"

    notes = base.get("notes", [])
    if "Global universe sourced from CompaniesMarketCap top 100 by market cap." not in notes:
        notes.append("Global universe sourced from CompaniesMarketCap top 100 by market cap.")
    if "Global price data sourced from free public providers (Stooq, Yahoo Finance)." not in notes:
        notes.append("Global price data sourced from free public providers (Stooq, Yahoo Finance).")
    base["notes"] = notes

    base_path.write_text(json.dumps(base, indent=2), encoding="utf-8")
    print(f"Updated global_top10 in {base_path}")


if __name__ == "__main__":
    main()
