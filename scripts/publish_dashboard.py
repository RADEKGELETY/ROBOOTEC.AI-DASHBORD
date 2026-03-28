import json
from pathlib import Path
from datetime import datetime

repo = Path(__file__).resolve().parents[1]
source_many = repo / "data" / "backtests.json"
source_last = repo / "data" / "last_backtest.json"
source_global = repo / "data" / "global_backtests.json"
source_global_universe = repo / "data" / "global_top100.json"
output = repo / "docs" / "data" / "dashboard.json"
output.parent.mkdir(parents=True, exist_ok=True)

markets_path = repo / "config" / "markets.json"
markets = json.loads(markets_path.read_text(encoding="utf-8"))

symbol_group = {}
for group, symbols in markets.get("symbols", {}).items():
    for s in symbols:
        symbol_group[s] = group

GROUP_MAP = {
    "nasdaq_top10": "US_STOCKS",
    "fx": "FX",
    "commodities": "GOLD",
    "crypto": "CRYPTO",
}


def load_backtests():
    if source_many.exists():
        return json.loads(source_many.read_text(encoding="utf-8"))
    if source_last.exists():
        return [json.loads(source_last.read_text(encoding="utf-8"))]
    return []


def load_global_backtests():
    if source_global.exists():
        return json.loads(source_global.read_text(encoding="utf-8"))
    return []


def load_global_universe():
    if source_global_universe.exists():
        payload = json.loads(source_global_universe.read_text(encoding="utf-8"))
        return payload.get("items", [])
    return []


def mean(values):
    values = [v for v in values if v is not None]
    return sum(values) / len(values) if values else 0.0


def build_curves(trades, initial=100000.0):
    equity = initial
    curve = [equity]
    for t in trades:
        pnl = t.get("pnl", 0.0) or 0.0
        equity += pnl
        curve.append(equity)

    peak = curve[0]
    drawdowns = []
    for e in curve:
        if e > peak:
            peak = e
        dd = (e - peak) / peak if peak else 0.0
        drawdowns.append(dd)

    norm = [(e / initial) - 1.0 for e in curve] if initial else [0.0]
    return norm, drawdowns


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


raw = load_backtests()
global_raw = load_global_backtests()
global_universe = load_global_universe()

# Anonymize strategies
strategy_ids = {}
strategy_desc = {}
next_id = [1]


def ensure_strategy_id(name: str) -> str:
    if name not in strategy_ids:
        strategy_ids[name] = f"S{next_id[0]}"
        strategy_desc[strategy_ids[name]] = strat_desc(name)
        next_id[0] += 1
    return strategy_ids[name]

records = []
for r in raw:
    strat = r.get("strategy") or "unknown"
    strategy_id = ensure_strategy_id(strat)
    symbol = r.get("symbol") or "unknown"
    group = GROUP_MAP.get(symbol_group.get(symbol, ""), "OTHER")
    metrics = r.get("metrics") or {}

    records.append(
        {
            "strategy_id": strategy_id,
            "strategy_desc": strategy_desc[strategy_id],
            "market": group,
            "symbol": symbol,
            "initial_cash": r.get("initial_cash"),
            "start": r.get("start"),
            "end": r.get("end"),
            "period_days": r.get("period_days"),
            "metrics": {
                "win_rate": metrics.get("win_rate"),
                "profit_factor": metrics.get("profit_factor"),
                "max_drawdown": metrics.get("max_drawdown"),
                "total_return": metrics.get("total_return"),
                "return_usd": metrics.get("return_usd"),
                "trades": metrics.get("trades"),
                "wins": metrics.get("wins"),
                "losses": metrics.get("losses"),
                "long_trades": metrics.get("long_trades"),
                "short_trades": metrics.get("short_trades"),
                "final_equity": metrics.get("final_equity"),
            },
            "trades": r.get("trades") or [],
        }
    )

# Aggregate per strategy (global)
by_strategy = {}
for rec in records:
    sid = rec["strategy_id"]
    by_strategy.setdefault(sid, []).append(rec)

strategies = []
for sid, items in by_strategy.items():
    starts = [i.get("start") for i in items if i.get("start")]
    ends = [i.get("end") for i in items if i.get("end")]
    start = min(starts) if starts else None
    end = max(ends) if ends else None
    strategies.append(
        {
            "id": sid,
            "desc": items[0].get("strategy_desc"),
            "win_rate": mean([i["metrics"]["win_rate"] for i in items]),
            "profit_factor": mean([i["metrics"]["profit_factor"] for i in items]),
            "max_drawdown": mean([i["metrics"]["max_drawdown"] for i in items]),
            "total_return": mean([i["metrics"]["total_return"] for i in items]),
            "return_usd": mean([i["metrics"]["return_usd"] for i in items]),
            "final_equity": mean([i["metrics"]["final_equity"] for i in items]),
            "initial_cash": mean([i.get("initial_cash") for i in items]),
            "start": start,
            "end": end,
            "period_days": int(mean([i.get("period_days") or 0 for i in items])),
            "trades": int(mean([i["metrics"]["trades"] or 0 for i in items])),
            "wins": int(mean([i["metrics"]["wins"] or 0 for i in items])),
            "losses": int(mean([i["metrics"]["losses"] or 0 for i in items])),
            "long_trades": int(mean([i["metrics"]["long_trades"] or 0 for i in items])),
            "short_trades": int(mean([i["metrics"]["short_trades"] or 0 for i in items])),
            "samples": len(items),
        }
    )

# Aggregate per market
by_market = {}
for rec in records:
    mk = rec["market"]
    by_market.setdefault(mk, []).append(rec)

markets_out = []
for mk, items in by_market.items():
    markets_out.append(
        {
            "market": mk,
            "win_rate": mean([i["metrics"]["win_rate"] for i in items]),
            "profit_factor": mean([i["metrics"]["profit_factor"] for i in items]),
            "max_drawdown": mean([i["metrics"]["max_drawdown"] for i in items]),
            "total_return": mean([i["metrics"]["total_return"] for i in items]),
            "return_usd": mean([i["metrics"]["return_usd"] for i in items]),
            "final_equity": mean([i["metrics"]["final_equity"] for i in items]),
            "initial_cash": mean([i.get("initial_cash") for i in items]),
            "period_days": int(mean([i.get("period_days") or 0 for i in items])),
            "samples": len(items),
        }
    )

# Top 5 strategies per market
by_market_strategy = {}
for rec in records:
    mk = rec["market"]
    sid = rec["strategy_id"]
    by_market_strategy.setdefault(mk, {}).setdefault(sid, []).append(rec)


def aggregate(items):
    win_rate = mean([i["metrics"]["win_rate"] for i in items])
    profit_factor = mean([i["metrics"]["profit_factor"] for i in items])
    max_drawdown = mean([i["metrics"]["max_drawdown"] for i in items])
    total_return = mean([i["metrics"]["total_return"] for i in items])
    return_usd = mean([i["metrics"]["return_usd"] for i in items])
    final_equity = mean([i["metrics"]["final_equity"] for i in items])
    initial_cash = mean([i.get("initial_cash") for i in items])

    trades = int(sum([i["metrics"]["trades"] or 0 for i in items]))
    wins = int(sum([i["metrics"]["wins"] or 0 for i in items]))
    losses = int(sum([i["metrics"]["losses"] or 0 for i in items]))
    long_trades = int(sum([i["metrics"]["long_trades"] or 0 for i in items]))
    short_trades = int(sum([i["metrics"]["short_trades"] or 0 for i in items]))

    starts = [i.get("start") for i in items if i.get("start")]
    ends = [i.get("end") for i in items if i.get("end")]
    start = min(starts) if starts else None
    end = max(ends) if ends else None
    period_days = int(mean([i.get("period_days") or 0 for i in items]))

    return {
        "market": items[0].get("market"),
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "max_drawdown": max_drawdown,
        "total_return": total_return,
        "return_usd": return_usd,
        "final_equity": final_equity,
        "initial_cash": initial_cash,
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "long_trades": long_trades,
        "short_trades": short_trades,
        "start": start,
        "end": end,
        "period_days": period_days,
        "samples": len(items),
    }


top_by_market = {}
market_strategy_rows = []
for mk, strat_map in by_market_strategy.items():
    rows = []
    for sid, items in strat_map.items():
        agg = aggregate(items)
        rows.append({"id": sid, "desc": items[0].get("strategy_desc"), **agg})
    rows.sort(key=lambda x: (x["total_return"], x["profit_factor"], x["win_rate"]), reverse=True)
    top_by_market[mk] = rows[:5]
    market_strategy_rows.extend(rows)

# Top 3 overall strategies across all market-strategy combinations
market_strategy_rows.sort(key=lambda x: (x["total_return"], x["profit_factor"], x["win_rate"]), reverse=True)
top_overall = market_strategy_rows

strategies_sorted = sorted(strategies, key=lambda x: (x["total_return"], x["profit_factor"], x["win_rate"]), reverse=True)
rg_focus = [s for s in market_strategy_rows if (s.get("desc") or "").startswith("RG ")]

# Global top 10 stocks (from global_top100 universe)
global_symbol_map = {}
for item in global_universe:
    sym = item.get("stooq")
    if sym:
        global_symbol_map[sym] = {
            "ticker": item.get("ticker") or sym,
            "name": item.get("name") or sym,
        }

global_rows = []
if global_raw:
    by_symbol = {}
    for rec in global_raw:
        strat = rec.get("strategy") or "unknown"
        strategy_id = ensure_strategy_id(strat)
        symbol = rec.get("symbol") or "unknown"
        metrics = rec.get("metrics") or {}
        by_symbol.setdefault(symbol, []).append(
            {
                "id": global_symbol_map.get(symbol, {}).get("ticker", symbol),
                "desc": f"{strategy_id} - {strategy_desc.get(strategy_id, strat_desc(strat))}",
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

    for symbol, items in by_symbol.items():
        items.sort(key=lambda x: (x["total_return"], x["profit_factor"], x["win_rate"]), reverse=True)
        global_rows.append(items[0])

global_rows.sort(key=lambda x: (x["total_return"], x["profit_factor"], x["win_rate"]), reverse=True)
global_top10 = global_rows[:10]

global_meta = {
    "universe_count": len(global_universe),
    "tested_count": len({r.get("symbol") for r in global_raw}),
}

summary = {
    "strategies": len(strategies),
    "markets": len(markets_out),
    "records": len(records),
    "avg_win_rate": mean([s["win_rate"] for s in strategies]),
    "avg_profit_factor": mean([s["profit_factor"] for s in strategies]),
    "avg_drawdown": mean([s["max_drawdown"] for s in strategies]),
    "avg_return": mean([s["total_return"] for s in strategies]),
    "initial_cash": 100000.0,
    "test_period": {
        "start": min([r.get("start") for r in records if r.get("start")], default=None),
        "end": max([r.get("end") for r in records if r.get("end")], default=None),
    },
}

targets = {
    "win_rate": 0.60,
    "total_return": 0.50,
}

# Select top strategy record for charts
best_record = None
best_pf = -1.0
for rec in records:
    pf = rec["metrics"].get("profit_factor") or 0.0
    if pf > best_pf:
        best_pf = pf
        best_record = rec

equity_curve, drawdown_curve = build_curves(best_record["trades"] if best_record else [])

payload = {
    "generated_at": datetime.utcnow().isoformat() + "Z",
    "scope": "anonymized",
    "summary": summary,
    "targets": targets,
    "top_overall": top_overall,
    "rg_focus": rg_focus,
    "strategies": strategies_sorted,
    "markets": sorted(markets_out, key=lambda x: x["profit_factor"], reverse=True),
    "top_by_market": top_by_market,
    "global_top10": global_top10,
    "global_meta": global_meta,
    "charts": {
        "equity_curve": equity_curve,
        "drawdown_curve": drawdown_curve,
        "label": best_record["strategy_id"] if best_record else "N/A",
    },
    "notes": [
        "Public anonymized dashboard export.",
        "No proprietary code included.",
        "Global universe sourced from CompaniesMarketCap top 100 by market cap.",
    ],
}

output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(f"Wrote {output}")
