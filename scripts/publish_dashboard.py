import json
from pathlib import Path
from datetime import datetime

repo = Path(__file__).resolve().parents[1]
source_many = repo / "data" / "backtests.json"
source_last = repo / "data" / "last_backtest.json"
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


def mean(values):
    values = [v for v in values if v is not None]
    return sum(values) / len(values) if values else 0.0


raw = load_backtests()

# Anonymize strategies
strategy_ids = {}
next_id = 1

records = []
for r in raw:
    strat = r.get("strategy") or "unknown"
    if strat not in strategy_ids:
        strategy_ids[strat] = f"S{next_id}"
        next_id += 1
    symbol = r.get("symbol") or "unknown"
    group = GROUP_MAP.get(symbol_group.get(symbol, ""), "OTHER")
    metrics = r.get("metrics") or {}

    records.append(
        {
            "strategy_id": strategy_ids[strat],
            "market": group,
            "metrics": {
                "win_rate": metrics.get("win_rate"),
                "profit_factor": metrics.get("profit_factor"),
                "max_drawdown": metrics.get("max_drawdown"),
                "total_return": metrics.get("total_return"),
                "trades": metrics.get("trades"),
            },
        }
    )

# Aggregate per strategy
by_strategy = {}
for rec in records:
    sid = rec["strategy_id"]
    by_strategy.setdefault(sid, []).append(rec)

strategies = []
for sid, items in by_strategy.items():
    strategies.append(
        {
            "id": sid,
            "win_rate": mean([i["metrics"]["win_rate"] for i in items]),
            "profit_factor": mean([i["metrics"]["profit_factor"] for i in items]),
            "max_drawdown": mean([i["metrics"]["max_drawdown"] for i in items]),
            "total_return": mean([i["metrics"]["total_return"] for i in items]),
            "trades": int(mean([i["metrics"]["trades"] or 0 for i in items])),
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
            "samples": len(items),
        }
    )

summary = {
    "strategies": len(strategies),
    "markets": len(markets_out),
    "records": len(records),
    "avg_win_rate": mean([s["win_rate"] for s in strategies]),
    "avg_profit_factor": mean([s["profit_factor"] for s in strategies]),
    "avg_drawdown": mean([s["max_drawdown"] for s in strategies]),
    "avg_return": mean([s["total_return"] for s in strategies]),
}

targets = {
    "win_rate": 0.60,
    "total_return": 0.50,
}

payload = {
    "generated_at": datetime.utcnow().isoformat() + "Z",
    "scope": "anonymized",
    "summary": summary,
    "targets": targets,
    "strategies": sorted(strategies, key=lambda x: x["profit_factor"], reverse=True),
    "markets": sorted(markets_out, key=lambda x: x["profit_factor"], reverse=True),
    "notes": [
        "Public anonymized dashboard export.",
        "No symbols or strategy code included.",
    ],
}

output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(f"Wrote {output}")
