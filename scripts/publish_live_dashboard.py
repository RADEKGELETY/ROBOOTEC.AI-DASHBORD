import json
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from apps.worker.live.db import connect, ensure_schema, fetch_recent_signals, fetch_journal


OUTPUT = ROOT_DIR / "docs" / "data" / "live.json"


def build_payload() -> dict:
    conn = connect()
    ensure_schema(conn)

    signals = {
        "NASDAQ": fetch_recent_signals(conn, "NASDAQ", limit=30),
        "GOLD": fetch_recent_signals(conn, "GOLD", limit=30),
        "BTC": fetch_recent_signals(conn, "BTC", limit=30),
        "EURUSD": fetch_recent_signals(conn, "EURUSD", limit=30),
    }

    # Normalize for front-end
    for key, rows in signals.items():
        for row in rows:
            row["strategy"] = row.get("name") or row.get("strategy_id")
            row["sl"] = row.pop("stop_loss", None)
            row["tp"] = row.pop("take_profit", None)

    journal = fetch_journal(conn, limit_per_strategy=50)
    for j in journal:
        for t in j.get("trades", []):
            t["entry"] = t.pop("entry_price", None)
            t["exit"] = t.pop("exit_price", None)
            t["sl"] = t.pop("stop_loss", None)
            t["tp"] = t.pop("take_profit", None)

    conn.close()

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "scope": "live",
        "summary": {
            "strategies": len(journal),
            "markets": 4,
            "records": sum(len(v) for v in signals.values()),
            "avg_win_rate": 0,
            "avg_profit_factor": 0,
            "avg_drawdown": 0,
            "avg_return": 0,
            "initial_cash": 100000,
            "test_period": {
                "start": None,
                "end": None
            }
        },
        "targets": {
            "win_rate": 0.6,
            "total_return": 0.5
        },
        "top_overall": [],
        "rg_focus": [],
        "strategies": [],
        "markets": [],
        "top_by_market": {
            "US_STOCKS": [],
            "FX": [],
            "GOLD": [],
            "CRYPTO": []
        },
        "global_top10": [],
        "global_meta": {
            "universe_count": 0,
            "tested_count": 0
        },
        "sp500_top10": [],
        "sp500_meta": {
            "universe_count": 0,
            "tested_count": 0
        },
        "signals_by_instrument": signals,
        "journal_strategies": journal,
        "charts": {
            "equity_curve": [],
            "drawdown_curve": [],
            "label": "N/A"
        },
        "notes": [
            "Live trading view (paper).",
            "Signals for NASDAQ/GOLD/EURUSD are non-executable.",
            "BTC uses Alpaca paper execution when enabled."
        ]
    }
    return payload


def main() -> None:
    payload = build_payload()
    OUTPUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
