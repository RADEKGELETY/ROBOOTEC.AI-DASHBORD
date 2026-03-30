import json
import time
from datetime import datetime, timedelta
from pathlib import Path

from apps.worker.backtest.strategies import get_strategy
from apps.worker.data.csv_loader import load_ohlcv_csv
from apps.worker.data.stooq_cache import save_symbol_csv
from apps.worker.live.alpaca_trade import submit_crypto_order, now_iso
from apps.worker.live.db import connect, ensure_schema, insert_strategy, insert_signal, insert_order

CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "live.json"


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing {CONFIG_PATH}")
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def fetch_candles(symbol: str, days: int = 180):
    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=days)
    csv_path = save_symbol_csv(symbol, interval="d", start=start_dt, end=end_dt, use_cache=False)
    return load_ohlcv_csv(str(csv_path))


def make_signal(strategy_name: str, instrument: str, candles, sl_pct: float, tp_pct: float) -> dict | None:
    if not candles:
        return None
    strat = get_strategy(strategy_name)
    signal = strat.on_bar(len(candles) - 1, candles)
    if signal.side == "NO_TRADE":
        return None
    last = candles[-1]
    entry = last.close
    if signal.side == "BUY":
        direction = "LONG"
        sl = entry * (1 - sl_pct)
        tp = entry * (1 + tp_pct)
    else:
        direction = "SHORT"
        sl = entry * (1 + sl_pct)
        tp = entry * (1 - tp_pct)
    return {
        "strategy_id": strategy_name,
        "instrument": instrument,
        "direction": direction,
        "entry": entry,
        "stop_loss": sl,
        "take_profit": tp,
        "confidence": signal.confidence,
        "timestamp": now_iso(),
        "reason": f"{strategy_name} signal",
        "status": "NEW",
        "source": "live_worker",
    }


def register_strategies(conn, strategies):
    for name in strategies:
        insert_strategy(conn, name, name)


def execute_if_allowed(config: dict, instrument: str, signal: dict):
    exec_cfg = (config.get("execution") or {}).get(instrument)
    if not exec_cfg or not exec_cfg.get("enabled"):
        return None
    if instrument != "BTC":
        return None
    order = submit_crypto_order(
        symbol=(config.get("instruments") or {}).get("BTC", {}).get("alpaca_symbol", "BTC/USD"),
        side="buy" if signal["direction"] == "LONG" else "sell",
        qty=float(exec_cfg.get("qty", 0.01)),
        order_type=exec_cfg.get("order_type", "market"),
    )
    return order


def run_once() -> None:
    config = load_config()
    strategies = config.get("strategies", [])
    instruments = config.get("instruments", {})
    risk = config.get("risk", {})
    sl_pct = float(risk.get("sl_pct", 0.01))
    tp_pct = float(risk.get("tp_pct", 0.02))

    conn = connect()
    ensure_schema(conn)
    register_strategies(conn, strategies)

    for instrument, meta in instruments.items():
        symbol = meta.get("symbol")
        if not symbol:
            continue
        candles = fetch_candles(symbol)
        for strat_name in strategies:
            sig = make_signal(strat_name, instrument, candles, sl_pct, tp_pct)
            if not sig:
                continue
            insert_signal(conn, sig)
            order = execute_if_allowed(config, instrument, sig)
            if order:
                insert_order(
                    conn,
                    {
                        "id": order.get("id"),
                        "strategy_id": strat_name,
                        "instrument": instrument,
                        "side": order.get("side"),
                        "qty": float(order.get("qty") or 0),
                        "order_type": order.get("type"),
                        "status": order.get("status"),
                        "created_at": order.get("created_at") or now_iso(),
                        "provider": "alpaca",
                        "raw": order,
                    },
                )

    conn.close()


def run_forever() -> None:
    config = load_config()
    interval = int(config.get("poll_interval_seconds", 300))
    while True:
        run_once()
        time.sleep(interval)


if __name__ == "__main__":
    run_forever()
