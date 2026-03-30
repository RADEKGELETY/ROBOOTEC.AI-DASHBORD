import os
from datetime import datetime
import requests

DEFAULT_BASE = "https://paper-api.alpaca.markets"


def _trade_base() -> str:
    return os.getenv("ALPACA_TRADE_BASE_URL", DEFAULT_BASE).rstrip("/")


def _auth_headers() -> dict:
    key = os.getenv("ALPACA_API_KEY_ID")
    secret = os.getenv("ALPACA_API_SECRET_KEY")
    if not key or not secret:
        raise RuntimeError("Missing ALPACA_API_KEY_ID / ALPACA_API_SECRET_KEY env vars")
    return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}


def submit_crypto_order(symbol: str, side: str, qty: float, order_type: str = "market", time_in_force: str = "gtc") -> dict:
    url = f"{_trade_base()}/v2/orders"
    payload = {
        "symbol": symbol,
        "qty": str(qty),
        "side": side.lower(),
        "type": order_type,
        "time_in_force": time_in_force,
    }
    resp = requests.post(url, headers=_auth_headers(), json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_order(order_id: str) -> dict:
    url = f"{_trade_base()}/v2/orders/{order_id}"
    resp = requests.get(url, headers=_auth_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


def list_positions() -> list[dict]:
    url = f"{_trade_base()}/v2/positions"
    resp = requests.get(url, headers=_auth_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


def cancel_order(order_id: str) -> None:
    url = f"{_trade_base()}/v2/orders/{order_id}"
    resp = requests.delete(url, headers=_auth_headers(), timeout=30)
    resp.raise_for_status()


def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"
