import json
import sqlite3
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).resolve().parents[3] / "data" / "live" / "live.db"


def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.executescript(
        """
        CREATE TABLE IF NOT EXISTS strategies (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            params_json TEXT,
            instrument_scope TEXT,
            enabled INTEGER DEFAULT 1,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_id TEXT,
            instrument TEXT,
            direction TEXT,
            entry REAL,
            stop_loss REAL,
            take_profit REAL,
            confidence REAL,
            timestamp TEXT,
            reason TEXT,
            status TEXT,
            source TEXT,
            FOREIGN KEY(strategy_id) REFERENCES strategies(id)
        );

        CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY,
            strategy_id TEXT,
            instrument TEXT,
            side TEXT,
            qty REAL,
            order_type TEXT,
            status TEXT,
            created_at TEXT,
            provider TEXT,
            raw_json TEXT,
            FOREIGN KEY(strategy_id) REFERENCES strategies(id)
        );

        CREATE TABLE IF NOT EXISTS fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT,
            price REAL,
            qty REAL,
            fees REAL,
            slippage REAL,
            timestamp TEXT,
            raw_json TEXT,
            FOREIGN KEY(order_id) REFERENCES orders(id)
        );

        CREATE TABLE IF NOT EXISTS trades (
            id TEXT PRIMARY KEY,
            strategy_id TEXT,
            instrument TEXT,
            entry_time TEXT,
            exit_time TEXT,
            entry_price REAL,
            exit_price REAL,
            stop_loss REAL,
            take_profit REAL,
            size REAL,
            risk REAL,
            confidence REAL,
            regime TEXT,
            entry_reason TEXT,
            exit_reason TEXT,
            pnl REAL,
            pnl_r REAL,
            fees REAL,
            slippage REAL,
            status TEXT,
            raw_json TEXT,
            FOREIGN KEY(strategy_id) REFERENCES strategies(id)
        );

        CREATE TABLE IF NOT EXISTS positions (
            strategy_id TEXT,
            instrument TEXT,
            side TEXT,
            qty REAL,
            entry_price REAL,
            current_price REAL,
            unrealized_pnl REAL,
            updated_at TEXT,
            PRIMARY KEY(strategy_id, instrument)
        );

        CREATE TABLE IF NOT EXISTS timeline (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT,
            event_type TEXT,
            details_json TEXT,
            timestamp TEXT,
            FOREIGN KEY(trade_id) REFERENCES trades(id)
        );
        """
    )
    conn.commit()


def insert_strategy(conn: sqlite3.Connection, strategy_id: str, name: str, description: str = "", params: dict | None = None, instrument_scope: str = "") -> None:
    payload = json.dumps(params or {})
    conn.execute(
        """
        INSERT OR REPLACE INTO strategies (id, name, description, params_json, instrument_scope, enabled, created_at)
        VALUES (?, ?, ?, ?, ?, COALESCE((SELECT enabled FROM strategies WHERE id = ?), 1), COALESCE((SELECT created_at FROM strategies WHERE id = ?), datetime('now')))
        """,
        (strategy_id, name, description, payload, instrument_scope, strategy_id, strategy_id),
    )
    conn.commit()


def insert_signal(conn: sqlite3.Connection, payload: dict) -> int:
    keys = [
        "strategy_id",
        "instrument",
        "direction",
        "entry",
        "stop_loss",
        "take_profit",
        "confidence",
        "timestamp",
        "reason",
        "status",
        "source",
    ]
    values = [payload.get(k) for k in keys]
    cursor = conn.execute(
        """
        INSERT INTO signals (strategy_id, instrument, direction, entry, stop_loss, take_profit, confidence, timestamp, reason, status, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        values,
    )
    conn.commit()
    return int(cursor.lastrowid)


def insert_order(conn: sqlite3.Connection, payload: dict) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO orders (id, strategy_id, instrument, side, qty, order_type, status, created_at, provider, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload.get("id"),
            payload.get("strategy_id"),
            payload.get("instrument"),
            payload.get("side"),
            payload.get("qty"),
            payload.get("order_type"),
            payload.get("status"),
            payload.get("created_at"),
            payload.get("provider"),
            json.dumps(payload.get("raw", {})),
        ),
    )
    conn.commit()


def insert_trade(conn: sqlite3.Connection, payload: dict) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO trades (id, strategy_id, instrument, entry_time, exit_time, entry_price, exit_price, stop_loss, take_profit,
            size, risk, confidence, regime, entry_reason, exit_reason, pnl, pnl_r, fees, slippage, status, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload.get("id"),
            payload.get("strategy_id"),
            payload.get("instrument"),
            payload.get("entry_time"),
            payload.get("exit_time"),
            payload.get("entry_price"),
            payload.get("exit_price"),
            payload.get("stop_loss"),
            payload.get("take_profit"),
            payload.get("size"),
            payload.get("risk"),
            payload.get("confidence"),
            payload.get("regime"),
            payload.get("entry_reason"),
            payload.get("exit_reason"),
            payload.get("pnl"),
            payload.get("pnl_r"),
            payload.get("fees"),
            payload.get("slippage"),
            payload.get("status"),
            json.dumps(payload.get("raw", {})),
        ),
    )
    conn.commit()


def insert_timeline(conn: sqlite3.Connection, trade_id: str, event_type: str, details: dict, timestamp: str) -> None:
    conn.execute(
        """
        INSERT INTO timeline (trade_id, event_type, details_json, timestamp)
        VALUES (?, ?, ?, ?)
        """,
        (trade_id, event_type, json.dumps(details), timestamp),
    )
    conn.commit()


def fetch_recent_signals(conn: sqlite3.Connection, instrument: str, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT s.strategy_id, st.name, s.instrument, s.direction, s.entry, s.stop_loss, s.take_profit, s.confidence, s.timestamp, s.reason, s.status
        FROM signals s
        LEFT JOIN strategies st ON st.id = s.strategy_id
        WHERE s.instrument = ?
        ORDER BY s.timestamp DESC
        LIMIT ?
        """,
        (instrument, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_journal(conn: sqlite3.Connection, limit_per_strategy: int = 50) -> list[dict[str, Any]]:
    strategies = conn.execute("SELECT id, name FROM strategies WHERE enabled = 1 ORDER BY name").fetchall()
    out = []
    for st in strategies:
        rows = conn.execute(
            """
            SELECT instrument, entry_price, exit_price, stop_loss, take_profit, size, risk, confidence, regime, entry_reason, exit_reason,
                   pnl, pnl_r, fees, slippage, status
            FROM trades
            WHERE strategy_id = ?
            ORDER BY entry_time DESC
            LIMIT ?
            """,
            (st["id"], limit_per_strategy),
        ).fetchall()
        out.append({"id": st["id"], "name": st["name"], "trades": [dict(r) for r in rows]})
    return out
