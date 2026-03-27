import json
from pathlib import Path


def load_markets() -> dict:
    base = Path(__file__).resolve().parents[2]
    path = base / "config" / "markets.json"
    return json.loads(path.read_text(encoding="utf-8"))
