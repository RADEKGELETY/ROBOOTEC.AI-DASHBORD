import json
from pathlib import Path
from typing import Any


def data_dir() -> Path:
    base = Path(__file__).resolve().parents[2]  # repo root
    d = base / "data"
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_json(name: str, payload: Any) -> Path:
    path = data_dir() / name
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def read_json(name: str, default: Any = None) -> Any:
    path = data_dir() / name
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))
