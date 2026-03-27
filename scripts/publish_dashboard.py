import json
from pathlib import Path
from datetime import datetime

repo = Path(__file__).resolve().parents[1]
source = repo / "data" / "last_backtest.json"
output = repo / "docs" / "data" / "last_backtest.json"
output.parent.mkdir(parents=True, exist_ok=True)

payload = {
    "generated_at": datetime.utcnow().isoformat() + "Z",
    "scope": "anonymized",
    "demo": True,
    "metrics": None,
    "notes": [
        "Anonymized export for GitHub Pages.",
        "No strategy code or symbols included.",
    ],
}

if source.exists():
    raw = json.loads(source.read_text(encoding="utf-8"))
    m = raw.get("metrics") or {}
    payload["demo"] = False
    payload["metrics"] = {
        "win_rate": m.get("win_rate"),
        "profit_factor": m.get("profit_factor"),
        "max_drawdown": m.get("max_drawdown"),
        "total_return": m.get("total_return"),
    }

output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(f"Wrote {output}")
