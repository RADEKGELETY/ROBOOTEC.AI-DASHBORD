from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Set

from apps.worker.news.gdelt import build_red_news_calendar


def _cache_path() -> Path:
    base = Path(__file__).resolve().parents[3]
    d = base / "data" / "news"
    d.mkdir(parents=True, exist_ok=True)
    return d / "red_news.json"


def load_red_news() -> Set[date]:
    path = _cache_path()
    if not path.exists():
        return set()
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {datetime.fromisoformat(d).date() for d in payload.get("red_days", [])}


def save_red_news(days: Iterable[date], keywords: Iterable[str]) -> Path:
    path = _cache_path()
    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "keywords": list(keywords),
        "red_days": sorted({d.isoformat() for d in days}),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def build_and_save_red_news(start: date, end: date, keywords: Iterable[str]) -> Path:
    days = build_red_news_calendar(start, end, keywords)
    return save_red_news(days, keywords)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Build red-news calendar from GDELT")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--keywords", default="TRUMP,FED,POWELL,FOMC")
    args = parser.parse_args()

    start = datetime.fromisoformat(args.start).date()
    end = datetime.fromisoformat(args.end).date()
    keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
    out = build_and_save_red_news(start, end, keywords)
    print(f"Saved {out}")
