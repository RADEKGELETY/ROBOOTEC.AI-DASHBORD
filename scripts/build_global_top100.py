import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, List

import requests

SOURCE_URL = "https://companiesmarketcap.com/"


@dataclass
class Company:
    rank: int
    name: str
    ticker: str
    stooq: Optional[str]
    yahoo: Optional[str]
    eligible: bool


def _extract_rows(html: str) -> List[str]:
    body_match = re.search(r"<tbody>(.*?)</tbody>", html, re.S)
    if not body_match:
        return []
    body = body_match.group(1)
    return re.findall(r"<tr>(.*?)</tr>", body, re.S)


def _strip_tags(text: str) -> str:
    return re.sub(r"<.*?>", "", text).strip()


def _parse_company(row: str, rank: int) -> Company:
    name_match = re.search(r'class="company-name">([^<]+)</div>', row)
    code_match = re.search(r'class="company-code">(.*?)</div>', row, re.S)
    name = name_match.group(1).strip() if name_match else f"Company {rank}"
    ticker = _strip_tags(code_match.group(1)) if code_match else ""
    stooq = to_stooq_symbol(ticker)
    yahoo = to_yahoo_symbol(ticker)
    eligible = bool(stooq or yahoo)
    return Company(rank=rank, name=name, ticker=ticker, stooq=stooq, yahoo=yahoo, eligible=eligible)


def to_stooq_symbol(ticker: str) -> Optional[str]:
    if not ticker:
        return None
    t = ticker.strip().upper()
    if t.isdigit():
        return None

    if "." in t:
        base, suffix = t.split(".", 1)
        if not base:
            return None
        suffix = suffix.upper()
        if suffix in {"T", "JP"}:
            return f"{base.lower()}.jp"
        if suffix in {"L", "LN"}:
            return f"{base.lower()}.uk"
        if suffix in {"DE", "F"}:
            return f"{base.lower()}.de"
        return None

    t = t.replace(".", "-")
    return f"{t.lower()}.us"


def to_yahoo_symbol(ticker: str) -> Optional[str]:
    if not ticker:
        return None
    t = ticker.strip().upper()
    if t.isdigit():
        return None
    if "." in t:
        return t
    return t


def main() -> None:
    html = requests.get(SOURCE_URL, timeout=30).text
    rows = _extract_rows(html)
    companies: List[Company] = []
    for i, row in enumerate(rows, start=1):
        companies.append(_parse_company(row, i))

    output = Path(__file__).resolve().parents[1] / "data" / "global_top100.json"
    output.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "source": SOURCE_URL,
        "items": [
            {
                "rank": c.rank,
                "name": c.name,
                "ticker": c.ticker,
                "stooq": c.stooq,
                "yahoo": c.yahoo,
                "eligible": c.eligible,
            }
            for c in companies
        ],
    }

    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    total = len(companies)
    eligible = sum(1 for c in companies if c.eligible)
    print(f"Saved {total} companies ({eligible} eligible for free data) -> {output}")


if __name__ == "__main__":
    main()
