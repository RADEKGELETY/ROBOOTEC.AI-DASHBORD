import json
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import requests

SOURCE_URL = "https://companiesmarketcap.com/usa/largest-companies-in-the-usa-by-market-cap/"


def strip_tags(text: str) -> str:
    return re.sub(r"<.*?>", "", text).strip()


def parse_table(html: str) -> List[Dict[str, str]]:
    body_match = re.search(r"<tbody>(.*?)</tbody>", html, re.S)
    if not body_match:
        return []
    body = body_match.group(1)
    rows = re.findall(r"<tr>(.*?)</tr>", body, re.S)
    out = []
    for idx, row in enumerate(rows, start=1):
        name_match = re.search(r'class="company-name">([^<]+)</div>', row)
        code_match = re.search(r'class="company-code">(.*?)</div>', row, re.S)
        name = strip_tags(name_match.group(1)) if name_match else f"Company {idx}"
        ticker = strip_tags(code_match.group(1)) if code_match else ""
        out.append({"rank": idx, "name": name, "ticker": ticker})
    return out


def to_stooq_symbol(ticker: str) -> str:
    return f"{ticker.replace('.', '-').lower()}.us"


def to_yahoo_symbol(ticker: str) -> str:
    return ticker.replace('.', '-')


def main() -> None:
    html = requests.get(SOURCE_URL, timeout=30).text
    rows = parse_table(html)
    top10 = rows[:10]

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "source": SOURCE_URL,
        "items": [
            {
                "rank": r["rank"],
                "name": r["name"],
                "ticker": r["ticker"],
                "stooq": to_stooq_symbol(r["ticker"]),
                "yahoo": to_yahoo_symbol(r["ticker"]),
            }
            for r in top10
        ],
    }

    out = Path(__file__).resolve().parents[1] / "data" / "sp500_top10.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Saved {len(top10)} S&P 500 top companies -> {out}")


if __name__ == "__main__":
    main()
