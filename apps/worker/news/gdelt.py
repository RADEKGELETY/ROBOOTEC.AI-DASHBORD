from __future__ import annotations

import csv
import io
import zipfile
from datetime import date, timedelta
from typing import Iterable, List, Set

import requests

BASE_URL = "https://data.gdeltproject.org/gkg"


def _date_to_str(d: date) -> str:
    return d.strftime("%Y%m%d")


def _download_gkg_zip(d: date) -> bytes:
    url = f"{BASE_URL}/{_date_to_str(d)}.gkg.csv.zip"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.content


def _scan_day(d: date, keywords: Iterable[str]) -> bool:
    keywords_u = [k.upper() for k in keywords]
    blob = _download_gkg_zip(d)
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        names = zf.namelist()
        if not names:
            return False
        with zf.open(names[0]) as f:
            reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8", errors="ignore"), delimiter="\t")
            for row in reader:
                joined = "\t".join(row).upper()
                if any(k in joined for k in keywords_u):
                    return True
    return False


def build_red_news_calendar(start: date, end: date, keywords: Iterable[str]) -> Set[date]:
    days: Set[date] = set()
    d = start
    while d <= end:
        try:
            if _scan_day(d, keywords):
                days.add(d)
        except requests.HTTPError:
            # Missing day or temporary error; skip
            pass
        d += timedelta(days=1)
    return days
