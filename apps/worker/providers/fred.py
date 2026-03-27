import os
import requests

BASE_URL = "https://api.stlouisfed.org/fred"


def fetch_series_observations(series_id: str, start_date: str | None = None) -> dict:
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise RuntimeError("Missing FRED_API_KEY env var")

    url = f"{BASE_URL}/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
    }
    if start_date:
        params["observation_start"] = start_date
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()
