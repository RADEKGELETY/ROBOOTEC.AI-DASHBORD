import requests

BASE_URL = "https://data-api.ecb.europa.eu/service/data"


def fetch_series(flow_ref: str, key: str, params: dict | None = None) -> str:
    url = f"{BASE_URL}/{flow_ref}/{key}"
    headers = {"Accept": "text/csv"}
    resp = requests.get(url, params=params or {}, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.text
