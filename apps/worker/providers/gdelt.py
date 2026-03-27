import requests

MASTERLIST_URL = "https://data.gdeltproject.org/gdeltv2/masterfilelist.txt"


def fetch_latest_gkg_url() -> str:
    resp = requests.get(MASTERLIST_URL, timeout=30)
    resp.raise_for_status()
    lines = resp.text.splitlines()
    # Each line: <size> <md5> <url>
    gkg_lines = [ln for ln in lines if ln.endswith(".gkg.csv.zip")]
    if not gkg_lines:
        raise RuntimeError("No GKG entries found in masterfilelist")
    latest = gkg_lines[-1]
    return latest.split()[-1]
