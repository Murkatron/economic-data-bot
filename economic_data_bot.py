import base64
import csv
import io
import os
import requests
from datetime import datetime, timedelta

FRED_API_KEY = os.getenv("FRED_API_KEY")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")

START_DATE = "2024-11-05"
END_DATE = datetime.today().strftime("%Y-%m-%d")

FRED_SERIES = {
    "gdp": "GDPC1",
    "unemployment": "UNRATE",
    "cpi": "CPIAUCSL",
    "fedfunds": "FEDFUNDS",
    "ust10y": "GS10",
    "sentiment": "UMCSENT",
    "mortgage": "MORTGAGE30US",
    "gas": "GASREGW",
    "sp500": "SP500",
    "debt": "GFDEBTN",
}

ALPHA_FUNCTIONS = {
    "wti": "WTI",
    "brent": "BRENT",
}

def fetch_fred_series(series_id):
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": START_DATE,
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()["observations"]

    cleaned = []
    for obs in data:
        if obs["value"] == ".":
            continue
        cleaned.append((obs["date"], float(obs["value"])))

    return cleaned

def fetch_alpha_series(function):
    url = "https://www.alphavantage.co/query"
    params = {
        "function": function,
        "interval": "daily",
        "apikey": os.getenv("ALPHAVANTAGE_API_KEY"),
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json().get("data", [])

    cleaned = []
    for row in data:
        if row["value"] == ".":
            continue
        cleaned.append((row["date"], float(row["value"])))

    return cleaned

def build_daily_series(series_data):
    series_data.sort()
    result = {}

    current_value = None
    idx = 0

    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.today()

    d = start
    while d <= end:
        while idx < len(series_data) and series_data[idx][0] <= d.strftime("%Y-%m-%d"):
            current_value = series_data[idx][1]
            idx += 1

        result[d.strftime("%Y-%m-%d")] = current_value
        d += timedelta(days=1)

    return result

def main():
    print("Fetching FRED data...")
    fred_data = {}
    for key, series in FRED_SERIES.items():
        fred_data[key] = build_daily_series(fetch_fred_series(series))

    print("Fetching Alpha data...")
    alpha_data = {}
    for key, fn in ALPHA_FUNCTIONS.items():
        alpha_data[key] = build_daily_series(fetch_alpha_series(fn))

    print("Building rows...")
    rows = []

    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.today()

    d = start
    while d <= end:
        day = d.strftime("%Y-%m-%d")

        row = {"snapshot_date": day}

        for key in FRED_SERIES:
            row[key] = fred_data[key].get(day)
            row[f"{key}_updated"] = day

        for key in ALPHA_FUNCTIONS:
            row[key] = alpha_data[key].get(day)
            row[f"{key}_updated"] = day

        rows.append(row)
        d += timedelta(days=1)

    print(f"Built {len(rows)} rows")

    print("Uploading to GitHub...")

    fieldnames = list(rows[0].keys())
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

    content = base64.b64encode(buf.getvalue().encode()).decode()

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/history.csv"

    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }

    payload = {
        "message": "Backfill history.csv",
        "content": content,
        "branch": "main",
    }

    r = requests.put(url, headers=headers, json=payload)
    r.raise_for_status()

    print("DONE: history.csv backfilled")

if __name__ == "__main__":
    main()
