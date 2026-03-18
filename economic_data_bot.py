#!/usr/bin/env python3
"""
Simple Economic Data Discord Webhook Poster

Tracks:
- Real GDP (Quarterly)            -> FRED GDPC1
- Unemployment (Monthly)          -> FRED UNRATE
- Inflation / CPI (Monthly)       -> FRED CPIAUCSL
- Mortgage Rate (Weekly)          -> FRED MORTGAGE30US
- Gas Price (Weekly)              -> FRED GASREGW
- WTI Oil (Daily-ish market data) -> Alpha Vantage WTI commodity endpoint

What it does:
1. Fetch latest values
2. Save one snapshot per day into history.csv
3. Build a trend chart from that saved history
4. Post an embed + chart to Discord webhook

Environment variables:
    DISCORD_WEBHOOK_URL=...
    FRED_API_KEY=...
    ALPHAVANTAGE_API_KEY=...
"""

from __future__ import annotations

import csv
import io
import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import requests

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
ALPHA_BASE = "https://www.alphavantage.co/query"
TIMEOUT = 30
HISTORY_FILE = "history.csv"

FRED_SERIES = {
    "gdp": {
        "series_id": "GDPC1",
        "name": "Real GDP",
        "cadence": "Quarterly",
        "prefix": "",
        "suffix": "",
    },
    "unemployment": {
        "series_id": "UNRATE",
        "name": "Unemployment",
        "cadence": "Monthly",
        "prefix": "",
        "suffix": "%",
    },
    "cpi": {
        "series_id": "CPIAUCSL",
        "name": "Inflation / CPI",
        "cadence": "Monthly",
        "prefix": "",
        "suffix": "",
    },
    "mortgage": {
        "series_id": "MORTGAGE30US",
        "name": "Mortgage Rate",
        "cadence": "Weekly",
        "prefix": "",
        "suffix": "%",
    },
    "gas": {
        "series_id": "GASREGW",
        "name": "Gas Price",
        "cadence": "Weekly",
        "prefix": "$",
        "suffix": "",
    },
}

DISPLAY_ORDER = ["gdp", "unemployment", "cpi", "mortgage", "gas", "wti"]

DISPLAY_META = {
    "wti": {
        "name": "WTI Oil",
        "cadence": "Daily",
        "prefix": "$",
        "suffix": "",
    }
}


@dataclass
class Point:
    date: datetime
    value: float


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def fmt_num(val: Optional[float], prefix: str = "", suffix: str = "") -> str:
    if val is None:
        return "n/a"
    if abs(val) >= 1000:
        return f"{prefix}{val:,.1f}{suffix}"
    if abs(val) >= 100:
        return f"{prefix}{val:,.1f}{suffix}"
    return f"{prefix}{val:,.2f}{suffix}"


def fred_latest_observation(api_key: str, series_id: str) -> Point:
    params = {
        "api_key": api_key,
        "file_type": "json",
        "series_id": series_id,
        "sort_order": "desc",
        "limit": 1,
    }

    last_error = None
    for attempt in range(5):
        try:
            r = requests.get(FRED_BASE, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            payload = r.json()

            observations = payload.get("observations", [])
            for obs in observations:
                value = obs.get("value")
                if value in (".", "", None):
                    continue
                dt = datetime.strptime(obs["date"], "%Y-%m-%d")
                return Point(date=dt, value=float(value))

            raise RuntimeError(f"No usable data returned for FRED series {series_id}")
        except Exception as e:
            last_error = e
            time.sleep(2 * (attempt + 1))

    raise RuntimeError(f"FRED failed for {series_id}: {last_error}")


def alpha_wti_latest(api_key: str) -> Point:
    params = {
        "function": "WTI",
        "interval": "daily",
        "apikey": api_key,
    }

    last_error = None
    for attempt in range(5):
        try:
            r = requests.get(ALPHA_BASE, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            payload = r.json()

            series = payload.get("data", [])
            if not isinstance(series, list) or not series:
                note = payload.get("Note") or payload.get("Information") or payload.get("Error Message")
                if note and "Please consider spreading out your free API requests" in str(note):
                    time.sleep(15)
                    continue
                raise RuntimeError(f"Unexpected Alpha Vantage response: {note or payload}")

            valid_rows = []
            for item in series:
                value = item.get("value")
                if value in (".", "", None):
                    continue
                try:
                    dt = datetime.strptime(item["date"], "%Y-%m-%d")
                    valid_rows.append(Point(date=dt, value=float(value)))
                except Exception:
                    continue

            if not valid_rows:
                raise RuntimeError("No usable data returned for WTI")

            valid_rows.sort(key=lambda p: p.date)
            return valid_rows[-1]
        except Exception as e:
            last_error = e
            time.sleep(15)

    raise RuntimeError(f"Alpha Vantage WTI failed: {last_error}")


def ensure_history_file() -> None:
    if os.path.exists(HISTORY_FILE):
        return

    with open(HISTORY_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "snapshot_date",
            "gdp",
            "gdp_updated",
            "unemployment",
            "unemployment_updated",
            "cpi",
            "cpi_updated",
            "mortgage",
            "mortgage_updated",
            "gas",
            "gas_updated",
            "wti",
            "wti_updated",
        ])


def load_history() -> List[Dict[str, str]]:
    ensure_history_file()
    rows: List[Dict[str, str]] = []

    with open(HISTORY_FILE, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    return rows


def save_history(rows: List[Dict[str, str]]) -> None:
    fieldnames = [
        "snapshot_date",
        "gdp",
        "gdp_updated",
        "unemployment",
        "unemployment_updated",
        "cpi",
        "cpi_updated",
        "mortgage",
        "mortgage_updated",
        "gas",
        "gas_updated",
        "wti",
        "wti_updated",
    ]

    with open(HISTORY_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def upsert_today_snapshot(data: Dict[str, Point]) -> List[Dict[str, str]]:
    rows = load_history()
    today = date.today().isoformat()

    snapshot = {
        "snapshot_date": today,
        "gdp": str(data["gdp"].value),
        "gdp_updated": data["gdp"].date.strftime("%Y-%m-%d"),
        "unemployment": str(data["unemployment"].value),
        "unemployment_updated": data["unemployment"].date.strftime("%Y-%m-%d"),
        "cpi": str(data["cpi"].value),
        "cpi_updated": data["cpi"].date.strftime("%Y-%m-%d"),
        "mortgage": str(data["mortgage"].value),
        "mortgage_updated": data["mortgage"].date.strftime("%Y-%m-%d"),
        "gas": str(data["gas"].value),
        "gas_updated": data["gas"].date.strftime("%Y-%m-%d"),
        "wti": str(data["wti"].value),
        "wti_updated": data["wti"].date.strftime("%Y-%m-%d"),
    }

    replaced = False
    for i, row in enumerate(rows):
        if row.get("snapshot_date") == today:
            rows[i] = snapshot
            replaced = True
            break

    if not replaced:
        rows.append(snapshot)

    rows.sort(key=lambda r: r["snapshot_date"])
    save_history(rows)
    return rows


def parse_float(value: Optional[str]) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def build_chart(history_rows: List[Dict[str, str]]) -> bytes:
    plt.figure(figsize=(12, 7))

    metric_keys = ["gdp", "unemployment", "cpi", "mortgage", "gas", "wti"]
    plotted_any = False

    # Normalize each series to first available value = 100 so different scales are comparable
    for key in metric_keys:
        xs: List[datetime] = []
        ys: List[float] = []

        for row in history_rows[-90:]:
            val = parse_float(row.get(key))
            if val is None:
                continue
            try:
                dt = datetime.strptime(row["snapshot_date"], "%Y-%m-%d")
            except Exception:
                continue
            xs.append(dt)
            ys.append(val)

        if len(xs) < 2:
            continue

        base = ys[0]
        if base == 0:
            continue

        normalized = [(y / base) * 100.0 for y in ys]
        label = (
            FRED_SERIES[key]["name"] if key in FRED_SERIES
            else DISPLAY_META[key]["name"]
        )

        plt.plot(xs, normalized, marker="o", label=label)
        plotted_any = True

    if plotted_any:
        plt.title("Economic Trends (Normalized, base = 100)")
        plt.xlabel("Snapshot Date")
        plt.ylabel("Normalized Index")
        plt.legend()
    else:
        plt.text(0.5, 0.5, "Not enough history yet for a trend chart", ha="center", va="center")
        plt.axis("off")

    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=160)
    plt.close()
    buf.seek(0)
    return buf.read()


def build_embed_rows(data: Dict[str, Point]) -> List[Dict[str, str]]:
    rows = []

    for key in DISPLAY_ORDER:
        if key == "wti":
            meta = DISPLAY_META["wti"]
            point = data["wti"]
        else:
            meta = FRED_SERIES[key]
            point = data[key]

        rows.append({
            "name": f"{meta['name']} ({meta['cadence']})",
            "latest_text": fmt_num(point.value, prefix=meta["prefix"], suffix=meta["suffix"]),
            "updated_text": point.date.strftime("%Y-%m-%d"),
        })

    return rows


def discord_payload(rows: List[Dict[str, str]]) -> dict:
    description_parts = []

    for item in rows:
        description_parts.append(
            f"**{item['name']}**\n"
            f"Latest: {item['latest_text']}\n"
            f"Updated: {item['updated_text']}"
        )

    description = "\n\n".join(description_parts)[:4096]
    now = datetime.now(timezone.utc).isoformat()

    return {
        "embeds": [
            {
                "title": "Economic Snapshot",
                "description": description,
                "footer": {
                    "text": "Bot runs daily • Some metrics update weekly/monthly/quarterly"
                },
                "timestamp": now,
                "image": {"url": "attachment://economic_trends.png"},
            }
        ]
    }


def post_to_discord(webhook_url: str, payload: dict, image_bytes: bytes) -> None:
    files = {
        "file": ("economic_trends.png", image_bytes, "image/png"),
        "payload_json": (None, json.dumps(payload), "application/json"),
    }
    r = requests.post(webhook_url, files=files, timeout=TIMEOUT)
    r.raise_for_status()


def main() -> int:
    webhook = require_env("DISCORD_WEBHOOK_URL")
    fred_key = require_env("FRED_API_KEY")
    alpha_key = require_env("ALPHAVANTAGE_API_KEY")

    data: Dict[str, Point] = {}

    data["gdp"] = fred_latest_observation(fred_key, FRED_SERIES["gdp"]["series_id"])
    data["unemployment"] = fred_latest_observation(fred_key, FRED_SERIES["unemployment"]["series_id"])
    data["cpi"] = fred_latest_observation(fred_key, FRED_SERIES["cpi"]["series_id"])
    data["mortgage"] = fred_latest_observation(fred_key, FRED_SERIES["mortgage"]["series_id"])
    data["gas"] = fred_latest_observation(fred_key, FRED_SERIES["gas"]["series_id"])

    time.sleep(15)  # helps avoid free-tier Alpha Vantage rate-limit weirdness
    data["wti"] = alpha_wti_latest(alpha_key)

    history_rows = upsert_today_snapshot(data)
    chart = build_chart(history_rows)
    rows = build_embed_rows(data)
    payload = discord_payload(rows)

    post_to_discord(webhook, payload, chart)

    print("Posted economic snapshot to Discord.")
    print(f"History rows stored: {len(history_rows)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
