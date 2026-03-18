#!/usr/bin/env python3
"""
Economic Data Discord Webhook Poster
Sources:
- FRED (GDP, CPI, Unemployment, Fed Funds Rate)
- Alpha Vantage (WTI and Brent crude)

Features:
- Pulls latest data
- Builds a 12-month trend chart
- Posts a Discord embed + chart image to a webhook
- Safe defaults and simple scheduling via cron / Task Scheduler

Environment variables:
    DISCORD_WEBHOOK_URL=...
    FRED_API_KEY=...
    ALPHAVANTAGE_API_KEY=...
"""
from __future__ import annotations

import io
import os
import sys
import math
import json
import textwrap
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import requests
import matplotlib.pyplot as plt

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
ALPHA_BASE = "https://www.alphavantage.co/query"

TIMEOUT = 30

SERIES = {
    "GDP": {"series_id": "GDPC1", "label": "Real GDP", "units": "Billions (chained 2017$)"},
    "CPI": {"series_id": "CPIAUCSL", "label": "CPI", "units": "Index 1982-84=100"},
    "UNRATE": {"series_id": "UNRATE", "label": "Unemployment", "units": "%"},
    "FEDFUNDS": {"series_id": "FEDFUNDS", "label": "Fed Funds", "units": "%"},
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


def fred_observations(api_key: str, series_id: str, limit: int = 24) -> List[Point]:
    params = {
        "api_key": api_key,
        "file_type": "json",
        "series_id": series_id,
        "sort_order": "asc",
        "limit": limit,
    }
    r = requests.get(FRED_BASE, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    payload = r.json()
    out: List[Point] = []
    for obs in payload.get("observations", []):
        value = obs.get("value")
        if value in (".", None, ""):
            continue
        try:
            dt = datetime.strptime(obs["date"], "%Y-%m-%d")
            out.append(Point(date=dt, value=float(value)))
        except Exception:
            continue
    if not out:
        raise RuntimeError(f"No usable data returned for FRED series {series_id}")
    return out


def alpha_commodity(api_key: str, symbol: str, interval: str = "monthly") -> List[Point]:
    params = {
        "function": symbol,
        "interval": interval,
        "apikey": api_key,
    }
    r = requests.get(ALPHA_BASE, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    payload = r.json()

    # Alpha Vantage commodity responses usually store the series in "data"
    # with entries like {"date":"2026-03-01", "value":"67.32"}.
    series = payload.get("data", [])
    if not isinstance(series, list) or not series:
        note = payload.get("Note") or payload.get("Information") or payload.get("Error Message")
        raise RuntimeError(f"Unexpected Alpha Vantage response for {symbol}: {note or payload}")

    out: List[Point] = []
    for item in series:
        value = item.get("value")
        if value in (".", None, ""):
            continue
        try:
            dt = datetime.strptime(item["date"], "%Y-%m-%d")
            out.append(Point(date=dt, value=float(value)))
        except Exception:
            continue

    out.sort(key=lambda p: p.date)
    if not out:
        raise RuntimeError(f"No usable data returned for Alpha Vantage commodity {symbol}")
    return out


def pct_change(new: float, old: float) -> Optional[float]:
    if old == 0:
        return None
    return ((new - old) / old) * 100.0


def latest_and_prev(points: List[Point]) -> Tuple[Point, Optional[Point]]:
    if not points:
        raise RuntimeError("No points available")
    if len(points) == 1:
        return points[-1], None
    return points[-1], points[-2]


def fmt_num(val: float, suffix: str = "") -> str:
    if abs(val) >= 1000:
        return f"{val:,.1f}{suffix}"
    return f"{val:,.2f}{suffix}" if abs(val) < 100 else f"{val:,.1f}{suffix}"


def change_text(current: float, previous: Optional[float], suffix: str = "") -> str:
    if previous is None:
        return "n/a"
    chg = current - previous
    pct = pct_change(current, previous)
    if pct is None:
        return f"{chg:+.2f}{suffix}"
    return f"{chg:+.2f}{suffix} ({pct:+.2f}%)"


def build_chart(gdp: List[Point], cpi: List[Point], unrate: List[Point], fedfunds: List[Point], wti: List[Point], brent: List[Point]) -> bytes:
    # Separate plot only; no custom style/colors.
    plt.figure(figsize=(12, 7))
    for label, points in [
        ("Real GDP", gdp[-12:]),
        ("CPI", cpi[-12:]),
        ("Unemployment", unrate[-12:]),
        ("Fed Funds", fedfunds[-12:]),
        ("WTI", wti[-12:]),
        ("Brent", brent[-12:]),
    ]:
        xs = [p.date for p in points]
        ys = [p.value for p in points]
        plt.plot(xs, ys, marker="o", label=label)

    plt.title("Economic Data Trends (latest 12 observations)")
    plt.xlabel("Date")
    plt.ylabel("Value")
    plt.legend()
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=160)
    plt.close()
    buf.seek(0)
    return buf.read()


def discord_payload(summary: dict) -> dict:
    lines = []
    for item in summary["rows"]:
        lines.append(
            f"**{item['name']}**\n"
            f"Latest: {item['latest_text']}\n"
            f"Prev: {item['prev_text']}\n"
            f"Change: {item['change_text']}\n"
            f"Updated: {item['date_text']}"
        )

    description = "\n\n".join(lines)
    now = datetime.now(timezone.utc).isoformat()

    return {
        "embeds": [
            {
                "title": "Economic Snapshot",
                "description": description[:4096],
                "footer": {"text": "Sources: FRED + Alpha Vantage"},
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

    gdp = fred_observations(fred_key, SERIES["GDP"]["series_id"], limit=24)
    cpi = fred_observations(fred_key, SERIES["CPI"]["series_id"], limit=24)
    unrate = fred_observations(fred_key, SERIES["UNRATE"]["series_id"], limit=24)
    fedfunds = fred_observations(fred_key, SERIES["FEDFUNDS"]["series_id"], limit=24)
    wti = alpha_commodity(alpha_key, "WTI", interval="monthly")
    brent = alpha_commodity(alpha_key, "BRENT", interval="monthly")

    rows = []
    for name, points, suffix in [
        ("Real GDP", gdp, ""),
        ("CPI", cpi, ""),
        ("Unemployment", unrate, "%"),
        ("Fed Funds", fedfunds, "%"),
        ("WTI Oil", wti, "$"),
        ("Brent Oil", brent, "$"),
    ]:
        latest, prev = latest_and_prev(points)
        rows.append({
            "name": name,
            "latest_text": fmt_num(latest.value, suffix),
            "prev_text": fmt_num(prev.value, suffix) if prev else "n/a",
            "change_text": change_text(latest.value, prev.value if prev else None, suffix),
            "date_text": latest.date.strftime("%Y-%m-%d"),
        })

    payload = discord_payload({"rows": rows})
    chart = build_chart(gdp, cpi, unrate, fedfunds, wti, brent)
    post_to_discord(webhook, payload, chart)
    print("Posted economic snapshot to Discord.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
