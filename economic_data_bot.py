#!/usr/bin/env python3
"""
Economic Data Discord Webhook Poster
Sources:
- FRED (GDP, CPI, Unemployment, Fed Funds Rate)
- Alpha Vantage (WTI and Brent crude)

Environment variables:
    DISCORD_WEBHOOK_URL=...
    FRED_API_KEY=...
    ALPHAVANTAGE_API_KEY=...
"""

from __future__ import annotations

import io
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import matplotlib.pyplot as plt
import requests

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
ALPHA_BASE = "https://www.alphavantage.co/query"
TIMEOUT = 30

SERIES = {
    "GDP": {"series_id": "GDPC1", "label": "Real GDP"},
    "CPI": {"series_id": "CPIAUCSL", "label": "CPI"},
    "UNRATE": {"series_id": "UNRATE", "label": "Unemployment"},
    "FEDFUNDS": {"series_id": "FEDFUNDS", "label": "Fed Funds"},
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
        "sort_order": "desc",
        "limit": limit,
    }

    last_error = None
    for attempt in range(5):
        try:
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

        except Exception as e:
            last_error = e
            time.sleep(2 * (attempt + 1))

    raise RuntimeError(f"FRED failed for {series_id}: {last_error}")


def alpha_commodity(api_key: str, symbol: str, interval: str = "monthly") -> List[Point]:
    params = {
        "function": symbol,
        "interval": interval,
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

        except Exception as e:
            last_error = e
            time.sleep(15)

    raise RuntimeError(f"Alpha Vantage failed for {symbol}: {last_error}")


def latest_and_prev(points: List[Point]) -> Tuple[Point, Optional[Point]]:
    if not points:
        raise RuntimeError("No points available")
    if len(points) == 1:
        return points[-1], None
    return points[-1], points[-2]


def pct_change(new: float, old: float) -> Optional[float]:
    if old == 0:
        return None
    return ((new - old) / old) * 100.0


def fmt_num(val: float, prefix: str = "", suffix: str = "") -> str:
    if abs(val) >= 1000:
        return f"{prefix}{val:,.1f}{suffix}"
    if abs(val) >= 100:
        return f"{prefix}{val:,.1f}{suffix}"
    return f"{prefix}{val:,.2f}{suffix}"


def change_text(current: float, previous: Optional[float], prefix: str = "", suffix: str = "") -> str:
    if previous is None:
        return "n/a"
    chg = current - previous
    pct = pct_change(current, previous)
    if pct is None:
        return f"{prefix}{chg:+.2f}{suffix}"
    return f"{prefix}{chg:+.2f}{suffix} ({pct:+.2f}%)"


def safe_fetch(name: str, fetch_func):
    try:
        return fetch_func(), None
    except Exception as e:
        return None, f"{name}: {e}"


def build_chart(series_map: dict) -> bytes:
    plt.figure(figsize=(12, 7))

    plotted_any = False
    for label, points in series_map.items():
        if not points:
            continue
        xs = [p.date for p in points[-12:]]
        ys = [p.value for p in points[-12:]]
        if xs and ys:
            plt.plot(xs, ys, marker="o", label=label)
            plotted_any = True

    if not plotted_any:
        plt.text(0.5, 0.5, "No chart data available", ha="center", va="center")
        plt.axis("off")
    else:
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


def discord_payload(rows: List[dict], errors: List[str]) -> dict:
    description_parts = []

    if rows:
        for item in rows:
            description_parts.append(
                f"**{item['name']}**\n"
                f"Latest: {item['latest_text']}\n"
                f"Prev: {item['prev_text']}\n"
                f"Change: {item['change_text']}\n"
                f"Updated: {item['date_text']}"
            )

    if errors:
        description_parts.append("**Warnings**\n" + "\n".join(f"- {e}" for e in errors[:10]))

    description = "\n\n".join(description_parts)[:4096]
    now = datetime.now(timezone.utc).isoformat()

    return {
        "embeds": [
            {
                "title": "Economic Snapshot",
                "description": description or "No data available.",
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

    errors: List[str] = []

    gdp, err = safe_fetch("Real GDP", lambda: fred_observations(fred_key, SERIES["GDP"]["series_id"], limit=24))
    if err:
        errors.append(err)

    cpi, err = safe_fetch("CPI", lambda: fred_observations(fred_key, SERIES["CPI"]["series_id"], limit=24))
    if err:
        errors.append(err)

    unrate, err = safe_fetch("Unemployment", lambda: fred_observations(fred_key, SERIES["UNRATE"]["series_id"], limit=24))
    if err:
        errors.append(err)

    fedfunds, err = safe_fetch("Fed Funds", lambda: fred_observations(fred_key, SERIES["FEDFUNDS"]["series_id"], limit=24))
    if err:
        errors.append(err)

    wti, err = safe_fetch("WTI Oil", lambda: alpha_commodity(alpha_key, "WTI", interval="monthly"))
    if err:
        errors.append(err)

    time.sleep(15)

    brent, err = safe_fetch("Brent Oil", lambda: alpha_commodity(alpha_key, "BRENT", interval="monthly"))
    if err:
        errors.append(err)

    rows = []

    for name, points, prefix, suffix in [
        ("Real GDP", gdp, "", ""),
        ("CPI", cpi, "", ""),
        ("Unemployment", unrate, "", "%"),
        ("Fed Funds", fedfunds, "", "%"),
        ("WTI Oil", wti, "$", ""),
        ("Brent Oil", brent, "$", ""),
    ]:
        if not points:
            continue

        latest, prev = latest_and_prev(points)
        rows.append(
            {
                "name": name,
                "latest_text": fmt_num(latest.value, prefix=prefix, suffix=suffix),
                "prev_text": fmt_num(prev.value, prefix=prefix, suffix=suffix) if prev else "n/a",
                "change_text": change_text(latest.value, prev.value if prev else None, prefix=prefix, suffix=suffix),
                "date_text": latest.date.strftime("%Y-%m-%d"),
            }
        )

    if not rows and errors:
        raise RuntimeError("All data sources failed:\n" + "\n".join(errors))

    chart = build_chart(
        {
            "Real GDP": gdp,
            "CPI": cpi,
            "Unemployment": unrate,
            "Fed Funds": fedfunds,
            "WTI": wti,
            "Brent": brent,
        }
    )

    payload = discord_payload(rows, errors)
    post_to_discord(webhook, payload, chart)

    print("Posted economic snapshot to Discord.")
    if errors:
        print("Completed with warnings:")
        for e in errors:
            print("-", e)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
