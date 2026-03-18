#!/usr/bin/env python3
from __future__ import annotations

import base64
import csv
import io
import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Tuple

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

DISPLAY_META = {
    "wti": {
        "name": "WTI Oil",
        "cadence": "Daily",
        "prefix": "$",
        "suffix": "",
    }
}

FIELDNAMES = [
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

MACRO_KEYS = ["gdp", "unemployment", "cpi"]
COST_KEYS = ["mortgage", "gas", "wti"]
DISPLAY_ORDER = ["gdp", "unemployment", "cpi", "mortgage", "gas", "wti"]


@dataclass
class Point:
    date: datetime
    value: float


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def optional_env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def fmt_num(val: Optional[float], prefix: str = "", suffix: str = "") -> str:
    if val is None:
        return "n/a"
    if abs(val) >= 1000:
        return f"{prefix}{val:,.1f}{suffix}"
    if abs(val) >= 100:
        return f"{prefix}{val:,.1f}{suffix}"
    return f"{prefix}{val:,.2f}{suffix}"


def pct_change(new: float, old: float) -> Optional[float]:
    if old == 0:
        return None
    return ((new - old) / old) * 100.0


def trend_arrow(delta: Optional[float]) -> str:
    if delta is None:
        return "➡️"
    if delta > 0:
        return "📈"
    if delta < 0:
        return "📉"
    return "➡️"


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


def github_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }


def fetch_history_from_github(repo: str, branch: str, token: str) -> List[Dict[str, str]]:
    url = f"https://api.github.com/repos/{repo}/contents/{HISTORY_FILE}"
    params = {"ref": branch}

    r = requests.get(url, headers=github_headers(token), params=params, timeout=TIMEOUT)

    if r.status_code == 404:
        return []

    r.raise_for_status()
    payload = r.json()
    content = payload.get("content", "")
    decoded = base64.b64decode(content).decode("utf-8")

    rows: List[Dict[str, str]] = []
    reader = csv.DictReader(io.StringIO(decoded))
    for row in reader:
        rows.append(row)
    return rows


def push_history_to_github(repo: str, branch: str, token: str, rows: List[Dict[str, str]]) -> None:
    existing_sha = None
    get_url = f"https://api.github.com/repos/{repo}/contents/{HISTORY_FILE}"
    params = {"ref": branch}

    existing = requests.get(get_url, headers=github_headers(token), params=params, timeout=TIMEOUT)
    if existing.status_code == 200:
        existing_sha = existing.json().get("sha")
    elif existing.status_code != 404:
        existing.raise_for_status()

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=FIELDNAMES)
    writer.writeheader()
    writer.writerows(rows)
    raw_csv = buf.getvalue()

    payload = {
        "message": f"Update economic history for {date.today().isoformat()}",
        "content": base64.b64encode(raw_csv.encode("utf-8")).decode("utf-8"),
        "branch": branch,
    }
    if existing_sha:
        payload["sha"] = existing_sha

    put_url = f"https://api.github.com/repos/{repo}/contents/{HISTORY_FILE}"
    r = requests.put(put_url, headers=github_headers(token), json=payload, timeout=TIMEOUT)
    r.raise_for_status()


def parse_float(value: Optional[str]) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def upsert_today_snapshot(rows: List[Dict[str, str]], data: Dict[str, Point]) -> List[Dict[str, str]]:
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
    return rows


def normalized_series(history_rows: List[Dict[str, str]], key: str, limit: int = 90) -> Tuple[List[datetime], List[float]]:
    xs: List[datetime] = []
    ys: List[float] = []

    for row in history_rows[-limit:]:
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
        return [], []

    base = ys[0]
    if base == 0:
        return [], []

    normalized = [(y / base) * 100.0 for y in ys]
    return xs, normalized


def make_chart(history_rows: List[Dict[str, str]], keys: List[str], title: str) -> bytes:
    plt.figure(figsize=(12, 7))
    plotted_any = False

    for key in keys:
        xs, ys = normalized_series(history_rows, key)
        if not xs or not ys:
            continue

        label = FRED_SERIES[key]["name"] if key in FRED_SERIES else DISPLAY_META[key]["name"]
        plt.plot(xs, ys, marker="o", label=label)
        plotted_any = True

    if plotted_any:
        plt.title(title)
        plt.xlabel("Snapshot Date")
        plt.ylabel("Normalized Index (base = 100)")
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


def latest_change_from_history(history_rows: List[Dict[str, str]], key: str) -> Tuple[Optional[float], Optional[float]]:
    vals = []
    for row in history_rows[-8:]:
        val = parse_float(row.get(key))
        if val is not None:
            vals.append(val)

    if not vals:
        return None, None
    if len(vals) == 1:
        return vals[-1], None
    return vals[-1], vals[-1] - vals[-2]


def metric_meta(key: str) -> dict:
    if key in FRED_SERIES:
        return FRED_SERIES[key]
    return DISPLAY_META[key]


def build_embed_rows(data: Dict[str, Point], history_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    rows = []

    for key in DISPLAY_ORDER:
        meta = metric_meta(key)
        point = data[key]

        _, delta = latest_change_from_history(history_rows, key)
        arrow = trend_arrow(delta)

        delta_text = "n/a"
        if delta is not None:
            delta_text = f"{arrow} {fmt_num(delta, prefix=meta['prefix'], suffix=meta['suffix'])}"

        rows.append({
            "name": f"{meta['name']} ({meta['cadence']})",
            "latest_text": fmt_num(point.value, prefix=meta["prefix"], suffix=meta["suffix"]),
            "updated_text": point.date.strftime("%Y-%m-%d"),
            "trend_text": delta_text,
        })

    return rows


def build_summary(history_rows: List[Dict[str, str]]) -> str:
    bits = []

    for key in ["wti", "gas", "mortgage", "unemployment"]:
        meta = metric_meta(key)
        latest, delta = latest_change_from_history(history_rows, key)
        if latest is None:
            continue

        arrow = trend_arrow(delta)
        if delta is None or delta == 0:
            bits.append(f"{meta['name']} {arrow} flat")
        else:
            bits.append(f"{meta['name']} {arrow} {fmt_num(abs(delta), prefix=meta['prefix'], suffix=meta['suffix'])}")

    return " • ".join(bits[:4]) if bits else "Daily update posted."


def post_to_discord_single(webhook_url: str, payload: dict, image_bytes: bytes, filename: str) -> None:
    files = {
        "file": (filename, image_bytes, "image/png"),
        "payload_json": (None, json.dumps(payload), "application/json"),
    }
    r = requests.post(webhook_url, files=files, timeout=TIMEOUT)
    r.raise_for_status()


def discord_payload(rows: List[Dict[str, str]], summary: str, title: str, image_name: str) -> dict:
    description_parts = [f"**Summary**\n{summary}"]

    for item in rows:
        description_parts.append(
            f"**{item['name']}**\n"
            f"Latest: {item['latest_text']}\n"
            f"Trend: {item['trend_text']}\n"
            f"Updated: {item['updated_text']}"
        )

    return {
        "embeds": [
            {
                "title": title,
                "description": "\n\n".join(description_parts)[:4096],
                "footer": {
                    "text": "Bot runs daily • Metrics update on their own cadence"
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "image": {"url": f"attachment://{image_name}"},
            }
        ]
    }


def main() -> int:
    webhook = require_env("DISCORD_WEBHOOK_URL")
    fred_key = require_env("FRED_API_KEY")
    alpha_key = require_env("ALPHAVANTAGE_API_KEY")
    github_token = require_env("GITHUB_TOKEN")
    github_repo = require_env("GITHUB_REPO")
    github_branch = optional_env("GITHUB_BRANCH", "main")

    data: Dict[str, Point] = {}

    data["gdp"] = fred_latest_observation(fred_key, FRED_SERIES["gdp"]["series_id"])
    data["unemployment"] = fred_latest_observation(fred_key, FRED_SERIES["unemployment"]["series_id"])
    data["cpi"] = fred_latest_observation(fred_key, FRED_SERIES["cpi"]["series_id"])
    data["mortgage"] = fred_latest_observation(fred_key, FRED_SERIES["mortgage"]["series_id"])
    data["gas"] = fred_latest_observation(fred_key, FRED_SERIES["gas"]["series_id"])

    time.sleep(15)
    data["wti"] = alpha_wti_latest(alpha_key)

    history_rows = fetch_history_from_github(github_repo, github_branch, github_token)
    history_rows = upsert_today_snapshot(history_rows, data)
    push_history_to_github(github_repo, github_branch, github_token, history_rows)

    rows = build_embed_rows(data, history_rows)
    summary = build_summary(history_rows)

    macro_chart = make_chart(history_rows, MACRO_KEYS, "Macro Trends (Normalized, base = 100)")
    cost_chart = make_chart(history_rows, COST_KEYS, "Cost Trends (Normalized, base = 100)")

    macro_payload = discord_payload(rows[:3], summary, "Economic Snapshot — Macro", "macro_trends.png")
    cost_payload = discord_payload(rows[3:], summary, "Economic Snapshot — Costs", "cost_trends.png")

    post_to_discord_single(webhook, macro_payload, macro_chart, "macro_trends.png")
    post_to_discord_single(webhook, cost_payload, cost_chart, "cost_trends.png")

    print("Posted economic snapshot to Discord.")
    print(f"History rows stored in GitHub: {len(history_rows)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
