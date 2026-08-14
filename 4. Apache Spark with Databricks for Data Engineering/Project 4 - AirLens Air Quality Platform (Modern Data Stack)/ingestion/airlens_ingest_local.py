"""AirLens local ingestion: fetch 3 air quality APIs, land JSON, upload to the lakehouse.

Why this runs on your laptop and not in a Databricks notebook: serverless
compute on Free Edition sits behind a domain allowlist, so two of our three
APIs are unreachable from the warehouse. That is not a bug, it is an
architecture lesson: extraction belongs at the edge (your machine, later
Airflow), storage and transformation belong in the lakehouse.

Usage:
  export OPENWEATHER_KEY=... WAQI_TOKEN=... OPENAQ_KEY=...
  export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
  python3 airlens_ingest_local.py            # fetch all three sources + upload
  python3 airlens_ingest_local.py --no-upload  # fetch only, inspect ./landing first

Auth: first run opens your browser for Databricks OAuth (no tokens in code).
"""

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

OPENWEATHER_KEY = os.environ.get("OPENWEATHER_KEY", "")
WAQI_TOKEN = os.environ.get("WAQI_TOKEN", "")
OPENAQ_KEY = os.environ.get("OPENAQ_KEY", "")
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")

VOLUME_ROOT = "/Volumes/airlens/core/landing"
LOCAL_LANDING = Path(__file__).parent / "landing"

CITIES = [
    ("Delhi", "IN", 28.6139, 77.2090),
    ("Mumbai", "IN", 19.0760, 72.8777),
    ("Bengaluru", "IN", 12.9716, 77.5946),
    ("Kolkata", "IN", 22.5726, 88.3639),
    ("Chennai", "IN", 13.0827, 80.2707),
    ("Hyderabad", "IN", 17.3850, 78.4867),
    ("Beijing", "CN", 39.9042, 116.4074),
    ("Shanghai", "CN", 31.2304, 121.4737),
    ("Tokyo", "JP", 35.6762, 139.6503),
    ("Jakarta", "ID", -6.2088, 106.8456),
    ("Bangkok", "TH", 13.7563, 100.5018),
    ("Dubai", "AE", 25.2048, 55.2708),
    ("London", "GB", 51.5074, -0.1278),
    ("Paris", "FR", 48.8566, 2.3522),
    ("Berlin", "DE", 52.5200, 13.4050),
    ("New York", "US", 40.7128, -74.0060),
    ("Los Angeles", "US", 34.0522, -118.2437),
    ("Mexico City", "MX", 19.4326, -99.1332),
    ("Sao Paulo", "BR", -23.5505, -46.6333),
    ("Lagos", "NG", 6.5244, 3.3792),
]


def fetch_with_backoff(url, params=None, headers=None, max_retries=4):
    """GET with exponential backoff on 429/5xx (401/403 fail fast: retrying won't fix auth)."""
    delay = 2
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429 or resp.status_code >= 500:
            print(f"    got {resp.status_code}, retry {attempt}/{max_retries} in {delay}s")
            time.sleep(delay)
            delay *= 2
            continue
        resp.raise_for_status()
    raise RuntimeError(f"gave up after {max_retries} retries: {url}")


def land(source, city, payload, run_id):
    """Write one raw JSON file locally, mirroring the Volume layout."""
    now = datetime.now(timezone.utc)
    envelope = {
        "run_id": run_id,
        "source": source,
        "city": city,
        "fetched_at_utc": now.isoformat(),
        "payload": payload,
    }
    day = now.strftime("%Y-%m-%d")
    rel = Path(source) / day / f"{city.lower().replace(' ', '_')}_{int(now.timestamp())}.json"
    path = LOCAL_LANDING / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(envelope))
    return rel


def fetch_openweather(run_id):
    n = 0
    for city, country, lat, lon in CITIES:
        pollution = fetch_with_backoff(
            "https://api.openweathermap.org/data/2.5/air_pollution",
            params={"lat": lat, "lon": lon, "appid": OPENWEATHER_KEY},
        )
        weather = fetch_with_backoff(
            "https://api.openweathermap.org/data/2.5/weather",
            params={"lat": lat, "lon": lon, "appid": OPENWEATHER_KEY, "units": "metric"},
        )
        land("openweather", city, {"pollution": pollution, "weather": weather}, run_id)
        n += 1
        time.sleep(1)  # 2 calls/city: stay politely under 60 calls/min
    return n


def fetch_waqi(run_id):
    n = 0
    for city, country, lat, lon in CITIES:
        data = fetch_with_backoff(
            f"https://api.waqi.info/feed/geo:{lat};{lon}/",
            params={"token": WAQI_TOKEN},
        )
        land("waqi", city, data, run_id)
        n += 1
        time.sleep(1)
    return n


def fetch_openaq(run_id):
    n = 0
    headers = {"X-API-Key": OPENAQ_KEY}
    for city, country, lat, lon in CITIES:
        data = fetch_with_backoff(
            "https://api.openaq.org/v3/locations",
            params={"coordinates": f"{lat},{lon}", "radius": 25000, "limit": 5},
            headers=headers,
        )
        land("openaq", city, data, run_id)
        n += 1
        time.sleep(1)
    return n


def upload_landing():
    """Push every local landing file into the Unity Catalog Volume, then archive it locally."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(host=DATABRICKS_HOST, auth_type="external-browser")
    uploaded = 0
    for path in sorted(LOCAL_LANDING.rglob("*.json")):
        rel = path.relative_to(LOCAL_LANDING)
        target = f"{VOLUME_ROOT}/{rel}"
        with open(path, "rb") as f:
            w.files.upload(target, f, overwrite=True)
        uploaded += 1
        if uploaded % 10 == 0:
            print(f"  uploaded {uploaded} files...")
    return uploaded


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-upload", action="store_true", help="fetch only, skip the Volume upload")
    ap.add_argument("--sources", default="openweather,waqi,openaq", help="comma list of sources to fetch")
    args = ap.parse_args()

    missing = [k for k, v in {
        "OPENWEATHER_KEY": OPENWEATHER_KEY, "WAQI_TOKEN": WAQI_TOKEN, "OPENAQ_KEY": OPENAQ_KEY,
    }.items() if not v and k.split("_")[0].lower()[:6] in args.sources.replace("openaq", "openaq_")]
    if not DATABRICKS_HOST and not args.no_upload:
        sys.exit("set DATABRICKS_HOST or pass --no-upload")

    run_id = str(uuid.uuid4())[:8]
    sources = [s.strip() for s in args.sources.split(",")]
    print(f"run {run_id}: {len(CITIES)} cities, sources: {', '.join(sources)}")

    fetchers = {"openweather": fetch_openweather, "waqi": fetch_waqi, "openaq": fetch_openaq}
    for s in sources:
        n = fetchers[s](run_id)
        print(f"{s}: {n} cities landed locally")

    if args.no_upload:
        print(f"done (local only). Inspect {LOCAL_LANDING}")
    else:
        total = upload_landing()
        print(f"run {run_id} complete: {total} files uploaded to {VOLUME_ROOT}")
