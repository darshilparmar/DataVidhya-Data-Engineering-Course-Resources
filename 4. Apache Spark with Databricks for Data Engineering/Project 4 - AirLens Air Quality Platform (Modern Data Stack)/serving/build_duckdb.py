"""Build the local AirLens serving database from landed JSON without cloud access."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LANDING = PROJECT_ROOT / "resources" / "landing"
DEFAULT_DATABASE = Path(__file__).resolve().parent / "airlens.duckdb"

CITIES = [
    ("Delhi", "IN", "Asia/Kolkata", 28.6139, 77.2090),
    ("Mumbai", "IN", "Asia/Kolkata", 19.0760, 72.8777),
    ("Bengaluru", "IN", "Asia/Kolkata", 12.9716, 77.5946),
    ("Kolkata", "IN", "Asia/Kolkata", 22.5726, 88.3639),
    ("Chennai", "IN", "Asia/Kolkata", 13.0827, 80.2707),
    ("Hyderabad", "IN", "Asia/Kolkata", 17.3850, 78.4867),
    ("Beijing", "CN", "Asia/Shanghai", 39.9042, 116.4074),
    ("Shanghai", "CN", "Asia/Shanghai", 31.2304, 121.4737),
    ("Tokyo", "JP", "Asia/Tokyo", 35.6762, 139.6503),
    ("Jakarta", "ID", "Asia/Jakarta", -6.2088, 106.8456),
    ("Bangkok", "TH", "Asia/Bangkok", 13.7563, 100.5018),
    ("Dubai", "AE", "Asia/Dubai", 25.2048, 55.2708),
    ("London", "GB", "Europe/London", 51.5074, -0.1278),
    ("Paris", "FR", "Europe/Paris", 48.8566, 2.3522),
    ("Berlin", "DE", "Europe/Berlin", 52.5200, 13.4050),
    ("New York", "US", "America/New_York", 40.7128, -74.0060),
    ("Los Angeles", "US", "America/Los_Angeles", 34.0522, -118.2437),
    ("Mexico City", "MX", "America/Mexico_City", 19.4326, -99.1332),
    ("Sao Paulo", "BR", "America/Sao_Paulo", -23.5505, -46.6333),
    ("Lagos", "NG", "Africa/Lagos", 6.5244, 3.3792),
]


def parse_iso(value: str) -> datetime:
    """Parse an API timestamp and keep it timezone-aware."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def epoch_utc(value: int | float) -> datetime:
    """Convert epoch seconds to an aware UTC datetime."""
    return datetime.fromtimestamp(value, tz=timezone.utc)


def parse_waqi_local(time_payload: dict[str, Any]) -> datetime:
    """Convert WAQI station-local time plus numeric offset to UTC."""
    local = datetime.strptime(
        f"{time_payload['s']}{time_payload['tz']}",
        "%Y-%m-%d %H:%M:%S%z",
    )
    return local.astimezone(timezone.utc)


def load_rows(landing_root: Path) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]], list[tuple[Any, ...]], int]:
    """Parse the real envelopes into silver-shaped air, weather, and station rows."""
    air_rows: list[tuple[Any, ...]] = []
    weather_rows: list[tuple[Any, ...]] = []
    station_rows: list[tuple[Any, ...]] = []
    skipped_waqi_errors = 0

    files = sorted(landing_root.glob("*/*/*.json"))
    if not files:
        raise FileNotFoundError(f"no landed JSON files found under {landing_root}")

    seen_sources: set[str] = set()
    for path in files:
        envelope = json.loads(path.read_text(encoding="utf-8"))
        source = envelope["source"]
        city = envelope["city"]
        fetched_at = parse_iso(envelope["fetched_at_utc"])
        payload = envelope["payload"]
        seen_sources.add(source)

        if source == "openweather":
            pollution = payload["pollution"]["list"][0]
            components = pollution["components"]
            weather = payload["weather"]
            air_rows.append(
                (
                    city,
                    source,
                    epoch_utc(pollution["dt"]),
                    fetched_at,
                    components.get("pm2_5"),
                    components.get("pm10"),
                    components.get("o3"),
                    components.get("no2"),
                    pollution["main"]["aqi"],
                    "openweather-1-5",
                    None,
                )
            )
            weather_rows.append(
                (
                    city,
                    epoch_utc(weather["dt"]),
                    weather["main"].get("temp"),
                    weather["main"].get("humidity"),
                    weather.get("wind", {}).get("speed"),
                    weather.get("weather", [{}])[0].get("main"),
                )
            )
        elif source == "waqi":
            if payload.get("status") != "ok" or not isinstance(payload.get("data"), dict):
                skipped_waqi_errors += 1
                continue
            data = payload["data"]
            air_rows.append(
                (
                    city,
                    source,
                    parse_waqi_local(data["time"]),
                    fetched_at,
                    None,
                    None,
                    None,
                    None,
                    data["aqi"],
                    "us-epa-0-500",
                    data.get("city", {}).get("name"),
                )
            )
        elif source == "openaq":
            for station in payload.get("results", []):
                country = station.get("country") or {}
                provider = station.get("provider") or {}
                coordinates = station.get("coordinates") or {}
                station_rows.append(
                    (
                        city,
                        station.get("id"),
                        station.get("name"),
                        country.get("code"),
                        station.get("timezone"),
                        provider.get("name"),
                        station.get("isMonitor"),
                        coordinates.get("latitude"),
                        coordinates.get("longitude"),
                    )
                )
        else:
            raise ValueError(f"unsupported source {source!r} in {path}")

    expected = {"openweather", "waqi", "openaq"}
    if seen_sources != expected:
        raise ValueError(f"expected sources {sorted(expected)}, found {sorted(seen_sources)}")
    return air_rows, weather_rows, station_rows, skipped_waqi_errors


def build_database(landing_root: Path, database_path: Path) -> dict[str, int]:
    """Create all serving tables in one transaction so reruns are idempotent."""
    air_rows, weather_rows, station_rows, skipped_waqi_errors = load_rows(landing_root)
    database_path.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(database_path))
    try:
        connection.execute("begin transaction")
        connection.execute(
            """
            create or replace temp table silver_air_quality (
                city varchar,
                source varchar,
                measured_at_utc timestamptz,
                fetched_at_utc timestamptz,
                pm25_ugm3 double,
                pm10_ugm3 double,
                o3_ugm3 double,
                no2_ugm3 double,
                aqi_value double,
                aqi_scale varchar,
                station_name varchar
            )
            """
        )
        connection.executemany(
            "insert into silver_air_quality values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            air_rows,
        )
        connection.execute(
            """
            create or replace temp table silver_weather (
                city varchar,
                measured_at_utc timestamptz,
                temp_c double,
                humidity_pct double,
                wind_ms double,
                conditions varchar
            )
            """
        )
        connection.executemany(
            "insert into silver_weather values (?, ?, ?, ?, ?, ?)",
            weather_rows,
        )
        connection.execute(
            """
            create or replace temp table silver_stations (
                query_city varchar,
                station_id bigint,
                station_name varchar,
                country_code varchar,
                station_timezone varchar,
                provider varchar,
                is_reference_monitor boolean,
                lat double,
                lon double
            )
            """
        )
        connection.executemany(
            "insert into silver_stations values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            station_rows,
        )
        connection.execute(
            """
            create or replace temp table city_dimension (
                city varchar,
                country varchar,
                timezone varchar,
                lat double,
                lon double
            )
            """
        )
        connection.executemany("insert into city_dimension values (?, ?, ?, ?, ?)", CITIES)

        connection.execute(
            """
            create or replace table city_air_quality as
            with hourly_air as (
                select
                    city,
                    source,
                    aqi_scale,
                    date_trunc('hour', measured_at_utc) as measured_at_utc,
                    avg(aqi_value) as aqi_value,
                    avg(pm25_ugm3) as pm25_ugm3,
                    avg(pm10_ugm3) as pm10_ugm3,
                    avg(o3_ugm3) as o3_ugm3,
                    avg(no2_ugm3) as no2_ugm3,
                    max(station_name) as station_name,
                    max(fetched_at_utc) as latest_fetch_at_utc
                from silver_air_quality
                group by city, source, aqi_scale, date_trunc('hour', measured_at_utc)
            ),
            hourly_weather as (
                select
                    city,
                    date_trunc('hour', measured_at_utc) as measured_at_utc,
                    avg(temp_c) as temp_c,
                    avg(humidity_pct) as humidity_pct,
                    avg(wind_ms) as wind_ms,
                    max(conditions) as conditions
                from silver_weather
                group by city, date_trunc('hour', measured_at_utc)
            )
            select
                concat(a.city, '|', a.source, '|', cast(a.measured_at_utc as varchar)) as city_source_measurement_hour,
                a.city,
                d.country,
                d.timezone,
                d.lat,
                d.lon,
                a.source,
                a.aqi_scale,
                a.measured_at_utc,
                a.aqi_value,
                a.pm25_ugm3,
                a.pm10_ugm3,
                a.o3_ugm3,
                a.no2_ugm3,
                a.station_name,
                a.latest_fetch_at_utc,
                w.temp_c,
                w.humidity_pct,
                w.wind_ms,
                w.conditions,
                case
                    when a.pm25_ugm3 is null then 'not_available'
                    when a.pm25_ugm3 <= 9.0 then 'good'
                    when a.pm25_ugm3 <= 35.4 then 'moderate'
                    when a.pm25_ugm3 <= 55.4 then 'unhealthy_for_sensitive_groups'
                    when a.pm25_ugm3 <= 125.4 then 'unhealthy'
                    when a.pm25_ugm3 <= 225.4 then 'very_unhealthy'
                    else 'hazardous'
                end as pm25_category,
                case when a.pm25_ugm3 is null then null else a.pm25_ugm3 <= 15.0 end as within_who_24h_guideline
            from hourly_air a
            join city_dimension d using (city)
            left join hourly_weather w
                on a.city = w.city
                and a.measured_at_utc = w.measured_at_utc
            """
        )
        connection.execute(
            """
            create or replace table city_daily as
            select
                concat(city, '|', source, '|', cast(cast(measured_at_utc as date) as varchar)) as city_source_date,
                city,
                country,
                timezone,
                lat,
                lon,
                source,
                aqi_scale,
                cast(measured_at_utc as date) as measurement_date,
                avg(aqi_value) as average_aqi_value,
                avg(pm25_ugm3) as average_pm25_ugm3,
                avg(pm10_ugm3) as average_pm10_ugm3,
                avg(o3_ugm3) as average_o3_ugm3,
                avg(no2_ugm3) as average_no2_ugm3,
                avg(temp_c) as average_temp_c,
                count(*) as measurement_hours
            from city_air_quality
            group by city, country, timezone, lat, lon, source, aqi_scale, cast(measured_at_utc as date)
            """
        )
        connection.execute(
            """
            -- stations is a DIMENSION, not an event stream: the registry is
            -- re-fetched on every ingestion run, so the same station arrives
            -- once per run. Deduplicate on the natural key or the table doubles
            -- every time the pipeline runs (idempotency bug caught on run 2).
            create or replace table stations as
            select distinct on (query_city, station_id)
                concat(query_city, '|', cast(station_id as varchar)) as station_key,
                query_city,
                station_id,
                station_name,
                country_code,
                station_timezone,
                provider,
                is_reference_monitor,
                lat,
                lon
            from silver_stations
            order by query_city, station_id
            """
        )
        connection.execute("commit")

        counts = {
            table: connection.execute(f"select count(*) from {table}").fetchone()[0]
            for table in ("city_air_quality", "city_daily", "stations")
        }
    except Exception:
        connection.execute("rollback")
        raise
    finally:
        connection.close()

    print(f"landed JSON files: {len(list(landing_root.glob('*/*/*.json')))}")
    print(f"WAQI error envelopes skipped: {skipped_waqi_errors}")
    for table, count in counts.items():
        print(f"{table}: {count} rows")
    print(f"database: {database_path}")
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--landing-root", type=Path, default=DEFAULT_LANDING)
    parser.add_argument("--output", type=Path, default=DEFAULT_DATABASE)
    args = parser.parse_args()
    build_database(args.landing_root.resolve(), args.output.resolve())


if __name__ == "__main__":
    main()
