import polars as pl
from typing import Any, List


def flatten_observations(observations: list[dict]) -> pl.DataFrame:
    """
    Transform a list of NWS observation feature dicts into a Polars DataFrame.
    Extracts and normalizes required fields:
      - station identifier, name, timezone, lat/lon, timestamp, temp, wind speed, humidity
    Rounds numeric fields to two decimals.
    """
    records = []
    for obs in observations:
        props = obs.get("properties", {})
        station = props.get("station", "")
        timestamp = props.get("timestamp", None)
        temp = props.get("temperature", {}).get("value")
        wind = props.get("windSpeed", {}).get("value")
        humidity = props.get("relativeHumidity", {}).get("value")
        # These may be None if not present
        records.append({
            "station_id": station.split("/")[-1] if station else None,
            "observation_timestamp": timestamp,
            "temperature": round(temp, 2) if temp is not None else None,
            "wind_speed": round(wind, 2) if wind is not None else None,
            "humidity": round(humidity, 2) if humidity is not None else None,
        })
    return pl.DataFrame(records)


def enrich_with_station(df: pl.DataFrame, station_meta: dict) -> pl.DataFrame:
    """
    Add station name, timezone, latitude, longitude to each row in the DataFrame.
    Extract lat/lon from station_meta['geometry']['coordinates'] if present (geometry is top-level).
    """
    name = station_meta.get("name")
    timezone = station_meta.get("timeZone")
    geometry = station_meta.get("geometry")
    lat = lon = None
    if geometry and isinstance(geometry, dict):
        coords = geometry.get("coordinates")
        if coords and isinstance(coords, (list, tuple)) and len(coords) == 2:
            lon, lat = coords[0], coords[1]
    return df.with_columns([
        pl.lit(name).alias("station_name"),
        pl.lit(timezone).alias("station_timezone"),
        pl.lit(lat).alias("latitude"),
        pl.lit(lon).alias("longitude")
    ])
