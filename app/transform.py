import polars as pl


def flatten_observations(observations: list[dict]) -> pl.DataFrame:
    """
    Flatten a list of NWS API observation dicts into a Polars DataFrame.

    Args:
        observations (list[dict]): List of NWS API observation dicts.
    Returns:
        pl.DataFrame: Flattened DataFrame with columns for station_id, timestamp, temperature, wind_speed, humidity.
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
