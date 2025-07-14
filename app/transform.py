import polars as pl


def flatten_observations(observations: list[dict]) -> pl.DataFrame:
    """
    Flatten a list of NWS API observation dicts into a Polars DataFrame.

    Args:
        observations (list[dict]): List of NWS API observation dicts.
    Returns:
        pl.DataFrame: Flattened DataFrame with columns for station_id, timestamp, temperature, wind_speed, relative_humidity, barometric_pressure, precipitation_last_hour, dewpoint.
    """
    records = []
    for obs in observations:
        props = obs.get("properties", {})
        station = props.get("station", "")
        timestamp = props.get("timestamp", None)
        temp = props.get("temperature", {}).get("value")
        wind = props.get("windSpeed", {}).get("value")
        relative_humidity = props.get("relativeHumidity", {}).get("value")
        barometric_pressure = props.get("barometricPressure", {}).get("value")
        precipitation_last_hour = props.get("precipitationLastHour", {}).get("value")
        dewpoint = props.get("dewpoint", {}).get("value")
        records.append({
            "station_id": station.split("/")[-1] if station else None,
            "observation_timestamp": timestamp,
            "temperature": round(temp, 2) if temp is not None else None,
            "wind_speed": round(wind, 2) if wind is not None else None,
            "relative_humidity": round(relative_humidity, 2) if relative_humidity is not None else None,
            "barometric_pressure": round(barometric_pressure, 2) if barometric_pressure is not None else None,
            "precipitation_last_hour": round(precipitation_last_hour, 2) if precipitation_last_hour is not None else None,
            "dewpoint": round(dewpoint, 2) if dewpoint is not None else None,
        })
    return pl.DataFrame(records)
