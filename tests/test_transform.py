import polars as pl
import pytest
import transform

def test_transform_module_exists():
    assert hasattr(transform, "pl")

def test_flatten_observations_rounding():
    obs = [{"properties": {"station": "s/KATL", "timestamp": "2024-01-01T00:00:00Z", "temperature": {"value": 12.3456}, "windSpeed": {"value": 7.891}, "relativeHumidity": {"value": 55.555}}}]
    df = transform.flatten_observations(obs)
    assert df["temperature"][0] == 12.35
    assert df["wind_speed"][0] == 7.89
    assert df["humidity"][0] == 55.56

def test_flatten_observations_missing():
    obs = [{"properties": {"station": "s/KATL", "timestamp": "2024-01-01T00:00:00Z"}}]
    df = transform.flatten_observations(obs)
    assert df["temperature"][0] is None
    assert df["wind_speed"][0] is None
    assert df["humidity"][0] is None

def test_enrich_with_station():
    df = pl.DataFrame({"station_id": ["KATL"]})
    meta = {
        "name": "Test",
        "timeZone": "UTC",
        "geometry": {"type": "Point", "coordinates": [-84.42694, 33.64028]}
    }
    enriched = transform.enrich_with_station(df, meta)
    assert enriched["station_name"][0] == "Test"
    assert enriched["station_timezone"][0] == "UTC"
    assert enriched["latitude"][0] == 33.64028
    assert enriched["longitude"][0] == -84.42694

def test_enrich_with_station_missing_geometry():
    df = pl.DataFrame({"station_id": ["KATL"]})
    meta = {
        "name": "Test",
        "timeZone": "UTC"
        # No geometry
    }
    enriched = transform.enrich_with_station(df, meta)
    assert enriched["latitude"][0] is None
    assert enriched["longitude"][0] is None 