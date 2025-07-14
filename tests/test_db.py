import pytest
import polars as pl
from app import db
from psycopg import OperationalError
from typing import Any


class DummyCursor:
    def __init__(self):
        self.executed = []
        self.results = []
        self.fetchone_result: Any = None

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.fetchone_result

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class DummyConn:
    def __init__(self):
        self.committed = False
        self.cursor_obj = DummyCursor()

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True


def test_create_schema_idempotent():
    conn = DummyConn()
    db.create_schema(conn)  # type: ignore[arg-type]
    db.create_schema(conn)  # type: ignore[arg-type]
    assert any("CREATE TABLE" in sql for sql, _ in conn.cursor_obj.executed)
    assert conn.committed


def test_upsert_station_full_partial():
    conn = DummyConn()
    # Full metadata
    meta = {"station_id": "KATL", "name": "Test", "timeZone": "UTC", "geometry": {"coordinates": [-84.4, 33.6]}}
    db.upsert_station(conn, meta)  # type: ignore[arg-type]
    # Partial metadata
    meta2 = {"station_id": "KATL", "name": "Test2"}
    db.upsert_station(conn, meta2)  # type: ignore[arg-type]
    assert any("INSERT INTO wxinfo.stations" in sql for sql, _ in conn.cursor_obj.executed)
    assert conn.committed


def test_upsert_weather_data_empty_valid():
    conn = DummyConn()
    df_empty = pl.DataFrame({"station_id": []})
    n = db.upsert_weather_data(conn, df_empty)  # type: ignore[arg-type]
    assert n == 0
    df = pl.DataFrame({
        "station_id": ["KATL"],
        "station_name": ["Test"],
        "station_timezone": ["UTC"],
        "latitude": [1.0],
        "longitude": [2.0],
        "observation_timestamp": ["2024-01-01T00:00:00Z"],
        "temperature": [10.0],
        "wind_speed": [5.0],
        "relative_humidity": [50.0],
        "dewpoint": [10.0],
    })
    n2 = db.upsert_weather_data(conn, df)  # type: ignore[arg-type]
    assert n2 == 1
    assert any("INSERT INTO wxinfo.weather_observations" in sql for sql, _ in conn.cursor_obj.executed)
    assert conn.committed


def test_get_latest_observation_timestamp_none_and_value():
    conn = DummyConn()
    # No data
    conn.cursor_obj.fetchone_result = (None,)
    ts = db.get_latest_observation_timestamp(conn, "KATL")  # type: ignore[arg-type]
    assert ts is None
    # With data
    from datetime import datetime
    now = datetime.now()
    conn.cursor_obj.fetchone_result = (now,)
    ts2 = db.get_latest_observation_timestamp(conn, "KATL")  # type: ignore[arg-type]
    assert ts2 == now.isoformat()


def test_get_connection_fail(monkeypatch):
    def fail_connect(*args, **kwargs):
        raise OperationalError("fail")
    monkeypatch.setattr(db.psycopg, "connect", fail_connect)
    with pytest.raises(OperationalError):
        db.get_connection("bad_url")
