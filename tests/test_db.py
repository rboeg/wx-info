import pytest
import polars as pl
import db
from psycopg import OperationalError

class DummyCursor:
    def __init__(self):
        self.executed = []
    def execute(self, sql, params=None):
        self.executed.append((sql, params))
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


def test_create_schema():
    conn = DummyConn()
    db.create_schema(conn)
    assert any("CREATE TABLE" in sql for sql, _ in conn.cursor_obj.executed)
    assert conn.committed

def test_upsert_weather_data():
    conn = DummyConn()
    df = pl.DataFrame({
        "station_id": ["KATL"],
        "station_name": ["Test"],
        "station_timezone": ["UTC"],
        "latitude": [1.0],
        "longitude": [2.0],
        "observation_timestamp": ["2024-01-01T00:00:00Z"],
        "temperature": [10.0],
        "wind_speed": [5.0],
        "humidity": [50.0],
    })
    n = db.upsert_weather_data(conn, df)
    assert n == 1
    assert any("INSERT INTO weather_observations" in sql for sql, _ in conn.cursor_obj.executed)
    assert conn.committed

def test_upsert_weather_data_empty():
    conn = DummyConn()
    df = pl.DataFrame({"station_id": []})
    n = db.upsert_weather_data(conn, df)
    assert n == 0

def test_get_connection_fail(monkeypatch):
    def fail_connect(*args, **kwargs):
        raise OperationalError("fail")
    monkeypatch.setattr(db.psycopg, "connect", fail_connect)
    with pytest.raises(OperationalError):
        db.get_connection("bad_url") 