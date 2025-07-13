from app.pipeline import WeatherPipeline
import pytest


def test_pipeline_init():
    pipeline = WeatherPipeline('KATL', 'postgresql://user:pass@localhost/db')
    assert pipeline.station_id == 'KATL'
    assert pipeline.db_url == 'postgresql://user:pass@localhost/db'


def test_pipeline_run_no_data(monkeypatch):

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

        def cursor(self):

            class Cur:
                def execute(self, sql, params=None):
                    pass

                def fetchone(self):
                    return (None,)

            return Cur()

        def commit(self):
            pass

    monkeypatch.setattr('app.db.get_connection', lambda db_url: DummyConn())
    monkeypatch.setattr('app.db.create_schema', lambda conn: None)
    monkeypatch.setattr('app.db.get_latest_observation_timestamp', lambda conn, sid: None)
    monkeypatch.setattr('app.api.fetch_observations', lambda sid, start, end: [])
    pipeline = WeatherPipeline('KATL', 'postgresql://user:pass@localhost/db')
    pipeline.run()  # Should not raise


def test_pipeline_run_db_error(monkeypatch):

    class DummyConn:
        def __enter__(self):
            raise Exception('DB error')

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    monkeypatch.setattr('app.db.get_connection', lambda db_url: DummyConn())
    pipeline = WeatherPipeline('KATL', 'postgresql://user:pass@localhost/db')
    with pytest.raises(Exception):
        pipeline.run()
