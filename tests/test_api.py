from fastapi.testclient import TestClient
from app import api
import app.pipeline

client = TestClient(api.app)


class DummyResponse:
    def __init__(self, json_data=None):
        self._json = json_data or {}

    def json(self):
        return self._json

    def raise_for_status(self):
        pass


def test_fetch_observations_success(monkeypatch):
    def mock_get(url, params, timeout, **kwargs):
        return DummyResponse(json_data={"features": [{"properties": {"station": "KATL"}}]})
    monkeypatch.setattr(api.httpx, "get", mock_get)
    obs = api.fetch_observations("KATL", "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z")
    assert isinstance(obs, list)
    assert obs[0]["properties"]["station"] == "KATL"


def test_run_pipeline_success(monkeypatch):
    class DummyPipeline:
        def run(self):
            print("Pipeline started")
    monkeypatch.setattr(app.pipeline, "WeatherPipeline", lambda *a, **kw: DummyPipeline())
    response = client.post("/v1/run-pipeline")
    assert response.status_code == 200
    assert "Pipeline started" in response.json().get("output", "")


def test_run_pipeline_no_new_data(monkeypatch):
    class DummyPipeline:
        def run(self):
            raise RuntimeError("No new observations to process.")
    monkeypatch.setattr(app.pipeline, "WeatherPipeline", lambda *a, **kw: DummyPipeline())
    response = client.post("/v1/run-pipeline")
    assert response.status_code == 200
    assert response.json().get("status") == "No new observations to process."


def test_run_pipeline_error(monkeypatch):
    class DummyPipeline:
        def run(self):
            raise Exception("Some error")
    monkeypatch.setattr(app.pipeline, "WeatherPipeline", lambda *a, **kw: DummyPipeline())
    response = client.post("/v1/run-pipeline")
    assert response.status_code == 200
    assert "Pipeline failed" in response.json().get("error", "")


def test_average_temperature_db_error(monkeypatch):
    def fail_get_connection(*args, **kwargs):
        raise api.OperationalError("fail")
    monkeypatch.setattr(api, "get_connection", fail_get_connection)
    response = client.get("/v1/metrics/average-temperature")
    assert response.status_code == 503
    assert "Database not reachable" in response.json()["detail"]


def test_max_wind_speed_change_db_error(monkeypatch):
    def fail_get_connection(*args, **kwargs):
        raise api.OperationalError("fail")
    monkeypatch.setattr(api, "get_connection", fail_get_connection)
    response = client.get("/v1/metrics/max-wind-speed-change")
    assert response.status_code == 503
    assert "Database not reachable" in response.json()["detail"]


def test_health_healthy(monkeypatch):
    def ok_get_connection(*args, **kwargs):
        class Conn:
            def cursor(self):
                class Cur:
                    def execute(self, sql):
                        pass
                return Cur()

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass
        return Conn()
    monkeypatch.setattr(api, "get_connection", ok_get_connection)
    response = client.get("/v1/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_health_unhealthy(monkeypatch):
    def fail_get_connection(*args, **kwargs):
        raise Exception("fail")
    monkeypatch.setattr(api, "get_connection", fail_get_connection)
    response = client.get("/v1/health")
    assert response.status_code == 503
    assert "Database not reachable" in response.json()["detail"]
