import pytest
from fastapi.testclient import TestClient
import api

client = TestClient(api.app)

class DummyResponse:
    def __init__(self, status_code=200, json_data=None):
        self._json = json_data or {}
        self.status_code = status_code
    def raise_for_status(self):
        if self.status_code != 200:
            raise Exception("HTTP error")
    def json(self):
        return self._json

def test_base_url():
    assert api.BASE_URL.startswith("https://") 

def test_fetch_station_metadata_success(monkeypatch):
    def mock_get(url, timeout):
        return DummyResponse(json_data={"properties": {"name": "Test Station"}})
    monkeypatch.setattr(api.httpx, "get", mock_get)
    meta = api.fetch_station_metadata("KATL")
    assert meta["name"] == "Test Station"

def test_fetch_station_metadata_error(monkeypatch):
    def mock_get(url, timeout):
        raise api.httpx.RequestError("fail", request=None)
    monkeypatch.setattr(api.httpx, "get", mock_get)
    with pytest.raises(api.WeatherAPIError):
        api.fetch_station_metadata("KATL")

def test_fetch_observations_success(monkeypatch):
    def mock_get(url, params, timeout):
        return DummyResponse(json_data={"features": [{"properties": {"station": "KATL"}}]})
    monkeypatch.setattr(api.httpx, "get", mock_get)
    obs = api.fetch_observations("KATL")
    assert isinstance(obs, list)
    assert obs[0]["properties"]["station"] == "KATL" 

def test_run_pipeline_db_fail(monkeypatch):
    # Simulate DB connection failure
    def fail_main():
        raise Exception("DB connection failed")
    monkeypatch.setattr(api, "main", fail_main)
    response = client.post("/run-pipeline")
    assert response.status_code == 500
    assert "Pipeline failed" in response.json()["detail"]

def test_run_pipeline_no_observations(monkeypatch):
    # Simulate no observations error
    def fail_main():
        raise RuntimeError("No observations to process.")
    monkeypatch.setattr(api, "main", fail_main)
    response = client.post("/run-pipeline")
    assert response.status_code == 400
    assert "No observations to process" in response.json()["detail"] 