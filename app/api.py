"""
API module for the Weather Data Pipeline project.

This FastAPI app exposes endpoints to:
- Trigger the weather data pipeline (fetch, transform, and store weather data)
- Query weather metrics (average temperature, max wind speed change)
- Check service health

Usage hints:
- All endpoints are versioned under /v1/
- Requires environment variables: NWS_API_BASE_URL (optional), DATABASE_URL (required)
- Returns granular error codes (400, 404, 503, 500) and logs to stdout
- Designed for integration with Airflow, Docker, and CI/CD
"""
import contextlib
import io
import logging
import os
from dotenv import load_dotenv
from typing import Any, Dict, List, Optional, Union

from app.db import get_connection
from fastapi import FastAPI, HTTPException, Body
from fastapi import APIRouter
import httpx
from app.metrics import (
    get_average_temperature_last_week,
    get_max_wind_speed_change_last_7_days,
)
from psycopg import OperationalError
from pydantic import BaseModel

NWS_API_BASE_URL = os.environ.get("NWS_API_BASE_URL", "https://api.weather.gov")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
NWS_USER_AGENT = os.environ.get("NWS_USER_AGENT", "myweatherapp.com, contact@myweatherapp.com")

# Load environment variables from app/.env if it exists, otherwise from .env in the project root
app_env_path = os.path.join(os.path.dirname(__file__), ".env")
root_env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))
if os.path.exists(app_env_path):
    load_dotenv(app_env_path)
else:
    load_dotenv(root_env_path)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

api_v1 = APIRouter()


class RunPipelineRequest(BaseModel):
    station_ids: Optional[Union[str, List[str]]] = None


def fetch_observations(station_id: str, start: str, end: str) -> list[dict]:
    """
    Fetch weather observations for a station between start and end datetimes.

    Args:
        station_id (str): The station identifier.
        start (str): ISO8601 start datetime.
        end (str): ISO8601 end datetime.
    Returns:
        list[dict]: List of observation features (dicts).
    Raises:
        HTTPException: If the NWS API is unreachable or returns an error.
    """
    url = f"{NWS_API_BASE_URL}/stations/{station_id}/observations"
    params = {"start": start, "end": end}
    headers = {"User-Agent": NWS_USER_AGENT}
    try:
        resp = httpx.get(url, params=params, timeout=30, headers=headers)
        resp.raise_for_status()
    except httpx.RequestError as e:
        # Network or connection error to the NWS API
        raise HTTPException(status_code=503, detail=f"Weather API not reachable: {e}")
    except httpx.HTTPStatusError as e:
        # NWS API returned an error status code
        raise HTTPException(status_code=resp.status_code, detail=f"Weather API error: {e}")
    return resp.json().get("features", [])


def fetch_station_metadata(station_id: str) -> dict:
    """
    Fetch station metadata from the NWS /stations/{stationId} endpoint.
    Returns a dict with canonical info including timeZone, latitude, and longitude.
    """
    url = f"{NWS_API_BASE_URL}/stations/{station_id}"
    headers = {"User-Agent": NWS_USER_AGENT}
    resp = httpx.get(url, timeout=10, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    props = data.get("properties", {})
    geometry = data.get("geometry", {})
    coordinates = geometry.get("coordinates", [None, None])
    props["latitude"] = coordinates[1]
    props["longitude"] = coordinates[0]
    return props


@api_v1.post("/run-pipeline")
def run_pipeline(
    req: Optional[RunPipelineRequest] = Body(
        None,
        examples={
            "single": {"summary": "Single station", "value": {"station_ids": "KATL"}},
            "multiple": {"summary": "Multiple stations", "value": {"station_ids": ["KATL", "003PG"]}},
        }  # type: ignore[arg-type]
    )
) -> Dict[str, Any]:
    """
    Trigger the weather data pipeline (fetch, transform, load).
    Accepts a JSON body with a 'station_ids' field (string or list of station IDs).
    Captures all stdout and returns it in the response for Airflow log visibility.

    Request body (optional):
        {
            "station_ids": "KATL"            # Single station (string)
        }
        or
        {
            "station_ids": ["KATL", "003PG"]  # Multiple stations (list)
        }
    Returns:
        dict: {"status": ..., "output": ...} on success, or {"error": ..., "output": ...} on failure.
    """
    import json
    import os as _os
    output = io.StringIO()
    try:
        # Prefer explicit request body, fallback to env var
        station_ids = None
        if req and req.station_ids is not None:
            # If station_ids is provided in the request body, normalize to a list
            if isinstance(req.station_ids, str):
                station_ids = [req.station_ids]
            elif isinstance(req.station_ids, list):
                station_ids = req.station_ids
            else:
                # Invalid type for station_ids
                raise HTTPException(status_code=400, detail="station_ids must be a string or list of strings.")
        else:
            # If not provided in the request, fallback to environment variable
            NWS_STATION_ID = _os.environ.get("NWS_STATION_ID")
            if not NWS_STATION_ID:
                raise RuntimeError("NWS_STATION_ID environment variable is required.")
            try:
                # Try to parse as JSON array or string
                station_ids = json.loads(NWS_STATION_ID)
                if isinstance(station_ids, str):
                    station_ids = [station_ids]
                elif not isinstance(station_ids, list):
                    raise ValueError
            except Exception:
                # If parsing fails, treat as a single station string
                station_ids = [NWS_STATION_ID]
        # Capture all stdout for Airflow/CI log visibility
        with contextlib.redirect_stdout(output):
            from app.pipeline import WeatherPipeline
            pipeline = WeatherPipeline(station_ids, DATABASE_URL)
            pipeline.run()
        return {"status": "Pipeline started", "output": output.getvalue()}
    except RuntimeError as e:
        # Special case: no new data is not an error, just a status
        if str(e) == "No new observations to process.":
            return {"status": "No new observations to process.", "output": output.getvalue()}
        logger.error(f"Bad input: {e}")
        return {"error": str(e), "output": output.getvalue()}
    except HTTPException as e:
        # Propagate FastAPI/HTTP errors
        logger.error(f"HTTPException: {e.detail}")
        return {"error": str(e), "output": output.getvalue()}
    except OperationalError as e:
        # Database connection error
        logger.error(f"Database not reachable: {e}")
        return {"error": f"Database not reachable: {e}", "output": output.getvalue()}
    except httpx.HTTPStatusError as e:
        # NWS API returned an error status code
        logger.error(f"Weather API HTTP error: {e}")
        if e.response.status_code == 404:
            return {"error": "Station not found", "output": output.getvalue()}
        elif e.response.status_code == 503:
            return {"error": "Weather API unavailable", "output": output.getvalue()}
        else:
            return {"error": f"Weather API error: {e}", "output": output.getvalue()}
    except httpx.RequestError as e:
        # Network or connection error to the NWS API
        logger.error(f"Weather API not reachable: {e}")
        return {"error": f"Weather API not reachable: {e}", "output": output.getvalue()}
    except Exception as e:
        # Catch-all for unexpected errors
        logger.error(f"Pipeline failed: {e}")
        return {"error": f"Pipeline failed: {e}", "output": output.getvalue()}


@api_v1.get("/metrics/average-temperature")
def average_temperature() -> list[dict]:
    """
    Get the average temperature for the last week from the database.

    Returns:
        list[dict]: List of dicts with station_id, average_temperature, first_observation, and last_observation
    Raises:
        HTTPException: 503 if the database is unreachable.
    """
    db_url = DATABASE_URL
    try:
        with get_connection(db_url) as conn:
            result = get_average_temperature_last_week(conn)
    except OperationalError as e:
        # Database connection error
        raise HTTPException(status_code=503, detail=f"Database not reachable: {e}")
    return result


@api_v1.get("/metrics/max-wind-speed-change")
def max_wind_speed_change() -> list[dict]:
    """
    Get the maximum wind speed change over the last 7 days from the database.

    Returns:
        list[dict]: List of dicts with station_id, max_wind_speed_change, first_observation, and last_observation
    Raises:
        HTTPException: 503 if the database is unreachable.
    """
    db_url = DATABASE_URL
    try:
        with get_connection(db_url) as conn:
            result = get_max_wind_speed_change_last_7_days(conn)
    except OperationalError as e:
        # Database connection error
        raise HTTPException(status_code=503, detail=f"Database not reachable: {e}")
    return result


@api_v1.get("/health")
def health() -> Dict[str, str]:
    """
    Health check endpoint. Verifies database connectivity.

    Returns:
        dict: {"status": "ok"} if healthy.
    Raises:
        HTTPException: 503 if the database is unreachable.
    """
    try:
        with get_connection(DATABASE_URL) as conn:
            conn.cursor().execute("SELECT 1;")
        return {"status": "ok"}
    except Exception:
        # Any error means the DB is not reachable
        raise HTTPException(status_code=503, detail="Database not reachable")


app = FastAPI()
app.include_router(api_v1, prefix="/v1")
