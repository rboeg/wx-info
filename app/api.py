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
from typing import Any, Dict

from app.db import get_connection
from fastapi import FastAPI, HTTPException
from fastapi import APIRouter
import httpx
from app.metrics import (
    get_average_temperature_last_week,
    get_max_wind_speed_change_last_7_days,
)
from psycopg import OperationalError


NWS_API_BASE_URL = os.environ.get("NWS_API_BASE_URL", "https://api.weather.gov")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

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
    try:
        resp = httpx.get(url, params=params, timeout=30)
        resp.raise_for_status()
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"Weather API not reachable: {e}")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=resp.status_code, detail=f"Weather API error: {e}")
    return resp.json().get("features", [])


@api_v1.post("/run-pipeline")
def run_pipeline() -> Dict[str, Any]:
    """
    Trigger the weather data pipeline (fetch, transform, load).
    Captures all stdout and returns it in the response for Airflow log visibility.

    Returns:
        dict: {"status": ..., "output": ...} on success, or {"error": ..., "output": ...} on failure.
    """
    output = io.StringIO()
    try:
        with contextlib.redirect_stdout(output):
            from app.pipeline import main
            main()
        return {"status": "Pipeline started", "output": output.getvalue()}
    except RuntimeError as e:
        if str(e) == "No new observations to process.":
            return {"status": "No new observations to process.", "output": output.getvalue()}
        logger.error(f"Bad input: {e}")
        return {"error": str(e), "output": output.getvalue()}
    except HTTPException as e:
        logger.error(f"HTTPException: {e.detail}")
        return {"error": str(e), "output": output.getvalue()}
    except OperationalError as e:
        logger.error(f"Database not reachable: {e}")
        return {"error": f"Database not reachable: {e}", "output": output.getvalue()}
    except httpx.HTTPStatusError as e:
        logger.error(f"Weather API HTTP error: {e}")
        if e.response.status_code == 404:
            return {"error": "Station not found", "output": output.getvalue()}
        elif e.response.status_code == 503:
            return {"error": "Weather API unavailable", "output": output.getvalue()}
        else:
            return {"error": f"Weather API error: {e}", "output": output.getvalue()}
    except httpx.RequestError as e:
        logger.error(f"Weather API not reachable: {e}")
        return {"error": f"Weather API not reachable: {e}", "output": output.getvalue()}
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return {"error": f"Pipeline failed: {e}", "output": output.getvalue()}


@api_v1.get("/metrics/average-temperature")
def average_temperature() -> Dict[str, Any]:
    """
    Get the average temperature for the last week from the database.

    Returns:
        dict: {"average_temperature": float}
    Raises:
        HTTPException: 503 if the database is unreachable.
    """
    db_url = DATABASE_URL
    try:
        with get_connection(db_url) as conn:
            result = get_average_temperature_last_week(conn)
    except OperationalError as e:
        raise HTTPException(status_code=503, detail=f"Database not reachable: {e}")
    return {"average_temperature": result}


@api_v1.get("/metrics/max-wind-speed-change")
def max_wind_speed_change() -> Dict[str, Any]:
    """
    Get the maximum wind speed change over the last 7 days from the database.

    Returns:
        dict: {"max_wind_speed_change": float}
    Raises:
        HTTPException: 503 if the database is unreachable.
    """
    db_url = DATABASE_URL
    try:
        with get_connection(db_url) as conn:
            result = get_max_wind_speed_change_last_7_days(conn)
    except OperationalError as e:
        raise HTTPException(status_code=503, detail=f"Database not reachable: {e}")
    return {"max_wind_speed_change": result}


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
        raise HTTPException(status_code=503, detail="Database not reachable")


app = FastAPI()
app.include_router(api_v1, prefix="/v1")
