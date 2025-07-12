import os
from db import get_connection
from metrics import (
    get_average_temperature_last_week,
    get_max_wind_speed_change_last_7_days,
)
from pipeline import main
from fastapi import BackgroundTasks, FastAPI, HTTPException
import httpx
from psycopg import OperationalError
import logging
from fastapi import APIRouter
import io
import sys
import contextlib

NWS_API_BASE_URL = os.environ.get("NWS_API_BASE_URL", "https://api.weather.gov")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

api_v1 = APIRouter()

def fetch_station_metadata(station_id: str) -> dict:
    url = f"{NWS_API_BASE_URL}/stations/{station_id}"
    try:
        resp = httpx.get(url, timeout=10)
        resp.raise_for_status()
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"Weather API not reachable: {e}")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=resp.status_code, detail=f"Weather API error: {e}")
    data = resp.json()

    properties = data["properties"].copy()
    if "geometry" in data:
        properties["geometry"] = data["geometry"]
    properties["station_id"] = station_id  # Ensure station_id is always present
    return properties

def fetch_observations(station_id: str, start: str, end: str) -> list:
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
def run_pipeline():
    output = io.StringIO()
    try:
        with contextlib.redirect_stdout(output):
            from pipeline import main
            main()
        return {"status": "Pipeline started", "output": output.getvalue()}
    except HTTPException as e:
        logger.error(f"HTTPException: {e.detail}")
        return {"error": str(e), "output": output.getvalue()}
    except OperationalError as e:
        logger.error(f"Database not reachable: {e}")
        return {"error": f"Database not reachable: {e}", "output": output.getvalue()}
    except RuntimeError as e:
        logger.error(f"Bad input: {e}")
        return {"error": str(e), "output": output.getvalue()}
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
def average_temperature():
    db_url = DATABASE_URL
    try:
        with get_connection(db_url) as conn:
            result = get_average_temperature_last_week(conn)
    except OperationalError as e:
        raise HTTPException(status_code=503, detail=f"Database not reachable: {e}")
    return {"average_temperature": result}

@api_v1.get("/metrics/max-wind-speed-change")
def max_wind_speed_change():
    db_url = DATABASE_URL
    try:
        with get_connection(db_url) as conn:
            result = get_max_wind_speed_change_last_7_days(conn)
    except OperationalError as e:
        raise HTTPException(status_code=503, detail=f"Database not reachable: {e}")
    return {"max_wind_speed_change": result}

@api_v1.get("/health")
def health():
    try:
        with get_connection(DATABASE_URL) as conn:
            conn.cursor().execute("SELECT 1;")
        return {"status": "ok"}
    except Exception:
        raise HTTPException(status_code=503, detail="Database not reachable")

app = FastAPI()
app.include_router(api_v1, prefix="/v1") 