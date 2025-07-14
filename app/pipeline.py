"""
Weather Data Pipeline for the Weather Data Pipeline project.

This module provides the ETL pipeline logic to:
- Fetch weather observations from the NWS API for a given station
- Transform and normalize the data for analytics
- Upsert station metadata and weather observations into a normalized Postgres schema
- Support incremental loading (fetch only new data since last observation)

Usage hints:
- Expects environment variables: NWS_STATION_ID, DATABASE_URL (see .env.example)
- Designed for use as a CLI entrypoint or via FastAPI/Airflow
- All print output is captured by the API for Airflow log visibility
- Handles missing data, incremental fetch, and robust error reporting
"""
import os

from app import api, db, transform
from dotenv import load_dotenv
from typing import List


class WeatherPipeline:
    """
    Pipeline to fetch, transform, and store weather data for one or more stations.
    """
    def __init__(self, station_ids: List[str], db_url: str) -> None:
        """
        Initialize pipeline with station IDs and database URL.

        Args:
            station_ids (List[str]): List of NWS station identifiers
            db_url (str): PostgreSQL connection string
        """
        self.station_ids: List[str] = station_ids
        self.db_url: str = db_url

    def run(self) -> None:
        """
        Run the pipeline: fetch, transform, and upsert weather data for all stations.
        Handles incremental fetch and robust error reporting.
        Prints all output for Airflow log capture.
        """
        from datetime import datetime, timedelta, timezone
        for station_id in self.station_ids:
            print(f"Starting pipeline for station {station_id}")
            try:
                with db.get_connection(self.db_url) as conn:
                    db.create_schema(conn)
                    # Determine the time range for incremental fetch
                    latest_ts = db.get_latest_observation_timestamp(conn, station_id)
                    end = datetime.now(timezone.utc)
                    if latest_ts is None:
                        # First run: fetch last 7 days for this station
                        start = end - timedelta(days=7)
                        print("No previous data found. Fetching last 7 days.")
                    else:
                        # Incremental: fetch only new data since last observation
                        start = datetime.fromisoformat(latest_ts)
                        print(f"Latest observation in DB: {latest_ts}. Fetching new data since then.")
                        # Add 1 second to start to avoid duplicate fetch of the last record
                        start = start + timedelta(seconds=1)
                    try:
                        # Fetch observations from the NWS API for the given time window
                        observations = api.fetch_observations(
                            station_id,
                            start=start.isoformat(),
                            end=end.isoformat(),
                        )
                    except Exception as e:
                        # If the API call fails, log and raise the error
                        print(f"Error fetching observations: {e}")
                        raise
                    if not observations:
                        # No new data to process for this station
                        print(f"No new observations to process for station {station_id}.")
                        continue  # Move to next station
                    # Fetch canonical station metadata from the NWS stations endpoint
                    station_props = api.fetch_station_metadata(station_id)
                    station_meta = {
                        "station_id": station_props.get("stationIdentifier") or station_id,
                        "name": station_props.get("name"),
                        "timeZone": station_props.get("timeZone"),
                        "latitude": station_props.get("latitude"),
                        "longitude": station_props.get("longitude")
                    }
                    # Upsert station metadata to ensure referential integrity
                    db.upsert_station(conn, station_meta)
                    # Transform observations to Polars DataFrame for efficient upsert
                    df = transform.flatten_observations(observations)
                    if df.is_empty():
                        # Defensive: no valid data after transformation
                        print(f"No new observations to process for station {station_id}.")
                        continue
                    # Upsert weather data for this station
                    n = db.upsert_weather_data(conn, df)
                    print(f"Upserted {n} records for station {station_id}")
            except Exception as e:
                # Log and continue to next station on error
                print(f"Error processing station {station_id}: {e}")
                continue


def main() -> None:
    """
    Entry point for running the weather pipeline.
    Loads environment variables, validates required config, and runs the pipeline.
    Prints all output for Airflow/CI log capture.
    """
    import json
    # Load environment variables from app/.env if it exists, otherwise from .env in the project root
    app_env_path = os.path.join(os.path.dirname(__file__), ".env")
    root_env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))
    if os.path.exists(app_env_path):
        load_dotenv(app_env_path)
    else:
        load_dotenv(root_env_path)

    NWS_STATION_ID = os.environ.get("NWS_STATION_ID")
    DATABASE_URL = os.environ.get("DATABASE_URL")
    if not NWS_STATION_ID:
        raise RuntimeError("NWS_STATION_ID environment variable is required.")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is required.")

    # Parse NWS_STATION_ID as JSON array, or fallback to single string
    try:
        station_ids = json.loads(NWS_STATION_ID)
        if isinstance(station_ids, str):
            station_ids = [station_ids]
        elif not isinstance(station_ids, list):
            raise ValueError
    except Exception:
        # Fallback: treat as single station string
        station_ids = [NWS_STATION_ID]

    # Run the pipeline for the given station(s)
    pipeline = WeatherPipeline(station_ids, DATABASE_URL)
    pipeline.run()


if __name__ == "__main__":
    main()
