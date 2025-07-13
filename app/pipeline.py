"""
Weather Data Pipeline for the Weather Data Pipeline project.

This module provides the ETL pipeline logic to:
- Fetch weather observations from the NWS API for a given station
- Transform and normalize the data for analytics
- Upsert station metadata and weather observations into a normalized Postgres schema
- Support incremental loading (fetch only new data since last observation)

Usage hints:
- Expects environment variables: WX_STATION_ID, DATABASE_URL (see .env.example)
- Designed for use as a CLI entrypoint or via FastAPI/Airflow
- All print output is captured by the API for Airflow log visibility
- Handles missing data, incremental fetch, and robust error reporting
"""
import os

from app import api, db, transform
from dotenv import load_dotenv


class WeatherPipeline:
    """
    Pipeline to fetch, transform, and store weather data for a station.

    Steps:
    1. Connect to the database and ensure schema exists
    2. Determine the time range for incremental fetch (last 7 days or since last observation)
    3. Fetch observations from the NWS API
    4. Extract and upsert station metadata
    5. Transform and upsert weather observations
    6. Print progress and errors for Airflow/CI visibility
    """
    def __init__(self, station_id: str, db_url: str) -> None:
        """
        Initialize pipeline with station ID and database URL.

        Args:
            station_id (str): NWS station identifier
            db_url (str): PostgreSQL connection string
        """
        self.station_id: str = station_id
        self.db_url: str = db_url

    def run(self) -> None:
        """
        Run the pipeline: fetch, transform, and upsert weather data.
        Handles incremental fetch and robust error reporting.
        Prints all output for Airflow log capture.
        """
        print(f"Starting pipeline for station {self.station_id}")
        from datetime import datetime, timedelta, timezone
        try:
            with db.get_connection(self.db_url) as conn:
                db.create_schema(conn)
                # Determine the time range for incremental fetch
                latest_ts = db.get_latest_observation_timestamp(conn, self.station_id)
                end = datetime.now(timezone.utc)
                if latest_ts is None:
                    # First run: fetch last 7 days
                    start = end - timedelta(days=7)
                    print("No previous data found. Fetching last 7 days.")
                else:
                    # Incremental: fetch only new data
                    start = datetime.fromisoformat(latest_ts)
                    print(f"Latest observation in DB: {latest_ts}. Fetching new data since then.")
                    # Add 1 second to start to avoid duplicate fetch
                    start = start + timedelta(seconds=1)
                try:
                    observations = api.fetch_observations(
                        self.station_id,
                        start=start.isoformat(),
                        end=end.isoformat(),
                    )
                except Exception as e:
                    print(f"Error fetching observations: {e}")
                    raise
                if not observations:
                    print("No new observations to process.")
                    return  # Exit cleanly, do not raise error
                # Extract station metadata from the first observation (NWS API embeds it)
                first_obs = observations[0]
                props = first_obs.get("properties", {})
                geometry = first_obs.get("geometry", {})
                station_meta = {
                    # Use stationId if present, else fallback to parsing from 'station' URL
                    "station_id": props.get("stationId") or props.get("station", "").split("/")[-1],
                    "name": props.get("stationName"),
                    "timeZone": props.get("timeZone", None),
                    "geometry": geometry,
                }
                db.upsert_station(conn, station_meta)
                # Transform observations to Polars DataFrame for efficient upsert
                df = transform.flatten_observations(observations)
                if df.is_empty():
                    print("No new observations to process.")
                    return  # Exit cleanly, do not raise error
                n = db.upsert_weather_data(conn, df)
                print(f"Upserted {n} records for station {self.station_id}")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise


def main() -> None:
    """
    Entry point for running the weather pipeline.
    Loads environment variables, validates required config, and runs the pipeline.
    Prints all output for Airflow/CI log capture.
    """
    # Load environment variables from app/.env if it exists, otherwise from .env in the project root
    app_env_path = os.path.join(os.path.dirname(__file__), ".env")
    root_env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))
    if os.path.exists(app_env_path):
        load_dotenv(app_env_path)
    else:
        load_dotenv(root_env_path)

    WX_STATION_ID = os.environ.get("WX_STATION_ID")
    DATABASE_URL = os.environ.get("DATABASE_URL")
    if not WX_STATION_ID:
        raise RuntimeError("WX_STATION_ID environment variable is required.")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is required.")

    pipeline = WeatherPipeline(WX_STATION_ID, DATABASE_URL)
    pipeline.run()


if __name__ == "__main__":
    main()
