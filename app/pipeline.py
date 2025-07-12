import os
from typing import Any
import api
import db
import transform
import httpx
import polars as pl
import psycopg
from dotenv import load_dotenv

class WeatherPipeline:
    """Pipeline to fetch, transform, and store weather data for a station."""
    def __init__(self, station_id: str, db_url: str) -> None:
        """Initialize pipeline with station ID and database URL."""
        self.station_id: str = station_id
        self.db_url: str = db_url

    def run(self) -> None:
        """Run the pipeline: fetch, transform, and upsert weather data."""
        print(f"Starting pipeline for station {self.station_id}")
        from datetime import datetime, timedelta, timezone
        try:
            with db.get_connection(self.db_url) as conn:
                db.create_schema(conn)
                latest_ts = db.get_latest_observation_timestamp(conn, self.station_id)
                end = datetime.now(timezone.utc)
                if latest_ts is None:
                    start = end - timedelta(days=7)
                    print("No previous data found. Fetching last 7 days.")
                else:
                    start = datetime.fromisoformat(latest_ts)
                    print(f"Latest observation in DB: {latest_ts}. Fetching new data since then.")
                try:
                    station_meta = api.fetch_station_metadata(self.station_id)
                    db.upsert_station(conn, station_meta)
                except Exception as e:
                    print(f"Error fetching or upserting station metadata: {e}")
                    raise
                try:
                    observations = api.fetch_observations(
                        self.station_id,
                        start=start.isoformat(),
                        end=end.isoformat(),
                    )
                except Exception as e:
                    print(f"Error fetching observations: {e}")
                    raise
                df = transform.flatten_observations(observations)
                if df.is_empty():
                    print("No observations to process.")
                    raise RuntimeError("No observations to process.")
                # No need to enrich with station fields, as those are now in the stations table
                n = db.upsert_weather_data(conn, df)
                print(f"Upserted {n} records for station {self.station_id}")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise

def main() -> None:
    """Entry point for running the weather pipeline. Validates required environment variables."""
    dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(dotenv_path, override=True)

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