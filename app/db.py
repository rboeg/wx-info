from typing import Any
import polars as pl
import psycopg
from psycopg import OperationalError

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS stations (
    station_id TEXT PRIMARY KEY,
    station_name TEXT,
    station_timezone TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS weather_observations (
    station_id TEXT NOT NULL REFERENCES stations(station_id),
    observation_timestamp TIMESTAMPTZ NOT NULL,
    temperature DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    PRIMARY KEY (station_id, observation_timestamp)
);
"""

def get_connection(db_url: str) -> psycopg.Connection:
    """Get a psycopg3 connection. Raises OperationalError with diagnostics if connection fails."""
    try:
        return psycopg.connect(db_url)
    except OperationalError as e:
        diag = getattr(e, 'diag', None)
        msg = f"Could not connect to database: {e}"
        if diag:
            msg += f" | SQLSTATE: {getattr(diag, 'sqlstate', None)} | Message: {getattr(diag, 'message_primary', None)}"
        raise OperationalError(msg) from e

def check_postgres_service(db_url: str, timeout: int = 5) -> bool:
    """Check if the Postgres service is running and the database is reachable."""
    try:
        with psycopg.connect(db_url, connect_timeout=timeout) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return True
    except OperationalError:
        return False

def create_schema(conn: psycopg.Connection) -> None:
    """Create the stations and weather_observations tables if they do not exist."""
    with conn.cursor() as cur:
        for statement in SCHEMA_SQL.strip().split(';'):
            if statement.strip():
                cur.execute(statement)
    conn.commit()

def upsert_station(conn: psycopg.Connection, station_meta: dict) -> None:
    """Upsert a station record into the stations table."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stations (station_id, station_name, station_timezone, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (station_id) DO UPDATE SET
                station_name = EXCLUDED.station_name,
                station_timezone = EXCLUDED.station_timezone,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude;
            """,
            (
                station_meta.get("station_id"),
                station_meta.get("name"),
                station_meta.get("timeZone"),
                station_meta.get("geometry", {}).get("coordinates", [None, None])[1],
                station_meta.get("geometry", {}).get("coordinates", [None, None])[0],
            )
        )
    conn.commit()

def upsert_weather_data(conn: psycopg.Connection, df: pl.DataFrame) -> int:
    """Upsert weather data from a Polars DataFrame into the weather_observations table. Returns the number of rows upserted."""
    if df.is_empty():
        return 0
    records = df.to_dicts()
    with conn.cursor() as cur:
        for rec in records:
            cur.execute(
                """
                INSERT INTO weather_observations (
                    station_id, observation_timestamp, temperature, wind_speed, humidity
                ) VALUES (%(station_id)s, %(observation_timestamp)s, %(temperature)s, %(wind_speed)s, %(humidity)s)
                ON CONFLICT (station_id, observation_timestamp) DO UPDATE SET
                    temperature = EXCLUDED.temperature,
                    wind_speed = EXCLUDED.wind_speed,
                    humidity = EXCLUDED.humidity;
                """,
                rec
            )
    conn.commit()
    return len(records)

def get_latest_observation_timestamp(conn: psycopg.Connection, station_id: str) -> str | None:
    """Return the latest observation timestamp for the given station_id, or None if no data exists."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(observation_timestamp)
            FROM weather_observations
            WHERE station_id = %s
            """,
            (station_id,)
        )
        result = cur.fetchone()
        return result[0].isoformat() if result and result[0] else None 