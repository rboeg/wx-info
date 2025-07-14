"""
Database utilities for the Weather Data Pipeline project.

This module provides functions to:
- Connect to the PostgreSQL database (psycopg3)
- Create and manage the normalized schema (stations, weather_observations)
- Upsert station metadata and weather observation data
- Query for the latest observation timestamp (for incremental fetch)
- Check database health (for API health checks)

Usage hints:
- Expects a valid PostgreSQL DATABASE_URL (see .env.example)
- All schema changes are managed via create_schema()
- Upserts use ON CONFLICT for idempotency and incremental loading
- Designed for use with Polars DataFrames for efficient ETL
"""

from typing import Optional

import polars as pl
import psycopg
from psycopg import OperationalError
from psycopg import Connection

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS wxinfo;

CREATE TABLE IF NOT EXISTS wxinfo.stations (
    station_id TEXT PRIMARY KEY,
    station_name TEXT,
    station_timezone TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS wxinfo.weather_observations (
    station_id TEXT NOT NULL REFERENCES wxinfo.stations(station_id),
    observation_timestamp TIMESTAMPTZ NOT NULL,
    temperature DOUBLE PRECISION,
    dewpoint DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    barometric_pressure DOUBLE PRECISION,
    relative_humidity DOUBLE PRECISION,
    precipitation_last_hour DOUBLE PRECISION,
    PRIMARY KEY (station_id, observation_timestamp)
);
"""


def get_connection(db_url: str) -> Connection:
    """
    Get a psycopg3 connection to the database, and set search_path to wxinfo.

    Args:
        db_url (str): PostgreSQL connection string (see .env.example)
    Returns:
        psycopg.Connection: Active database connection
    Raises:
        OperationalError: If connection fails, with diagnostics for debugging
    """
    try:
        conn = psycopg.connect(db_url)
        with conn.cursor() as cur:
            cur.execute("SET search_path TO wxinfo;")
        return conn
    except OperationalError as e:
        diag = getattr(e, 'diag', None)
        msg = f"Could not connect to database: {e}"
        if diag:
            # Add extra diagnostic info if available (SQLSTATE, message)
            msg += f" | SQLSTATE: {getattr(diag, 'sqlstate', None)} | Message: {getattr(diag, 'message_primary', None)}"
        raise OperationalError(msg) from e


def check_postgres_service(db_url: str, timeout: int = 5) -> bool:
    """
    Check if the Postgres service is running and the database is reachable.

    Args:
        db_url (str): PostgreSQL connection string
        timeout (int): Connection timeout in seconds (default: 5)
    Returns:
        bool: True if reachable, False otherwise
    """
    try:
        with psycopg.connect(db_url, connect_timeout=timeout) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return True
    except OperationalError:
        # Connection failed or DB not reachable
        return False


def create_schema(conn: Connection) -> None:
    """
    Create the stations and weather_observations tables if they do not exist.
    Idempotent: safe to call multiple times.

    Args:
        conn (psycopg.Connection): Active database connection
    """
    with conn.cursor() as cur:
        # Split SCHEMA_SQL on semicolons to execute each statement separately
        for statement in SCHEMA_SQL.strip().split(';'):
            if statement.strip():
                cur.execute(statement)
    conn.commit()


def upsert_station(conn: Connection, station_meta: dict) -> None:
    """
    Upsert a station record into the stations table.
    Uses ON CONFLICT for idempotency (safe for repeated loads).

    Args:
        conn (psycopg.Connection): Active database connection
        station_meta (dict): Station metadata (from NWS API observation)
            Required keys: station_id, name, timeZone, latitude, longitude
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO wxinfo.stations (station_id, station_name, station_timezone, latitude, longitude)
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
                station_meta.get("latitude"),
                station_meta.get("longitude"),
            )
        )
    conn.commit()


def upsert_weather_data(conn: Connection, df: pl.DataFrame) -> int:
    """
    Upsert weather data from a Polars DataFrame into the weather_observations table.
    Uses ON CONFLICT for idempotency and incremental loading.

    Args:
        conn (psycopg.Connection): Active database connection
        df (pl.DataFrame): DataFrame with columns: station_id, observation_timestamp, temperature, wind_speed, barometric_pressure, relative_humidity, precipitation_last_hour, dewpoint
    Returns:
        int: Number of rows upserted
    """
    if df.is_empty():
        return 0
    records = df.to_dicts()
    with conn.cursor() as cur:
        for rec in records:
            cur.execute(
                """
                INSERT INTO wxinfo.weather_observations (
                    station_id, observation_timestamp, temperature, wind_speed, barometric_pressure, relative_humidity, precipitation_last_hour, dewpoint
                ) VALUES (
                    %(station_id)s, %(observation_timestamp)s, %(temperature)s, %(wind_speed)s, %(barometric_pressure)s, %(relative_humidity)s, %(precipitation_last_hour)s, %(dewpoint)s
                )
                ON CONFLICT (station_id, observation_timestamp) DO UPDATE SET
                    temperature = EXCLUDED.temperature,
                    wind_speed = EXCLUDED.wind_speed,
                    barometric_pressure = EXCLUDED.barometric_pressure,
                    relative_humidity = EXCLUDED.relative_humidity,
                    precipitation_last_hour = EXCLUDED.precipitation_last_hour,
                    dewpoint = EXCLUDED.dewpoint;
                """,
                rec
            )
    conn.commit()
    return len(records)


def get_latest_observation_timestamp(conn: Connection, station_id: str) -> Optional[str]:
    """
    Return the latest observation timestamp for the given station_id, or None if no data exists.
    Used for incremental fetch (only fetch new data after this timestamp).

    Args:
        conn (psycopg.Connection): Active database connection
        station_id (str): The station identifier
    Returns:
        str | None: ISO8601 timestamp string, or None if no data
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(observation_timestamp)
            FROM wxinfo.weather_observations
            WHERE station_id = %s
            """,
            (station_id,)
        )
        result = cur.fetchone()
        # result[0] is a datetime or None
        return result[0].isoformat() if result and result[0] else None
