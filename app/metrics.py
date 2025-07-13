"""
Metrics module for the Weather Data Pipeline project.

Provides SQL-based analytics functions for:
- Average observed temperature for the last full week (Mon-Sun)
- Maximum wind speed change in the last 7 days (rolling window)

Each function expects an open database connection and returns query results for analytics endpoints.
"""

from typing import Any, List
from psycopg import Connection


def get_average_temperature_last_week(conn: Connection) -> List[Any]:
    """
    Compute the average observed temperature for the last full week (Mon-Sun) for each station.

    Args:
        conn (Connection): psycopg database connection
    Returns:
        List[Any]: List of tuples with station metadata and average temperature
    """
    sql = """
    SELECT
      o.station_id,
      s.station_name,
      s.station_timezone,
      s.latitude,
      s.longitude,
      AVG(o.temperature) AS avg_temperature
    FROM weather_observations o
    JOIN stations s ON o.station_id = s.station_id
    WHERE o.observation_timestamp >= date_trunc('week', now()) - interval '7 days'
      AND o.observation_timestamp < date_trunc('week', now())
    GROUP BY o.station_id, s.station_name, s.station_timezone, s.latitude, s.longitude;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def get_max_wind_speed_change_last_7_days(conn: Connection) -> List[Any]:
    """
    Find the maximum wind speed change between consecutive observations in the last 7 days (rolling window).

    Args:
        conn (Connection): psycopg database connection
    Returns:
        List[Any]: List of tuples with station metadata and max wind speed change
    """
    sql = """
    SELECT
      o.station_id,
      s.station_name,
      s.station_timezone,
      s.latitude,
      s.longitude,
      MAX(wind_speed_change) AS max_wind_speed_change
    FROM (
      SELECT
        o.station_id,
        ABS(o.wind_speed - LAG(o.wind_speed) OVER (PARTITION BY o.station_id ORDER BY o.observation_timestamp)) AS wind_speed_change,
        o.observation_timestamp
      FROM weather_observations o
      WHERE o.observation_timestamp >= now() - interval '7 days'
    ) t
    JOIN stations s ON t.station_id = s.station_id
    GROUP BY t.station_id, s.station_name, s.station_timezone, s.latitude, s.longitude;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()
