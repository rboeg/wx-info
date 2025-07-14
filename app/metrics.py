"""
Metrics module for the Weather Data Pipeline project.

Provides SQL-based analytics functions for:
- Average observed temperature for the last full week (Mon-Sun)
- Maximum wind speed change in the last 7 days (rolling window)

Each function expects an open database connection and returns query results for analytics endpoints.
"""

from psycopg import Connection


def get_average_temperature_last_week(conn: Connection) -> list[dict]:
    """
    Compute the average observed temperature for the last full week (Mon-Sun) for each station.

    Args:
        conn (Connection): psycopg database connection
    Returns:
        List[dict]: List of dicts with station_id, avg_temperature, first and last observation timestamps for the week
    """
    sql = """
    WITH filtered AS (
      SELECT
        station_id,
        temperature,
        observation_timestamp
      FROM wxinfo.weather_observations
      WHERE observation_timestamp >= date_trunc('week', now()) - interval '7 days'
        AND observation_timestamp < date_trunc('week', now())
        AND temperature IS NOT NULL
    )
    SELECT
      station_id,
      ROUND(AVG(temperature)::numeric, 2) AS avg_temperature,
      MIN(observation_timestamp) AS first_observation,
      MAX(observation_timestamp) AS last_observation
    FROM filtered
    GROUP BY station_id;
    """
    # The query computes the average temperature for each station for the last *full* week (Mon-Sun)
    # and also returns the first and last observation timestamps considered for the week.
    # date_trunc('week', now()) gives the start of the current week (Monday 00:00:00).
    # Subtracting 7 days gives the start of the previous week.
    # The WHERE clause selects all records from last week's Monday (inclusive) up to this week's Monday (exclusive
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        # Map to list of dicts for API response
        return [
            {
                "station_id": row[0],
                "avg_temperature": float(row[1]) if row[1] is not None else None,
                "first_observation": row[2].isoformat() if row[2] else None,
                "last_observation": row[3].isoformat() if row[3] else None,
            }
            for row in rows
        ]


def get_max_wind_speed_change_last_7_days(conn: Connection) -> list[dict]:
    """
    Find the maximum wind speed change between consecutive observations in the last 7 days (rolling window).

    Args:
        conn (Connection): psycopg database connection
    Returns:
        List[dict]: List of dicts with station_id, max_wind_speed_change, and the timestamps of the two consecutive observations involved
    """
    sql = """
    WITH lagged AS (
      SELECT
        station_id,
        wind_speed,
        observation_timestamp,
        LAG(wind_speed) OVER (PARTITION BY station_id ORDER BY observation_timestamp) AS prev_wind_speed,
        LAG(observation_timestamp) OVER (PARTITION BY station_id ORDER BY observation_timestamp) AS prev_observation_timestamp
      FROM wxinfo.weather_observations
      WHERE observation_timestamp >= now() - interval '7 days'
        AND wind_speed IS NOT NULL
    ),
    diffs AS (
      SELECT
        station_id,
        ABS(wind_speed - prev_wind_speed) AS wind_speed_change,
        prev_observation_timestamp,
        observation_timestamp
      FROM lagged
      WHERE prev_wind_speed IS NOT NULL
    ),
    ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY wind_speed_change DESC NULLS LAST) AS rn
      FROM diffs
    )
    SELECT
      station_id,
      ROUND(wind_speed_change::numeric, 2) AS max_wind_speed_change,
      prev_observation_timestamp AS first_observation,
      observation_timestamp AS last_observation
    FROM ranked
    WHERE rn = 1;
    """
    # Query steps (complex because we want the maximum difference per station, along with the timestamps of the two consecutive observations involved):
    # 1. lagged: For each observation, get the previous wind_speed and timestamp for the same station (using LAG window function).
    # 2. diffs: Compute the absolute difference between each wind_speed and its previous value, keeping the relevant timestamps.
    # 3. ranked: Rank the differences per station, so the largest difference per station is ranked first.
    # 4. Final SELECT: For each station, return the row with the maximum wind speed change and the timestamps of the two consecutive observations involved.
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        return [
            {
                "station_id": row[0],
                "max_wind_speed_change": float(row[1]) if row[1] is not None else None,
                "first_observation": row[2].isoformat() if row[2] else None,
                "last_observation": row[3].isoformat() if row[3] else None,
            }
            for row in rows
        ]
