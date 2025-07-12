import psycopg

def get_average_temperature_last_week(conn):
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
    WHERE o.observation_timestamp >= date_trunc('week', now())
      AND o.observation_timestamp < date_trunc('week', now()) + interval '7 days'
    GROUP BY o.station_id, s.station_name, s.station_timezone, s.latitude, s.longitude;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()

def get_max_wind_speed_change_last_7_days(conn):
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