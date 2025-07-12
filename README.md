# wx-info: Weather Data Pipeline

## Overview
A modular, containerized weather data pipeline using FastAPI, Airflow, Docker, PostgreSQL, and Python. The pipeline fetches weather data from the National Weather Service API, stores it in a database, and exposes metrics via an API.

---

## Usage Examples

### Trigger the Pipeline
**Request:**
```bash
curl -X POST http://localhost:8000/run-pipeline
```
**Response (success):**
```json
{"status": "Pipeline started"}
```
**Response (error, e.g., API not reachable):**
```json
{"detail": "Weather API not reachable: ..."}
```

### Get Average Temperature (last week)
**Request:**
```bash
curl http://localhost:8000/metrics/average-temperature
```
**Response:**
```json
{"average_temperature": 23.5}
```

### Get Max Wind Speed Change (last 7 days)
**Request:**
```bash
curl http://localhost:8000/metrics/max-wind-speed-change
```
**Response:**
```json
{"max_wind_speed_change": 12.3}
```

---

## Assumptions
- **Station ID** is provided via the `WX_STATION_ID` environment variable (see `.env.example`).
- The database schema is fixed and created automatically if missing (see `app/db.py`).
- The pipeline fetches 7 days of data on first run, then only new data (incremental fetch).
- The API and pipeline are designed for a single station at a time, but can be extended for multiple stations.
- All configuration is via environment variables (see `.env.example`).
- The system is intended to run in Docker Compose, with service names used for inter-container networking.

---

## Pipeline Flow

1. **Trigger**: The pipeline is triggered via the `/run-pipeline` API or Airflow DAG.
2. **Environment Validation**: Required environment variables are checked at startup.
3. **DB Connection**: Connect to PostgreSQL and ensure the schema exists.
4. **Determine Fetch Window**: If no data exists, fetch last 7 days; otherwise, fetch only new data since the latest observation.
5. **Fetch Metadata**: Retrieve station metadata from the NWS API.
6. **Fetch Observations**: Retrieve weather observations for the determined window.
7. **Transform**: Normalize and enrich the data.
8. **Upsert**: Insert or update records in the database, avoiding duplicates.
9. **Metrics**: Metrics endpoints query the database for analytics.

### Pipeline Diagram

```
flowchart TD
    A[Trigger: /run-pipeline or Airflow] --> B[Validate env vars]
    B --> C[Connect to DB & ensure schema]
    C --> D[Determine fetch window]
    D --> E[Fetch station metadata]
    D --> F[Fetch observations]
    E & F --> G[Transform & enrich data]
    G --> H[Upsert into DB]
    H --> I[Metrics available via API]
```

---

## See Also
- [env.example](./env.example) for required environment variables
- [wx_info_specifications.md](./wx_info_specifications.md) for the original project specification 