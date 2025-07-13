# wx-info: Weather Data Pipeline

## Overview

A modular, containerized weather data pipeline using FastAPI, Airflow, Docker, PostgreSQL, and Python. The pipeline fetches weather data from the National Weather Service API, stores it in a database, and exposes metrics via an API.

---

## Usage Examples

### Trigger the Pipeline

**Request:**

```bash
curl -X POST http://localhost:8000/v1/run-pipeline
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
curl http://localhost:8000/v1/metrics/average-temperature
```

**Response:**

```json
{"average_temperature": 23.5}
```

### Get Max Wind Speed Change (last 7 days)

**Request:**

```bash
curl http://localhost:8000/v1/metrics/max-wind-speed-change
```

**Response:**

```json
{"max_wind_speed_change": 12.3}
```

---

## Assumptions

- **Station ID** is provided via the `WX_STATION_ID` environment variable (see [`env.example`](./env.example)).
- The database schema is fixed and created automatically if missing (see `app/db.py`).
- The pipeline fetches 7 days of data on first run, then only new data (incremental fetch).
- The API and pipeline are designed for a single station at a time, but can be extended for multiple stations.
- All configuration is via environment variables (see [`env.example`](./env.example)).
- The system is intended to run in Docker Compose, with service names used for inter-container networking.

---

## Pipeline Flow

- A. **Trigger**: The pipeline is triggered via the `/v1/run-pipeline` API or Airflow DAG.
- B. **Environment validation**: Required environment variables are checked at startup.
- C. **DB connection**: Connect to PostgreSQL and ensure the schema exists.
- D. **Determine fetch window**: If no data exists, fetch last 7 days; otherwise, fetch only new data since the latest observation.
- E. **Fetch metadata**: Retrieve station metadata from the NWS API.
- F. **Fetch observations**: Retrieve weather observations for the determined window.
- G. **Transform**: Normalize the data.
- H. **Upsert**: Insert or update records in the database, avoiding duplicates.
- I. **Metrics**: Metrics endpoints query the database for analytics.

### Pipeline Diagram

```text
flowchart TD
    A[Trigger: /v1/run-pipeline or Airflow] --> B[Validate env vars]
    B --> C[Connect to DB & ensure schema]
    C --> D[Determine fetch window]
    D --> E[Fetch station metadata]
    D --> F[Fetch observations]
    E & F --> G[Transform data]
    G --> H[Upsert into DB]
    H --> I[Metrics available via API]
```

---

## Development Environment

- The `scripts/env_setup.sh` script creates a local virtual environment (named `wx-info-env`) for development use if Conda/Miniconda is not found, or sets up a Conda environment if available. This ensures all dependencies are installed and isolated for development.

---

## Metrics

This project exposes two key metrics via the API, as required by the specification:

### 1. Average Observed Temperature for Last Week

- **Requirement:**
  - Compute the average observed temperature for last week (Mon-Sun).
- **Assumption:**
  - "Last week (Mon-Sun)" means the most recent *full* week prior to the current week. For example, if today is Wednesday, June 18, 2025, the last week is Monday, June 9, 2025 00:00:00 to Sunday, June 15, 2025 23:59:59.
- **SQL Query:**

  ```sql
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
  ```

- **Explanation:**
  - `date_trunc('week', now())` gives the timestamp for the most recent Monday at 00:00:00 (start of the current week).
  - Subtracting 7 days gives the start of the previous week (Monday 00:00:00).
  - The query selects all records from last week's Monday (inclusive) up to this week's Monday (exclusive), covering exactly one full week (Mon-Sun).

### 2. Maximum Wind Speed Change in the Last 7 Days

- **Requirement:**
  - Find the maximum wind speed change between two consecutive observations in the last 7 days.
- **Assumption:**
  - The "last 7 days" means the 7 days prior to the current moment (rolling window).
- **SQL Query:**

  ```sql
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
  ```

- **Explanation:**
  - Uses a window function (`LAG`) to compute the difference in wind speed between consecutive observations for each station.
  - Considers only observations from the last 7 days (rolling window).
  - Returns the maximum observed change per station.

---

## Deployment

To deploy the wx-info Weather Data Pipeline, follow these steps:

### 0. Prerequisites

**Host Requirements:**

- Docker and Docker Compose installed and running
- Python 3.10+ (for running scripts or local development)
- Bash shell (for running provided `.sh` scripts)

**Network:**

- Internet access (to pull Docker images and fetch weather data from the NWS API)

### 1. Environment Setup

a. **Clone the repository:**

  ```bash
  git clone git@github.com:rboeg/wx-info.git
  cd wx-info
  ```

b. **Configure environment variables:**

- Edit `app/.env` to set your `DATABASE_URL`, `WX_STATION_ID`, and any other required variables.
- (Optional) If using Docker Compose, ensure a `.env` file exists in the project root with the same variables for Compose substitution.

c. **Run environment setup script:**

- This script installs Python dependencies and prepares the environment (for local dev):

  ```bash
  ./scripts/env_setup.sh
  ```

### 2. Deploy with Docker Compose

a. **Before starting services, you must create the required Docker network:**

  ```bash
  ./scripts/docker_network_setup.sh
  ```

b. **Start all services (API, DB, Airflow, etc.):**

  ```bash
  docker compose up --build
  ```

- This will build and start all containers as defined in `docker-compose.yml`.
- The FastAPI app will be available at `http://localhost:8000` by default.
- The Airflow web interface will be available at `http://localhost:8080` by default.

### 3. Running the Pipeline

**Trigger the pipeline via API:**

- Use the `/run-pipeline` endpoint to start the ETL process:

  ```bash
  curl -X POST http://localhost:8000/v1/run-pipeline
  ```

- Or trigger via Airflow if configured.

### 4. Stopping the Services

**Stop all containers:**

  ```bash
  docker compose down
  ```

---

## Docker Containers and Services

The following containers are created and managed by `docker compose` using `docker-compose.yml`:

### Application Containers (API and Database)

- **wx-info-app-1**: The FastAPI application container exposing the weather data API and pipeline endpoints.
- **wx-info-postgres-1**: PostgreSQL database for storing weather observations and station metadata (used by the app).

### Airflow Containers

- **wx-info-airflow-init-1**: Initializes Airflow metadata DB and environment on first run.
- **wx-info-airflow-webserver-1**: Airflow web UI for managing and monitoring DAGs.
- **wx-info-airflow-triggerer-1**: Handles triggering of deferred tasks in Airflow 2.x.
- **wx-info-airflow-worker-1**: Executes Airflow tasks using the CeleryExecutor.
- **wx-info-airflow-scheduler-1**: Schedules DAG runs and tasks.
- **wx-info-postgres-airflow-1**: Dedicated PostgreSQL instance for Airflow metadata.
- **wx-info-redis-1**: Redis broker for Celery task queueing in Airflow.

Each container is networked via the `wx-info-net` Docker network for secure inter-service communication.

---

## Running the Airflow DAG

To run the weather data pipeline DAG in Airflow:

1. Open the Airflow web interface at [http://localhost:8080](http://localhost:8080).
2. Log in with username: `airflow` and password: `airflow` (default credentials).
3. In the DAGs list, find `weather_pipeline_dag` (it will appear automatically).
4. You can trigger the DAG manually by clicking the "play" (trigger) button next to its name, or let it run on its schedule.

---

## Accessing the Application Database

You can connect to the application's PostgreSQL database using any standard database client (e.g., DBeaver, TablePlus, psql, DataGrip).

- The connection details (host, port, user, password, database name) are provided in your `.env` file (see [`env.example`](./env.example)).
- If running via Docker Compose, the default port is `5432` and the service name is `postgres`.
- Example connection string:

  ```bash
  postgresql://<user>:<password>@localhost:5432/<database>
  ```

- Make sure the database container is running before attempting to connect.

---

## Database Architecture

### Schema Overview

- **stations** (dimension table):
  - Contains metadata for each weather station (ID, name, timezone, latitude, longitude).
  - Each station is stored only once, reducing redundancy.
- **weather_observations** (fact table):
  - Stores time-series weather data (temperature, wind speed, humidity, timestamp) for each station.
  - Each observation references a station via a foreign key.

#### Entity Relationship Diagram

```text
stations (dimension)
-------------------
station_id (PK)
station_name
station_timezone
latitude
longitude

weather_observations (fact)
--------------------------
station_id (FK to stations) (PK)
observation_timestamp (PK)
temperature
wind_speed
humidity
```

---

## Tests

Automated tests have been implemented using [pytest](https://pytest.org/). The test suite covers core functionality and a wide range of edge cases, including error handling, database connectivity, and data validation. Tests are located in the `tests/` directory and are designed to ensure the robustness and reliability of the pipeline and API.

As part of the development workflow, tests are automatically run before commits and on pull requests using both GitHub hooks and GitHub Actions. This helps maintain code quality and prevent regressions throughout the development process.

---
