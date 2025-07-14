"""
Airflow DAG for the Weather Data Pipeline project.

This DAG orchestrates the daily ETL process:
- Triggers the FastAPI pipeline endpoint to fetch, transform, and load weather data
- Provides start and end dummy tasks for clear DAG visualization
- Uses HttpOperator to call the API endpoint inside the app container

Usage hints:
- Requires an Airflow HTTP connection named 'app_api' (see Airflow UI > Admin > Connections)
- The 'app_api' connection is created automatically with the Airflow container using the environment variable:
    AIRFLOW_CONN_APP_API=http://app:8000
- The FastAPI app must be running and accessible from the Airflow container.
- Schedule and start_date can be adjusted as needed.
"""

from airflow.decorators import dag, task
from airflow.providers.http.operators.http import HttpOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# Define the DAG
@dag(
    default_args=default_args,
    description='Weather data pipeline DAG',
    schedule_interval='@hourly',
    start_date=datetime(2025, 7, 10),
    catchup=False,
    tags=['weather', 'pipeline']
)
def weather_pipeline():
    """
    Orchestrates the weather data pipeline ETL process.
    - start: Dummy task for DAG start
    - run_pipeline_task: Triggers the FastAPI /v1/run-pipeline endpoint
    - end: Dummy task for DAG end

    To trigger the pipeline for multiple stations, set the 'data' parameter in HttpOperator as follows:
        data=json.dumps({"station_ids": ["KATL", "003PG"]})
    If 'data' is not set, the API will use the NWS_STATION_ID environment variable (which can be a string or JSON array).
    """
    # import json  # Uncomment if you use the 'data' parameter below
    @task
    def start():
        """Dummy start task for DAG visualization."""
        print("Starting the weather pipeline DAG.")

    @task
    def end():
        """Dummy end task for DAG visualization."""
        print("Weather pipeline DAG completed.")

    start_task = start()
    # HttpOperator triggers the FastAPI pipeline endpoint
    run_pipeline_task = HttpOperator(
        task_id='trigger_weather_pipeline',
        http_conn_id='app_api',  # Must match Airflow connection name
        endpoint='v1/run-pipeline',
        method='POST',
        headers={'Content-Type': 'application/json'},
        # Uncomment and edit the next line to trigger for specific stations:
        # data=json.dumps({"station_ids": ["KATL", "003PG"]}),
        log_response=True
    )
    end_task = end()

    # Set task dependencies for clear DAG flow
    start_task >> run_pipeline_task >> end_task


weather_pipeline_dag = weather_pipeline()
