# NOTE: If you see linter errors for Airflow imports, they may be false positives when linting outside the Airflow container.
# The imports below are correct for Airflow 2.x with the HTTP provider installed.
from airflow.decorators import dag, task
from airflow.providers.http.operators.http import HttpOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    description='Weather data pipeline DAG',
    schedule_interval='@daily',
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=['weather', 'pipeline']
)
def weather_pipeline():
    @task
    def start():
        print("Starting the weather pipeline DAG.")

    @task
    def end():
        print("Weather pipeline DAG completed.")

    start_task = start()
    run_pipeline_task = HttpOperator(
        task_id='trigger_weather_pipeline',
        http_conn_id='app_api',
        endpoint='v1/run-pipeline',
        method='POST',
        headers={'Content-Type': 'application/json'},
        log_response=True
    )
    end_task = end()

    start_task >> run_pipeline_task >> end_task

weather_pipeline_dag = weather_pipeline()