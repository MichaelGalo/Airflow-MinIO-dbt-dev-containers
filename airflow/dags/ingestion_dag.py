import os
import sys
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import timedelta

workspace_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../'))
if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)

# Test import after path modification
try:
    from src.api_ingestion.space_weather import fetch_api_data, load_api_data
    print("SUCCESS: Import worked after path modification!")
except ImportError as e:
    print(f"FAILED: Import still failed: {e}")
print("===================")


def _run_space_weather_ingestion(**kwargs):
    """Consolidated function to run the entire space weather data pipeline"""
    # Get Airflow's task instance for logging
    ti = kwargs['ti']

    url = os.getenv("NASA_WEATHER_ALERTS_ENDPOINT")
    ti.log.info(f"Starting space weather pipeline with URL: {url}")

    ti.log.info("Fetching data from NASA Weather Alerts API")
    data = fetch_api_data(url)

    if data is not None:
        ti.log.info(
            "Data fetched successfully, proceeding to load data to MinIO")

        result = load_api_data(data)
        if result:
            ti.log.info(
                f"Pipeline completed successfully. File uploaded: {result}")
            return f"Pipeline completed successfully. File: {result}"
        else:
            ti.log.error("Failed to upload data to MinIO")
            raise Exception("Failed to upload data to MinIO")
    else:
        ti.log.error("Failed to fetch data from API")
        raise Exception("Failed to fetch data from API")


with DAG(
    dag_id="ingestion_dag",
    description="Raw data ingestion pipeline - runs every 10 days",
    schedule_interval=timedelta(days=10),
    start_date=days_ago(1),
    catchup=False,
) as dag:

    space_weather_task = PythonOperator(
        task_id="space_weather_pipeline",
        python_callable=_run_space_weather_ingestion,
        provide_context=True
    )
