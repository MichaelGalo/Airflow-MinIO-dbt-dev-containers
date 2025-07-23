from datetime import datetime, timedelta
import logging
from pathlib import Path

from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore
from airflow.utils.dates import days_ago  # type: ignore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "bootcamp-student",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "catchup": False,
}

dag = DAG(
    "staging_dag",
    default_args=default_args,
    description="dbt staging pipeline",
    schedule_interval=timedelta(minutes=3),
    max_active_runs=1
)

# dbt project paths (in container)
DBT_PROJECT_DIR = "/opt/airflow/dbt/my_dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt/my_dbt_project"

# dbt debug - check connections and configuration
dbt_debug = BashOperator(
    task_id="dbt_debug",
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt debug --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)

# dbt deps - install dependencies (if any)
dbt_deps = BashOperator(
    task_id="dbt_deps",
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)

# dbt run - execute staging models
dbt_run_staging = BashOperator(
    task_id="dbt_run_staging",
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select path:models/silver --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)

# dbt test - run tests on staging models
# dbt_test_staging = BashOperator(
#     task_id="dbt_test_staging",
#     bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select path:models/silver --profiles-dir {DBT_PROFILES_DIR}",
#     dag=dag,
# )

# actually enforcing the test from sources
# dbt_test_staging = BashOperator(
#     task_id="dbt_test_staging",
#     bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --models staging.* --profiles-dir {DBT_PROFILES_DIR}",
#     dag=dag,
# )

# Task dependencies - simple linear flow
dbt_debug >> dbt_deps >> dbt_run_staging
