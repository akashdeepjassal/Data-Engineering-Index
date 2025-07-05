from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {"owner": "airflow", "start_date": datetime(2024, 1, 1)}

with DAG(
    "daily_and_index",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Run daily_load.py, daily_financials.py, then create_index.py",
) as dag:

    run_daily_load = BashOperator(
        task_id="run_daily_load",
        bash_command="python3 /app/daily_load.py",
        cwd="/app",
    )

    run_daily_financials = BashOperator(
        task_id="run_daily_financials",
        bash_command="python3 /app/daily_financials.py",
        cwd="/app",
    )

    run_create_index = BashOperator(
        task_id="run_create_index",
        bash_command="python3 /app/create_index.py",
        cwd="/app",
    )

    run_daily_load >> run_daily_financials >> run_create_index
