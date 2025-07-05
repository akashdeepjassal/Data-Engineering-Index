from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {"owner": "airflow", "start_date": datetime(2024, 1, 1)}

with DAG(
    "historical_and_financials",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Run historical_load.py then daily_financials.py",
) as dag:

    run_historical_load = BashOperator(
        task_id="run_historical_load",
        bash_command="python3 /app/historical_load.py",
        cwd="/app",
    )

    run_daily_financials = BashOperator(
        task_id="run_daily_financials",
        bash_command="python3 /app/daily_financials.py",
        cwd="/app",
        
    )

    run_historical_load >> run_daily_financials
