from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


def greet_message():
    """Simple Python function for a task."""
    print("👋 Hello, Airflow! Starting the pipeline.")


def finish_message():
    """Simple function to end."""
    print("✅ DAG completed successfully!")

with DAG(
    dag_id="simple_hello_world",
    start_date=datetime(2025, 9, 27, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["essential", "example"],
) as dag:
    
    # 1. Python task using traditional PythonOperator
    hello_python = PythonOperator(
        task_id="greet_task",
        python_callable=greet_message,
    )

    # 2. Bash Task
    goodbye_bash = BashOperator(
        task_id="log_end_of_pipeline",
        bash_command='echo "--- Executing Bash command: Airflow is great. ---"',
    )

    # 3. Empty Task (EmptyOperator) to mark the end
    pipeline_finish = EmptyOperator(
        task_id="finish_task"
    )

    # Definition of dependencies
    hello_python >> goodbye_bash >> pipeline_finish