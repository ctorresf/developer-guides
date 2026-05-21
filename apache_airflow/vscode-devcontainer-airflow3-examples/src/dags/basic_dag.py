
from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="my_basic_dag",
    description="A simple DAG to demonstrate basic Airflow functionality",
    schedule="@daily",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=True,
    tags=["example", "simple_dag"],
) as dag:
    first_step = EmptyOperator(task_id="first_step")
    second_step = EmptyOperator(task_id="second_step")
    end_step = EmptyOperator(task_id="end_step")

    first_step >> second_step >> end_step 