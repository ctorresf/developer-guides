
from __future__ import annotations

import pendulum

from airflow.operators.empty import EmptyOperator
from airflow import DAG

with DAG(
    "my_basic_dag",
    description="A simple DAG to demonstrate basic Airflow functionality",
    schedule="@daily", #https://airflow.apache.org/docs/apache-airflow/2.11.0/authoring-and-scheduling/cron.html
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=True,
    tags=["example", "simple_dag"],
) as dag:
    first_step = EmptyOperator(task_id="First_step")
    second_step = EmptyOperator(task_id="Second_step")
    end_step = EmptyOperator(task_id="End_step")

    first_step >> second_step >> end_step 