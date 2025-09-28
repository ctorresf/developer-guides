
from __future__ import annotations

import pendulum

from airflow.operators.empty import EmptyOperator
from airflow import DAG

with DAG(
    "my_basic_dag_2",
    description="A simple DAG to demonstrate basic Airflow functionality",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 6, 1, tz="UTC"),
    max_active_runs=1,
    catchup=True,
) as dag:
    first_step = EmptyOperator(task_id="First_step")
    second_step = EmptyOperator(task_id="Second_step")
    parallel_a_step = EmptyOperator(task_id="Step_A")
    parallel_b_step = EmptyOperator(task_id="Step_B")
    end_step = EmptyOperator(task_id="End_step")

    first_step >> second_step >> [parallel_a_step, parallel_b_step] >> end_step 