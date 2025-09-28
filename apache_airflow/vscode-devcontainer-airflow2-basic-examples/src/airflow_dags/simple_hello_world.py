from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# 

def greet_message():
    """Función simple de Python para una tarea."""
    print("👋 ¡Hola, Airflow! Empezando el pipeline.")

def finish_message():
    """Función simple para finalizar."""
    print("✅ ¡DAG completado con éxito!")

with DAG(
    dag_id="simple_hello_world",
    start_date=pendulum.datetime(2025, 9, 27, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["basico", "ejemplo"],
) as dag:
    
    # 1. Tarea Python usando PythonOperator tradicional
    hello_python = PythonOperator(
        task_id="greet_task",
        python_callable=greet_message,
    )

    # 2. Tarea Bash
    goodbye_bash = BashOperator(
        task_id="log_end_of_pipeline",
        bash_command='echo "--- Ejecutando comando Bash: Airflow es genial. ---"',
    )

    # 3. Tarea Vacía (EmptyOperator) para marcar el final
    pipeline_finish = EmptyOperator(
        task_id="finish_task"
    )

    # Definición de dependencias
    hello_python >> goodbye_bash >> pipeline_finish