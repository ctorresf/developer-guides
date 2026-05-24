from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id='00_infra_setup_dbt_venv',
    start_date=datetime(2026, 1, 1),
    schedule='@once', # Se ejecuta una sola vez al arrancar
    catchup=False,
    #is_paused_upon_creation=False
) as dag:

    # Creamos el entorno virtual e instalamos dbt-core y dbt-postgres de forma aislada
    setup_venv = BashOperator(
        task_id='create_isolated_dbt_environment',
        bash_command="""
            python -m venv /home/airflow/dbt_venv && \
            /home/airflow/dbt_venv/bin/pip install --upgrade pip && \
            /home/airflow/dbt_venv/bin/pip install dbt-core==1.11.11 dbt-postgres==1.10.0
        """,
    )

    