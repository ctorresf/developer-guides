
from __future__ import annotations

import pendulum

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.models.param import Param
from pathlib import Path

with DAG(
    dag_id=Path(__file__).stem,  # Dynamically set dag_id to the filename
    description="A simple DAG to demonstrate basic Airflow functionality",
    schedule=None,
    start_date=pendulum.datetime(2025, 3, 1, tz="UTC"),
    catchup=False,
    tags=["example", "params", "branching"],
    params={
        "Asignatura": Param(
            type="string",
            description="Define la lista se asignaturas.",
            enum = ["Programación", "Base de datos", "Sistemas Operativos"],
            default="Programación",
            title="Asignaturas",
        ),
        "Nombre": Param("Alumno ", type="string", title="Nombre Alumnos"),
        "Puntaje": Param(0, type="integer", title="Nota final"),
    },
) as dag:
    first_step = EmptyOperator(task_id="First_step")

    def choose_branch(result):
        print(f"Evaluating result: {result}")
        if int(result) > 5:
            return ['Step_A']
        return ['Step_B']

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=choose_branch,
        op_args=["{{ params.Puntaje }}"],
    )

    parallel_a_step = BashOperator(
        task_id="Step_A",
        bash_command="echo \"$ALUMNO aprobo $ASIGNATURA\"",
        env={"ALUMNO": "{{ params.Nombre }}", "ASIGNATURA": "{{ params.Asignatura }}"}
    )
    parallel_b_step = BashOperator(
        task_id="Step_B",
        bash_command="echo \"$ALUMNO debe elevar solicitud a dirección\"",
        env={"ALUMNO": "{{ params.Nombre }}"}
    )

    first_step >> branching >> [parallel_a_step, parallel_b_step]  