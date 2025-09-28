from __future__ import annotations

import pendulum
import random

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# 

def check_data_quality():
    """
    Function that simulates a data quality check and
    decides which path to follow.
    """
    # 70% chance that the quality is "acceptable"
    quality_ok = random.random() < 0.7 
    
    if quality_ok:
        print("Data quality: ACCEPTABLE!")
        return "process_normal_flow"
    else:
        print("Data quality: LOW! Triggering reprocessing.")
        return "trigger_reprocess_alert"

def normal_processing():
    print("Normal flow: Standard processing is running.")

def reprocess_and_alert():
    print("Reprocessing flow: Alert sent and reprocessing started.")

with DAG(
    dag_id="conditional_data_branch",
    start_date=pendulum.datetime(2025, 9, 27, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["advanced", "branch", "logica"],
) as dag:
    
    # 1. Branching task
    data_quality_check = BranchPythonOperator(
        task_id="branch_on_data_quality",
        python_callable=check_data_quality,
    )

    # 2. Possible paths
    normal_flow = PythonOperator(
        task_id="process_normal_flow",
        python_callable=normal_processing,
    )

    reprocess_flow = PythonOperator(
        task_id="trigger_reprocess_alert",
        python_callable=reprocess_and_alert,
    )

    # 3. Merge task
    pipeline_end = EmptyOperator(
        task_id="final_merge_point",
        # The trigger rule must be ONE_SUCCESS for it to run
        # regardless of which path was taken, as long as at least one has finished.
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # Definition of dependencies
    # The check branches into one of the two paths
    data_quality_check >> [normal_flow, reprocess_flow]
    
    # Both paths must point to the final junction point
    normal_flow >> pipeline_end
    reprocess_flow >> pipeline_end