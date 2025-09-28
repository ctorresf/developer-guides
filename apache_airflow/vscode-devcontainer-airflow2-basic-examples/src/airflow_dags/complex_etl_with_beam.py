from __future__ import annotations

import pendulum
import logging
import os
import csv
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

# --- Setting Up the Staging Environment ---
# These paths simulate a local filesystem, but Beam could read/write directly to S3/GCS.
CONTAINER_BASE_PATH = "/workspaces/developer-guides/apache_airflow/vscode-devcontainer-airflow2-basic-examples"
VSA_PATH = CONTAINER_BASE_PATH + "/output/volatile_staging_beam"
PSA_PATH = CONTAINER_BASE_PATH + "/output/persistent_staging_beam"

os.makedirs(VSA_PATH, exist_ok=True)
os.makedirs(PSA_PATH, exist_ok=True)

# ----------------- Functions -----------------

@task
def extract_from_api() -> str:
    """Extracts data from a simulated source and saves the raw file to VSA."""
    logging.info("Starting raw data extraction.")
    
    data = [
        {'user_id': 101, 'name_raw': 'jOHn DoE', 'location_code': 'US_NY', 'order_value': 150.55, 'is_active_flag': 1},
        {'user_id': 102, 'name_raw': 'ALICE smith', 'location_code': 'GB_LDN', 'order_value': 20.00, 'is_active_flag': 0},
        {'user_id': 103, 'name_raw': 'Bob Jr.', 'location_code': 'US_CA', 'order_value': 500.99, 'is_active_flag': 1},
        {'user_id': 104, 'name_raw': 'Eve', 'location_code': 'FR_PAR', 'order_value': 12.30, 'is_active_flag': 1}
    ]
    
    filename = f"raw_customers_{pendulum.now().int_timestamp}.csv"
    vsa_filepath = os.path.join(VSA_PATH, filename)
    
    # Write the raw CSV
    fieldnames = data[0].keys()
    with open(vsa_filepath, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    logging.info(f"Raw data stored in VSA: {vsa_filepath}")
    return vsa_filepath

@task
def persist_data(filename: str):
    """Moves a copy of the raw data to the Persistent Staging Area (PSA)."""
    vsa_filepath = os.path.join(VSA_PATH, "Raw", filename)
    psa_filepath = os.path.join(PSA_PATH, f"ARCHIVE_{filename}")

    # In this example, we copy the file to PSA and leave it in VSA
    os.system(f"cp {vsa_filepath} {psa_filepath}")
    logging.info(f"Raw data persisted in PSA: {psa_filepath}")


@task
def load_to_data_warehouse(filename: str):
    """Loads the final data from VSA to the Data Warehouse (simulated)."""
    logging.info("Loading Records into a Data Warehouse...")
    # Load simulation to DW
    logging.info("✅ Load completed. Data is ready in the DW.")


# ----------------- Definition of DAG -----------------

@dag(
    dag_id="complex_etl_with_beam",
    start_date=pendulum.datetime(2025, 9, 27, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["advanced", "beam", "etl", "dataflow"],
)
def complex_data_pipeline_beam():
    """
    DAG that implements complex ETL with Apache Beam for large-scale processing, 
    using VSA and PSA.
    """
    
    raw_filename = extract_from_api() 
    
    persist_data_task = persist_data(raw_filename)
    
    BEAM_PIPELINE_NORMALIZED_PATH = os.path.join(CONTAINER_BASE_PATH, "src/beam_pipelines/transformation_normalize.py")
    NORMALIZED_PATH = os.path.join(VSA_PATH, "normalized_data")
    normalized_filename = BeamRunPythonPipelineOperator(
        task_id="normalized_filename",
        py_file=BEAM_PIPELINE_NORMALIZED_PATH,
        #py_options=["-m"],
        pipeline_options={
            "input": "{{ ti.xcom_pull(task_ids='extract_from_api') }}",
            "output": NORMALIZED_PATH,
        },
        runner="DirectRunner",
        #py_requirements=["apache-beam[gcp]==2.65.0"],
    )

    BEAM_PIPELINE_ENRICHED_PATH = os.path.join(CONTAINER_BASE_PATH, "src/beam_pipelines/transformation_enrich.py")
    enriched_filename  = BeamRunPythonPipelineOperator(
        task_id="enriched_filename",
        py_file=BEAM_PIPELINE_ENRICHED_PATH,
        pipeline_options={
            "input": NORMALIZED_PATH + "*",
            "output": os.path.join(PSA_PATH, "enriched_data"),
        },
        runner="DirectRunner",
        #py_requirements=["apache-beam[gcp]==2.67.0"],
    )
    
    load_task = load_to_data_warehouse(enriched_filename)
    
    # Volatile Staging Area (VSA) Cleaning
    clean_vsa = BashOperator(
        task_id="cleanup_volatile_staging",
        bash_command=f"rm -rf {VSA_PATH}/*",
        trigger_rule="all_success", 
    )

    #  Flow Definition
    raw_filename >> persist_data_task >> normalized_filename >> enriched_filename >> load_task >> clean_vsa

complex_data_pipeline_beam()