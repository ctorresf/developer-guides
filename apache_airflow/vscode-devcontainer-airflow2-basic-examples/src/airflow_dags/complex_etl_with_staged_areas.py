from __future__ import annotations

import pendulum
import pandas as pd
import logging
import os

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

# --- Setting Up the Staging Environment ---
VSA_PATH = "/tmp/volatile_staging"
PSA_PATH = "/opt/airflow/persistent_staging"

# Ensure the directories exist (simulation)
os.makedirs(VSA_PATH, exist_ok=True)
os.makedirs(PSA_PATH, exist_ok=True)

# ----------------- Functions -----------------

@task
def extract_from_api() -> str:
    """Extracts data from a simulated source (API) and saves the raw file."""
    logging.info("Starting raw data extraction.")
    
    # Simulation of extracted data (e.g. JSON or CSV)
    data = {
        'user_id': [101, 102, 103, 104],
        'name_raw': ['jOHn DoE', 'ALICE smith', 'Bob Jr.', 'Eve'],
        'location_code': ['US_NY', 'GB_LDN', 'US_CA', 'FR_PAR'],
        'order_value': [150.55, 20.00, 500.99, 12.30],
        'is_active_flag': [1, 0, 1, 1]
    }
    df_raw = pd.DataFrame(data)
    
    # 1. Dump to a file for transfer
    filename = f"raw_customers_{pendulum.now().int_timestamp}.csv"
    vsa_filepath = os.path.join(VSA_PATH, filename)
    df_raw.to_csv(vsa_filepath, index=False)
    
    logging.info(f"Raw data stored in VSA: {vsa_filepath}")
    return filename

@task
def persist_data(filename: str):
    """Moves a copy of the raw data to the Persistent Staging Area (PSA)."""
    vsa_filepath = os.path.join(VSA_PATH, filename)
    psa_filepath = os.path.join(PSA_PATH, f"ARCHIVE_{filename}")

    # Simulation of moving/copying the file to PSA for auditing
    os.rename(vsa_filepath, psa_filepath)

    logging.info(f"Raw data persisted in PSA: {psa_filepath}")
    # NOTE: The data is NOT copied back to VSA for the next task.
    # The next task must read directly from the renamed file in PSA.
    # For this example, we move the file back to VSA to simulate the data handoff.
    # In a real case, the file would be copied, not moved.
    os.rename(psa_filepath, vsa_filepath)
    logging.info(f"File temporarily moved back to VSA for processing: {vsa_filepath}")


@task
def transformation_1_normalize(filename: str) -> str:
    """FIRST TRANSFORMATION: Normalizes (cleans) column names and data."""
    vsa_filepath = os.path.join(VSA_PATH, filename)
    df = pd.read_csv(vsa_filepath)
    
    logging.info("Transformation 1: Normalizing data...")
    
    # Name normalization: 'jOHn DoE' -> 'John Doe'
    df['name'] = df['name_raw'].str.title()
    
    # Column standardization: rename
    df = df.rename(columns={'location_code': 'country_city_code'})
    
    # Raw Column Removal
    df = df.drop(columns=['name_raw'])
    
    # Overwrite the file in VSA with the transformed data
    df.to_csv(vsa_filepath, index=False)
    logging.info(f"Transformation 1 completed. File updated in VSA: {vsa_filepath}")
    return filename

@task
def transformation_2_enrich(filename: str) -> str:
    """SECOND TRANSFORMATION: Enrichment (e.g. code mapping)."""
    vsa_filepath = os.path.join(VSA_PATH, filename)
    df = pd.read_csv(vsa_filepath)
    
    logging.info("Transformation 2: Enriching data...")
    
    # Simple Code Mapping (Enrichment)
    location_map = {
        'US_NY': 'America/New York', 
        'GB_LDN': 'Europe/London', 
        'US_CA': 'America/Los Angeles', 
        'FR_PAR': 'Europe/Paris'
    }
    df['timezone'] = df['country_city_code'].map(location_map)
    
    # Creating a derived column (high value flag)
    df['is_high_value'] = df['order_value'] > 100
    
    # Overwrite the file in VSA with the enriched data
    df.to_csv(vsa_filepath, index=False)
    logging.info(f"Transformation 2 completed. File updated in VSA: {vsa_filepath}")
    return filename


@task
def load_to_data_warehouse(filename: str):
    """Loads the final data from VSA to the Data Warehouse (simulated)."""
    vsa_filepath = os.path.join(VSA_PATH, filename)
    df_final = pd.read_csv(vsa_filepath)

    logging.info(f"Loading {len(df_final)} records to Data Warehouse...")

    # Simulating a connection to a database or DW (e.g. using a Hook)
    # db_hook = PostgresHook(postgres_conn_id='data_warehouse')
    # db_hook.insert_rows(table='dim_customers', rows=df_final.values.tolist())
    
    logging.info("✅ Loading complete. Data ready in the DW.")


# ----------------- Definition of DAG -----------------

@dag(
    dag_id="complex_etl_with_staged_areas",
    start_date=pendulum.datetime(2025, 9, 27, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["advanced", "etl", "staging", "transformacion"],
)
def complex_data_pipeline():
    """
    DAG that implements a complex ETL using a volatile staging area
    (VSA) for processing and a persistent (PSA) for archiving.
    """

    # The file is created and passed to the next task via XComs
    raw_filename = extract_from_api()
    
    # Save the raw file in PSA (Persistent)
    persist_data_task = persist_data(raw_filename)
    
    # First Transformation: Normalizes names
    normalized_filename = transformation_1_normalize(raw_filename)
    
    # Second Transformation: Enrichment
    enriched_filename = transformation_2_enrich(normalized_filename)
    
    # Final load
    load_task = load_to_data_warehouse(enriched_filename)
    
    # Volatile Staging Area (VSA) cleaning ONLY if everything was successful
    clean_vsa = BashOperator(
        task_id="cleanup_volatile_staging",
        bash_command=f"rm -rf {VSA_PATH}/*",
        # THIS IS KEY: Only runs if the final load is successful
        trigger_rule="all_success",
    )

    # Definition of the Flow
    # Persistence and normalization can be parallel, but persistence
    # is often a validation/archiving step before processing continues.
    # In this example, the flow is sequential:
    # 1. Extraction
    # 2. Persistence (Archiving)
    # 3. Transformation 1 (Normalization)
    # 4. Transformation 2 (Enrichment)
    # 5. Loading to DW
    # 6. Cleanup (Only if 5 was successful)
    
    raw_filename >> persist_data_task >> normalized_filename >> enriched_filename >> load_task >> clean_vsa

complex_data_pipeline()