from __future__ import annotations

import pendulum
import random

from airflow.decorators import dag, task

# 

@dag(
    dag_id="data_transfer_xcom_taskflow",
    start_date=pendulum.datetime(2025, 9, 27, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["intermediate", "xcom", "taskflow"],
    doc_md=__doc__,
)
def data_pipeline_with_xcom():
    """
    This DAG demonstrates data transfer between tasks (XComs)
    using the TaskFlow API (@task).
    """

    @task
    def extract_data():
        """Simulates data extraction and returns a list of IDs."""
        print("Extracting 10 random IDs...")
        data_ids = [random.randint(100, 999) for _ in range(10)]
        # The return is automatically handled as XCom
        return data_ids 

    @task
    def transform_data(ids: list[int]):
        """Receives IDs from the previous task, transforms them, and returns the count."""
        print(f"IDs received for transformation: {ids}")
        # Simulates a transformation: converts to string and joins
        processed_data = [str(i) for i in ids]
        return len(processed_data)

    @task
    def load_data(count: int):
        """Receives the count and loads it, logging a final message."""
        print(f"Loading {count} transformed records to a final destination.")
        print("✅ Data transfer pipeline completed.")

    # Definition of the flow using TaskFlow syntax
    extracted_ids = extract_data()
    record_count = transform_data(extracted_ids)
    load_data(record_count)

data_transfer_xcom_dag = data_pipeline_with_xcom()