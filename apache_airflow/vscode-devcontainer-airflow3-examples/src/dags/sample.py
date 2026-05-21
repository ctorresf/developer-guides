from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="my_first_airflow3_dag",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "airflow3"]
)
def my_first_airflow3_dag_pipeline():
    @task
    def extract_data():
        """Extracts data from a source."""
        print("Extracting data...")
        return {"data": "some_raw_data"}

    @task
    def transform_data(raw_data):
        """Transforms the extracted data."""
        print(f"Transforming: {raw_data['data']}")
        transformed = raw_data['data'].upper()
        return {"transformed_data": transformed}

    @task
    def load_data(final_data):
        """Loads the transformed data into a destination."""
        print(f"Loading: {final_data['transformed_data']}")
        print("Data loaded successfully!")

    # Define the task dependencies
    extracted_info = extract_data()
    transformed_info = transform_data(extracted_info)
    load_data(transformed_info)

# Instantiate the DAG
my_first_airflow3_dag_pipeline()