import argparse
import json
import os
import tempfile
import apache_beam as beam
from apache_beam.io import avroio, parquetio
import pyarrow as pa

# --- 1. Define Schemas for Avro and Parquet ---

# Avro Schema, for a Json schema you can use json.dumps
AVRO_SCHEMA = {
    "type": "record",
    "name": "UserRecord",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": "string"},
        {"name": "active", "type": "boolean"}
    ]
}

# PyArrow Schema for Parquet
PARQUET_SCHEMA = pa.schema([
    ('id', pa.int64()),
    ('name', pa.string()),
    ('email', pa.string()),
    ('active', pa.bool_()),
])

# --- 2. Helper Function to Create Dummy JSON Data ---

def create_dummy_json(file_path):
    """
    Creates a sample JSON file with one record per line.
    """
    data = [
        {"id": 1, "name": "Alice", "email": "alice@example.com", "active": True},
        {"id": 2, "name": "Bob", "email": "bob@example.com", "active": False},
        {"id": 3, "name": "Charlie", "email": "charlie@example.com", "active": True},
    ]
    with open(file_path, 'w') as f:
        for record in data:
            f.write(json.dumps(record) + '\n')
    print(f"Created dummy input file at {file_path}")

# --- 3. The Main Pipeline ---

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_dir',
        dest='output_dir',
        required=True,
        help='Directory to write the Avro and Parquet files.')
    
    known_args, pipeline_args = parser.parse_known_args()

    # Create a temporary input file for demonstration
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.json') as temp_file:
        input_file_path = temp_file.name
        create_dummy_json(input_file_path)

    try:
        # Create the pipeline object
        with beam.Pipeline(argv=pipeline_args) as pipeline:
            
            # 1. Read the JSON input file
            # Each line is read as a string.
            json_strings = pipeline | 'Read JSON lines' >> beam.io.ReadFromText(input_file_path)

            # 2. Parse each JSON string into a Python dictionary
            # This is the core transformation step
            parsed_records = json_strings | 'Parse JSON strings' >> beam.Map(json.loads)
            
            # 3. Write the parsed records to Avro format
            # The Avro writer needs a file path prefix and a schema
            parsed_records | 'Write to Avro' >> avroio.WriteToAvro(
                file_path_prefix=os.path.join(known_args.output_dir, 'users_avro'),
                schema=AVRO_SCHEMA,
                file_name_suffix='.avro'
            )
            
            # 4. Write the parsed records to Parquet format
            # The Parquet writer needs a file path prefix and a PyArrow schema
            parsed_records | 'Write to Parquet' >> parquetio.WriteToParquet(
                file_path_prefix=os.path.join(known_args.output_dir, 'users_parquet'),
                schema=PARQUET_SCHEMA,
                file_name_suffix='.parquet'
            )

    finally:
        # Clean up the temporary input file
        os.remove(input_file_path)
        print(f"Cleaned up temporary input file at {input_file_path}")

if __name__ == '__main__':
    main()