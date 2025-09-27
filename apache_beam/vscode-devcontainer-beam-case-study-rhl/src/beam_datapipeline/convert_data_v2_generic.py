import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import json
import pyarrow as pa
import pyarrow.json as pj
import os
from fastavro.schema import load_schema
import io


# Requires the libraries: `pip install 'apache-beam[gcp]' pyarrow fastavro`

# --- Read Avro Schema ---
def load_avro_schema(schema_path: str) -> dict:
    """
    Loads and returns an Avro schema from a JSON file.

    Args:
        schema_path (str): The full path to the schema file (.avsc).

    Returns:
        dict: The Avro schema as a Python dictionary.
        
    Raises:
        FileNotFoundError: If the schema file is not found at the specified path.
        json.JSONDecodeError: If the content of the file is not valid JSON.
        ValueError: If the Avro schema is not a valid dictionary.
    """
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    try:
        with open(schema_path, 'r') as f:
            avro_schema = json.load(f)
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Error decoding JSON schema: {e}", e.doc, e.pos)

    if not isinstance(avro_schema, dict):
        raise ValueError("Avro schema must be a dictionary.")

    return avro_schema

def avro_type_to_pyarrow_type(avro_type):
    if isinstance(avro_type, list): # Handle unions
        if "null" in avro_type:
            non_null_types = [t for t in avro_type if t != "null"]
            if len(non_null_types) == 1:
                return avro_type_to_pyarrow_type(non_null_types[0]), True
        # More complex union handling might be needed
        raise NotImplementedError("Complex Avro union types not fully supported")
    elif avro_type == "int":
        return pa.int32(), False
    elif avro_type == "long":
        return pa.int64(), False
    elif avro_type == "string":
        return pa.string(), False
    elif avro_type == "float":
        return pa.float32(), False
    elif avro_type == "double":
        return pa.float64(), False
    # Add more mappings for other Avro types
    else:
        raise NotImplementedError(f"Avro type '{avro_type}' not supported")


def convert_avro_schema_to_pyarrow_schema(avro_schema):    
    pyarrow_fields = []
    print(f"avro {avro_schema}")
    for field in avro_schema["fields"]:
        pa_type, nullable = avro_type_to_pyarrow_type(field["type"])
        pyarrow_fields.append(pa.field(field["name"], pa_type, nullable=nullable))

    pyarrow_schema = pa.schema(pyarrow_fields)
    return pyarrow_schema

def run_pipeline():
    """Run the Beam pipeline."""
    
    # 1. Define command line options.
    # Add an option to select the output format.
    class MyOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument(
                '--input_path',
                dest='input_path',
                default='./telemetry.jsonl',
                help='Path to the input .json/.jsonl file.')
            parser.add_argument(
                '--avro_schema_path',
                dest='avro_schema_path',
                help='Path to the Avro schema file (.avsc).')
            parser.add_argument(
                '--output_path',
                dest='output_path',
                default='./output/telemetry',
                help='Path to the output data files.')
            parser.add_argument(
                '--output_format',
                dest='output_format',
                choices=['avro', 'parquet'],
                default='parquet',
                help='Output format: "avro" or "parquet".')
    
    options = PipelineOptions().view_as(MyOptions)

    # 2. Build the pipeline.
    with beam.Pipeline(options=options) as p:
        # Read data from the .jsonl file
        data = (
            p | 'ReadFromJSONL' >> beam.io.ReadFromText(options.input_path)
              | 'ParseJSON' >> beam.Map(json.loads)
        )
        
        # 4. Decide the output format based on user choice.
        if options.output_format == 'avro':
            # read Avro schema
            avro_schema = load_schema(options.avro_schema_path)
            data | 'WriteToAvro' >> beam.io.avroio.WriteToAvro(
                file_path_prefix=options.output_path,
                schema=avro_schema,
                file_name_suffix='.avro',       # File extension
                num_shards=1                    # Optional: Number of output files (shards)
            )
            print(f"Data written in Avro format to: {options.output_path}-xxxx-of-yyyy.avro")
            
        elif options.output_format == 'parquet':
            # use avro schema to infer parquet schema
            # read Avro schema
            avro_schema = load_schema(options.avro_schema_path)
            parquet_schema = convert_avro_schema_to_pyarrow_schema(avro_schema)
            print(f"Inferred Parquet schema: {parquet_schema}")
            data | 'WriteToParquet' >> beam.io.WriteToParquet(
                    file_path_prefix=options.output_path,
                    schema=parquet_schema,
                    file_name_suffix='.parquet',  # File extension
                    num_shards=1                  # Optional: Number of output files (shards)
                    )
            
            print(f"Data written in Parquet format to: {options.output_path}-xxxx-of-yyyy.parquet")
            
        else:
            raise ValueError(f"Invalid output format: {options.output_format}")

if __name__ == '__main__':
    run_pipeline()