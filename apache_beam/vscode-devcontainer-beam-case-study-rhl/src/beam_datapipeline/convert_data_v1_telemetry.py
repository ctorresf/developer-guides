import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import json
import pyarrow as pa
import os

# Requires the libraries: `pip install 'apache-beam[gcp]' pyarrow fastavro`

# --- Data Schemas ---
# Different schemas are needed for Avro and Parquet.

# Schema for Avro (Python dictionary format)
telemetry_avro_schema = {
    "type": "record",
    "name": "HelicopterTelemetry",
    "fields": [
        {"name": "HelicopterID", "type": "string"},
        {"name": "LapNumber", "type": "int"},
        {"name": "Time_s", "type": "float"},
        {"name": "Speed_km_h", "type": "float"},
        {"name": "Altitude_m", "type": "float"},
        {"name": "FuelLevel_percentage", "type": "float"},
        {"name": "EngineTemp_c", "type": "int"},
        {"name": "RotorRPM", "type": "int"},
        {"name": "Latitude", "type": "double"},
        {"name": "Longitude", "type": "double"}
    ]
}

# Schema for Parquet (using the pyarrow library)
telemetry_parquet_schema = pa.schema([
    pa.field('HelicopterID', pa.string()),
    pa.field('LapNumber', pa.int64()),
    pa.field('Time_s', pa.float32()),
    pa.field('Speed_km_h', pa.float32()),
    pa.field('Altitude_m', pa.float32()),
    pa.field('FuelLevel_percentage', pa.float32()),
    pa.field('EngineTemp_c', pa.int64()),
    pa.field('RotorRPM', pa.int64()),
    pa.field('Latitude', pa.float64()),
    pa.field('Longitude', pa.float64())
])

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
            data | 'WriteToAvro' >> beam.io.avroio.WriteToAvro(
                file_path_prefix=options.output_path,
                schema=telemetry_avro_schema,
                file_name_suffix='.avro',       # File extension
                num_shards=1                    # Optional: Number of output files (shards)
            )
            print(f"Data written in Avro format to: {options.output_path}-xxxx-of-yyyy.avro")
            
        elif options.output_format == 'parquet':
            data | 'WriteToParquet' >> beam.io.WriteToParquet(
                file_path_prefix=options.output_path,
                schema=telemetry_parquet_schema,
                file_name_suffix='.parquet',  # File extension
                num_shards=1                  # Optional: Number of output files (shards)
                )
            print(f"Data written in Parquet format to: {options.output_path}-xxxx-of-yyyy.parquet")
            
        else:
            raise ValueError(f"Invalid output format: {options.output_format}")

if __name__ == '__main__':
    run_pipeline()