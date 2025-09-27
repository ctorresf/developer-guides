import argparse
import logging
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.io.kafka import ReadFromKafka
from ..utils import utils
from .. import settings

# Configuración de las variables de entorno para boto3
# Esto permite que boto3 se conecte a MinIO en lugar de AWS S3
os.environ['AWS_ACCESS_KEY_ID'] = settings.MINIO_ACCESS_KEY
os.environ['AWS_SECRET_ACCESS_KEY'] = settings.MINIO_SECRET_KEY
os.environ['AWS_REGION_NAME'] = 'us-east-1' # La región es arbitraria para MinIO local
os.environ['AWS_ENDPOINT_URL'] = f"http://{settings.MINIO_ENDPOINT}"

def main(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--output_bucket',
        required=True,
        help='')
    
    parser.add_argument(
        '--output_file_prefix',
        required=True,
        help='')
    
    parser.add_argument(
        '--input_events_topic',
        required=True,
        help='')
    
    parser.add_argument(
        '--input_telemetry_topic',
        required=True,
        help='')
    
    parser.add_argument(
        "--num_records",
        type=int,
        default=1,
        help=""
    )

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True

    s3_prefix = "s3://"
    if known_args.output_bucket.startswith(s3_prefix):
        bucket = known_args.output_bucket.removeprefix(s3_prefix)
        utils.ensure_minio_bucket_exists(bucket, logging)

    with beam.Pipeline(options=pipeline_options) as p:

        def format_as_json(element):
            if isinstance(element, tuple):
                return element[1].decode('utf-8')
            else:
                raise RuntimeError('unknown record type: %s' % type(element))

        output_path = f"{known_args.output_bucket}/{known_args.output_file_prefix}"
        
        events_records = (
            p
            # Read messages from an Apache Kafka topic.
            | "ReadEvents" >> ReadFromKafka(
                consumer_config={'bootstrap.servers': settings.KAFKA_BROKERS,
                                 'auto.offset.reset': 'earliest'}, # https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#auto-offset-reset
                topics=[known_args.input_events_topic],
                max_num_records=known_args.num_records  # for testing purposes
            )
            | 'FormatEventsOutputToJson' >> beam.Map(format_as_json)
        )
        (events_records
            # Subdivide the output into fixed 5-second windows.
            | "EWindow" >> beam.WindowInto(beam.window.FixedWindows(5))
            | "EventsWrite">> beam.io.WriteToText(output_path+"_race", file_name_suffix='.json')
        )

        telemetry_records = (
            p
            # Read messages from an Apache Kafka topic.
            | "ReadTelemetry" >> ReadFromKafka(
                consumer_config={'bootstrap.servers': settings.KAFKA_BROKERS,
                                 'auto.offset.reset': 'earliest'}, # https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#auto-offset-reset
                topics=[known_args.input_telemetry_topic],
                max_num_records=known_args.num_records  # for testing purposes
            )
            | 'FormatTelemetryOutputToJson' >> beam.Map(format_as_json)
        )
        (telemetry_records
            # Subdivide the output into fixed 5-second windows.
            | "TWindow" >> beam.WindowInto(beam.window.FixedWindows(5))
            | "TelemetryWrite" >> beam.io.WriteToText(output_path + "_telemetry", file_name_suffix='.json')
        )
        


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
