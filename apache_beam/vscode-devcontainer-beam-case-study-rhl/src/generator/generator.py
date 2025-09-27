import argparse
import os
from . import race_event_data_generator
from . import telemetry_data_generator
from . import helicopter_teams_generator
from . import fan_engagement_data_generator
from ..utils import utils
from confluent_kafka import Producer
from .. import settings

def execute_generator(generator_type, *args, **kwargs):
    generators = {
        "race_event": lambda num_records, race_id, num_teams: race_event_data_generator.generate_random_race_event_data(num_records, race_id, num_teams),
        "telemetry": lambda num_records, race_id, num_teams:telemetry_data_generator.generate_random_telemetry_data(num_records, race_id, num_teams),
        "helicopter_teams": lambda num_records, race_id, num_teams:helicopter_teams_generator.generate_random_helicopter_teams(num_teams),
        "fan_engagement": lambda num_records, race_id, num_teams:fan_engagement_data_generator.generate_random_data_generator(num_records, race_id)
    }
    
    action_func = generators.get(generator_type)
    if action_func:
        try:
            # Desempaquetamos *args y **kwargs para pasarlos a la función lambda
            return action_func(*args, **kwargs)
        except TypeError as e:
            raise TypeError(f"Error al ejecutar la acción '{generator_type}'. Detalles: {e}")
    else:
        raise ValueError("Operación no soportada") 

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A Python script to generate random data.")

    parser.add_argument(
        "--output",
        type=str,
        required=False,
        default="stdout",
        help="The output to write the data. Can be file_path, kafka topic, minio bucket, stdout (by default)"
    )

    parser.add_argument(
        "--num_records",
        type=int,
        default=1,
        help="The amount of records to generate. 0 for infinite generation. Default is 1."
    )

    parser.add_argument(
        "--output_path",
        type=str,
        required=False,
        default="",
        help="The output path to write the data. If output is file_path, this is required."
    )

    parser.add_argument(
        "--race_id",
        type=str,
        required=False,
        default="Demo01",
        help="Identificador de la carrera. Default es Demo01"
    )

    parser.add_argument(
        "--generator_type",
        type=str,
        required=True,
        help="El generador a ser ejecutado"
    )

    parser.add_argument(
        "--num_teams",
        type=int,
        default=6,
        help="Cantidad de equipos que participan en la carrera. Default es 6."
    )

    args = parser.parse_args()

    output = args.output
    num_records = args.num_records
    race_id = args.race_id
    generator_type = args.generator_type
    num_teams = args.num_teams

    generator = execute_generator(generator_type, num_records, race_id, num_teams)
    if output == "stdout":
        for record in generator:
            print(record)
    elif output == "file_path":
        if args.output_path == "":
            raise ValueError("output_path must be specified when output is file_path.")
        os.makedirs(os.path.dirname(args.output_path), exist_ok=True)
        with open(args.output_path, 'w') as file:
            for record in generator:
                file.write(f"{record}\n")
        print(f"Data written to {args.output_path}")
    elif output == "minio":
        if args.output_path == "":
            raise ValueError("output_path must be specified when output is minio.")

        sample_data = "" 
        for record in generator:
            sample_data += f"{record}\n"

        #print(sample_data)
        utils.write_json_to_minio(sample_data, args.output_path)

    elif output == "kafka":
        # output_path es usado como el topico Kafka
        topic = args.output_path
        conf = {
            'bootstrap.servers': settings.KAFKA_BROKERS,
            'client.id': 'python-json-producer',
            # 'security.protocol': 'SASL_SSL', # Uncomment for secure clusters
            # 'sasl.mechanisms': 'PLAIN',      # Uncomment for secure clusters
            # 'sasl.username': 'your_username',# Uncomment for secure clusters
            # 'sasl.password': 'your_password' # Uncomment for secure clusters
        }
        producer = Producer(conf)
        for record in generator:
            utils.send_json_to_kafka(record, topic, producer)
        # Wait for any outstanding messages to be delivered and delivery reports to be received
        print("Waiting for messages to be delivered...")
        producer.poll(1)
        # Flush the producer to ensure all messages are sent
        print("Flushing producer...")   
        producer.flush(timeout=10) 
