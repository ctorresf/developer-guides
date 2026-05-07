import os
import sys

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import pika
import time

sys.path.insert(0, os.path.dirname(__file__))
from vsa_shared import build_vsa_pipeline

try:
    from apache_beam.io.rabbitmqio import ReadFromRabbitMQ
except ImportError:  # RabbitMQ connector is optional in this environment
    ReadFromRabbitMQ = None


DEFAULT_RABBITMQ_HOST = "localhost"
DEFAULT_RABBITMQ_PORT = 5672
DEFAULT_RABBITMQ_QUEUE = "telemetry"
DEFAULT_FALLBACK_INPUT = "data/vsa_pipeline/telemetry_stream.json"
DEFAULT_ALERT_PREFIX = "output/vsa/rabbitmq_alerts"
DEFAULT_SUMMARY_PREFIX = "output/vsa/rabbitmq_summary"
DEFAULT_BAD_PREFIX = "output/vsa/rabbitmq_bad"


def _decode_payload(payload):
    if isinstance(payload, bytes):
        return payload.decode("utf-8")
    return payload

class ReadFromRabbitMQ(beam.DoFn):
    """Función personalizada para leer de RabbitMQ usando pika"""
    def __init__(self, host, port, queue):
        self.host = host
        self.port = port
        self.queue = queue

    def process(self, element):
        # Establecer conexión
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port))
        channel = connection.channel()
        
        # Obtener mensajes (esto es una simplificación para modo Batch o streaming controlado)
        method_frame, header_alpha, body = channel.basic_get(self.queue)
        if method_frame:
            yield body.decode('utf-8')
            channel.basic_ack(method_frame.delivery_tag)
        
        connection.close()

class RabbitMQStreamingConsumer(beam.DoFn):
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def setup(self):
        """Se ejecuta una vez al iniciar el worker"""
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, heartbeat=600)
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def process(self, _):
        """Generador que mantiene la lectura activa"""
        # generator para consumir mensajes indefinidamente
        for method_frame, properties, body in self.channel.consume(self.queue_name, auto_ack=False):
            if body:
                # Confirmamos la recepción para que RabbitMQ lo elimine de la cola ANTES de procesar
                self.channel.basic_ack(method_frame.delivery_tag)
                yield body.decode('utf-8')
            
            # Pequeña pausa para no saturar el hilo en caso de colas vacías
            # (El generador de pika.consume ya es eficiente, pero esto ayuda en ciertos Runners)
            if not body:
                time.sleep(0.1)

    def teardown(self):
        """Limpieza al cerrar el worker"""
        if self.connection and not self.connection.is_closed:
            self.connection.close()

def create_rabbitmq_source(pipeline: beam.Pipeline, host: str, port: int, queue: str, fallback_path: str = None):
    if ReadFromRabbitMQ is not None:
        source = (pipeline 
            | "Start" >> beam.Create([None])
            #| "Read from RabbitMQ" >> beam.ParDo(ReadFromRabbitMQ(host=host, port=port, queue=queue))
            | "Read from RabbitMQ" >> beam.ParDo(RabbitMQStreamingConsumer(host=host, port=port, queue=queue))
        )
        return source | "Decode RabbitMQ payload" >> beam.Map(_decode_payload)

    if fallback_path:
        return pipeline | "Read fallback telemetry" >> beam.io.ReadFromText(fallback_path)

    raise RuntimeError(
        "RabbitMQ IO connector is not installed and no fallback_path was provided."
    )


def run_logitrans_vsa_rabbitmq(
    host: str = DEFAULT_RABBITMQ_HOST,
    port: int = DEFAULT_RABBITMQ_PORT,
    queue: str = DEFAULT_RABBITMQ_QUEUE,
    fallback_path: str = DEFAULT_FALLBACK_INPUT,
    alert_prefix: str = DEFAULT_ALERT_PREFIX,
    summary_prefix: str = DEFAULT_SUMMARY_PREFIX,
    bad_prefix: str = DEFAULT_BAD_PREFIX,
):
    options = PipelineOptions(streaming=True)
    with beam.Pipeline(options=options) as pipeline:
        source = create_rabbitmq_source(pipeline, host, port, queue, fallback_path)
        build_vsa_pipeline(source, alert_prefix, summary_prefix, bad_prefix)


if __name__ == "__main__":
    run_logitrans_vsa_rabbitmq()
