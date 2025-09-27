# utils.py
# Este archivo contiene una colección de métodos de utilidad reutilizables.
import uuid
import io
import logging
from minio import Minio
from minio.error import S3Error
from .. import settings

def get_helicopter_teams_id(num_teams):
    helicopter_ids = []
    for i in range(1, num_teams + 1): 
        helicopter_ids.append(f"HRL{i:03d}") # Formatea el número a tres dígitos (ej. 001)

    return helicopter_ids

def write_json_to_minio(data, object_name):
    
    # Initialize the MinIO client
    minio_client = Minio(
        settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=settings.MINIO_SECURE
    )

    try:
        # 1. Encode the JSON string to bytes
        # MinIO (and S3) expects bytes for object content
        json_bytes = data.encode('utf-8')

        # 2. Create a BytesIO object from the bytes
        # put_object expects a file-like object
        data_stream = io.BytesIO(json_bytes)
        data_length = len(json_bytes)

        # 3. Ensure the bucket exists (optional, but good practice)
        if not minio_client.bucket_exists(settings.bucket_name):
            print(f"Bucket '{settings.bucket_name}' does not exist. Creating it...")
            minio_client.make_bucket(settings.bucket_name)
            print(f"Bucket '{settings.bucket_name}' created successfully.")
        else:
            print(f"Bucket '{settings.bucket_name}' already exists.")

        # 4. Upload the JSON bytes to MinIO
        minio_client.put_object(
            settings.bucket_name,
            object_name,
            data_stream,
            data_length,
            content_type='application/json' # Set content type for proper serving
        )

        print(f"Successfully wrote JSON to '{settings.bucket_name}/{object_name}'")

    except S3Error as exc:
        print(f"MinIO error occurred: {exc}")
    except Exception as exc:
        print(f"An unexpected error occurred: {exc}")

def delivery_report(err, msg):
    """
    Callback function to handle delivery reports of produced messages.
    Invoked once for each message produced to indicate success or failure.
    """
    if err is not None:
        print(f"Failed to deliver message to topic {msg.topic()} [{msg.partition()}]: {err}")
    else:
        print(f"Message delivered to topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        print(f"Key: {msg.key().decode('utf-8') if msg.key() else 'N/A'}, Value: {msg.value().decode('utf-8')}")

def send_json_to_kafka(json_string, topic, producer):
    """
    Sends a Python dictionary as a JSON string to a Kafka topic.
    """
    try:
        # 1. Encode the JSON string to bytes
        # Kafka expects message values (and keys) to be bytes
        json_bytes = json_string.encode('utf-8')

        # 2. Define a key (optional, but good for partitioning)
        # Using a part of your data as a key can ensure messages with the same key
        # go to the same partition, maintaining order for that key.
        # Make sure the key is also encoded to bytes.
        message_key = str(uuid.uuid4()).encode('utf-8') # Example key

        # 3. Produce the message asynchronously
        # The delivery_report callback will be invoked upon delivery success or failure.
        producer.produce(
            topic=topic,
            key=message_key,
            value=json_bytes,
            callback=delivery_report # Asynchronous send
        )
        print("Attempted to send message")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# --- Función para inicializar el cliente S3 (boto3) ---
def get_s3_client(endpoint_url=settings.MINIO_ENDPOINT, user=settings.MINIO_ACCESS_KEY, password=settings.MINIO_SECRET_KEY):
    """
    Crea y retorna un cliente S3 configurado para MinIO.
    """
    import boto3
    from botocore.config import Config

    return boto3.client(
        's3',
        endpoint_url=f"http://{endpoint_url}",
        aws_access_key_id=user,
        aws_secret_access_key=password,
        config=Config(signature_version='s3v4'),
        verify=False # Necesario si MinIO usa un certificado autofirmado o HTTP
    )

# --- Función para asegurar que el bucket exista ---
def ensure_minio_bucket_exists(bucket_name, logging):
    """
    Verifica si un bucket existe en MinIO y lo crea si no.
    """
    s3_client = get_s3_client()
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logging.info(f"El bucket '{bucket_name}' ya existe en MinIO.")
    except s3_client.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            logging.info(f"El bucket '{bucket_name}' no existe, creándolo...")
            s3_client.create_bucket(Bucket=bucket_name)
            logging.info(f"Bucket '{bucket_name}' creado exitosamente en Minio.")
        else:
            logging.error(f"Error al verificar/crear el bucket '{bucket_name}': {e}")
            raise

