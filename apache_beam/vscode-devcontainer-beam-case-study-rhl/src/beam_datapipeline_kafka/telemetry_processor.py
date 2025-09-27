import argparse
import logging
import uuid
import os
import json
import redis
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.jdbc import WriteToJdbc
import psycopg
from psycopg import sql # Para construir consultas SQL de forma segura
from ..utils import utils
from .. import settings

# Configuración de las variables de entorno para boto3
# Esto permite que boto3 se conecte a MinIO en lugar de AWS S3
os.environ['AWS_ACCESS_KEY_ID'] = settings.MINIO_ACCESS_KEY
os.environ['AWS_SECRET_ACCESS_KEY'] = settings.MINIO_SECRET_KEY
os.environ['AWS_REGION_NAME'] = 'us-east-1' # La región es arbitraria para MinIO local
os.environ['AWS_ENDPOINT_URL'] = f"http://{settings.MINIO_ENDPOINT}"

# --- Clase para el cliente de Redis (para gestionar la conexión en Beam) ---
# Es importante que el cliente de Redis se cree por trabajador (o por proceso)
# para evitar problemas de serialización y gestión de conexiones.
class RedisClientFactory:
    """
    Una fábrica simple para crear y gestionar la conexión a Redis por trabajador.
    """
    def __init__(self, host, port, db):
        self.host = host
        self.port = port
        self.db = db
        self._redis_client = None

    def get_client(self):
        """
        Retorna una instancia del cliente Redis. Crea una si no existe.
        """
        if self._redis_client is None:
            logging.info(f"Creando nueva conexión a Redis en {self.host}:{self.port}/{self.db}...")
            self._redis_client = redis.StrictRedis(host=self.host, port=self.port, db=self.db, decode_responses=True)
            # Opcional: Probar la conexión
            try:
                self._redis_client.ping()
                logging.info("Conexión a Redis exitosa.")
            except redis.exceptions.ConnectionError as e:
                logging.error(f"Error al conectar a Redis: {e}")
                raise
        return self._redis_client


# Instancia global de la fábrica de clientes Redis.
# Esta instancia será serializada y enviada a los trabajadores,
# y cada trabajador creará su propia conexión real a través de get_client().
redis_factory = RedisClientFactory(settings.REDIS_HOST, settings.REDIS_PORT, settings.REDIS_DB)


# --- Función para escribir datos individuales en Redis ---
def write_to_redis(element):
    """
    Escribe un elemento de datos en Redis.
    Este método se ejecutará en cada trabajador de Beam.
    """
    r = redis_factory.get_client() # Obtener la conexión a Redis para este trabajador

    # Los elementos de la PCollection son tuplas KV
    key = element[0]
    value = element[1]
    
    if key and value:
        try:
            # Ejemplo 1: Guardar como un string simple
            r.set(f"simple_key:{key}", value)
            logging.info(f"Redis: SET simple_key:{key} = {value}")

            # Ejemplo 2: Guardar como un hash (para datos más estructurados)
            # Asumimos que `element` puede tener otros campos para el hash
            if 'details' in element:
                hash_key = f"hash_data:{key}"
                r.hmset(hash_key, element['details'])
                logging.info(f"Redis: HMSET {hash_key} = {element['details']}")
        except redis.exceptions.RedisError as e:
            logging.error(f"Error al escribir en Redis para la clave {key}: {e}")
    else:
        logging.warning(f"Elemento sin clave o valor: {element}. No se escribió en Redis.")


# --- Clase para la fábrica de conexiones de PostgreSQL ---
# Es crucial gestionar las conexiones a la base de datos por trabajador/proceso
# para evitar problemas de serialización y asegurar que cada trabajador tenga su propia conexión.
class PostgreSQLClientFactory:
    """
    Una fábrica simple para crear y gestionar la conexión a PostgreSQL por trabajador.
    """
    def __init__(self, host, port, dbname, user, password):
        self.conn_params = {
            'host': host,
            'port': port,
            'dbname': dbname,
            'user': user,
            'password': password
        }
        self._conn = None

    def get_connection(self):
        """
        Retorna una instancia de conexión a PostgreSQL. Crea una si no existe o si se desconectó.
        """
        if self._conn is None or self._conn.closed:
            logging.info(f"Creando nueva conexión a PostgreSQL en {self.conn_params['host']}:{self.conn_params['port']}/{self.conn_params['dbname']}...")
            try:
                self._conn = psycopg.connect(**self.conn_params)
                self._conn.autocommit = False # Usar transacciones explícitas
                logging.info("Conexión a PostgreSQL exitosa.")
            except psycopg.Error as e:
                logging.error(f"Error al conectar a PostgreSQL: {e}")
                raise
        return self._conn

# --- Función para preparar la base de datos (crear tabla si no existe) ---
# Esta función se ejecuta ANTES de que el pipeline comience, no dentro de Beam.
def setup_database(table_name):
    """
    Se asegura de que la base de datos y la tabla de destino existan en PostgreSQL.
    """
    try:
        
        conn = psycopg.connect(
            host=settings.PG_HOST, 
            port=settings.PG_PORT, 
            dbname=settings.PG_DB, 
            user=settings.PG_USER, 
            password=settings.PG_PASSWORD
        )
        conn.autocommit = True # Necesario para CREATE DATABASE/TABLE
        cur = conn.cursor()

        # Crear la base de datos si no existe (puede requerir conexión a 'postgres' o 'template1')
        try:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(settings.PG_DB)))
            logging.info(f"Base de datos '{settings.PG_DB}' creada exitosamente.")
        except psycopg.errors.DuplicateDatabase:
            logging.info(f"Base de datos '{settings.PG_DB}' ya existe.")

        conn.close() # Cerrar conexión y reabrir a la DB recién creada/existente

        # Reabrir conexión a la base de datos específica para crear la tabla
        pg_factory = PostgreSQLClientFactory(
            settings.PG_HOST, 
            settings.PG_PORT, 
            settings.PG_DB, 
            settings.PG_USER, 
            settings.PG_PASSWORD
        )
        conn = pg_factory.get_connection()
        conn.autocommit = True # Necesario para CREATE TABLE (aunque la factory pone False)
                              # La pondremos True aquí para el DDL, luego se usará con False en el Map

        cur = conn.cursor()

        # Crear la tabla si no existe
        # Asumimos que los datos del diccionario tendrán 'id', 'value', 'email', 'age', 'price', 'category', etc.
        # Ajusta este esquema de tabla según los datos reales que pasarás.
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id VARCHAR(255) PRIMARY KEY,
                HelicopterID VARCHAR(255),
                LapNumber INTEGER,
                Time_s  NUMERIC(10, 5),
                Speed_km_h  NUMERIC(10, 5),
                Altitude_m NUMERIC(10, 5),
                FuelLevel_percentaje NUMERIC(10, 3),
                EngineTemp_c NUMERIC(10, 3),
                RotorRPM INTEGER,
                Latitude NUMERIC(10, 5),
                Longitude NUMERIC(10, 5)
            );
        """).format(sql.Identifier(table_name))
        
                
        cur.execute(create_table_query)
        
        logging.info(f"Tabla '{table_name}' asegurada/creada en '{settings.PG_DB}'.")
        
        cur.close()
        conn.close() # Cerrar la conexión usada para setup
    except psycopg.Error as e:
        logging.error(f"Error al configurar la base de datos: {e}")
        raise


class DoFnWriteToPostgres(beam.DoFn):
    def __init__(self, output_table_name):
        print('__init__')
        self.output_table_name = output_table_name

    def setup(self):
        print('setup')
        self.pg_factory = PostgreSQLClientFactory(
            settings.PG_HOST, 
            settings.PG_PORT, 
            settings.PG_DB, 
            settings.PG_USER, 
            settings.PG_PASSWORD
        )
        self.conn = self.pg_factory.get_connection() # Obtener la conexión a PostgreSQL para este trabajador

    def start_bundle(self):
        print('start_bundle')
        self.cur = self.conn.cursor() 

    def process(self, element, window=beam.DoFn.WindowParam):
        print("process")
        # Extraer datos del diccionario de entrada.
        record = json.loads(element)
        HelicopterID = record.get('HelicopterID')
        LapNumber = record.get('LapNumber')
        Time_s = record.get('Time_s')
        Speed_km_h = record.get('Speed_km_h')
        Altitude_m = record.get('Altitude_m')
        FuelLevel_percentaje = record.get('FuelLevel_percentaje')
        EngineTemp_c = record.get('EngineTemp_c')
        RotorRPM = record.get('RotorRPM')
        Latitude = record.get('Latitude')
        Longitude = record.get('Longitude')
        
        
        if not HelicopterID:
            logging.warning(f"Elemento sin 'id'. No se escribió en PostgreSQL: {element}")
            return

        try:
            # Construir la consulta INSERT de forma segura usando psycopg2.sql
            # Solo insertamos los campos que realmente tenemos valores, para evitar errores de null
            # en columnas NOT NULL si un campo no está presente.
            
            # Mapeo de campos del dict a columnas de la tabla SQL
            columns_map = {
                "id": str(uuid.uuid4()),
                'helicopterid': HelicopterID,
                'lapnumber': LapNumber,
                'time_s': Time_s,
                'speed_km_h': Speed_km_h,
                'altitude_m': Altitude_m,
                'fuellevel_percentaje': FuelLevel_percentaje,
                'enginetemp_c': EngineTemp_c,
                'rotorrpm': RotorRPM,
                'latitude': Latitude,
                'longitude': Longitude
            }

            # Filtra columnas con valores None o vacíos si no quieres insertarlos
            # Depende de la definición de tu tabla si acepta NULLs o no.
            # Aquí, filtramos None, pero si tu columna es NOT NULL y el valor puede ser None, esto fallaría.
            # Para simplificar, insertaremos todos los campos que tenemos.
            
            insert_columns = []
            insert_values = []
            
            # Iterar sobre las columnas_map para construir la consulta dinámicamente
            for col_name, col_value in columns_map.items():
                insert_columns.append(sql.Identifier(col_name))
                insert_values.append(sql.Literal(col_value)) # psycopg maneja el escapado de valores

            # Construir la consulta INSERT final
            query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (id) DO UPDATE SET {}") \
                        .format(
                            sql.Identifier(self.output_table_name),
                            sql.SQL(', ').join(insert_columns),
                            sql.SQL(', ').join(insert_values),
                            # Para ON CONFLICT, actualizamos todos los campos excepto la clave primaria
                            sql.SQL(', ').join([
                                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
                                for col in columns_map.keys() if col != 'id'
                            ])
                        )
            
            self.cur.execute(query)
            self.conn.commit() # ¡Importante! Confirmar la transacción
            logging.info(f"PostgreSQL: Insertado/Actualizado elemento con ID: {HelicopterID}")

        except psycopg.Error as e:
            self.conn.rollback() # Revertir la transacción en caso de error
            logging.error(f"Error al escribir en PostgreSQL para ID {HelicopterID}: {e}")

    def finish_bundle(self):
        print("finish_bundle")
        self.cur.close()

    def teardown(self):
        print('teardown')
        self.conn.close()

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

    parser.add_argument(
        "--output_table_name",
        required=True,
        help=""
    )

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True

    # Asegurarse de que la base de datos y la tabla existan antes de ejecutar el pipeline
    logging.info("Asegurando la configuración de la base de datos de PostgreSQL...")
    setup_database(known_args.output_table_name)
    logging.info("Configuración de la base de datos completada. Iniciando pipeline...")

    with beam.Pipeline(options=pipeline_options) as p:

        def format_as_json(element):
            if isinstance(element, tuple):
                return element[1].decode('utf-8')
            else:
                raise RuntimeError('unknown record type: %s' % type(element))

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

        (
            events_records
                | 'Filter Lap Completed Events' >> beam.Filter(lambda x: 'completes lap' in json.loads(x)["Details"])
                | 'To KVs' >> beam.Map(lambda x: (json.loads(x)["HelicopterID"], x))
                | 'Write to Redis' >> beam.Map(write_to_redis)
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
            | 'Write to jdbc' >>beam.ParDo(DoFnWriteToPostgres(known_args.output_table_name))
        )
        


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
