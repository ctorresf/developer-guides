# configuraciones

# --- Configuración de MinIO ---
MINIO_ENDPOINT = 'rhl-minio:9000'  #  'your_minio_server:9000'
MINIO_ACCESS_KEY = 'minio-root-user'   # Reemplace con su nombre de usuario MinIO
MINIO_SECRET_KEY = 'minio-root-password'   # Reemplace con su contraseña MinIO
MINIO_SECURE = False #  True si usa HTTPS
bucket_name = 'race-event-bucket'    # El bucket donde se almacenarán los datos

# --- Configuración de Kafka ---
KAFKA_BROKERS = 'rhl-kafka:19092'  # direccion del Kafka broker, 'server:port'
KAFKA_TOPIC = 'race_monitoring_topic'     #  Kafka topic

# --- Configuración de Redis ---
REDIS_HOST = 'rhl-redis'
REDIS_PORT = 6379
REDIS_DB = 0 # Base de datos por defecto de Redis

# --- Configuración de PostgreSQL ---
PG_HOST = 'rhl-postgresql'
PG_PORT = '5432'
PG_DB = 'my_database'
PG_USER = 'my_user'
PG_PASSWORD = 'password123'
