from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping
from cosmos.config import RenderConfig
from cosmos.constants import LoadMode, ExecutionMode
from pathlib import Path

# =============================================================================
# PASO 1: Configuración de Alertas (on_failure_callback)
# =============================================================================
def alert_on_failure(context):
    """
    Captura las excepciones del flujo y extrae metadatos clave para enviar
    una alerta inmediata (puede adaptarse a Slack, Teams, Email o Logs).
    """
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    exec_date = context.get('execution_date')
    exception = context.get('exception')
    
    # Mensaje estructurado de error
    alert_message = f"""
    ❌ ALERTA DE FALLO EN PIPELINE GLOBAL-MART
    --------------------------------------------------
    DAG: {dag_id}
    Tarea: {task_id}
    Fecha de Ejecución: {exec_date}
    Error detectado: {exception}
    --------------------------------------------------
    """
    print(alert_message)
    # Aquí se integraría el cliente de Slack o e-mail, por ejemplo:
    # SlackWebhookHook(http_conn_id='slack_conn', message=alert_message).execute()

# =============================================================================
# CONFIGURACIÓN POR DEFECTO DEL DAG
# =============================================================================
default_args = {
    'owner': 'Data Engineering Team',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries:': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': alert_on_failure, # Alerta global para cualquier tarea del DAG
}

with DAG(
    dag_id='etl_beam_dbt_with_cosmos',
    default_args=default_args,
    description='Pipeline resiliente de consolidación para Global-Mart (Beam -> Sensor -> dbt)',
    schedule='@daily',
    catchup=False,
    tags=['global-mart', 'beam', 'dbt', 'silver', 'gold'],
) as dag:

    # =============================================================================
    # PASO 2: Tarea de Ingesta y Limpieza (Apache Beam) -> Capa Silver
    # =============================================================================
    # Invoca el script de Beam que procesa los CSVs (Bronze) y genera el Parquet (Silver).
    run_beam_pipeline = BashOperator(
        task_id='run_beam_silver_layer',
        bash_command='python /opt/airflow/beam/beam_clean_sales.py',
        doc_md="""\
        ### Tarea Apache Beam
        Procesa, limpia y cruza las ventas con los logs de estado de las sucursales.
        Genera archivos estructurados en formato **Parquet** anidado en el Data Lake.
        """,
    )

    # =============================================================================
    # PASO 3: Validación de Datos (FileSensor)
    # =============================================================================
    # Bloquea el flujo hasta que el archivo Parquet realmente exista en el almacenamiento.
    # Requiere configurar una conexión de tipo 'File (path)' en la UI de Airflow (fs_default).
    wait_for_silver_parquet = FileSensor(
        task_id='wait_for_silver_parquet',
        #filepath='/opt/airflow/data/silver/ventas_consolidadas*.parquet', # Ruta absoluta al Data Lake local
        filepath='silver/ventas_consolidadas*.parquet', # Ruta relativa al Data Lake local
        fs_conn_id='fs_default',
        poke_interval=30,  # Verifica cada 30 segundos
        timeout=600,       # Tiempo límite de 10 minutos antes de fallar
        mode='poke',
        doc_md="""\
        ### FileSensor de Validación
        Monitorea el Data Lake local esperando la aparición de `ventas_consolidadas.parquet`.
        Garantiza que la Capa Silver esté lista antes de disparar el modelado analítico.
        """,
    )

    # =============================================================================
    # PASO INTERMEDIO OPTIMIZADO: Mapeo de Parquet Nativo en Postgres
    # =============================================================================
    copy_parquet_to_postgres_table = SQLExecuteQueryOperator(
        task_id='copy_parquet_to_postgres_table',
        conn_id='postgres_db',
        sql="""
            -- 1. Aseguramos que exista la extensión en esta base de datos
            CREATE EXTENSION IF NOT EXISTS pg_parquet;
            
            -- 2. Creamos el esquema de la capa Silver si no existe
            CREATE SCHEMA IF NOT EXISTS silver;
            
            -- 3. Creamos una TABLA FÍSICA REAL en Postgres (no extranjera)
            CREATE TABLE IF NOT EXISTS silver.ventas_consolidadas (
                id_venta TEXT,
                fecha TEXT,
                sucursal_id TEXT,
                monto DOUBLE PRECISION,
                estado_sucursal TEXT,
                region TEXT
            );
            
            -- 4. Limpiamos los datos anteriores para evitar duplicados en cargas diarias (Truncate)
            TRUNCATE TABLE silver.ventas_consolidadas;
            
            -- 5. ¡LA MAGIA DE PG_PARQUET!: Copia directa del archivo a la tabla física
            COPY silver.ventas_consolidadas 
            FROM '/datalake/silver/ventas_consolidadas.parquet' 
            WITH (FORMAT 'parquet');
        """,
        doc_md="""\
        ### Ingesta Nativa con pg_parquet COPY
        Utiliza el comando COPY optimizado por la extensión para volcar el archivo Parquet 
        directamente en una tabla física indexable de Postgres. Carga ultra rápida y eficiente.
        """,
    )

    # =============================================================================
    # PASO 4: Tarea de Transformación y KPIs (dbt) -> Capa Gold
    # =============================================================================
    # Ejecuta el modelado en dbt para procesar los KPIs de finanzas y cargarlos en PostgreSQL.
    DBT_PROJECT_PATH = "/sources/dbt_project"
    CONNECTION_ID = "postgres_db"
    SCHEMA_NAME = "gold"
    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id=CONNECTION_ID,
            profile_args={"schema": SCHEMA_NAME},
        ),
    )

    execution_config = ExecutionConfig(
        #execution_mode=ExecutionMode.SUBPROCESS, # Invoca '/home/airflow/.local/bin/dbt' directamente en bash
        #dbt_executable_path="/home/airflow/.local/bin/dbt"
        # Opcional: Si tienes dbt en un entorno virtual aislado, usarías:
        #execution_mode=ExecutionMode.VIRTUALENV,
        #virtualenv_dir=Path("/home/airflow/dbt_venv")
        dbt_executable_path="/home/airflow/.local/bin/dbt" 
    )

    run_dbt_gold_layer = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            #manifest_path=DBT_PROJECT_PATH + "/target/manifest.json",
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        # render_config=RenderConfig(
        #     load_method=LoadMode.DBT_MANIFEST
        # ),
        # OPTIONAL: your execution config if you are using a virtual environment
        # execution_config=execution_config,
        # operator_args={
        #     "vars": '{"my_name": {{ params.my_name }} }',
        # },
        default_args={"retries": 2},
    )
    
    # BashOperator(
    #     task_id='run_dbt_gold_layer',
    #     bash_command=(
    #         'cd /sources/dbt_project && '
    #         'dbt deps --profiles-dir . && '
    #         'dbt run --profiles-dir . && '
    #         'dbt test --profiles-dir .'
    #     ),
    #     doc_md="""\
    #     ### Tarea dbt (Data Build Tool)
    #     Ejecuta las transformaciones de la Capa Gold (KPIs financieros) y realiza las 
    #     pruebas de calidad de datos (`dbt test`) sobre la base de datos analítica PostgreSQL.
    #     """,
    # )

    # =============================================================================
    # DEFINICIÓN EXPLÍCITA DE DEPENDENCIAS
    # =============================================================================
    run_beam_pipeline >> wait_for_silver_parquet >> copy_parquet_to_postgres_table >> run_dbt_gold_layer