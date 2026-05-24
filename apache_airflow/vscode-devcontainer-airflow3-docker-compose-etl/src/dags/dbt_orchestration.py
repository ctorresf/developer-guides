from datetime import datetime
from pathlib import Path
from airflow.models import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Definir las rutas dentro del contenedor de Airflow
DBT_PROJECT_DIR = Path("/sources/dbt_project")

# Configurar el perfil de conexión mapeándolo a la conexión que creamos en la UI de Airflow
profile_config = ProfileConfig(
    profile_name="default", # El nombre del perfil en tu dbt_project
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_db", # El ID de la conexión de Airflow
        profile_args={"schema": "public"},
    ),
)

# Crear el DAG de dbt de forma automática
dbt_analytics_dag = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_DIR),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/home/airflow/.local/bin/dbt" # Ruta donde se instala dbt vía pip adicional
    ),
    # Parámetros estándar de Airflow
    dag_id="dbt_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
)