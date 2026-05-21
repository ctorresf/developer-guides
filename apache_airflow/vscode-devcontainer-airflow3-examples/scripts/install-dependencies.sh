# !/bin/bash

# Update and upgrade the system
sudo apt update
sudo apt upgrade -y
# install dependencies for dvirtz.parquet-viewer vscode plugin
sudo apt install -y -V yarnpkg
# create a symbolic link to use yarn command
sudo ln -s /usr/bin/yarnpkg /usr/bin/yarn
yarn add parquet-wasm

# Install the latest version of pip
python3 -m pip install --upgrade pip

#  Command line (CLI) tool to inspect Apache Parquet files on the go
pip install parquet-cli 


# Install Apache Airflow 
AIRFLOW_VERSION=3.2.1
# Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
# See above for supported versions.
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow[google, amazon]==${AIRFLOW_VERSION}"   --constraint "${CONSTRAINT_URL}"
pip install apache-airflow-providers-apache-beam==6.2.3
pip install apache_beam[dataframe]==2.73.0
pip install virtualenv


echo "Inyectando variables de entorno en .bashrc..."

# Obtener la ruta dinámica del espacio de trabajo en el contenedor
WORKSPACE_DIR=$(pwd)
AIRFLOW_HOME="/home/vscode/airflow"


# Crear la carpeta AIRFLOW_HOME si no existe
if [ ! -d "$AIRFLOW_HOME" ]; then
    echo "La carpeta $AIRFLOW_HOME no existe. Creándola ahora..."
    mkdir -p "$AIRFLOW_HOME"
fi

{
    echo ""
    echo "# Configuración de Airflow para DevContainer"
    echo "export AIRFLOW__CORE__DAGS_FOLDER=\"$WORKSPACE_DIR/src/dags\""
    echo "export AIRFLOW__CORE__LOAD_EXAMPLES=False"
} >> "$HOME/.bashrc"

# Forzar la carga en la sesión actual del script
source "$HOME/.bashrc"

echo "Configurando contraseña personalizada para el administrador..."
cat << EOF > "$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
{
  "admin": "MiPasswordSecreto123"
}
EOF

echo "Inicializando la base de datos de Airflow..."
#airflow db migrate

