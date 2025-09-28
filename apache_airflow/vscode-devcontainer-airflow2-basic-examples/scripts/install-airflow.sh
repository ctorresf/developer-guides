#!/usr/bin/bash 
AIRFLOW_VERSION=2.11.0

# Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
# See above for supported versions.
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 3.0.0 with python 3.9: https://raw.githubusercontent.com/apache/airflow/constraints-3.0.2/constraints-3.9.txt
python3 -m pip install --upgrade pip
pip install "apache-airflow[google, amazon]==${AIRFLOW_VERSION}"   --constraint "${CONSTRAINT_URL}"
pip install apache-airflow-providers-apache-beam==6.1.5
pip install apache_beam[dataframe]
pip install virtualenv
# Initialize the database and configure Airflow
airflow db migrate

# Copy custom configuration file
cp scripts/airflow.cfg ~/airflow/airflow.cfg 
cp scripts/webserver_config.py ~/airflow/webserver_config.py

# Create a symbolic link to the dags folder in the workspace
ln -s $PWD/src/ /home/vscode/airflow/dags

# Initialize the Airflow database with custom settings
airflow db reset -y

  