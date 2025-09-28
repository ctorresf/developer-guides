# Install airflow

Use the script in scripts/install-airflow.sh to install Apache Airflow

## Configurations in scripts/airflow.cfg

This installation change the default configurations of Airflow, the changes are:

- dags_folder: changed to current src folder
- simple_auth_manager_all_admins: changed to True to disable login screen
- load_examples: changed to False to avoid the load of examples in our Airflow installation
- port: changed to 8081 to access Airflow using http://localhost:8081

# Run Airflow

Just execute
```
airflow standalone
```

# Dag examples

## basic_dag
A simple dag to show the basic structure

## basic_dag_branch
A simple dag to show the input params and the branch selection

## run_beam
A simpple dag to run a Apache Beam Python job

