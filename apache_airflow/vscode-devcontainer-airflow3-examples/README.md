# Apache Airflow 3.2.1 - DevContainer Examples

This project contains practical examples of **Apache Airflow 3.2.1** configured in a VS Code DevContainer. It includes sample DAGs that demonstrate everything from basic concepts to complex ETL workflows with Apache Beam.

## 📋 Project Structure

```
.
├── src/dags/                          # Airflow DAGs
│   ├── simple_hello_world.py         # Basic DAG with simple tasks
│   ├── basic_dag.py                  # Fundamental DAG
│   ├── complex_etl_with_beam.py      # Advanced ETL with Apache Beam
│   ├── data_transfer_xcom.py         # Data transfer between tasks
│   └── sample.py                     # Additional example
├── src/beam_pipelines/                # Apache Beam Pipelines
│   ├── transformation_enrich.py      # Data enrichment
│   └── transformation_normalize.py   # Data normalization
├── scripts/
│   └── install-dependencies.sh       # Installation script
└── output/                            # Processed data output
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- VS Code with Remote - Containers extension
- At least 4GB RAM available

### 1. Install Dependencies

```bash
bash scripts/install-dependencies.sh
```

This script will:
- ✅ Update the system
- ✅ Install Apache Airflow 3.2.1
- ✅ Configure Apache Beam
- ✅ Set environment variables
- ✅ Create necessary directory structure

### 2. Initialize Airflow Database

```bash
source $HOME/.bashrc
airflow db migrate
```

### 3. Create Admin User (if needed)

```bash
airflow users create \
  --username admin \
  --firstname Airflow \
  --lastname Admin \
  --role Admin \
  --email admin@example.com \
  --password "YourPassword"
```

### 4. Start Airflow

#### Option A: Run Airflow Standalone (Recommended for development)

```bash
airflow standalone
```

This starts:
- 🌐 Airflow UI at `http://localhost:8080`
- 🔄 Scheduler
- 💼 Local executor

**Default credentials:**
- Username: `admin`
- Password: `standalone_admin_password` (or the one you configured)

#### Option B: Run Scheduler and Webserver Separately

**Terminal 1 - Webserver:**
```bash
airflow webserver --port 8080
```

**Terminal 2 - Scheduler:**
```bash
airflow scheduler
```

## 📊 Available DAGs

### 1. **simple_hello_world** 
A basic DAG perfect for getting started:
- Simple Python task
- Bash task
- Task dependencies

### 2. **basic_dag**
Fundamental DAG with basic Airflow concepts.

### 3. **complex_etl_with_beam**
Advanced ETL pipeline demonstrating:
- Simulated data extraction
- Processing with Apache Beam
- Storage in staging areas (VSA/PSA)
- Data transformation and enrichment

### 4. **data_transfer_xcom**
Example of data transfer between tasks using XCom.

### 5. **sample**
Additional example DAG.

## 🔧 Useful Commands

```bash
# List all DAGs
airflow dags list

# Trigger a DAG manually
airflow dags trigger simple_hello_world

# View task logs
airflow tasks logs simple_hello_world greet_task

# List DAG tasks
airflow tasks list simple_hello_world

# Delete a DAG
airflow dags delete simple_hello_world

# Validate DAG syntax
airflow dags validate
```

## 🌐 Access the UI

Once Airflow is running:

- **URL:** [http://localhost:8080](http://localhost:8080)
- **Username:** `admin`
- **Password:** (the one you set during installation)

## 📁 Environment Variables

The project automatically configures:

```bash
AIRFLOW__CORE__DAGS_FOLDER="/workspaces/.../src/dags"
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW_HOME="/home/vscode/airflow"
```

## 🛠️ Development and Debugging

### Create a New DAG

1. Create a file in `src/dags/`
2. Define your DAG following the structure of the examples
3. Airflow will automatically detect the file within 30 seconds
4. It will appear in the UI

### Debug Issues

```bash
# Check scheduler logs
tail -f /home/vscode/airflow/logs/scheduler/latest/

# Review current configuration
airflow config list

# Validate DAGs
airflow dags validate
```

## 📦 Installed Dependencies

- **Apache Airflow** 3.2.1
- **Apache Beam** 2.73.0 (with DataFrame support)
- **Parquet Wasm** (for Parquet file visualization)
- **Parquet CLI** (command-line tool)
- **Apache Beam Providers** 6.2.3

## 🐛 Troubleshooting

**Issue:** DAGs not appearing in the UI
```bash
# Solution: Verify DAGs folder path
echo $AIRFLOW__CORE__DAGS_FOLDER

# Refresh the UI (Ctrl+F5)
```

**Issue:** Permission errors
```bash
# Solution: Ensure user has proper permissions
export AIRFLOW_HOME=/home/vscode/airflow
chmod -R 755 $AIRFLOW_HOME
```

**Issue:** Database corruption
```bash
# Solution: Reset database (warning: deletes data)
airflow db reset
airflow db migrate
```

## 📖 Resources

- [Official Airflow Documentation](https://airflow.apache.org/)
- [Apache Beam Python SDK](https://beam.apache.org/documentation/sdks/python/)
- [Airflow DevContainer Setup](https://airflow.apache.org/docs/docker-stack/build.html)

## 📝 Notes

- The default admin user can be changed in `install-dependencies.sh`
- Processed data is saved in `output/`
- Logs are located in `$AIRFLOW_HOME/logs/`

---

**Ready to get started!** 🎉 Run `airflow standalone` and access the UI in your browser.
