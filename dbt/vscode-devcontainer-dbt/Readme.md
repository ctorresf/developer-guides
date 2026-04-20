# dbt Transformation Case Study

This repository contains a practical data engineering project using **dbt (data build tool)**. The goal is to transform raw electrical sensor data from **Electrical Company** into actionable analytical insights regarding grid stability and customer consumption.

## 1. Project Overview

### Scenario
Electrical Company operates a network of smart meters across various communes. Currently, the data exists in two raw states within the Data Warehouse:
* `raw_readings`: High-frequency logs (every 10 seconds) containing voltage and consumption metrics.
* `raw_meters`: Master data containing meter metadata (Commune, Customer Type).

### Business Objective
To design and implement a data model that:
1.  Identifies which **communes** experience the most unstable voltage levels.
2.  Calculates **accumulated energy consumption** per customer.
3.  Provides a clean, tested "Single Source of Truth" for the operations team.

---

## 2. Tech Stack

* **Database Engine:** PostgreSQL 15
* **Transformation:** dbt Core (v1.x)
* **Orchestration:** Docker Compose
* **Local Data Lake:** CSV-based local storage (via dbt seeds)
* **Visualization/Management:** pgAdmin 4

---

## 3. Project Architecture & Lineage

The project follows the **Medallion Architecture** (simplified) using a structured folder hierarchy:

```text
├── .dbt                   # Connection profiles
├── .devcontainer          # VS Code Docker environment
├── models
│   ├── staging            # Layer 1: Cleaning & Casting (stg_meters, stg_readings)
│   └── marts              # Layer 2: Business Logic & Aggregations (fct_daily_monitoring)
├── seeds                  # Raw CSV data (Source of truth for this case)
└── tests                  # Data quality assertions
```

### Data Flow
1.  **Seeds:** Raw data is loaded from CSV to Postgres.
2.  **Staging:** `stg_meters` and `stg_readings` clean the raw data (e.g., fixing data types like `STRING` to `VARCHAR`).
3.  **Marts:** `fct_daily_monitoring` joins the sources to create a final fact table for analysis.

---

## 4. Getting Started

### Prerequisites
* Docker & Docker Desktop
* VS Code with the "Dev Containers" extension

### Deployment Steps

The environment is pre-configured to install dependencies automatically via the **Dev Container**. Once the container is running, execute the following commands in the terminal:

#### 0. Validate Data Quality
Install dependencies defined in packages.yml:
```bash
dbt deps
```

#### 1. Load Raw Data
Load the CSV files into your PostgreSQL instance:
```bash
dbt seed --profiles-dir .dbt
```

#### 2. Run Transformations
Build the staging views and the final fact tables:
```bash
dbt run --profiles-dir .dbt
```

#### 3. Validate Data Quality
Run tests to ensure there are no null IDs or invalid sensor values:
```bash
dbt test --profiles-dir .dbt
```

#### 4. Generate Documentation
To view the Lineage Graph and model descriptions:
```bash
dbt docs generate --profiles-dir .dbt 
dbt docs serve --port 8001 --profiles-dir .dbt
```

---

## 5. Maintenance & Useful Commands

### Full Refresh
If you modify the seed CSV files and need to recreate the tables from scratch:
```bash
dbt seed --full-refresh --profiles-dir .dbt
```

### Database Management
You can access **pgAdmin 4** to explore the created tables:
* **URL:** `http://localhost:8080`
* **Login:** `admin@company.cl` / `admin_password`

---

## 6. Project Constraints (Best Practices)
* **Naming Conventions:** All models use a prefix (`stg_` for staging, `fct_` for facts).
* **Schema Enforcement:** Data types are strictly cast in the staging layer to ensure downstream reliability.
* **Testing:** Every model must have at least `unique` and `not_null` tests on primary keys.

---
