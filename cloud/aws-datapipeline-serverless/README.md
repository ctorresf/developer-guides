# LatamBuy Data Lake - Serverless Pipeline

Serverless data ingestion and transformation pipeline for LatamBuy's financial consolidation using AWS S3, Glue Catalog, Athena, and Step Functions.

## 📋 Project Description

This project implements a Data Lake with medallion architecture (Bronze → Silver → Gold) to consolidate sales and exchange rates from multiple branches across Latin America:

- **Raw Zone (Bronze)**: CSV file ingestion from branches
- **Trusted Zone (Silver)**: Transformed and enriched data with exchange rates
- **Athena Staging**: Query results and technical staging

### Main Components

- **S3 Buckets**: Storage for raw data, transformed data, and Athena results
- **AWS Glue**: Data Catalog for governance and metadata
- **Amazon Athena**: SQL query engine for transformations
- **AWS Step Functions**: Serverless pipeline orchestration

---

## 🚀 Prerequisites

- AWS CLI configured with valid credentials (`aws configure`)
- Python 3.8+ (for Python version of ingestion)
- Bash (for shell version of ingestion)
- Required IAM permissions for S3, Glue, Athena, and Step Functions

---

## 📦 Installation

### 1. Provision Infrastructure with CloudFormation

The CloudFormation template creates the necessary base infrastructure (S3 buckets, Glue Database, external tables).

#### Steps:

```bash
# 1. Navigate to project directory
cd /path/to/project

# 2. Validate the template (optional but recommended)
aws cloudformation validate-template \
  --template-body file://Cloudformation-base-infrastructure.yaml

# 3. Create the stack with a unique suffix
aws cloudformation create-stack \
  --stack-name latambuy-datalake-dev \
  --template-body file://Cloudformation-base-infrastructure.yaml \
  --parameters ParameterKey=BucketSuffix,ParameterValue=<your-unique-suffix>

# 4. Monitor progress
aws cloudformation wait stack-create-complete \
  --stack-name latambuy-datalake-dev

# Check status
aws cloudformation describe-stacks \
  --stack-name latambuy-datalake-dev \
  --query 'Stacks[0].StackStatus'
```

#### CloudFormation Parameters:

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `BucketSuffix` | String | Unique suffix for bucket names (prevents global collisions) | `develop`, `dev`, `prod-001` |

#### Created Resources:

- ✅ `latambuy-data-lake-raw-{BucketSuffix}` - Ingestion bucket (Bronze)
- ✅ `latambuy-data-lake-trusted-{BucketSuffix}` - Transformed bucket (Silver)
- ✅ `latambuy-data-lake-athena-staging-{BucketSuffix}` - Athena staging bucket
- ✅ `db_latambuy` - Glue Database
- ✅ `db_latambuy.ventas_raw` - External table for raw sales
- ✅ `db_latambuy.tipo_cambio` - External table for exchange rates

---

## 📥 Data Ingestion

The project offers two options for data ingestion: **Python** or **Bash**. Both load CSV files to S3 with date-partitioned structure.

### Option A: Python Ingestion (Recommended)

```bash
# 1. Run the Python script
python load_ingest_data.py

# 2. When prompted, enter the bucket suffix
👉 Please enter your unique bucket suffix (e.g., develop): develop
```

**What it does:**
- Automatically calculates yesterday's date
- Reads transaction and exchange rate CSV files
- Loads data to S3 with Hive structure: `s3://latambuy-data-lake-raw-{suffix}/ventas/year={Y}/month={M}/day={D}/`
- Cleans up temporary local files

**Ingested files:**
- `sales_*.csv` → `ventas/` in Raw bucket
- `exchange_rate_*.csv` → `tipo_cambio/` in Raw bucket

### Option B: Bash Ingestion

```bash
# 1. Grant execution permissions
chmod +x load_ingest_data.sh

# 2. Run the script
./load_ingest_data.sh

# 3. When prompted, enter the bucket suffix
👉 Please enter your unique bucket suffix (e.g., develop): develop
```

**What it does:**
- Automatically calculates yesterday's date (compatible with macOS and Linux)
- Loads CSV files using AWS CLI
- Maintains partitioned structure in S3

### Sample Data

The CSV files in the `data/` folder contain sample data for:
- `sales_buenos_aires_2026-07-14.csv` - Buenos Aires transactions
- `sales_lima_2026-07-14.csv` - Lima transactions
- `sales_santiago_2026-07-14.csv` - Santiago transactions
- `exchange_rate_2026-07-14.csv` - Exchange rates

---

## 🔄 AWS Step Functions Pipeline

The `StepFunctions-state-machine.json` file defines the state machine that orchestrates the pipeline.

### Pipeline Flow Description:

```
┌─────────────────────────────────────────────────────────────┐
│ 1. CleanTrustedTable                                        │
│    ↓                                                        │
│    DROP TABLE IF EXISTS db_latambuy.ventas_consolidadas     │
└──────────────────┬──────────────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────────────┐
│ 2. RegisterRawPartitions                                    │
│    ↓                                                        │
│    MSCK REPAIR TABLE db_latambuy.ventas_raw;                │
│    (Registers new partitions in Glue Catalog)               │
└──────────────────┬──────────────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────────────┐
│ 3. ExecuteCTASTransformation                                │
│    ↓                                                        │
│    CREATE TABLE ventas_consolidadas AS                      │
│    SELECT * FROM ventas_raw JOIN tipo_cambio                │
│    (Transforms data and converts amounts to USD)            │
└──────────────────┬──────────────────────────────────────────┘
                   │
         ┌─────────┴─────────┐
         │                   │
    ✅ Success        ❌ Failure: NotifyPipelineFailure
```

### Step Function Input Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `bucket_suffix` | String | Bucket suffix (must match CloudFormation) | `develop` |
| `year` | String | Data year (format YYYY) | `2026` |
| `month` | String | Data month (format MM) | `07` |
| `day` | String | Data day (format DD) | `14` |

### Execute the Pipeline

```bash
# 1. Get the State Machine ARN (replace stack-name if different)
STEP_FUNCTION_ARN=$(aws cloudformation describe-stack-resources \
  --stack-name latambuy-datalake-dev \
  --query "StackResources[?LogicalResourceId=='StepFunctionsStateMachine'].PhysicalResourceId" \
  --output text)

# 2. Prepare the JSON parameters
cat > execution-input.json << 'EOF'
{
  "bucket_suffix": "develop",
  "year": "2026",
  "month": "07",
  "day": "14"
}
EOF

# 3. Start the execution
aws stepfunctions start-execution \
  --state-machine-arn "$STEP_FUNCTION_ARN" \
  --name "execution-2026-07-14" \
  --input file://execution-input.json

# 4. Monitor the execution
aws stepfunctions describe-execution \
  --execution-arn "<execution-arn-here>"
```

---

## 🔍 Monitoring and Debugging

### View CloudWatch Logs

```bash
# View Step Functions logs
aws logs tail /aws/stepfunctions/latambuy-datalake --follow

# View Athena logs
aws logs tail /aws/athena/latambuy-datalake --follow
```

### Query Data in Athena

```bash
# Connect to Athena (via AWS Console or AWS CLI)
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM db_latambuy.ventas_raw;" \
  --query-execution-context Database=db_latambuy \
  --result-configuration OutputLocation=s3://latambuy-data-lake-athena-staging-develop/
```

### Verify Data in S3

```bash
# List ingested files
aws s3 ls s3://latambuy-data-lake-raw-develop/ventas/ --recursive

# List consolidated data
aws s3 ls s3://latambuy-data-lake-trusted-develop/ventas_consolidadas/ --recursive
```

---

## 📊 Data Architecture

### Partitioning Scheme

```
s3://latambuy-data-lake-raw-{suffix}/
├── ventas/
│   └── year=2026/month=07/day=14/
│       ├── sales_buenos_aires.csv
│       ├── sales_lima.csv
│       └── sales_santiago.csv
└── tipo_cambio/
    └── year=2026/month=07/day=14/
        └── exchange_rate.csv

s3://latambuy-data-lake-trusted-{suffix}/
└── ventas_consolidadas/
    └── year=2026/month=07/day=14/
        └── [compressed Parquet files]
```

### Glue Catalog Tables

#### `db_latambuy.ventas_raw`
- **Type**: External table
- **Location**: `s3://latambuy-data-lake-raw-{suffix}/ventas/`
- **Format**: CSV
- **Partitions**: year, month, day
- **Columns**: transaction_id, date, product_id, product_name, category, quantity, total_amount, currency

#### `db_latambuy.tipo_cambio`
- **Type**: External table
- **Location**: `s3://latambuy-data-lake-raw-{suffix}/tipo_cambio/`
- **Format**: CSV
- **Partitions**: year, month, day
- **Columns**: currency, rate_to_usd, description

#### `db_latambuy.ventas_consolidadas`
- **Type**: External table (created by CTAS in pipeline)
- **Location**: `s3://latambuy-data-lake-trusted-{suffix}/ventas_consolidadas/`
- **Format**: Parquet (compressed with SNAPPY)
- **Columns**: transaction_id, fecha_utc, product_id, product_name, category, quantity, monto_moneda_local, currency, monto_usd

---

## 🛠️ Maintenance

### Update the Stack

```bash
# If you need to modify the CloudFormation template
aws cloudformation update-stack \
  --stack-name latambuy-datalake-dev \
  --template-body file://Cloudformation-base-infrastructure.yaml \
  --parameters ParameterKey=BucketSuffix,ParameterValue=develop
```

### Delete Infrastructure (⚠️ Caution)

```bash
# WARNING: This will delete all buckets and data
aws cloudformation delete-stack \
  --stack-name latambuy-datalake-dev

# Monitor deletion
aws cloudformation wait stack-delete-complete \
  --stack-name latambuy-datalake-dev
```

---

## 📝 Important Notes

- ✅ **Bucket naming**: Suffix must be globally unique in AWS
- ✅ **Dates**: Scripts automatically calculate yesterday's date
- ✅ **Partitioning**: Critical for Athena performance
- ✅ **Currency conversion**: Pipeline converts all amounts to USD using ingested exchange rates
- ✅ **Idempotency**: Pipeline can safely run multiple times (DROP IF EXISTS)

---

## 📞 Support

To report issues or suggestions, check logs from:
- CloudFormation: AWS Console → CloudFormation
- Step Functions: AWS Console → Step Functions
- Athena: AWS Console → Athena → Query History
- CloudWatch Logs: `/aws/stepfunctions/` and `/aws/athena/`

---

**Last Updated**: 2026-07-16
