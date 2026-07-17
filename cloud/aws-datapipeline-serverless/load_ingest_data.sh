#!/bin/bash

# 1. Prompt user for their unique bucket suffix
echo "========================================================="
echo "   LatamBuy Data Lake Ingestion Tool (Bash / AWS CLI)   "
echo "========================================================="
echo -n "👉 Please enter your unique bucket suffix (e.g., torres): "
read BUCKET_SUFFIX

if [ -z "$BUCKET_SUFFIX" ]; then
    echo "❌ Error: Bucket suffix cannot be empty!"
    exit 1
fi

BUCKET_NAME="latambuy-data-lake-raw-${BUCKET_SUFFIX}"

# 2. Determine OS to calculate "yesterday's" date portably
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS / BSD date syntax
    YEAR=$(date -v-1d +%Y)
    MONTH=$(date -v-1d +%m)
    DAY=$(date -v-1d +%d)
else
    # Linux / GNU date syntax
    YEAR=$(date -d "yesterday" +%Y)
    MONTH=$(date -d "yesterday" +%m)
    DAY=$(date -d "yesterday" +%d)
fi

echo "📅 Processing ingestion for partitioned date: ${YEAR}-${MONTH}-${DAY}"

# 3. Partitioned target paths (Hive-style structure)
SALES_PATH="s3://${BUCKET_NAME}/ventas/year=${YEAR}/month=${MONTH}/day=${DAY}/"
EXCHANGE_RATE_PATH="s3://${BUCKET_NAME}/tipo_cambio/year=${YEAR}/month=${MONTH}/day=${DAY}/"

# 4. Upload files to S3
echo "🚀 Uploading sales transactions to S3..."
aws s3 cp data/ "${SALES_PATH}" --recursive --exclude "*" --include "sales*.csv"

echo "💵 Uploading exchange rate to S3..."
aws s3 cp data/exchange_rate*.csv "${EXCHANGE_RATE_PATH}exchange_rate.csv"

echo "✅ Ingestion completed successfully."
