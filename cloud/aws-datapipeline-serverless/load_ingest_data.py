import datetime
import boto3
import os

print("=========================================================")
print("     LatamBuy Data Lake Ingestion Tool (Python / Boto3)  ")
print("=========================================================")

# 1. Prompt user for their unique bucket suffix
bucket_suffix = input("👉 Please enter your unique bucket suffix (e.g., torres): ").strip()

if not bucket_suffix:
    print("❌ Error: Bucket suffix cannot be empty!")
    exit(1)

bucket_name = f"latambuy-data-lake-raw-{bucket_suffix}"

# 2. Calculate yesterday's date portably using datetime
yesterday = datetime.date.today() - datetime.timedelta(days=1)
year = yesterday.strftime("%Y")
month = yesterday.strftime("%m")
day = yesterday.strftime("%d")

print(f"📅 Processing ingestion for partitioned date: {year}-{month}-{day}")

# 3. Define local filenames and content
sales_filename = "sales_santiago.csv"
exchange_filename = "exchange_rate.csv"

sales_data = """transaction_id,date,product_id,product_name,category,quantity,total_amount,currency
T001,2026-07-15,P01,Polera Algodon,Ropa,2,30000,CLP
T002,2026-07-15,P02,Zapatillas Running,Calzado,1,85000,CLP
"""

exchange_data = """currency,rate_to_usd,description
CLP,0.0011,Peso Chileno
"""

# 4. Write data to local physical CSV files
print("📝 Creating local CSV files on disk...")
with open(sales_filename, "w", encoding="utf-8") as f:
    f.write(sales_data)

with open(exchange_filename, "w", encoding="utf-8") as f:
    f.write(exchange_data)

# 5. Initialize S3 client (inherits local AWS credentials)
s3_client = boto3.client('s3')

# 6. Define partitioned S3 object keys (Hive-style structure)
sales_s3_key = f"ventas/year={year}/month={month}/day={day}/{sales_filename}"
exchange_s3_key = f"tipo_cambio/year={year}/month={month}/day={day}/{exchange_filename}"

# 7. Read physical files and upload to S3
try:
    print("🚀 Uploading sales transactions to S3 from physical file...")
    with open(sales_filename, "rb") as f_sales:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=sales_s3_key,
            Body=f_sales
        )
    
    print("💵 Uploading exchange rates to S3 from physical file...")
    with open(exchange_filename, "rb") as f_exch:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=exchange_s3_key,
            Body=f_exch
        )
    
    print(f"✅ Ingestion completed successfully to s3://{bucket_name}/")

except Exception as e:
    print(f"❌ Error interacting with S3: {str(e)}")
    print("Please make sure you have configured your local AWS CLI by running 'aws configure'.")

finally:
    # 8. Clean up local files
    print("🧹 Cleaning up local physical files...")
    if os.path.exists(sales_filename):
        os.remove(sales_filename)
    if os.path.exists(exchange_filename):
        os.remove(exchange_filename)