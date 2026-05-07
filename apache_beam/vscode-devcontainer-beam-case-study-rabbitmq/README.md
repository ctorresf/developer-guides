# Apache Beam Retail Pipeline

This repository includes three Apache Beam pipeline scripts:
- `src/psa_ingestion.py`: reads CSV sales data, adds audit metadata, and writes Parquet files to the PSA layer.
- `src/dws_currency_conversion.py`: reads PSA Parquet records, validates and converts amounts from CLP to USD, and writes JSONL output for the DWS layer.
- `src/vsa_telemetry_pipeline.py`: reads telemetry JSONL, applies sliding-window vehicle safety analytics, and writes alerts, summaries, and bad records.
- `src/rabbitmq_vsa.py`: RabbitMQ-compatible version of the VSA pipeline; uses a fallback JSONL source if the RabbitMQ connector is not present.

## Requirements

Install the Python dependencies before running:

```bash
pip install -r requirements.txt
```

## Run the tests

After installing dependencies, run the unit tests to verify the code:

```bash
python -m pytest tests
```

## Sample data files

The repository includes example CSV inputs under `data/retail_pipeline/` and telemetry samples under `data/vsa_pipeline/`:

- `data/retail_pipeline/input_sales.csv`: core example dataset for PSA ingestion.
- `data/retail_pipeline/invalid_input_sales.csv`: invalid rows for PSA error handling and downstream validation.
- `data/retail_pipeline/invalid_input_sales_quick.csv`: smaller invalid dataset for faster testing.
- `data/vsa_pipeline/telemetry_stream.json`: example telemetry stream for the VSA pipeline and RabbitMQ fallback execution.

## Run the PSA ingestion pipeline locally

Use the PSA ingestion script and pass the input/output path options as needed.

```bash
python src/psa_ingestion.py \
  --input_path data/retail_pipeline/input_sales.csv \
  --output_prefix output/retail-lake/psa/sales_raw
```

The default values are:

- `--input_path`: `data/retail_pipeline/input_sales.csv`
- `--output_prefix`: `output/retail-lake/psa/sales_raw`
- `--error_prefix`: `output/retail-lake/psa/errors`

## Run the DWS conversion pipeline locally

Use the DWS conversion script to read PSA output, convert currency, and write downstream JSON.

```bash
python src/dws_currency_conversion.py \
  --dws_input_path output/retail-lake/psa/sales_raw-*.parquet \
  --dws_output_prefix output/retail-warehouse/gold/sales_final
```

The default values are:

- `--dws_input_path`: `output/retail-lake/psa/sales_raw-*.parquet`
- `--dws_output_prefix`: `output/retail-warehouse/gold/sales_final`
- `--dws_error_prefix`: `output/retail-lake/errors/failed_conversions`
- `--dws_exchange_rate`: `950.0`

## Run the VSA telemetry pipeline locally

Use the VSA pipeline with the sample telemetry JSONL input file.

```bash
python src/vsa_telemetry_pipeline.py
```

To run the RabbitMQ-compatible version with fallback to the same JSONL file:

```bash
python src/rabbitmq_vsa.py
```

### Load telemetry data into RabbitMQ

The devcontainer includes RabbitMQ and the management UI is available at `http://localhost:15672`.
Login with `guest` / `guest`, then create a queue named `telemetry`.

You can publish the sample telemetry file using `rabbitmqadmin`:

```bash
curl -O http://localhost:15672/cli/rabbitmqadmin
chmod +x rabbitmqadmin
./rabbitmqadmin declare queue name=telemetry durable=true
while IFS= read -r line; do
  ./rabbitmqadmin publish routing_key=telemetry payload="$line"
done < data/vsa_pipeline/telemetry_stream.json
rm rabbitmqadmin
```

If you prefer a Python loader, install `pika` and run:

```bash
pip install pika
python - <<'PY'
import pika
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='telemetry', durable=True)
with open('data/vsa_pipeline/telemetry_stream.json') as f:
    for line in f:
        body = line.strip()
        if body:
            channel.basic_publish(exchange='', routing_key='telemetry', body=body)
connection.close()
PY
```

Both VSA scripts write alert, summary, and bad-record outputs to the `output/vsa/` prefix by default.

## End-to-end execution example

To run both pipelines in sequence:

```bash
python src/psa_ingestion.py \
  --input_path data/retail_pipeline/*.csv \
  --output_prefix output/retail-lake/psa/sales_raw

python src/dws_currency_conversion.py \
  --dws_input_path output/retail-lake/psa/sales_raw-*.parquet \
  --dws_output_prefix output/retail-warehouse/gold/sales_final
```

This first generates the PSA Parquet dataset, then converts it into the DWS JSONL output.

## Error test sample files

Two invalid sample CSV files are included for error-path testing:

- `data/retail_pipeline/invalid_input_sales.csv`: contains mixed invalid rows for PSA validation plus one valid row for downstream DWS conversion.
- `data/retail_pipeline/invalid_input_sales_quick.csv`: a smaller set of invalid rows for faster verification.

Use these sample files with `src/psa_ingestion.py` to verify PSA error handling, then run `src/dws_currency_conversion.py` on the output Parquet to validate the end-to-end flow.

## Notes

- The pipelines use Apache Beam's local runner by default.
- For the PSA pipeline, the output prefix should be a directory prefix for Parquet shards.
- For the DWS pipeline, JSONL files are written to the configured prefix.
- To run on another Beam runner, add the usual Beam pipeline options when executing each script.
