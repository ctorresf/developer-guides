from __future__ import annotations

import csv
import datetime
import logging
import os
import uuid
from pathlib import Path
from typing import Dict, Optional

import apache_beam as beam
import pyarrow as pa
from apache_beam import pvalue
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.options.pipeline_options import PipelineOptions

setup_logging = lambda x: None  # Beam's setup_logging is optional
logger = logging.getLogger(__name__)

DEFAULT_INPUT_PATH = Path("data/retail_pipeline/input_sales.csv")
DEFAULT_OUTPUT_PREFIX = Path("output/retail-lake/psa/sales_raw")
DEFAULT_ERROR_PREFIX = Path("output/retail-lake/psa/errors")

# Schema definition for PSA (all strings to prevent casting errors)
PSA_SCHEMA = pa.schema([
    pa.field("timestamp", pa.string()),
    pa.field("store_id", pa.string()),
    pa.field("sku", pa.string()),
    pa.field("amount_clp", pa.string()),
    pa.field("ingestion_ts", pa.string()),
    pa.field("source_file", pa.string()),
    pa.field("batch_id", pa.string()),
])


class PipelineConfig:
    """Configuration container for PSA ingestion pipeline."""

    def __init__(
        self,
        input_path: str,
        output_prefix: str,
        error_prefix: Optional[str] = None,
        batch_id: Optional[str] = None,
        validate_headers: bool = True,
        partition_by_date: bool = False,
    ):
        self.input_path = input_path
        self.output_prefix = output_prefix
        self.error_prefix = error_prefix or str(DEFAULT_ERROR_PREFIX)
        self.batch_id = batch_id or str(uuid.uuid4())[:8]
        self.validate_headers = validate_headers
        self.partition_by_date = partition_by_date
        self.source_file = Path(input_path).name


class PSAIngestionOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_path",
            default=str(DEFAULT_INPUT_PATH),
            help="Path to the input CSV file.",
        )
        parser.add_argument(
            "--output_prefix",
            default=str(DEFAULT_OUTPUT_PREFIX),
            help="Output prefix for Parquet files.",
        )
        parser.add_argument(
            "--error_prefix",
            default=str(DEFAULT_ERROR_PREFIX),
            help="Output prefix for error records.",
        )
        parser.add_argument(
            "--batch_id",
            default=None,
            help="Unique batch identifier for this ingestion.",
        )
        parser.add_argument(
            "--validate_headers",
            default=True,
            type=bool,
            help="Validate CSV headers before processing.",
        )
        parser.add_argument(
            "--partition_by_date",
            default=False,
            type=bool,
            help="Partition output by ingestion date.",
        )


def parse_csv_line(line: str) -> Dict[str, str]:
    """Parse a CSV line into a record dictionary.
    
    Args:
        line: CSV line with 4 comma-separated values
        
    Returns:
        Dictionary with keys: timestamp, store_id, sku, amount_clp
        
    Raises:
        IndexError: If line has fewer than 4 columns
    """
    values = next(csv.reader([line]))
    if len(values) < 4:
        raise IndexError(f"Expected 4 columns, got {len(values)}")
    return {
        "timestamp": values[0],
        "store_id": values[1],
        "sku": values[2],
        "amount_clp": values[3],
    }


class ParseCsvDoFn(beam.DoFn):
    """DoFn for parsing CSV lines with error routing."""

    def process(self, line: str):
        try:
            result = parse_csv_line(line)
            yield result
        except Exception as e:
            logger.error(f"Error parsing CSV line: {e}. Line: {line[:50]}")
            yield pvalue.TaggedOutput("error", {"raw_line": line, "error": str(e)})


class ValidateRecordDoFn(beam.DoFn):
    """DoFn for validating records with error routing."""

    def process(self, record: Dict[str, str]):
        try:
            # Check for required fields
            required_fields = ["timestamp", "store_id", "sku", "amount_clp"]
            for field in required_fields:
                if not record.get(field) or not record[field].strip():
                    logger.warning(f"Missing or empty field '{field}' in record: {record}")
                    yield pvalue.TaggedOutput("error", {"record": record, "error": f"missing_field_{field}"})
                    return
            
            # Validate timestamp format (basic ISO check)
            if "T" not in record["timestamp"]:
                logger.warning(f"Invalid timestamp format in record: {record}")
                yield pvalue.TaggedOutput("error", {"record": record, "error": "invalid_timestamp_format"})
                return
            
            # Validate amount is numeric
            try:
                float(record["amount_clp"])
            except ValueError:
                logger.warning(f"Amount not numeric in record: {record}")
                yield pvalue.TaggedOutput("error", {"record": record, "error": "non_numeric_amount"})
                return
            
            yield record
        except Exception as e:
            logger.error(f"Error validating record: {e}")
            yield pvalue.TaggedOutput("error", {"record": record, "error": str(e)})


def add_ingestion_metadata(
    record: Dict[str, str],
    batch_id: str = None,
    source_file: str = None,
) -> Dict[str, str]:
    """Attach comprehensive ingestion metadata to the record."""
    now = datetime.datetime.now(datetime.timezone.utc)
    return {
        **record,
        "ingestion_ts": now.isoformat(),
        "source_file": source_file or "unknown",
        "batch_id": batch_id or "unknown",
    }


def build_pipeline(
    options: PipelineOptions,
    config: PipelineConfig,
) -> None:
    """Build and execute the Beam pipeline with data validation and error handling."""
    logger.info(f"Starting PSA ingestion pipeline. Batch ID: {config.batch_id}")
    logger.info(f"Input: {config.input_path}, Output: {config.output_prefix}")
    
    try:
        with beam.Pipeline(options=options) as pipeline:
            # Stage 1: Read CSV
            raw_lines = pipeline | "ReadRawCsv" >> beam.io.ReadFromText(config.input_path)
            
            # Skip header by filtering using a simple lambda
            data_lines = raw_lines | "SkipHeader" >> beam.Filter(lambda line: line != "timestamp,store_id,sku,amount_clp")
            
            # Stage 2: Parse CSV with error routing
            parse_results = (
                data_lines
                | "ParseCsvRow" >> beam.ParDo(ParseCsvDoFn()).with_outputs("error", main="valid")
            )
            
            # Stage 3: Validate records with error routing
            validation_results = (
                parse_results.valid
                | "ValidateRecord" >> beam.ParDo(ValidateRecordDoFn()).with_outputs("error", main="valid")
            )
            
            # Stage 4: Add metadata
            enriched_records = (
                validation_results.valid
                | "AddAuditMetadata" >> beam.Map(
                    lambda rec: add_ingestion_metadata(rec, config.batch_id, config.source_file)
                )
            )
            
            # Stage 5: Write valid records to Parquet
            (
                enriched_records
                | "WriteToParquet" >> WriteToParquet(
                    file_path_prefix=config.output_prefix,
                    schema=PSA_SCHEMA,
                    file_name_suffix=".parquet",
                    num_shards=1,
                )
            )
            
            # Stage 6: Collect all errors and write to error output
            all_errors = (
                (parse_results.error, validation_results.error)
                | "MergeErrors" >> beam.Flatten()
                | "FormatErrors" >> beam.Map(lambda x: str(x))
            )
            
            (
                all_errors
                | "WriteErrors" >> beam.io.WriteToText(
                    config.error_prefix,
                    file_name_suffix=".log",
                )
            )
            
            logger.info("Pipeline execution completed successfully.")
    
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        raise


def run() -> None:
    """Run the PSA ingestion pipeline using command-line options."""
    options_obj = PipelineOptions().view_as(PSAIngestionOptions)
    
    # Create pipeline configuration from options
    config = PipelineConfig(
        input_path=options_obj.input_path,
        output_prefix=options_obj.output_prefix,
        error_prefix=options_obj.error_prefix,
        batch_id=options_obj.batch_id,
        validate_headers=options_obj.validate_headers,
        partition_by_date=options_obj.partition_by_date,
    )
    
    # Set up runner-specific configurations from environment
    runner = os.getenv("BEAM_RUNNER", "DirectRunner")
    logger.info(f"Using Beam runner: {runner}")
    
    if runner == "DataflowRunner" and options_obj.max_num_workers:
        options_obj.runner = "DataflowRunner"
        options_obj.max_num_workers = options_obj.max_num_workers
    
    build_pipeline(options_obj, config)


if __name__ == "__main__":
    run()
