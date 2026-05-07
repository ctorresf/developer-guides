import importlib.util
from pathlib import Path
import datetime
import tempfile
import shutil
import re
from dateutil import parser as date_parser

import apache_beam as beam
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parents[1]
PSA_MODULE_PATH = ROOT / "src" / "psa_ingestion.py"

spec = importlib.util.spec_from_file_location("psa_ingestion", str(PSA_MODULE_PATH))
psa_ingestion = importlib.util.module_from_spec(spec)
spec.loader.exec_module(psa_ingestion)


def create_test_csv(file_path: Path, data: list):
    """Helper to create a test CSV file with given data rows."""
    lines = ["timestamp,store_id,sku,amount_clp"]
    lines.extend(data)
    file_path.write_text("\n".join(lines) + "\n")


def test_parse_csv_line():
    line = "2026-05-02T12:00:00,store-1,sku-123,12000"
    parsed = psa_ingestion.parse_csv_line(line)

    assert parsed == {
        "timestamp": "2026-05-02T12:00:00",
        "store_id": "store-1",
        "sku": "sku-123",
        "amount_clp": "12000",
    }


def test_parse_csv_line_with_quotes():
    line = '"2026-05-02T12:00:00","store-1","sku-123","12000"'
    parsed = psa_ingestion.parse_csv_line(line)

    assert parsed == {
        "timestamp": "2026-05-02T12:00:00",
        "store_id": "store-1",
        "sku": "sku-123",
        "amount_clp": "12000",
    }


def test_parse_csv_line_invalid_columns():
    line = "2026-05-02T12:00:00,store-1"  # Missing columns
    try:
        psa_ingestion.parse_csv_line(line)
        assert False, "Should raise IndexError"
    except IndexError:
        pass


def test_add_ingestion_metadata(monkeypatch):
    class FixedDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 5, 2, 12, 0, 0, tzinfo=tz)

    monkeypatch.setattr(psa_ingestion.datetime, "datetime", FixedDateTime)

    record = {
        "timestamp": "2026-05-02T12:00:00",
        "store_id": "store-1",
        "sku": "sku-123",
        "amount_clp": "12000",
    }
    result = psa_ingestion.add_ingestion_metadata(record, "batch-001", "test_input.csv")

    assert result["timestamp"] == record["timestamp"]
    assert result["store_id"] == record["store_id"]
    assert result["sku"] == record["sku"]
    assert result["amount_clp"] == record["amount_clp"]
    assert result["batch_id"] == "batch-001"
    assert result["source_file"] == "test_input.csv"
    assert "ingestion_ts" in result


def test_default_pipeline_options():
    options = psa_ingestion.PipelineOptions([]).view_as(psa_ingestion.PSAIngestionOptions)

    assert options.input_path == str(psa_ingestion.DEFAULT_INPUT_PATH)
    assert options.output_prefix == str(psa_ingestion.DEFAULT_OUTPUT_PREFIX)


def test_custom_pipeline_options():
    options = psa_ingestion.PipelineOptions([
        "--input_path", "custom_input.csv",
        "--output_prefix", "custom_output"
    ]).view_as(psa_ingestion.PSAIngestionOptions)

    assert options.input_path == "custom_input.csv"
    assert options.output_prefix == "custom_output"


def test_psa_schema_definition():
    assert psa_ingestion.PSA_SCHEMA.names == [
        "timestamp",
        "store_id",
        "sku",
        "amount_clp",
        "ingestion_ts",
        "source_file",
        "batch_id",
    ]
    assert all(field.type == pa.string() for field in psa_ingestion.PSA_SCHEMA)


def test_pipeline_config_defaults():
    """Test PipelineConfig with default values."""
    config = psa_ingestion.PipelineConfig(
        input_path="input.csv",
        output_prefix="output/data"
    )
    
    assert config.input_path == "input.csv"
    assert config.output_prefix == "output/data"
    assert config.source_file == "input.csv"
    assert config.batch_id is not None
    assert config.validate_headers is True


def test_pipeline_config_custom():
    """Test PipelineConfig with custom values."""
    config = psa_ingestion.PipelineConfig(
        input_path="custom_input.csv",
        output_prefix="custom_output",
        batch_id="batch-123",
    )
    
    assert config.batch_id == "batch-123"


def test_parse_csv_dofn_success():
    """Test ParseCsvDoFn with valid CSV line."""
    dofn = psa_ingestion.ParseCsvDoFn()
    line = "2026-05-02T12:00:00,store-1,sku-123,12000"
    
    results = list(dofn.process(line))
    assert len(results) == 1
    assert results[0] == {
        "timestamp": "2026-05-02T12:00:00",
        "store_id": "store-1",
        "sku": "sku-123",
        "amount_clp": "12000",
    }


def test_parse_csv_dofn_insufficient_columns():
    """Test ParseCsvDoFn with insufficient columns."""
    dofn = psa_ingestion.ParseCsvDoFn()
    line = "2026-05-02T12:00:00,store-1"
    
    results = list(dofn.process(line))
    assert len(results) == 1
    assert results[0].tag == "error"


def test_validate_record_dofn_success():
    """Test ValidateRecordDoFn with valid record."""
    dofn = psa_ingestion.ValidateRecordDoFn()
    record = {
        "timestamp": "2026-05-02T12:00:00",
        "store_id": "store-1",
        "sku": "sku-123",
        "amount_clp": "12000",
    }
    
    results = list(dofn.process(record))
    assert len(results) == 1
    assert results[0] == record


def test_validate_record_dofn_missing_field():
    """Test ValidateRecordDoFn with missing field."""
    dofn = psa_ingestion.ValidateRecordDoFn()
    record = {
        "timestamp": "2026-05-02T12:00:00",
        "store_id": "store-1",
        "amount_clp": "12000",
    }
    
    results = list(dofn.process(record))
    assert len(results) == 1
    assert results[0].tag == "error"


@pytest.mark.slow
def test_integration_pipeline_execution(temp_pipeline_setup):
    """Integration test: Run the pipeline with test data and verify output."""
    setup = temp_pipeline_setup
    input_csv = setup["input_csv"]
    output_prefix = setup["output_prefix"]

    # Create test CSV data
    create_test_csv(input_csv, ["2026-05-02T12:00:00,store-1,sku-123,12000"])

    # Create pipeline config
    config = psa_ingestion.PipelineConfig(
        input_path=str(input_csv),
        output_prefix=str(output_prefix),
        batch_id="test-batch"
    )

    # Run pipeline
    options = psa_ingestion.PipelineOptions([])
    psa_ingestion.build_pipeline(options, config)

    # Verify output Parquet file exists
    output_files = list(output_prefix.parent.glob(f"{output_prefix.name}*.parquet"))
    assert len(output_files) == 1

    # Read and verify content
    table = pq.read_table(str(output_files[0]))
    assert table.num_rows == 1
    assert "timestamp" in table.column_names
    assert "batch_id" in table.column_names
    row = table.to_pylist()[0]
    assert row["timestamp"] == "2026-05-02T12:00:00"
    assert row["batch_id"] == "test-batch"


@pytest.mark.slow
def test_integration_empty_input(temp_pipeline_setup):
    """Integration test: Handle empty input file."""
    setup = temp_pipeline_setup
    input_csv = setup["input_csv"]
    output_prefix = setup["output_prefix"]

    # Create empty CSV (just header)
    create_test_csv(input_csv, [])

    config = psa_ingestion.PipelineConfig(
        input_path=str(input_csv),
        output_prefix=str(output_prefix)
    )

    options = psa_ingestion.PipelineOptions([])
    psa_ingestion.build_pipeline(options, config)

    # Verify output Parquet file exists but is empty
    output_files = list(output_prefix.parent.glob(f"{output_prefix.name}*.parquet"))
    assert len(output_files) == 1

    table = pq.read_table(str(output_files[0]))
    assert table.num_rows == 0


@pytest.mark.slow
def test_error_handling_nonexistent_input(temp_pipeline_setup):
    """Test pipeline error handling with nonexistent input file."""
    setup = temp_pipeline_setup
    nonexistent_input = setup["temp_path"] / "nonexistent.csv"
    output_prefix = setup["output_prefix"]

    config = psa_ingestion.PipelineConfig(
        input_path=str(nonexistent_input),
        output_prefix=str(output_prefix)
    )

    options = psa_ingestion.PipelineOptions([])

    with pytest.raises(Exception):  # Beam raises various exceptions for missing files
        psa_ingestion.build_pipeline(options, config)


@pytest.mark.slow
def test_schema_validation_output_types(temp_pipeline_setup):
    """Test that output Parquet has correct data types."""
    setup = temp_pipeline_setup
    input_csv = setup["input_csv"]
    output_prefix = setup["output_prefix"]

    create_test_csv(input_csv, ["2026-05-02T12:00:00,store-1,sku-123,12000"])

    config = psa_ingestion.PipelineConfig(
        input_path=str(input_csv),
        output_prefix=str(output_prefix)
    )
    options = psa_ingestion.PipelineOptions([])
    psa_ingestion.build_pipeline(options, config)

    output_files = list(output_prefix.parent.glob(f"{output_prefix.name}*.parquet"))
    assert len(output_files) == 1

    table = pq.read_table(str(output_files[0]))
    schema = table.schema

    # Verify all fields are string type
    for field_name in ["timestamp", "store_id", "sku", "amount_clp", "ingestion_ts", "source_file", "batch_id"]:
        field = schema.field(field_name)
        assert pa.types.is_string(field.type), f"Field {field_name} should be string"


@pytest.mark.slow
def test_configuration_different_options(temp_pipeline_setup):
    """Test pipeline with different configuration options."""
    setup = temp_pipeline_setup
    input_csv = setup["input_csv"]
    output_prefix = setup["output_prefix"]

    create_test_csv(input_csv, ["2026-05-02T12:00:00,store-1,sku-123,12000"])

    config = psa_ingestion.PipelineConfig(
        input_path=str(input_csv),
        output_prefix=str(output_prefix)
    )
    options = psa_ingestion.PipelineOptions([
        "--job_name", "test-job",
        "--temp_location", str(setup["temp_path"] / "temp")
    ])
    psa_ingestion.build_pipeline(options, config)

    output_files = list(output_prefix.parent.glob(f"{output_prefix.name}*.parquet"))
    assert len(output_files) == 1


@pytest.mark.slow
def test_data_quality_no_nulls(temp_pipeline_setup):
    """Test data quality: ensure no null values in output."""
    setup = temp_pipeline_setup
    input_csv = setup["input_csv"]
    output_prefix = setup["output_prefix"]

    create_test_csv(input_csv, ["2026-05-02T12:00:00,store-1,sku-123,12000"])

    config = psa_ingestion.PipelineConfig(
        input_path=str(input_csv),
        output_prefix=str(output_prefix)
    )
    options = psa_ingestion.PipelineOptions([])
    psa_ingestion.build_pipeline(options, config)

    output_files = list(output_prefix.parent.glob(f"{output_prefix.name}*.parquet"))
    assert len(output_files) == 1

    table = pq.read_table(str(output_files[0]))
    df = table.to_pandas()

    # Check no null values
    assert not df.isnull().any().any(), "Output should have no null values"


@pytest.mark.slow
def test_data_quality_format_validation(temp_pipeline_setup):
    """Test data quality: validate timestamp and ingestion_ts formats."""
    setup = temp_pipeline_setup
    input_csv = setup["input_csv"]
    output_prefix = setup["output_prefix"]

    create_test_csv(input_csv, ["2026-05-02T12:00:00,store-1,sku-123,12000"])

    config = psa_ingestion.PipelineConfig(
        input_path=str(input_csv),
        output_prefix=str(output_prefix)
    )
    options = psa_ingestion.PipelineOptions([])
    psa_ingestion.build_pipeline(options, config)

    output_files = list(output_prefix.parent.glob(f"{output_prefix.name}*.parquet"))
    assert len(output_files) == 1

    table = pq.read_table(str(output_files[0]))
    row = table.to_pylist()[0]

    # Validate timestamp format using dateutil
    try:
        parsed_timestamp = date_parser.parse(row["timestamp"])
        assert parsed_timestamp.year == 2026
    except ValueError:
        pytest.fail("Timestamp format invalid")

    # Validate ingestion_ts format and recency
    try:
        parsed_ingestion = date_parser.parse(row["ingestion_ts"])
        now = datetime.datetime.now(datetime.timezone.utc)
        time_diff = abs((now - parsed_ingestion).total_seconds())
        assert time_diff < 60, "Ingestion timestamp should be recent (within 60 seconds)"
    except ValueError:
        pytest.fail("Ingestion timestamp format invalid")


@pytest.mark.parametrize("row_count", [1, 10, 100])
@pytest.mark.slow
def test_volume_different_sizes(temp_pipeline_setup, row_count):
    """Test pipeline with different data volumes."""
    setup = temp_pipeline_setup
    input_csv = setup["input_csv"]
    output_prefix = setup["output_prefix"]

    # Create test data with specified row count
    data_rows = [f"2026-05-02T12:{i:02d}:00,store-{i},sku-{i},1000{i}" for i in range(row_count)]
    create_test_csv(input_csv, data_rows)

    config = psa_ingestion.PipelineConfig(
        input_path=str(input_csv),
        output_prefix=str(output_prefix)
    )
    options = psa_ingestion.PipelineOptions([])
    psa_ingestion.build_pipeline(options, config)

    output_files = list(output_prefix.parent.glob(f"{output_prefix.name}*.parquet"))
    assert len(output_files) == 1

    table = pq.read_table(str(output_files[0]))
    assert table.num_rows == row_count


def test_pipeline_dag_structure():
    """Test pipeline components are wired correctly."""
    # Test that all required components are present
    assert hasattr(psa_ingestion, "ParseCsvDoFn")
    assert hasattr(psa_ingestion, "ValidateRecordDoFn")
    assert hasattr(psa_ingestion, "PipelineConfig")
    assert hasattr(psa_ingestion, "build_pipeline")
    
    # Test DoFn classes are callable
    parse_dofn = psa_ingestion.ParseCsvDoFn()
    validate_dofn = psa_ingestion.ValidateRecordDoFn()
    
    assert callable(parse_dofn.process)
    assert callable(validate_dofn.process)
    
    # Test that pipeline config creates valid batch_ids
    config1 = psa_ingestion.PipelineConfig("input1.csv", "output1")
    config2 = psa_ingestion.PipelineConfig("input2.csv", "output2")
    assert config1.batch_id is not None
    assert config2.batch_id is not None

