import pytest
import tempfile
from pathlib import Path


@pytest.fixture
def temp_pipeline_setup():
    """Fixture providing temporary directories for input/output in pipeline tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        input_csv = temp_path / "test_input.csv"
        output_prefix = temp_path / "test_output"
        yield {
            "temp_path": temp_path,
            "input_csv": input_csv,
            "output_prefix": output_prefix,
        }


def create_test_csv(file_path: Path, data: list):
    """Helper to create a test CSV file with given data rows."""
    lines = ["timestamp,store_id,sku,amount_clp"]
    lines.extend(data)
    file_path.write_text("\n".join(lines) + "\n")
