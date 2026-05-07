import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CASE1_MODULE_PATH = ROOT / "src" / "dws_currency_conversion.py"

spec = importlib.util.spec_from_file_location("dws_currency_conversion", str(CASE1_MODULE_PATH))
dws_currency_conversion = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = dws_currency_conversion
spec.loader.exec_module(dws_currency_conversion)


def test_currency_conversion_success():
    fn = dws_currency_conversion.CurrencyConversionFn(exchange_rate=950.0)
    element = {
        "timestamp": "2026-05-02T12:00:00",
        "store_id": "store-1",
        "sku": "sku-123",
        "amount_clp": "950.0",
    }

    output = list(fn.process(element))

    assert output == [
        {
            "sale_date": "2026-05-02",
            "product_sku": "sku-123",
            "amount_usd": 1.0,
            "store_id": "store-1",
        }
    ]


def test_currency_conversion_negative_amount():
    fn = dws_currency_conversion.CurrencyConversionFn(exchange_rate=950.0)
    element = {
        "timestamp": "2026-05-02T12:00:00",
        "store_id": "store-1",
        "sku": "sku-123",
        "amount_clp": "-500",
    }

    output = list(fn.process(element))

    assert len(output) == 1
    assert output[0].tag == "error_log"
    assert output[0].value["record"] == element
    assert output[0].value["error"] == "non_positive_amount"


def test_currency_conversion_invalid_amount():
    fn = dws_currency_conversion.CurrencyConversionFn(exchange_rate=950.0)
    element = {
        "timestamp": "2026-05-02T12:00:00",
        "store_id": "store-1",
        "sku": "sku-123",
        "amount_clp": "not-a-number",
    }

    output = list(fn.process(element))

    assert len(output) == 1
    assert output[0].tag == "error_log"
    assert output[0].value["record"] == element
    assert output[0].value["error"] == "invalid_amount"


def test_currency_conversion_missing_field():
    fn = dws_currency_conversion.CurrencyConversionFn(exchange_rate=950.0)
    element = {
        "timestamp": "2026-05-02T12:00:00",
        "store_id": "store-1",
        "amount_clp": "950.0",
    }

    output = list(fn.process(element))

    assert len(output) == 1
    assert output[0].tag == "error_log"
    assert output[0].value["record"] == element
    assert output[0].value["error"] == "missing_field"


def test_pipeline_config_defaults():
    config = dws_currency_conversion.DwsPipelineConfig(input_path="input.parquet", output_prefix="output/sales")

    assert config.input_path == "input.parquet"
    assert config.output_prefix == "output/sales"
    assert config.error_prefix == dws_currency_conversion.DEFAULT_ERROR_PREFIX
    assert config.exchange_rate == dws_currency_conversion.DEFAULT_EXCHANGE_RATE


def test_to_json_line():
    record = {
        "sale_date": "2026-05-02",
        "product_sku": "sku-123",
        "amount_usd": 1.0,
        "store_id": "store-1",
    }

    json_line = dws_currency_conversion.to_json_line(record)
    assert json.loads(json_line) == record
