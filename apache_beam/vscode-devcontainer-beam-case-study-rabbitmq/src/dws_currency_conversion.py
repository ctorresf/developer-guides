from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Dict

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromParquet, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

logger = logging.getLogger(__name__)

DEFAULT_INPUT_PATH = "output/retail-lake/psa/sales_raw-*.parquet"
DEFAULT_OUTPUT_PREFIX = "output/retail-warehouse/gold/sales_final"
DEFAULT_ERROR_PREFIX = "output/retail-lake/errors/failed_conversions"
DEFAULT_EXCHANGE_RATE = 950.0


@dataclass
class DwsPipelineConfig:
    input_path: str
    output_prefix: str
    error_prefix: str = DEFAULT_ERROR_PREFIX
    exchange_rate: float = DEFAULT_EXCHANGE_RATE


class DwsProcessingOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--dws_input_path",
            default=DEFAULT_INPUT_PATH,
            help="Path to PSA Parquet input files for the DWS pipeline.",
        )
        parser.add_argument(
            "--dws_output_prefix",
            default=DEFAULT_OUTPUT_PREFIX,
            help="Output prefix for the DWS JSON output.",
        )
        parser.add_argument(
            "--dws_error_prefix",
            default=DEFAULT_ERROR_PREFIX,
            help="Output prefix for DWS error records.",
        )
        parser.add_argument(
            "--dws_exchange_rate",
            default=DEFAULT_EXCHANGE_RATE,
            type=float,
            help="Exchange rate to convert CLP to USD.",
        )


class CurrencyConversionFn(beam.DoFn):
    """Beam DoFn that validates and converts CLP to USD."""

    def __init__(self, exchange_rate: float):
        self.exchange_rate = exchange_rate

    def process(self, element: Dict[str, str]):
        try:
            amount_clp = float(element["amount_clp"])
        except (KeyError, ValueError) as exc:
            yield pvalue.TaggedOutput(
                "error_log",
                {"record": element, "error": "invalid_amount", "detail": str(exc)},
            )
            return

        if amount_clp <= 0:
            yield pvalue.TaggedOutput(
                "error_log",
                {"record": element, "error": "non_positive_amount"},
            )
            return

        try:
            sale_date = element["timestamp"][0:10]
            product_sku = element["sku"]
            store_id = element["store_id"]
        except KeyError as exc:
            yield pvalue.TaggedOutput(
                "error_log",
                {"record": element, "error": "missing_field", "detail": str(exc)},
            )
            return

        yield {
            "sale_date": sale_date,
            "product_sku": product_sku,
            "amount_usd": round(amount_clp / self.exchange_rate, 2),
            "store_id": store_id,
        }


def to_json_line(record: Dict[str, object]) -> str:
    return json.dumps(record, ensure_ascii=False)


def build_dws_pipeline(options: PipelineOptions, config: DwsPipelineConfig) -> None:
    logger.info("Starting DWS processing pipeline")
    logger.info("Input path: %s", config.input_path)
    logger.info("Output prefix: %s", config.output_prefix)
    logger.info("Error prefix: %s", config.error_prefix)

    with beam.Pipeline(options=options) as pipeline:
        psa_data = pipeline | "ReadFromPSA" >> ReadFromParquet(config.input_path)

        processing_results = (
            psa_data
            | "ConvertCurrency" >> beam.ParDo(CurrencyConversionFn(config.exchange_rate)).with_outputs(
                "error_log", main="valid_sales"
            )
        )

        _ = (
            processing_results.valid_sales
            | "FormatValidSalesAsJson" >> beam.Map(to_json_line)
            | "WriteValidSales" >> WriteToText(
                config.output_prefix,
                file_name_suffix=".jsonl",
            )
        )

        _ = (
            processing_results.error_log
            | "FormatErrorsAsJson" >> beam.Map(to_json_line)
            | "WriteErrorRecords" >> WriteToText(
                config.error_prefix,
                file_name_suffix=".jsonl",
            )
        )


def run() -> None:
    options = PipelineOptions().view_as(DwsProcessingOptions)
    config = DwsPipelineConfig(
        input_path=options.dws_input_path,
        output_prefix=options.dws_output_prefix,
        error_prefix=options.dws_error_prefix,
        exchange_rate=options.dws_exchange_rate,
    )
    build_dws_pipeline(options, config)


if __name__ == "__main__":
    run()
