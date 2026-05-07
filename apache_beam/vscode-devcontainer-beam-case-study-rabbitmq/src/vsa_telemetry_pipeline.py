import os
import sys

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

sys.path.insert(0, os.path.dirname(__file__))
from vsa_shared import (
    build_vsa_pipeline,
    has_emergency_speed_range,
    parse_telemetry_event,
)

DEFAULT_INPUT_PATH = "data/vsa_pipeline/telemetry_stream.json"
DEFAULT_ALERT_PREFIX = "output/vsa/alerts"
DEFAULT_SUMMARY_PREFIX = "output/vsa/summary"
DEFAULT_BAD_PREFIX = "output/vsa/bad"


def run_logitrans_vsa(
    input_path: str = DEFAULT_INPUT_PATH,
    alert_prefix: str = DEFAULT_ALERT_PREFIX,
    summary_prefix: str = DEFAULT_SUMMARY_PREFIX,
    bad_prefix: str = DEFAULT_BAD_PREFIX,
) -> None:
    options = PipelineOptions()
    with beam.Pipeline(options=options) as pipeline:
        source = pipeline | "Read telemetry JSONL" >> beam.io.ReadFromText(input_path)
        build_vsa_pipeline(source, alert_prefix, summary_prefix, bad_prefix)


if __name__ == "__main__":
    run_logitrans_vsa()
