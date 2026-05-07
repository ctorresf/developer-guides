import json
from typing import Any, Dict, Iterable, Tuple

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.transforms.window import SlidingWindows
from apache_beam.typehints import with_input_types, with_output_types


def parse_telemetry_event(line: str) -> Dict[str, Any]:
    if not isinstance(line, str):
        raise ValueError("Telemetry line must be a JSON string")

    event = json.loads(line)
    if not isinstance(event, dict):
        raise ValueError("Telemetry event must deserialize to a JSON object")

    truck_id = event.get("truck_id")
    speed = event.get("speed")
    timestamp = event.get("ts")

    if not truck_id:
        raise ValueError("Missing required field: truck_id")
    if speed is None:
        raise ValueError("Missing required field: speed")
    if timestamp is None:
        raise ValueError("Missing required field: ts")

    try:
        speed_value = float(speed)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid speed value: {speed}") from exc

    try:
        timestamp_value = float(timestamp)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid ts value: {timestamp}") from exc

    parsed = {
        "truck_id": str(truck_id),
        "speed": speed_value,
        "ts": timestamp_value,
    }

    for key, value in event.items():
        if key not in parsed:
            parsed[key] = value

    return parsed


def has_emergency_speed_range(speeds: Iterable[float]) -> bool:
    speeds_list = list(speeds)
    if not speeds_list:
        return False
    return max(speeds_list) - min(speeds_list) > 30


def format_json_line(record: Dict[str, Any]) -> str:
    return json.dumps(record, ensure_ascii=False)


@with_input_types(str)
@with_output_types(Dict[str, Any])
class ParseTelemetryDoFn(beam.DoFn):
    BAD_OUTPUT = "bad_records"

    def process(self, element: str):
        try:
            parsed = parse_telemetry_event(element)
            yield parsed
        except Exception as exc:
            yield pvalue.TaggedOutput(self.BAD_OUTPUT, {
                "raw": element.strip(),
                "error": str(exc),
            })


@with_input_types(Dict[str, Any])
@with_output_types(Tuple[str, Dict[str, Any]])
class KeyTelemetryByTruckDoFn(beam.DoFn):

    def process(self, event: Dict[str, Any]):
        yield event["truck_id"], event


@with_input_types(Tuple[str, Iterable[Dict[str, Any]]])
@with_output_types(Dict[str, Any])
class EmergencyDetectorDoFn(beam.DoFn):
    EMERGENCY_OUTPUT = "emergency"

    def process(self, element, window=beam.DoFn.WindowParam):
        truck_id, records = element
        speeds = [record["speed"] for record in records]
        summary = {
            "truck_id": truck_id,
            "avg_speed": sum(speeds) / len(speeds) if speeds else 0.0,
            "record_count": len(speeds),
            "window_start": window.start.to_rfc3339(),
            "window_end": window.end.to_rfc3339(),
        }

        if has_emergency_speed_range(speeds):
            yield pvalue.TaggedOutput(self.EMERGENCY_OUTPUT, {
                "truck_id": truck_id,
                "severity": "HIGH",
                "msg": "Frenado brusco detectado",
                "window_start": summary["window_start"],
                "window_end": summary["window_end"],
            })

        yield summary


def build_vsa_pipeline(source: beam.PCollection, alert_prefix: str, summary_prefix: str, bad_prefix: str) -> None:
    parsing_result = (
        source
        | "Parse Telemetry" >> beam.ParDo(ParseTelemetryDoFn()).with_outputs(ParseTelemetryDoFn.BAD_OUTPUT, main="valid")
    )

    valid_records = parsing_result.valid
    bad_records = parsing_result.bad_records

    windowed_events = (
        valid_records
        | "Assign Timestamps" >> beam.Map(lambda event: beam.window.TimestampedValue(event, event["ts"]))
        | "Key by Truck" >> beam.ParDo(KeyTelemetryByTruckDoFn())
        | "VSA Sliding Window" >> beam.WindowInto(SlidingWindows(10, 5))
        | "Group by Truck" >> beam.GroupByKey()
    )

    analysis = (
        windowed_events
        | "Detect Emergencies" >> beam.ParDo(EmergencyDetectorDoFn()).with_outputs(EmergencyDetectorDoFn.EMERGENCY_OUTPUT, main="summary")
    )

    emergency_events = analysis.emergency
    summary_events = analysis.summary

    emergency_events | "Format Alerts" >> beam.Map(format_json_line) | "Write Alerts" >> beam.io.WriteToText(alert_prefix, file_name_suffix=".jsonl")
    summary_events | "Format Summary" >> beam.Map(format_json_line) | "Write Summaries" >> beam.io.WriteToText(summary_prefix, file_name_suffix=".jsonl")
    bad_records | "Format Bad Records" >> beam.Map(format_json_line) | "Write Bad Records" >> beam.io.WriteToText(bad_prefix, file_name_suffix=".jsonl")
