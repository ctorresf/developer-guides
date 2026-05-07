import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CASE2_MODULE_PATH = ROOT / "src" / "vsa_telemetry_pipeline.py"
spec = importlib.util.spec_from_file_location("vsa_telemetry_pipeline", str(CASE2_MODULE_PATH))
case2 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(case2)


def test_parse_telemetry_event_valid():
    event = case2.parse_telemetry_event('{"truck_id":"T1","speed":10,"ts":1700000000}')

    assert event["truck_id"] == "T1"
    assert event["speed"] == 10.0
    assert event["ts"] == 1700000000.0


def test_parse_telemetry_event_bad_json():
    with pytest.raises(ValueError):
        case2.parse_telemetry_event('{"truck_id":"T1","speed":10, ts: 1700000000}')


def test_parse_telemetry_event_missing_field():
    with pytest.raises(ValueError, match="Missing required field"):
        case2.parse_telemetry_event('{"truck_id":"T1","speed":10}')


def test_has_emergency_speed_range_true():
    speeds = [10.0, 45.0, 20.0]
    assert case2.has_emergency_speed_range(speeds)


def test_has_emergency_speed_range_false():
    speeds = [20.0, 30.0, 25.0]
    assert not case2.has_emergency_speed_range(speeds)
