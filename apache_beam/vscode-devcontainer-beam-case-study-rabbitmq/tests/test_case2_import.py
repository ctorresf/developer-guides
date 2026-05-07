import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CASE2_MODULE_PATH = ROOT / "src" / "vsa_telemetry_pipeline.py"

spec = importlib.util.spec_from_file_location("vsa_telemetry_pipeline", str(CASE2_MODULE_PATH))
case2 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(case2)


def test_case2_module_imports():
    assert hasattr(case2, "run_logitrans_vsa")
    assert callable(case2.run_logitrans_vsa)
