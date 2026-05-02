from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "plan_evaluator.py"
FIXTURE_PATH = PROJECT_ROOT / "examples" / "evaluator_fixtures" / "valid" / "guidance_raise_drift.yaml"
EVENT_REGISTRY_DIR = PROJECT_ROOT / "examples" / "event_registry" / "valid"


def test_evaluator_plan_cli_prints_local_dry_run_plan_json() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--fixture",
            str(FIXTURE_PATH),
            "--event-registry-dir",
            str(EVENT_REGISTRY_DIR),
        ],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )

    payload = json.loads(result.stdout)

    assert payload["plan_id"] == "PLAN-EV-GUIDANCE-RAISE-DRIFT-001"
    assert payload["status"] == "ready_for_local_evaluation"
    assert payload["fixture_id"] == "EV-GUIDANCE-RAISE-DRIFT-001"
    assert payload["event_registry_ids"] == ["REG-GUIDANCE-RAISE-001"]
    assert payload["output_column"] == "alpha_score"
    assert "realized_return" not in payload
    assert "alpha_performance" not in payload
    assert "orders" not in payload
