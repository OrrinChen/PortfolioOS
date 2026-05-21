from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "plan_evaluator.py"
FIXTURE_PATH = PROJECT_ROOT / "examples" / "evaluator_fixtures" / "valid" / "guidance_raise_drift.yaml"
EVENT_REGISTRY_DIR = PROJECT_ROOT / "examples" / "event_registry" / "valid"
FORBIDDEN_TRADING_KEYS = {"realized_return", "alpha_performance", "orders"}


def _run_plan_evaluator(
    fixture_path: Path,
    event_registry_dir: Path,
    *extra_args: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--fixture",
            str(fixture_path),
            "--event-registry-dir",
            str(event_registry_dir),
            *extra_args,
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
    )


def _copy_examples_with_signal_name_mismatch(tmp_path: Path) -> tuple[Path, Path]:
    copied_examples = tmp_path / "examples"
    shutil.copytree(PROJECT_ROOT / "examples", copied_examples)
    signal_path = copied_examples / "signal_guidance_raise_drift.yaml"
    signal_text = signal_path.read_text(encoding="utf-8")
    signal_path.write_text(
        signal_text.replace(
            "signal_name: guidance_raise_drift",
            "signal_name: mismatched_guidance_raise_drift",
        ),
        encoding="utf-8",
    )
    return (
        copied_examples / "evaluator_fixtures" / "valid" / "guidance_raise_drift.yaml",
        copied_examples / "event_registry" / "valid",
    )


def test_evaluator_plan_cli_prints_local_dry_run_plan_json() -> None:
    result = _run_plan_evaluator(FIXTURE_PATH, EVENT_REGISTRY_DIR)
    result.check_returncode()

    payload = json.loads(result.stdout)

    assert payload["plan_id"] == "PLAN-EV-GUIDANCE-RAISE-DRIFT-001"
    assert payload["status"] == "ready_for_local_evaluation"
    assert payload["fixture_id"] == "EV-GUIDANCE-RAISE-DRIFT-001"
    assert payload["event_registry_ids"] == ["REG-GUIDANCE-RAISE-001"]
    assert payload["output_column"] == "alpha_score"
    assert FORBIDDEN_TRADING_KEYS.isdisjoint(payload)


def test_evaluator_plan_cli_preserves_nonzero_exit_for_contract_rejection(tmp_path: Path) -> None:
    fixture_path, event_registry_dir = _copy_examples_with_signal_name_mismatch(tmp_path)

    result = _run_plan_evaluator(fixture_path, event_registry_dir)

    assert result.returncode != 0
    assert result.stdout == ""
    assert "signal_name" in result.stderr


def test_evaluator_plan_cli_can_emit_rejected_audit_json(tmp_path: Path) -> None:
    fixture_path, event_registry_dir = _copy_examples_with_signal_name_mismatch(tmp_path)

    result = _run_plan_evaluator(
        fixture_path,
        event_registry_dir,
        "--emit-rejected-json",
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["plan_id"] is None
    assert payload["status"] == "rejected"
    assert payload["fixture_path"] == str(fixture_path)
    assert payload["event_registry_dir"] == str(event_registry_dir)
    assert len(payload["rejection_reasons"]) == 1
    assert "signal_name" in payload["rejection_reasons"][0]
    assert FORBIDDEN_TRADING_KEYS.isdisjoint(payload)
