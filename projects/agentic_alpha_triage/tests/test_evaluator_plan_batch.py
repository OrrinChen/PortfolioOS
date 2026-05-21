from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from agentic_alpha_triage.evaluator_plan_batch import run_evaluator_plan_manifest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = PROJECT_ROOT / "examples" / "evaluator_plan_manifest.yaml"
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "plan_evaluator_manifest.py"
FORBIDDEN_TRADING_KEYS = {"realized_return", "alpha_performance", "orders"}


def test_run_evaluator_plan_manifest_returns_ready_and_rejected_payloads() -> None:
    result = run_evaluator_plan_manifest(MANIFEST_PATH)

    assert result.manifest_id == "MANIFEST-Q1-EVALUATOR-PLAN-001"
    assert [entry.entry_id for entry in result.entries] == [
        "ready_guidance_raise_drift",
        "rejected_guidance_raise_forward_return_leakage",
    ]
    assert [entry.observed_status for entry in result.entries] == [
        "ready_for_local_evaluation",
        "rejected",
    ]
    assert all(entry.matched_expected_status for entry in result.entries)

    ready_payload = result.entries[0].planner_payload
    assert ready_payload["plan_id"] == "PLAN-EV-GUIDANCE-RAISE-DRIFT-001"
    assert ready_payload["status"] == "ready_for_local_evaluation"
    assert ready_payload["output_column"] == "alpha_score"

    rejected_payload = result.entries[1].planner_payload
    assert rejected_payload["plan_id"] is None
    assert rejected_payload["status"] == "rejected"
    assert "uses_future_data_as_feature" in rejected_payload["rejection_reasons"][0]
    assert FORBIDDEN_TRADING_KEYS.isdisjoint(_collect_keys(result.model_dump(mode="json")))


def test_plan_evaluator_manifest_cli_prints_batch_payload() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--manifest",
            str(MANIFEST_PATH),
            "--indent",
            "0",
        ],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )

    payload = json.loads(result.stdout)

    assert payload["manifest_id"] == "MANIFEST-Q1-EVALUATOR-PLAN-001"
    assert [entry["entry_id"] for entry in payload["entries"]] == [
        "ready_guidance_raise_drift",
        "rejected_guidance_raise_forward_return_leakage",
    ]
    assert [entry["observed_status"] for entry in payload["entries"]] == [
        "ready_for_local_evaluation",
        "rejected",
    ]
    assert FORBIDDEN_TRADING_KEYS.isdisjoint(_collect_keys(payload))


def _collect_keys(payload: Any) -> set[str]:
    if isinstance(payload, dict):
        keys = set(payload)
        for value in payload.values():
            keys.update(_collect_keys(value))
        return keys
    if isinstance(payload, list):
        keys: set[str] = set()
        for value in payload:
            keys.update(_collect_keys(value))
        return keys
    return set()
