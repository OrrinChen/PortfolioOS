from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from agentic_alpha_triage.evaluator_plan_batch import (
    EvaluatorPlanBatchEntryResult,
    EvaluatorPlanBatchResult,
    summarize_evaluator_plan_batch,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = PROJECT_ROOT / "examples" / "evaluator_plan_manifest.yaml"
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "plan_evaluator_manifest.py"
FORBIDDEN_TRADING_KEYS = {
    "realized_return",
    "alpha_performance",
    "orders",
    "planner_payload",
    "cost_assumptions",
    "feature_columns",
}


def test_summarize_evaluator_plan_batch_counts_statuses() -> None:
    batch = EvaluatorPlanBatchResult(
        manifest_id="MANIFEST-SUMMARY",
        entries=[
            _entry("ready_entry", "ready_for_local_evaluation", "ready_for_local_evaluation"),
            _entry("rejected_entry", "rejected", "rejected"),
            _entry("mismatch_entry", "ready_for_local_evaluation", "rejected"),
        ],
    )

    summary = summarize_evaluator_plan_batch(batch)

    assert summary.manifest_id == "MANIFEST-SUMMARY"
    assert summary.total_entries == 3
    assert summary.ready_count == 1
    assert summary.rejected_count == 2
    assert summary.expected_status_mismatch_count == 1
    assert summary.expected_status_mismatches == ["mismatch_entry"]
    assert FORBIDDEN_TRADING_KEYS.isdisjoint(_collect_keys(summary.model_dump(mode="json")))


def test_plan_evaluator_manifest_cli_can_print_summary_only() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--manifest",
            str(MANIFEST_PATH),
            "--summary",
            "--indent",
            "0",
        ],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )

    payload = json.loads(result.stdout)

    assert payload == {
        "expected_status_mismatch_count": 0,
        "expected_status_mismatches": [],
        "manifest_id": "MANIFEST-Q1-EVALUATOR-PLAN-001",
        "ready_count": 1,
        "rejected_count": 1,
        "total_entries": 2,
    }
    assert FORBIDDEN_TRADING_KEYS.isdisjoint(_collect_keys(payload))


def _entry(
    entry_id: str,
    expected_status: str,
    observed_status: str,
) -> EvaluatorPlanBatchEntryResult:
    return EvaluatorPlanBatchEntryResult(
        entry_id=entry_id,
        expected_status=expected_status,
        observed_status=observed_status,
        matched_expected_status=expected_status == observed_status,
        fixture_path=f"{entry_id}.yaml",
        event_registry_dir="event_registry/valid",
        planner_payload={"status": observed_status},
    )


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
