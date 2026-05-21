from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from pydantic import BaseModel


REPO_ROOT = Path(__file__).resolve().parents[1]
for project_src in (
    REPO_ROOT / "projects" / "agentic_alpha_triage" / "src",
    REPO_ROOT / "projects" / "audit_report" / "src",
    REPO_ROOT / "projects" / "evidence_bundle" / "src",
    REPO_ROOT / "projects" / "execution_aware_optimizer" / "src",
    REPO_ROOT / "projects" / "promotion_gate" / "src",
):
    if str(project_src) not in sys.path:
        sys.path.insert(0, str(project_src))

from agentic_alpha_triage.evaluator_plan_batch import (
    run_evaluator_plan_manifest,
    summarize_evaluator_plan_batch,
)
from audit_report import build_demo_audit_report, load_demo_audit_manifest
from execution_aware_optimizer.execution_matrix import run_execution_matrix
from execution_aware_optimizer.experiment_config import ExperimentConfig
from promotion_gate import evaluate_promotion_candidate


FORBIDDEN_OUTPUT_TERMS = {
    "alpha_performance",
    "broker_output",
    "live_performance",
    "orders",
    "realized_return",
    "trading_instruction",
}


def test_q1_summary_promotion_q2_and_report_outputs_exclude_forbidden_terms() -> None:
    q1_batch = run_evaluator_plan_manifest(
        REPO_ROOT / "projects" / "agentic_alpha_triage" / "examples" / "evaluator_plan_manifest.yaml"
    )
    q1_summary = summarize_evaluator_plan_batch(q1_batch)
    promotion_decision = evaluate_promotion_candidate(
        REPO_ROOT / "projects" / "evidence_bundle" / "examples" / "valid_bundle.yaml"
    )
    manifest = load_demo_audit_manifest(
        REPO_ROOT / "projects" / "audit_report" / "examples" / "demo_audit_manifest.yaml"
    )
    q2_rows = run_execution_matrix(ExperimentConfig.model_validate(manifest.q2_execution))
    report = build_demo_audit_report(REPO_ROOT, manifest)

    for artifact in (q1_summary, promotion_decision, q2_rows, report.markdown):
        assert _has_no_forbidden_terms(artifact)


def _has_no_forbidden_terms(artifact: Any) -> bool:
    if isinstance(artifact, BaseModel):
        text = artifact.model_dump_json()
    elif isinstance(artifact, list):
        text = "\n".join(
            item.model_dump_json() if isinstance(item, BaseModel) else str(item)
            for item in artifact
        )
    else:
        text = str(artifact)
    lowered = text.lower()
    return all(term not in lowered for term in FORBIDDEN_OUTPUT_TERMS)
