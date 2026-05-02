"""Batch local dry-run planner wrapper for Q1 evaluator manifests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

from agentic_alpha_triage.evaluator_plan_manifest import (
    EvaluatorPlanManifestEntry,
    load_evaluator_plan_manifest,
)
from agentic_alpha_triage.evaluator_planner import (
    PlannerStatus,
    RejectedEvaluatorPlan,
    build_evaluator_plan,
)


class EvaluatorPlanBatchEntryResult(BaseModel):
    """One ready or rejected local dry-run result for a manifest entry."""

    entry_id: str
    expected_status: PlannerStatus
    observed_status: PlannerStatus
    matched_expected_status: bool
    fixture_path: str
    event_registry_dir: str
    planner_payload: dict[str, Any]


class EvaluatorPlanBatchResult(BaseModel):
    """Ordered batch dry-run output for a local Q1 evaluator-plan manifest."""

    manifest_id: str
    entries: list[EvaluatorPlanBatchEntryResult]


class EvaluatorPlanBatchSummary(BaseModel):
    """Summary counts for a local Q1 evaluator-plan manifest batch."""

    manifest_id: str
    total_entries: int
    ready_count: int
    rejected_count: int
    expected_status_mismatch_count: int
    expected_status_mismatches: list[str]


def run_evaluator_plan_manifest(manifest_path: str | Path) -> EvaluatorPlanBatchResult:
    """Build ready/rejected planner payloads for every local manifest entry."""

    resolved_manifest_path = Path(manifest_path)
    manifest = load_evaluator_plan_manifest(resolved_manifest_path)
    base_dir = resolved_manifest_path.parent
    results = [
        _run_manifest_entry(base_dir=base_dir, entry=entry)
        for entry in manifest.entries
    ]
    return EvaluatorPlanBatchResult(manifest_id=manifest.manifest_id, entries=results)


def summarize_evaluator_plan_batch(batch: EvaluatorPlanBatchResult) -> EvaluatorPlanBatchSummary:
    """Summarize ready/rejected status counts without including planner detail."""

    mismatches = [
        entry.entry_id
        for entry in batch.entries
        if not entry.matched_expected_status
    ]
    return EvaluatorPlanBatchSummary(
        manifest_id=batch.manifest_id,
        total_entries=len(batch.entries),
        ready_count=sum(
            1 for entry in batch.entries if entry.observed_status == "ready_for_local_evaluation"
        ),
        rejected_count=sum(1 for entry in batch.entries if entry.observed_status == "rejected"),
        expected_status_mismatch_count=len(mismatches),
        expected_status_mismatches=mismatches,
    )


def _run_manifest_entry(
    *,
    base_dir: Path,
    entry: EvaluatorPlanManifestEntry,
) -> EvaluatorPlanBatchEntryResult:
    fixture_path = base_dir / entry.fixture_path
    event_registry_dir = base_dir / entry.event_registry_dir

    try:
        plan = build_evaluator_plan(fixture_path, event_registry_dir=event_registry_dir)
    except (ValueError, ValidationError) as exc:
        rejected_plan = RejectedEvaluatorPlan(
            fixture_path=entry.fixture_path,
            event_registry_dir=entry.event_registry_dir,
            rejection_reasons=[str(exc)],
        )
        payload = rejected_plan.model_dump(mode="json")
    else:
        payload = plan.model_dump(mode="json")

    observed_status = payload["status"]
    if observed_status not in ("ready_for_local_evaluation", "rejected"):
        raise ValueError(f"Unsupported planner status: {observed_status}")

    return EvaluatorPlanBatchEntryResult(
        entry_id=entry.entry_id,
        expected_status=entry.expected_status,
        observed_status=observed_status,
        matched_expected_status=observed_status == entry.expected_status,
        fixture_path=entry.fixture_path,
        event_registry_dir=entry.event_registry_dir,
        planner_payload=payload,
    )
