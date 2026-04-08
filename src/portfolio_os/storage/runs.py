"""Run-id and output path helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from portfolio_os.domain.models import (
    ApprovalArtifacts,
    ExecutionArtifacts,
    PaperCalibrationArtifacts,
    ReplayArtifacts,
    RunArtifacts,
    ScenarioArtifacts,
)


def prepare_run_artifacts(output_dir: str | Path) -> RunArtifacts:
    """Create the output directory and return file paths for this run."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    created_at = datetime.now(timezone.utc).isoformat()
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid4().hex[:8]}"
    return RunArtifacts(
        run_id=run_id,
        output_dir=str(output_path),
        orders_path=str(output_path / "orders.csv"),
        orders_oms_path=str(output_path / "orders_oms.csv"),
        audit_path=str(output_path / "audit.json"),
        summary_path=str(output_path / "summary.md"),
        benchmark_json_path=str(output_path / "benchmark_comparison.json"),
        benchmark_markdown_path=str(output_path / "benchmark_comparison.md"),
        handoff_checklist_path=str(output_path / "handoff_checklist.md"),
        manifest_path=str(output_path / "run_manifest.json"),
        created_at=created_at,
    )


def prepare_replay_artifacts(output_dir: str | Path) -> ReplayArtifacts:
    """Create the replay output directory tree and return suite artifact paths."""

    output_path = Path(output_dir)
    sample_results_dir = output_path / "sample_results"
    output_path.mkdir(parents=True, exist_ok=True)
    sample_results_dir.mkdir(parents=True, exist_ok=True)
    created_at = datetime.now(timezone.utc).isoformat()
    run_id = f"replay_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid4().hex[:8]}"
    return ReplayArtifacts(
        run_id=run_id,
        output_dir=str(output_path),
        sample_results_dir=str(sample_results_dir),
        suite_results_path=str(output_path / "suite_results.json"),
        suite_summary_path=str(output_path / "suite_summary.md"),
        manifest_path=str(output_path / "run_manifest.json"),
        created_at=created_at,
    )


def prepare_scenario_artifacts(output_dir: str | Path) -> ScenarioArtifacts:
    """Create the scenario output directory tree and return suite artifact paths."""

    output_path = Path(output_dir)
    scenario_results_dir = output_path / "scenario_results"
    output_path.mkdir(parents=True, exist_ok=True)
    scenario_results_dir.mkdir(parents=True, exist_ok=True)
    created_at = datetime.now(timezone.utc).isoformat()
    run_id = f"scenario_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid4().hex[:8]}"
    return ScenarioArtifacts(
        run_id=run_id,
        output_dir=str(output_path),
        scenario_results_dir=str(scenario_results_dir),
        scenario_comparison_json_path=str(output_path / "scenario_comparison.json"),
        scenario_comparison_markdown_path=str(output_path / "scenario_comparison.md"),
        decision_pack_path=str(output_path / "decision_pack.md"),
        manifest_path=str(output_path / "run_manifest.json"),
        created_at=created_at,
    )


def prepare_approval_artifacts(output_dir: str | Path) -> ApprovalArtifacts:
    """Create the approval output directory and return artifact paths."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    created_at = datetime.now(timezone.utc).isoformat()
    run_id = f"approval_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid4().hex[:8]}"
    return ApprovalArtifacts(
        run_id=run_id,
        output_dir=str(output_path),
        approval_record_path=str(output_path / "approval_record.json"),
        approval_summary_path=str(output_path / "approval_summary.md"),
        freeze_manifest_path=str(output_path / "freeze_manifest.json"),
        final_orders_path=str(output_path / "final_orders.csv"),
        final_orders_oms_path=str(output_path / "final_orders_oms.csv"),
        final_audit_path=str(output_path / "final_audit.json"),
        final_summary_path=str(output_path / "final_summary.md"),
        handoff_checklist_path=str(output_path / "handoff_checklist.md"),
        manifest_path=str(output_path / "run_manifest.json"),
        created_at=created_at,
    )


def prepare_execution_artifacts(output_dir: str | Path) -> ExecutionArtifacts:
    """Create the execution-simulation output directory and return artifact paths."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    created_at = datetime.now(timezone.utc).isoformat()
    run_id = f"execution_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid4().hex[:8]}"
    return ExecutionArtifacts(
        run_id=run_id,
        output_dir=str(output_path),
        execution_report_json_path=str(output_path / "execution_report.json"),
        execution_report_markdown_path=str(output_path / "execution_report.md"),
        execution_fills_path=str(output_path / "execution_fills.csv"),
        execution_child_orders_path=str(output_path / "execution_child_orders.csv"),
        handoff_checklist_path=str(output_path / "handoff_checklist.md"),
        manifest_path=str(output_path / "run_manifest.json"),
        created_at=created_at,
    )


def prepare_paper_calibration_artifacts(output_dir: str | Path) -> PaperCalibrationArtifacts:
    """Create the paper-calibration output directory and return artifact paths."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    created_at = datetime.now(timezone.utc).isoformat()
    run_id = f"paper_calibration_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid4().hex[:8]}"
    return PaperCalibrationArtifacts(
        run_id=run_id,
        output_dir=str(output_path),
        target_path=str(output_path / "target.csv"),
        manifest_path=str(output_path / "paper_calibration_manifest.json"),
        payload_path=str(output_path / "paper_calibration_payload.json"),
        report_path=str(output_path / "paper_calibration_report.md"),
        created_at=created_at,
    )
