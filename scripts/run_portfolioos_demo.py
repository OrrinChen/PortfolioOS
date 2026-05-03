#!/usr/bin/env python3
"""Run the deterministic local PortfolioOS demo artifact pipeline."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
for path in (
    REPO_ROOT / "src",
    REPO_ROOT / "projects" / "audit_report" / "src",
    REPO_ROOT / "projects" / "agentic_alpha_triage" / "src",
    REPO_ROOT / "projects" / "evidence_bundle" / "src",
    REPO_ROOT / "projects" / "promotion_gate" / "src",
    REPO_ROOT / "projects" / "execution_aware_optimizer" / "src",
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from agentic_alpha_triage.evaluator_plan_batch import (  # noqa: E402
    run_evaluator_plan_manifest,
    summarize_evaluator_plan_batch,
)
from audit_report import build_demo_audit_report, load_demo_audit_manifest  # noqa: E402
from evidence_bundle import load_evidence_bundle  # noqa: E402
from execution_aware_optimizer.execution_matrix import (  # noqa: E402
    execution_matrix_rows_to_frame,
    run_execution_matrix,
)
from execution_aware_optimizer.experiment_config import ExperimentConfig  # noqa: E402
from portfolio_os.dashboard import render_static_dashboard  # noqa: E402
from portfolio_os.observability import TraceWriter  # noqa: E402
from portfolio_os.provenance import build_provenance_manifest, write_provenance_manifest  # noqa: E402
from promotion_gate import evaluate_promotion_candidate  # noqa: E402


Q1_MANIFEST = REPO_ROOT / "projects" / "agentic_alpha_triage" / "examples" / "evaluator_plan_manifest.yaml"
DEMO_MANIFEST = REPO_ROOT / "projects" / "audit_report" / "examples" / "demo_audit_manifest.yaml"
VALID_BUNDLE = REPO_ROOT / "projects" / "evidence_bundle" / "examples" / "valid_bundle.yaml"
REJECTED_BUNDLE = (
    REPO_ROOT / "projects" / "evidence_bundle" / "examples" / "rejected_bundle_forward_leakage.yaml"
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local PortfolioOS demo.")
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "outputs" / "demo"))
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    trace = TraceWriter(output_dir / "trace.jsonl")
    trace.write("demo_started", payload={"output_dir": _repo_relative(output_dir)})

    q1_batch = run_evaluator_plan_manifest(Q1_MANIFEST)
    q1_summary = summarize_evaluator_plan_batch(q1_batch)
    _write_json(output_dir / "q1_summary.json", q1_summary.model_dump(mode="json"))
    trace.write("q1_summary_written", payload={"ready_count": q1_summary.ready_count})

    evidence_bundle = load_evidence_bundle(VALID_BUNDLE)
    _write_json(output_dir / "evidence_bundle.json", evidence_bundle.model_dump(mode="json"))

    promoted_decision = evaluate_promotion_candidate(VALID_BUNDLE)
    rejected_decision = evaluate_promotion_candidate(REJECTED_BUNDLE)
    _write_json(
        output_dir / "promotion_decision.json",
        {
            "promoted_like_case": promoted_decision.model_dump(mode="json"),
            "rejected_leakage_case": rejected_decision.model_dump(mode="json"),
        },
    )
    trace.write(
        "promotion_decision_created",
        payload={"case": "promoted_like_case", "decision": promoted_decision.decision},
    )
    trace.write(
        "promotion_decision_created",
        payload={"case": "rejected_leakage_case", "decision": rejected_decision.decision},
    )

    demo_manifest = load_demo_audit_manifest(DEMO_MANIFEST)
    q2_config = ExperimentConfig.model_validate(demo_manifest.q2_execution)
    q2_rows = run_execution_matrix(q2_config)
    q2_frame = execution_matrix_rows_to_frame(q2_rows)
    q2_frame.to_csv(output_dir / "q2_execution_matrix.csv", index=False)
    _write_cost_sensitivity_placeholder(output_dir / "cost_sensitivity.csv", q2_rows)
    for row in q2_rows:
        if row.status == "unavailable":
            trace.write(
                "q2_scenario_unavailable",
                payload={"scenario_id": row.scenario_id, "layer_name": row.layer_name},
            )

    audit_report = build_demo_audit_report(REPO_ROOT, demo_manifest)
    (output_dir / "audit_report.md").write_text(audit_report.markdown, encoding="utf-8")

    provenance = build_provenance_manifest(
        repo_root=REPO_ROOT,
        run_id="portfolioos_demo",
        command=["scripts/run_portfolioos_demo.py", "--output-dir", _repo_relative(output_dir)],
        config_path=DEMO_MANIFEST,
        input_paths={
            "q1_manifest": Q1_MANIFEST,
            "demo_manifest": DEMO_MANIFEST,
            "valid_evidence_bundle": VALID_BUNDLE,
            "rejected_evidence_bundle": REJECTED_BUNDLE,
        },
        output_paths={
            "q1_summary": output_dir / "q1_summary.json",
            "evidence_bundle": output_dir / "evidence_bundle.json",
            "promotion_decision": output_dir / "promotion_decision.json",
            "q2_execution_matrix": output_dir / "q2_execution_matrix.csv",
            "audit_report": output_dir / "audit_report.md",
            "trace": output_dir / "trace.jsonl",
        },
        runner_version="portfolioos-one-command-demo-v1",
    )
    write_provenance_manifest(output_dir / "run_manifest.json", provenance)
    trace.write("run_manifest_written", payload={"path": _repo_relative(output_dir / "run_manifest.json")})

    render_static_dashboard(artifact_root=output_dir, output_path=output_dir / "dashboard.html")
    trace.write("dashboard_written", payload={"path": _repo_relative(output_dir / "dashboard.html")})
    trace.write("report_written", payload={"path": _repo_relative(output_dir / "audit_report.md")})

    print(f"portfolioos_demo: {output_dir}")


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_cost_sensitivity_placeholder(path: Path, rows: list[object]) -> None:
    cost_values = sorted({getattr(row, "cost_bps") for row in rows})
    lines = ["cost_bps,status", *[f"{cost_bps},unavailable" for cost_bps in cost_values]]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _repo_relative(path: str | Path) -> str:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = REPO_ROOT / resolved
    try:
        return resolved.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


if __name__ == "__main__":
    main()
