"""Local typed-alpha pilot builders."""

from __future__ import annotations

import csv
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Sequence

from evidence_bundle import load_evidence_bundle
from execution_aware_optimizer.experiment_config import ExperimentConfig
from execution_aware_optimizer.typed_execution_matrix import (
    render_typed_execution_matrix_report,
    run_typed_alpha_execution_matrix,
    summarize_typed_execution_matrix,
)
from portfolio_os.alpha.event_evaluation import EventWindowLabel, build_event_evidence_bundle, dump_event_evidence_bundle_json
from portfolio_os.alpha.projection import AlphaProjectionConfig, project_alpha_views_to_expected_returns
from portfolio_os.alpha.view_contract import dump_alpha_view_json, load_alpha_view
from promotion_gate.gate import evaluate_typed_promotion_candidate


@dataclass(frozen=True)
class SueTypedAlphaPilotResult:
    """Paths written by the local SUE typed-alpha pilot."""

    artifacts: dict[str, Path]


def run_sue_typed_alpha_pilot(
    *,
    output_dir: str | Path,
    alpha_view_path: str | Path,
    evidence_bundle_path: str | Path,
) -> SueTypedAlphaPilotResult:
    """Build deterministic local SUE typed-alpha pilot artifacts."""

    resolved_output_dir = Path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    alpha_view = load_alpha_view(alpha_view_path)
    evidence_bundle = load_evidence_bundle(evidence_bundle_path)

    event_bundle = build_event_evidence_bundle(
        alpha_view=alpha_view,
        labels=[
            EventWindowLabel(name="sue_plus_2", start_offset_days=2, end_offset_days=2),
            EventWindowLabel(name="sue_plus_2_to_3", start_offset_days=2, end_offset_days=3),
            EventWindowLabel(name="sue_plus_2_to_22", start_offset_days=2, end_offset_days=22),
        ],
        placebo_tests_required=["event_date_shift", "cross_section_shuffle"],
        overlap_reference_family_ids=["US_ANALYST_REVISION"],
    )
    projection = project_alpha_views_to_expected_returns(
        alpha_views=[alpha_view],
        config=AlphaProjectionConfig(
            rebalance_dates=["2025-02-10"],
            universe_symbols=["AAPL", "MSFT", "TSLA"],
            risk_horizon_days=21,
            cost_assumptions={"cost_bps": 5.0},
        ),
    )
    promotion_decision = evaluate_typed_promotion_candidate(
        bundle=evidence_bundle,
        alpha_view=alpha_view,
        projection_manifest=projection.alpha_projection_manifest,
        projection_diagnostics=projection.alpha_projection_diagnostics,
        alpha_abstain_report=projection.alpha_abstain_report,
        event_overlap_diagnostics=event_bundle.event_overlap_diagnostics,
    )
    q2_rows = run_typed_alpha_execution_matrix(
        config=ExperimentConfig.model_validate(
            {
                "cost_sensitivity_bps": [5],
                "execution_matrix": {
                    "participation_rates": [0.001],
                    "liquidity_buckets": ["high"],
                    "constraint_levels": ["full_execution_aware"],
                    "execution_modes": ["impact_aware"],
                },
            }
        ),
        q2_input_contract_v2=promotion_decision.q2_allowed_inputs.model_dump(mode="json")
        if promotion_decision.q2_allowed_inputs is not None
        else _unavailable_q2_contract(evidence_bundle.bundle_id, alpha_view.alpha_view_id),
        projection_manifest=projection.alpha_projection_manifest,
        expected_return_panel=projection.expected_return_panel,
        projection_diagnostics=projection.alpha_projection_diagnostics,
        alpha_abstain_report=projection.alpha_abstain_report,
        projection_policies=["event_window_decay"],
        abstain_policies=["explicit_abstain"],
        alpha_families=["SUE"],
    )
    q2_summary = summarize_typed_execution_matrix(q2_rows)
    q2_report = render_typed_execution_matrix_report(q2_rows, summary=q2_summary)

    artifacts = {
        "us_sue_event_alpha_view.json": resolved_output_dir / "us_sue_event_alpha_view.json",
        "us_sue_event_evidence_bundle.json": resolved_output_dir / "us_sue_event_evidence_bundle.json",
        "us_sue_projection_panel.csv": resolved_output_dir / "us_sue_projection_panel.csv",
        "us_sue_projection_manifest.json": resolved_output_dir / "us_sue_projection_manifest.json",
        "us_sue_projection_diagnostics.json": resolved_output_dir / "us_sue_projection_diagnostics.json",
        "us_sue_abstain_report.json": resolved_output_dir / "us_sue_abstain_report.json",
        "us_sue_promotion_decision_v2.json": resolved_output_dir / "us_sue_promotion_decision_v2.json",
        "us_sue_q2_matrix.csv": resolved_output_dir / "us_sue_q2_matrix.csv",
        "us_sue_audit_report.md": resolved_output_dir / "us_sue_audit_report.md",
    }
    artifacts["us_sue_event_alpha_view.json"].write_text(dump_alpha_view_json(alpha_view), encoding="utf-8")
    artifacts["us_sue_event_evidence_bundle.json"].write_text(
        dump_event_evidence_bundle_json(event_bundle),
        encoding="utf-8",
    )
    _write_csv(
        artifacts["us_sue_projection_panel.csv"],
        projection.expected_return_panel,
        [
            "date",
            "symbol",
            "expected_return",
            "active_alpha_views",
            "horizon_conversion",
            "decay_applied",
            "confidence_weight",
        ],
    )
    artifacts["us_sue_projection_manifest.json"].write_text(
        _dump_json(projection.alpha_projection_manifest),
        encoding="utf-8",
    )
    artifacts["us_sue_projection_diagnostics.json"].write_text(
        _dump_json({"diagnostics": projection.alpha_projection_diagnostics}),
        encoding="utf-8",
    )
    artifacts["us_sue_abstain_report.json"].write_text(
        _dump_json({"abstain_report": projection.alpha_abstain_report}),
        encoding="utf-8",
    )
    artifacts["us_sue_promotion_decision_v2.json"].write_text(
        _dump_json(promotion_decision.model_dump(mode="json")),
        encoding="utf-8",
    )
    _write_csv(
        artifacts["us_sue_q2_matrix.csv"],
        [row.model_dump(mode="json") for row in q2_rows],
        [
            "scenario_id",
            "source_config_hash",
            "cost_bps",
            "participation_rate",
            "liquidity_bucket",
            "constraint_level",
            "execution_mode",
            "projection_policy",
            "abstain_policy",
            "alpha_family",
            "status",
            "unavailable_reason",
            "active_rebalance_count",
            "active_name_count",
            "gross_to_net_retention",
            "turnover",
            "expected_return_used_share",
            "cost_drag",
            "constraint_repair_retention",
            "abstain_count",
            "sign_consistency",
            "view_overlap",
        ],
    )
    artifacts["us_sue_audit_report.md"].write_text(
        _render_sue_audit_report(
            promotion_decision=promotion_decision.model_dump(mode="json"),
            event_bundle=event_bundle.model_dump(mode="json"),
            projection_manifest=projection.alpha_projection_manifest,
            q2_report=q2_report,
            q2_status=q2_rows[0].status if q2_rows else "unavailable",
        ),
        encoding="utf-8",
    )
    return SueTypedAlphaPilotResult(artifacts=artifacts)


def _render_sue_audit_report(
    *,
    promotion_decision: dict[str, Any],
    event_bundle: dict[str, Any],
    projection_manifest: dict[str, Any],
    q2_report: str,
    q2_status: str,
) -> str:
    return "\n".join(
        [
            "# SUE Typed Alpha Pilot",
            "",
            "This is an integration benchmark, not production approval.",
            "",
            "## Discovery",
            "",
            f"- alpha_view_id: `{event_bundle['alpha_view_id']}`",
            "- mechanism: `SUE / PEAD event window`",
            "- No live trading instruction is generated.",
            "",
            "## Event Evidence",
            "",
            f"- event labels: `{len(event_bundle['event_window_grid'])}`",
            "- placebo tests are planned only; no realized alpha performance is fabricated.",
            "",
            "## Projection",
            "",
            f"- projection_schema: `{projection_manifest['schema_version']}`",
            f"- projection_panel_rows: `{projection_manifest['panel_row_count']}`",
            f"- abstain_rows: `{projection_manifest['abstain_row_count']}`",
            "",
            "## Promotion",
            "",
            f"- Promotion Gate v2 decision: `{promotion_decision['decision']}`",
            "",
            "## Q2 Typed Matrix",
            "",
            f"- Q2 typed matrix status: `{q2_status}`",
            "",
            q2_report,
        ]
    )


def _unavailable_q2_contract(bundle_id: str, alpha_view_id: str) -> dict[str, Any]:
    return {
        "alpha_abstain_report_artifact": "us_sue_abstain_report.json",
        "alpha_projection_diagnostics_artifact": "us_sue_projection_diagnostics.json",
        "alpha_view_id": alpha_view_id,
        "allowed_consumer": "projects/execution_aware_optimizer",
        "bundle_id": bundle_id,
        "direct_q2_execution_allowed": False,
        "expected_return_panel_artifact": "us_sue_projection_panel.csv",
        "input_type": "projected_expected_return_panel",
        "projection_manifest_hash": "",
    }


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], columns: Sequence[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns))
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _dump_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"
