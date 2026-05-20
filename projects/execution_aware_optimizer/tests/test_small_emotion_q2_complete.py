from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from execution_aware_optimizer.small_emotion_q2_complete import (
    run_small_emotion_q2_complete,
)


def test_q2_complete_requires_survival_optimizer_response_and_closed_downstream_paths(tmp_path: Path) -> None:
    intake_dir, survival_dir, optimizer_dir = _write_q2_dirs(tmp_path, candidate_name="rank2")

    result = run_small_emotion_q2_complete(
        q2_intake_dir=intake_dir,
        q2_survival_dir=survival_dir,
        optimizer_dry_run_dir=optimizer_dir,
        output_dir=tmp_path / "q2_complete",
    )

    assert result.summary["schema_version"] == "small_emotion_q2_complete_summary.v1"
    assert result.summary["stage"] == "Q2-SMALL-EMOTION-04"
    assert result.summary["candidate_count"] == 1
    assert result.summary["q2_complete_passed_count"] == 1
    assert result.summary["q2_complete_failed_count"] == 0
    assert result.summary["portfolio_construction_allowed"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["paper_ready"] is False
    assert result.summary["live_ready"] is False
    assert result.summary["broker_order_path_opened"] is False
    assert result.summary["production_approval_claimed"] is False

    matrix = pd.read_csv(result.artifacts["complete_matrix"])
    row = matrix.iloc[0]
    assert row["q2_complete_decision"] == "completed_q2_execution_survival"
    assert row["optimizer_panel_count"] == 3
    assert row["constraint_fail_count"] == 0
    assert row["signal_sign_response_status"] == "pass"
    assert bool(row["orders_written"]) is False
    assert bool(row["portfolio_construction_allowed"]) is False

    manifest = json.loads(result.artifacts["manifest"].read_text(encoding="utf-8"))
    assert manifest["content_hash"]
    assert manifest["orders_written"] is False
    assert manifest["production_approval_claimed"] is False

    report = result.artifacts["report"].read_text(encoding="utf-8")
    assert "Q2 execution-survival closeout only" in report
    assert "production approval: not claimed" in report
    assert "broker/order/live paths: closed" in report
    assert "paper ready" not in report.lower()


def test_q2_complete_fails_when_optimizer_constraints_fail(tmp_path: Path) -> None:
    intake_dir, survival_dir, optimizer_dir = _write_q2_dirs(tmp_path, candidate_name="rank_bad")
    constraint_path = optimizer_dir / "small_emotion_q2_optimizer_constraint_response.csv"
    constraints = pd.read_csv(constraint_path)
    constraints.loc[0, "status"] = "fail"
    constraints.to_csv(constraint_path, index=False)

    result = run_small_emotion_q2_complete(
        q2_intake_dir=intake_dir,
        q2_survival_dir=survival_dir,
        optimizer_dry_run_dir=optimizer_dir,
        output_dir=tmp_path / "q2_complete_failed",
    )

    matrix = pd.read_csv(result.artifacts["complete_matrix"])
    assert matrix.loc[0, "q2_complete_decision"] == "failed_optimizer_constraints"
    assert result.summary["q2_complete_passed_count"] == 0
    assert result.summary["q2_complete_failed_count"] == 1


def _write_q2_dirs(tmp_path: Path, *, candidate_name: str) -> tuple[Path, Path, Path]:
    intake_dir = tmp_path / "q2_intake"
    survival_dir = tmp_path / "q2_survival"
    optimizer_dir = tmp_path / "q2_optimizer"
    intake_dir.mkdir()
    survival_dir.mkdir()
    optimizer_dir.mkdir()

    pd.DataFrame(
        [
            {
                "candidate_name": candidate_name,
                "candidate_intake_status": "opened_q2_candidate",
                "measurement_spec_id": "spec_" + candidate_name,
                "measurement_spec_hash": "hash-" + candidate_name,
                "q2_entry_allowed": True,
                "optimizer_entry_allowed": False,
                "portfolio_construction_allowed": False,
            }
        ]
    ).to_csv(intake_dir / "small_emotion_q2_candidate_matrix.csv", index=False)
    pd.DataFrame(
        [
            {
                "candidate_name": candidate_name,
                "signal_state": "active",
                "q2_status": "opened_q2_candidate",
                "expected_return": -0.12,
            }
        ]
    ).to_csv(intake_dir / "small_emotion_q2_expected_return_panel.csv", index=False)
    (intake_dir / "small_emotion_q2_candidate_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "small_emotion_q2_candidate_summary.v1",
                "candidate_count": 1,
                "opened_q2_candidate_count": 1,
                "optimizer_entry_allowed": False,
                "portfolio_construction_allowed": False,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    pd.DataFrame(
        [
            {
                "candidate_name": candidate_name,
                "measurement_spec_id": "spec_" + candidate_name,
                "measurement_spec_hash": "hash-" + candidate_name,
                "primary_window": "post_1_22",
                "active_expected_return_rows": 10,
                "no_view_rows_excluded": 90,
                "cost_capacity_status": "pass",
                "optimizer_input_probe_status": "staged_optimizer_input_ready",
                "survival_decision": "execution_survival_passed",
                "actual_optimizer_run": False,
                "portfolio_construction_allowed": False,
                "no_view_not_zero_alpha": True,
            }
        ]
    ).to_csv(survival_dir / "small_emotion_q2_execution_survival_matrix.csv", index=False)
    (survival_dir / "small_emotion_q2_survival_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "small_emotion_q2_execution_survival_summary.v1",
                "candidate_count": 1,
                "survival_passed_count": 1,
                "optimizer_entry_allowed": False,
                "portfolio_construction_allowed": False,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    pd.DataFrame(
        [
            _optimizer_row(candidate_name, "live_panel", net_weight_change=-0.22),
            _optimizer_row(candidate_name, "sign_flipped_panel", net_weight_change=0.21),
            _optimizer_row(candidate_name, "zero_alpha_panel", net_weight_change=0.02),
        ]
    ).to_csv(optimizer_dir / "small_emotion_q2_optimizer_response_matrix.csv", index=False)
    pd.DataFrame(
        [
            _constraint_row(candidate_name, panel, constraint)
            for panel in ["live_panel", "sign_flipped_panel", "zero_alpha_panel"]
            for constraint in ["max_turnover", "participation_limit", "single_name_limit"]
        ]
    ).to_csv(optimizer_dir / "small_emotion_q2_optimizer_constraint_response.csv", index=False)
    (optimizer_dir / "small_emotion_q2_optimizer_dry_run_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "small_emotion_q2_optimizer_dry_run_summary.v1",
                "candidate_count": 1,
                "optimizer_observed_candidate_count": 1,
                "actual_local_optimizer_run": True,
                "orders_written": False,
                "portfolio_construction_allowed": False,
                "alpha_registry_update_allowed": False,
                "broker_order_path_opened": False,
                "production_approval_claimed": False,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return intake_dir, survival_dir, optimizer_dir


def _optimizer_row(candidate_name: str, panel_name: str, *, net_weight_change: float) -> dict[str, object]:
    return {
        "candidate_name": candidate_name,
        "panel_name": panel_name,
        "optimizer_dry_run_status": "observed",
        "optimizer_status": "optimal",
        "turnover": 0.23,
        "max_turnover_limit": 0.35,
        "max_participation": 0.001,
        "participation_limit": 0.05,
        "max_post_trade_weight": 0.03,
        "single_name_limit": 0.08,
        "constraint_residual_max": 0.0,
        "net_weight_change": net_weight_change,
        "actual_local_optimizer_run": True,
        "orders_written": False,
        "portfolio_construction_allowed": False,
        "no_view_not_zero_alpha": True,
    }


def _constraint_row(candidate_name: str, panel_name: str, constraint_name: str) -> dict[str, object]:
    return {
        "candidate_name": candidate_name,
        "panel_name": panel_name,
        "constraint_name": constraint_name,
        "observed_value": 0.1,
        "limit": 0.35,
        "status": "pass",
        "actual_local_optimizer_run": True,
        "no_view_not_zero_alpha": True,
    }
