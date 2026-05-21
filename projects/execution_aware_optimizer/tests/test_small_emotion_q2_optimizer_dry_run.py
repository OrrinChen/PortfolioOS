from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from execution_aware_optimizer.small_emotion_q2_optimizer_dry_run import (
    run_small_emotion_q2_optimizer_dry_run,
)


def test_optimizer_dry_run_connects_probe_panel_and_checks_constraint_response(tmp_path: Path) -> None:
    survival_dir = _write_survival_dir(
        tmp_path / "q2_survival",
        [
            _probe_row("rank2", "AAA", -0.15, 20.0, 2_000_000.0),
            _probe_row("rank2", "BBB", -0.15, 25.0, 3_000_000.0),
            _probe_row("rank2", "CCC", -0.15, 30.0, 4_000_000.0),
        ],
    )

    result = run_small_emotion_q2_optimizer_dry_run(
        q2_survival_dir=survival_dir,
        output_dir=tmp_path / "q2_optimizer_dry_run",
    )

    assert result.summary["schema_version"] == "small_emotion_q2_optimizer_dry_run_summary.v1"
    assert result.summary["stage"] == "Q2-SMALL-EMOTION-03"
    assert result.summary["candidate_count"] == 1
    assert result.summary["optimizer_observed_candidate_count"] == 1
    assert result.summary["actual_local_optimizer_run"] is True
    assert result.summary["orders_written"] is False
    assert result.summary["portfolio_construction_allowed"] is False
    assert result.summary["broker_order_path_opened"] is False
    assert result.summary["production_approval_claimed"] is False

    response = pd.read_csv(result.artifacts["optimizer_response_matrix"])
    assert set(response["panel_name"]) == {"live_panel", "sign_flipped_panel", "zero_alpha_panel"}
    assert set(response["optimizer_status"]) <= {"optimal", "optimal_inaccurate"}
    assert response["constraint_residual_max"].max() <= 0.1

    live = response.loc[response["panel_name"] == "live_panel"].iloc[0]
    flipped = response.loc[response["panel_name"] == "sign_flipped_panel"].iloc[0]
    zero = response.loc[response["panel_name"] == "zero_alpha_panel"].iloc[0]
    assert live["net_weight_change"] < zero["net_weight_change"]
    assert flipped["net_weight_change"] > zero["net_weight_change"]
    assert live["turnover"] <= live["max_turnover_limit"] + 1e-9
    assert flipped["turnover"] <= flipped["max_turnover_limit"] + 1e-9

    constraints = pd.read_csv(result.artifacts["constraint_response"])
    assert {"max_turnover", "participation_limit", "single_name_limit"}.issubset(set(constraints["constraint_name"]))
    assert set(constraints["status"]) == {"pass"}

    snapshot = pd.read_csv(result.artifacts["optimizer_input_snapshot"])
    assert {"ticker", "expected_return", "expected_return_source", "adv_shares", "estimated_price"}.issubset(
        snapshot.columns
    )
    assert snapshot["expected_return_source"].eq("small_emotion_q2_optimizer_probe").all()


def test_optimizer_dry_run_rejects_non_surviving_candidate(tmp_path: Path) -> None:
    survival_dir = _write_survival_dir(
        tmp_path / "q2_survival_rejected",
        [_probe_row("rank_bad", "BAD", -0.15, 20.0, 2_000_000.0)],
        survival_decision="cost_capacity_failed",
    )

    result = run_small_emotion_q2_optimizer_dry_run(
        q2_survival_dir=survival_dir,
        output_dir=tmp_path / "q2_optimizer_dry_run_rejected",
    )

    response = pd.read_csv(result.artifacts["optimizer_response_matrix"])
    assert response.loc[0, "optimizer_dry_run_status"] == "skipped_not_execution_survival_passed"
    assert result.summary["optimizer_observed_candidate_count"] == 0


def _write_survival_dir(path: Path, rows: list[dict[str, object]], *, survival_decision: str = "execution_survival_passed") -> Path:
    path.mkdir(parents=True)
    candidate = str(rows[0]["candidate_name"])
    pd.DataFrame(rows).to_csv(path / "small_emotion_q2_optimizer_input_probe.csv", index=False)
    pd.DataFrame(
        [
            {
                "schema_version": "small_emotion_q2_execution_survival_matrix.v1",
                "stage": "Q2-SMALL-EMOTION-02",
                "candidate_name": candidate,
                "measurement_spec_id": "spec_" + candidate,
                "measurement_spec_hash": "hash-" + candidate,
                "primary_window": "post_1_22",
                "active_expected_return_rows": len(rows),
                "no_view_rows_excluded": 0,
                "cost_capacity_status": "pass" if survival_decision == "execution_survival_passed" else "fail",
                "optimizer_input_probe_status": "staged_optimizer_input_ready",
                "survival_decision": survival_decision,
                "actual_optimizer_run": False,
                "portfolio_construction_allowed": False,
                "no_view_not_zero_alpha": True,
            }
        ]
    ).to_csv(path / "small_emotion_q2_execution_survival_matrix.csv", index=False)
    (path / "small_emotion_q2_survival_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "small_emotion_q2_execution_survival_summary.v1",
                "stage": "Q2-SMALL-EMOTION-02",
                "candidate_count": 1,
                "survival_passed_count": 1 if survival_decision == "execution_survival_passed" else 0,
                "optimizer_entry_allowed": False,
                "portfolio_construction_allowed": False,
            }
        ),
        encoding="utf-8",
    )
    return path


def _probe_row(candidate: str, ticker: str, expected_return: float, close: float, adv_dollars: float) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_q2_optimizer_input_probe.v1",
        "stage": "Q2-SMALL-EMOTION-02",
        "candidate_name": candidate,
        "measurement_spec_id": "spec_" + candidate,
        "measurement_spec_hash": "hash-" + candidate,
        "date": "2021-01-29",
        "ticker": ticker,
        "asset_id": "asset-" + ticker,
        "event_id": "event-" + ticker,
        "expected_return": expected_return,
        "expected_return_source": "small_emotion_q2_candidate_intake",
        "signal_value": -1.0,
        "close": close,
        "adv_dollars": adv_dollars,
        "adv_shares": adv_dollars / close,
        "bid_ask_spread": 0.02,
        "optimizer_input_probe_status": "staged_optimizer_input_ready",
        "actual_optimizer_run": False,
        "no_view_not_zero_alpha": True,
    }
