from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from execution_aware_optimizer.small_emotion_q2_portfolio_replay import (
    run_small_emotion_q2_portfolio_replay,
)


def test_q2_portfolio_replay_builds_nav_cost_and_policy_artifacts_without_downstream_paths(tmp_path: Path) -> None:
    q2_complete_dir, q2_intake_dir, q1_panels = _write_inputs(tmp_path)

    result = run_small_emotion_q2_portfolio_replay(
        q2_complete_dir=q2_complete_dir,
        q2_intake_dir=q2_intake_dir,
        q1_window_panels=q1_panels,
        output_dir=tmp_path / "q2_portfolio_replay",
        minimum_event_count=3,
        minimum_event_month_count=2,
    )

    assert result.summary["schema_version"] == "small_emotion_q2_portfolio_replay_summary.v1"
    assert result.summary["stage"] == "Q2-SMALL-EMOTION-05"
    assert result.summary["candidate_count"] == 1
    assert result.summary["portfolio_replay_completed_count"] == 1
    assert result.summary["orders_written"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["broker_order_path_opened"] is False
    assert result.summary["production_approval_claimed"] is False

    monthly = pd.read_csv(result.artifacts["monthly_returns"])
    assert {"candidate_name", "event_month", "gross_event_return", "net_event_return", "event_count"} <= set(
        monthly.columns
    )
    assert monthly["event_count"].sum() == 3

    nav = pd.read_csv(result.artifacts["nav_curve"])
    assert {"candidate_name", "event_month", "gross_nav", "net_nav", "drawdown"}.issubset(nav.columns)
    assert nav["net_nav"].notna().all()

    costs = pd.read_csv(result.artifacts["cost_attribution"])
    assert {"candidate_name", "total_cost_return", "avg_cost_return", "max_participation"}.issubset(costs.columns)
    assert costs.loc[0, "total_cost_return"] > 0.0

    policy = pd.read_csv(result.artifacts["policy_gate"])
    assert {"candidate_name", "policy_name", "status"}.issubset(policy.columns)
    assert set(policy["status"]) <= {"pass", "fail", "unavailable"}

    matrix = pd.read_csv(result.artifacts["replay_matrix"])
    assert matrix.loc[0, "portfolio_replay_decision"] == "portfolio_replay_completed"
    assert bool(matrix.loc[0, "portfolio_quant_replay_run"]) is True
    assert bool(matrix.loc[0, "orders_written"]) is False

    report = result.artifacts["report"].read_text(encoding="utf-8")
    assert "Q2 portfolio quant replay only" in report
    assert "production approval: not claimed" in report
    assert "paper-ready" not in report.lower()


def test_q2_portfolio_replay_blocks_candidates_not_completed_in_q2(tmp_path: Path) -> None:
    q2_complete_dir, q2_intake_dir, q1_panels = _write_inputs(tmp_path, q2_decision="failed_optimizer_constraints")

    result = run_small_emotion_q2_portfolio_replay(
        q2_complete_dir=q2_complete_dir,
        q2_intake_dir=q2_intake_dir,
        q1_window_panels=q1_panels,
        output_dir=tmp_path / "q2_portfolio_replay_blocked",
        minimum_event_count=3,
        minimum_event_month_count=2,
    )

    matrix = pd.read_csv(result.artifacts["replay_matrix"])
    assert matrix.loc[0, "portfolio_replay_decision"] == "blocked_q2_incomplete"
    assert result.summary["portfolio_replay_completed_count"] == 0


def _write_inputs(
    tmp_path: Path,
    *,
    q2_decision: str = "completed_q2_execution_survival",
) -> tuple[Path, Path, dict[str, Path]]:
    q2_complete_dir = tmp_path / "q2_complete"
    q2_intake_dir = tmp_path / "q2_intake"
    q1_dir = tmp_path / "q1_rank2"
    q2_complete_dir.mkdir()
    q2_intake_dir.mkdir()
    q1_dir.mkdir()
    candidate_name = "rank2"

    pd.DataFrame(
        [
            {
                "candidate_name": candidate_name,
                "measurement_spec_id": "small_emotion_rank2_v0",
                "measurement_spec_hash": "hash-rank2",
                "q2_complete_decision": q2_decision,
                "orders_written": False,
                "portfolio_construction_allowed": False,
                "alpha_registry_update_allowed": False,
                "broker_order_path_opened": False,
                "production_approval_claimed": False,
                "no_view_not_zero_alpha": True,
            }
        ]
    ).to_csv(q2_complete_dir / "small_emotion_q2_complete_matrix.csv", index=False)
    (q2_complete_dir / "small_emotion_q2_complete_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "small_emotion_q2_complete_summary.v1",
                "candidate_count": 1,
                "q2_complete_passed_count": 1 if q2_decision == "completed_q2_execution_survival" else 0,
                "orders_written": False,
                "production_approval_claimed": False,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    pd.DataFrame(
        [
            _expected_row(candidate_name, "e1", "AAA", "2021-01-05", -0.10, 0.01, 2_000_000.0),
            _expected_row(candidate_name, "e2", "BBB", "2021-01-06", -0.10, 0.02, 2_500_000.0),
            _expected_row(candidate_name, "e3", "CCC", "2021-02-03", -0.10, 0.03, 3_000_000.0),
        ]
    ).to_csv(q2_intake_dir / "small_emotion_q2_expected_return_panel.csv", index=False)
    pd.DataFrame(
        [
            {
                "candidate_name": candidate_name,
                "measurement_spec_id": "small_emotion_rank2_v0",
                "measurement_spec_hash": "hash-rank2",
                "primary_window": "post_1_22",
                "q2_status": "opened_q2_candidate",
            }
        ]
    ).to_csv(q2_intake_dir / "small_emotion_q2_candidate_matrix.csv", index=False)

    pd.DataFrame(
        [
            _q1_row("e1", "AAA", "2021-01-05", "post_1_22", 0.12),
            _q1_row("e2", "BBB", "2021-01-06", "post_1_22", 0.08),
            _q1_row("e3", "CCC", "2021-02-03", "post_1_22", 0.10),
        ]
    ).to_csv(q1_dir / "q1_window_return_panel.csv", index=False)
    return q2_complete_dir, q2_intake_dir, {candidate_name: q1_dir / "q1_window_return_panel.csv"}


def _expected_row(
    candidate_name: str,
    event_id: str,
    symbol: str,
    date: str,
    expected_return: float,
    spread: float,
    dollar_volume: float,
) -> dict[str, object]:
    return {
        "candidate_name": candidate_name,
        "event_id": event_id,
        "symbol": symbol,
        "date": date,
        "primary_window": "post_1_22",
        "signal_state": "active",
        "expected_return": expected_return,
        "bid_ask_spread": spread,
        "dollar_volume": dollar_volume,
        "adv20": dollar_volume,
        "adjusted_close": 10.0,
        "volume": dollar_volume / 10.0,
        "no_view_not_zero_alpha": True,
    }


def _q1_row(event_id: str, ticker: str, date: str, window: str, directional_return: float) -> dict[str, object]:
    return {
        "event_id": event_id,
        "ticker": ticker,
        "date": date,
        "event_month": date[:7],
        "window": window,
        "label_status": "observed",
        "directional_return": directional_return,
        "no_view_not_zero_alpha": True,
    }
