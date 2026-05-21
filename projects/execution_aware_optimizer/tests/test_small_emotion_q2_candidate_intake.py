from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from execution_aware_optimizer.small_emotion_q2_candidate_intake import (
    SmallEmotionQ2CandidateInput,
    run_small_emotion_q2_candidate_intake,
)


def test_small_emotion_q2_intake_opens_promoted_candidates_without_optimizer(tmp_path: Path) -> None:
    candidate = _write_candidate(tmp_path, "rank2", promotion_decision="promote_to_q2_candidate")

    result = run_small_emotion_q2_candidate_intake(
        candidates=[candidate],
        output_dir=tmp_path / "q2",
    )

    assert result.summary["schema_version"] == "small_emotion_q2_candidate_intake_summary.v1"
    assert result.summary["stage"] == "Q2-SMALL-EMOTION-01"
    assert result.summary["candidate_count"] == 1
    assert result.summary["opened_q2_candidate_count"] == 1
    assert result.summary["optimizer_entry_allowed"] is False
    assert result.summary["portfolio_construction_allowed"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["paper_ready"] is False
    assert result.summary["live_ready"] is False
    assert result.summary["broker_order_path_opened"] is False
    assert result.summary["production_approval_claimed"] is False

    panel = pd.read_csv(result.artifacts["expected_return_panel"])
    assert set(panel["q2_status"]) == {"opened_q2_candidate"}
    assert set(panel["signal_state"]) == {"active"}
    assert panel["expected_return"].notna().all()
    assert (panel["expected_return"] < 0.0).all()
    assert {"adv20", "bid_ask_spread", "adjusted_close", "volume", "industry"}.issubset(panel.columns)
    assert "no_view" not in set(panel["signal_state"].astype(str).str.lower())

    matrix = pd.read_csv(result.artifacts["candidate_matrix"])
    assert matrix.loc[0, "q2_status"] == "opened_q2_candidate"
    assert bool(matrix.loc[0, "expected_return_capped"]) is True

    report = result.artifacts["report"].read_text(encoding="utf-8").lower()
    assert "q2 candidate intake only" in report
    for forbidden in ["paper ready", "live trading", "broker execution", "order generation", "production approved"]:
        assert forbidden not in report


def test_small_emotion_q2_intake_rejects_unpromoted_candidates(tmp_path: Path) -> None:
    candidate = _write_candidate(tmp_path, "rank1", promotion_decision="promising_needs_full_replay_or_breadth")

    result = run_small_emotion_q2_candidate_intake(
        candidates=[candidate],
        output_dir=tmp_path / "q2_rejected",
    )

    assert result.summary["candidate_count"] == 1
    assert result.summary["opened_q2_candidate_count"] == 0
    assert result.summary["blocked_candidate_count"] == 1
    panel = pd.read_csv(result.artifacts["expected_return_panel"])
    assert panel.empty
    matrix = pd.read_csv(result.artifacts["candidate_matrix"])
    assert matrix.loc[0, "q2_status"] == "blocked_before_q2"
    assert "promotion_decision_not_promoted" in matrix.loc[0, "block_reason"]


def test_small_emotion_q2_intake_rejects_measurement_spec_hash_mismatch(tmp_path: Path) -> None:
    candidate = _write_candidate(tmp_path, "rank3", promotion_decision="promote_to_q2_candidate")
    candidate = SmallEmotionQ2CandidateInput(
        candidate_name=candidate.candidate_name,
        measurement_spec_path=candidate.measurement_spec_path,
        q1_output_dir=candidate.q1_output_dir,
        promotion_gate_dir=candidate.promotion_gate_dir,
        required_measurement_spec_hash="not-the-real-hash",
    )

    with pytest.raises(ValueError, match="MeasurementSpec hash mismatch"):
        run_small_emotion_q2_candidate_intake(
            candidates=[candidate],
            output_dir=tmp_path / "q2_hash_mismatch",
        )


def _write_candidate(tmp_path: Path, name: str, *, promotion_decision: str) -> SmallEmotionQ2CandidateInput:
    base = tmp_path / name
    spec_path = base / "measurement_spec.yaml"
    q1_dir = base / "q1"
    pg_dir = base / "pg"
    q1_dir.mkdir(parents=True)
    pg_dir.mkdir(parents=True)
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_path.write_text(
        "\n".join(
            [
                "schema_version: small_emotion_measurement_spec.v1",
                f"measurement_spec_id: small_emotion_{name}_v0",
                "candidate_id: small_emotion_rank_v0",
                "signal_definition:",
                "  mechanism: up_shock_reversal",
                "  signal_value:",
                "    active_signal: -1.0",
                "label_contract:",
                "  primary_window: post_1_22",
                "",
            ]
        ),
        encoding="utf-8",
    )
    spec_hash = hashlib.sha256(spec_path.read_bytes()).hexdigest()
    (q1_dir / "q1_decision_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "small_emotion_q1_oos_summary.v1",
                "stage": "Q1-SMALL-EMOTION-01",
                "measurement_spec_id": f"small_emotion_{name}_v0",
                "q1_decision": "passed_q1_research_review",
                "observed_primary_label_count": 2,
                "active_event_count": 2,
                "event_month_count": 2,
                "mean_primary_directional_return": 0.44,
                "oos_test_mean_directional_return": 0.31,
                "falsifier_dominance_count": 0,
                "promotion_gate_allowed": True,
                "q2_entry_allowed": False,
                "optimizer_entry_allowed": False,
                "expected_return_panel_written": False,
                "alpha_registry_update_allowed": False,
                "production_approval_claimed": False,
                "no_view_not_zero_alpha": True,
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "event_id": "e1",
                "asset_id": "10001",
                "ticker": "AAA",
                "date": "2021-01-04",
                "event_month": "2021-01",
                "signal_state": "active",
                "signal_value": -1.0,
                "adv20": 1_000_000.0,
                "bid_ask_spread": 0.02,
                "adjusted_close": 10.0,
                "volume": 100_000.0,
                "market_cap": 100_000_000.0,
                "industry": "tech",
                "coverage_state": "active",
                "no_view_reason": "",
            },
            {
                "event_id": "e2",
                "asset_id": "10002",
                "ticker": "BBB",
                "date": "2021-02-04",
                "event_month": "2021-02",
                "signal_state": "active",
                "signal_value": -1.0,
                "adv20": 2_000_000.0,
                "bid_ask_spread": 0.03,
                "adjusted_close": 20.0,
                "volume": 100_000.0,
                "market_cap": 120_000_000.0,
                "industry": "health",
                "coverage_state": "active",
                "no_view_reason": "",
            },
            {
                "event_id": "e3",
                "asset_id": "10003",
                "ticker": "CCC",
                "date": "2021-03-04",
                "event_month": "2021-03",
                "signal_state": "no_view",
                "signal_value": "",
                "adv20": "",
                "bid_ask_spread": "",
                "adjusted_close": "",
                "volume": "",
                "market_cap": "",
                "industry": "",
                "coverage_state": "no_view",
                "no_view_reason": "missing_label",
            },
        ]
    ).to_csv(q1_dir / "q1_event_panel.csv", index=False)
    (pg_dir / "pg_decision_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "small_emotion_promotion_gate_summary.v1",
                "stage": "PG-SMALL-EMOTION-01",
                "measurement_spec_id": f"small_emotion_{name}_v0",
                "measurement_spec_hash": spec_hash,
                "required_measurement_spec_hash": spec_hash,
                "q1_decision": "passed_q1_research_review",
                "promotion_decision": promotion_decision,
                "promotion_gate_allowed": promotion_decision == "promote_to_q2_candidate",
                "observed_primary_label_count": 2,
                "mean_primary_directional_return": 0.44,
                "oos_test_mean_directional_return": 0.31,
                "search_burden_status": "warning",
                "tail_status": "pass",
                "anomaly_status": "pass",
                "cost_liquidity_status": "pass",
                "time_breadth_status": "pass",
                "q2_entry_allowed": False,
                "optimizer_entry_allowed": False,
                "expected_return_panel_written": False,
                "alpha_registry_update_allowed": False,
                "production_approval_claimed": False,
                "no_view_not_zero_alpha": True,
            }
        ),
        encoding="utf-8",
    )
    return SmallEmotionQ2CandidateInput(
        candidate_name=name,
        measurement_spec_path=spec_path,
        q1_output_dir=q1_dir,
        promotion_gate_dir=pg_dir,
        required_measurement_spec_hash=spec_hash,
    )
