from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from execution_aware_optimizer.small_emotion_q2_robustness_audit import (
    run_small_emotion_q2_robustness_audit,
)


def test_q2_robustness_audit_writes_concentration_overlap_and_horizon_artifacts(tmp_path: Path) -> None:
    q2_complete_dir, event_panels, window_panels = _write_inputs(tmp_path)

    result = run_small_emotion_q2_robustness_audit(
        q2_complete_dir=q2_complete_dir,
        q1_event_panels=event_panels,
        q1_window_panels=window_panels,
        output_dir=tmp_path / "robustness",
        minimum_event_count=4,
        minimum_event_month_count=3,
        bootstrap_trials=25,
        random_seed=7,
    )

    assert result.summary["schema_version"] == "small_emotion_q2_robustness_audit_summary.v1"
    assert result.summary["stage"] == "Q2-SMALL-EMOTION-07"
    assert result.summary["candidate_count"] == 2
    assert result.summary["orders_written"] is False
    assert result.summary["portfolio_construction_artifact_written"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["production_approval_claimed"] is False

    matrix = pd.read_csv(result.artifacts["robustness_matrix"])
    assert {"candidate_name", "audit_decision", "mean_directional_return", "t_stat", "positive_month_share"} <= set(
        matrix.columns
    )
    assert set(matrix["audit_decision"]) == {"robustness_profile_passed"}

    horizon = pd.read_csv(result.artifacts["horizon_decay_matrix"])
    assert {"candidate_name", "window", "mean_directional_return", "hit_rate"}.issubset(horizon.columns)
    assert {"post_1_5", "post_1_10", "post_1_22"} <= set(horizon["window"])

    concentration = pd.read_csv(result.artifacts["concentration_matrix"])
    assert {"candidate_name", "concentration_type", "top_bucket_share", "top_bucket_abs_return_share"} <= set(
        concentration.columns
    )
    assert {"ticker", "event_month", "sector"} <= set(concentration["concentration_type"])

    overlap = pd.read_csv(result.artifacts["overlap_matrix"])
    assert overlap.loc[0, "left_candidate"] == "rank1"
    assert overlap.loc[0, "right_candidate"] == "rank2"
    assert overlap.loc[0, "event_jaccard"] > 0.0

    bootstrap = pd.read_csv(result.artifacts["bootstrap_matrix"])
    assert {"candidate_name", "bootstrap_mean", "ci_05", "ci_95", "positive_bootstrap_share"}.issubset(
        bootstrap.columns
    )
    assert (bootstrap["positive_bootstrap_share"] > 0.5).all()

    report = result.artifacts["report"].read_text(encoding="utf-8")
    assert "Q2 robustness profile audit only" in report
    assert "production approval: not claimed" in report
    assert "paper-ready" not in report.lower()


def test_q2_robustness_audit_blocks_incomplete_q2_candidate(tmp_path: Path) -> None:
    q2_complete_dir, event_panels, window_panels = _write_inputs(tmp_path, q2_decision="failed_execution_survival")

    result = run_small_emotion_q2_robustness_audit(
        q2_complete_dir=q2_complete_dir,
        q1_event_panels=event_panels,
        q1_window_panels=window_panels,
        output_dir=tmp_path / "blocked",
        minimum_event_count=4,
        minimum_event_month_count=3,
        bootstrap_trials=10,
    )

    matrix = pd.read_csv(result.artifacts["robustness_matrix"])
    assert set(matrix["audit_decision"]) == {"blocked_q2_incomplete"}
    assert result.summary["robustness_passed_count"] == 0


def _write_inputs(
    tmp_path: Path,
    *,
    q2_decision: str = "completed_q2_execution_survival",
) -> tuple[Path, dict[str, Path], dict[str, Path]]:
    q2_complete_dir = tmp_path / "q2_complete"
    q2_complete_dir.mkdir()
    candidates = ["rank1", "rank2"]
    pd.DataFrame(
        [
            {
                "candidate_name": name,
                "measurement_spec_id": f"{name}_post_1_22_v0",
                "measurement_spec_hash": f"hash-{name}",
                "q2_complete_decision": q2_decision,
                "orders_written": False,
                "portfolio_construction_allowed": False,
                "alpha_registry_update_allowed": False,
                "broker_order_path_opened": False,
                "production_approval_claimed": False,
                "no_view_not_zero_alpha": True,
            }
            for name in candidates
        ]
    ).to_csv(q2_complete_dir / "small_emotion_q2_complete_matrix.csv", index=False)
    (q2_complete_dir / "small_emotion_q2_complete_summary.json").write_text(
        json.dumps({"schema_version": "small_emotion_q2_complete_summary.v1"}, sort_keys=True),
        encoding="utf-8",
    )

    event_panels: dict[str, Path] = {}
    window_panels: dict[str, Path] = {}
    for candidate in candidates:
        q1_dir = tmp_path / candidate
        q1_dir.mkdir()
        event_rows = []
        window_rows = []
        for idx in range(12):
            event_id = f"e{idx + 1}"
            ticker = f"T{idx % 5}"
            date = f"2021-{idx % 6 + 1:02d}-05"
            event_rows.append(
                {
                    "event_id": event_id,
                    "asset_id": 100 + idx,
                    "ticker": ticker,
                    "date": date,
                    "event_month": date[:7],
                    "signal_state": "active",
                    "sector": "technology" if idx % 2 == 0 else "healthcare",
                    "industry": "sic_1234",
                    "market_cap_bucket": "small",
                    "liquidity_bucket": "high",
                    "spread_bucket": "tight",
                    "no_view_not_zero_alpha": True,
                }
            )
            base = 0.08 + idx * 0.005
            for window, scale in [("post_1_5", 0.6), ("post_1_10", 0.8), ("post_1_22", 1.0)]:
                window_rows.append(
                    {
                        "event_id": event_id,
                        "asset_id": 100 + idx,
                        "ticker": ticker,
                        "date": date,
                        "event_month": date[:7],
                        "window": window,
                        "label_status": "observed",
                        "asset_return": base * scale,
                        "benchmark_return": 0.01,
                        "abnormal_return": base * scale - 0.01,
                        "directional_return": base * scale,
                        "no_view_not_zero_alpha": True,
                    }
                )
        event_path = q1_dir / "q1_event_panel.csv"
        window_path = q1_dir / "q1_window_return_panel.csv"
        pd.DataFrame(event_rows).to_csv(event_path, index=False)
        pd.DataFrame(window_rows).to_csv(window_path, index=False)
        event_panels[candidate] = event_path
        window_panels[candidate] = window_path
    return q2_complete_dir, event_panels, window_panels
