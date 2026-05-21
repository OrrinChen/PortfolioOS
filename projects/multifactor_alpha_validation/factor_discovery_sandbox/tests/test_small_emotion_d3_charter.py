from __future__ import annotations

import json
from pathlib import Path

import yaml

from factor_discovery_sandbox.small_emotion_d3_charter import write_small_emotion_d3_charter


def test_d3_charter_freezes_top_pocket_without_measurement_spec_or_q1(tmp_path: Path) -> None:
    replay_dir = tmp_path / "replay"
    replay_dir.mkdir()
    freeze_review = replay_dir / "candidate_freeze_review.json"
    summary_path = replay_dir / "top_pocket_replay_summary.json"
    metrics_path = replay_dir / "top_pocket_chunk_metrics.csv"
    freeze_review.write_text(
        json.dumps(
            {
                "schema_version": "small_emotion_candidate_freeze_review.v1",
                "stage": "E0-SMALL-EMOTION-02A",
                "candidate_can_be_reviewed_for_d3_freeze": True,
                "overall_decision": "candidate_stable_enough_for_manual_d3_freeze_review",
                "candidate": {
                    "mechanism": "up_shock_reversal",
                    "shock_threshold": 0.05,
                    "volume_spike_threshold": 1.5,
                    "market_cap_bucket": "all_small_cap",
                    "liquidity_filter": "all",
                    "stale_filter": "medium",
                    "adv_min_dollars": 250000.0,
                    "window": "post_1_22",
                },
            }
        ),
        encoding="utf-8",
    )
    summary_path.write_text(
        json.dumps(
            {
                "schema_version": "small_emotion_top_pocket_replay_summary.v1",
                "stage": "E0-SMALL-EMOTION-02A",
                "aggregate_active_event_count": 52562,
                "observed_chunk_count": 6,
                "positive_chunk_count": 4,
                "weighted_mean_directional_return": 0.004788,
                "weighted_hit_rate": 0.579278,
            }
        ),
        encoding="utf-8",
    )
    metrics_path.write_text(
        "chunk_index,active_event_count,mean_directional_return,chunk_positive\n"
        "1,100,0.01,True\n"
        "2,120,-0.02,False\n",
        encoding="utf-8",
    )

    result = write_small_emotion_d3_charter(
        freeze_review_path=freeze_review,
        top_pocket_summary_path=summary_path,
        chunk_metrics_path=metrics_path,
        output_dir=tmp_path / "d3",
    )

    assert result.summary["schema_version"] == "small_emotion_d3_charter_summary.v1"
    assert result.summary["stage"] == "D3-SMALL-EMOTION-03"
    assert result.summary["d3_charter_written"] is True
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["formula_score_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["optimizer_entry_allowed"] is False
    assert result.summary["production_approval_claimed"] is False

    charter = yaml.safe_load(result.artifacts["d3_candidate_charter"].read_text(encoding="utf-8"))
    assert charter["candidate_id"] == "small_cap_up_shock_reversal_post_1_22_v0"
    assert charter["candidate"]["mechanism"] == "up_shock_reversal"
    assert charter["candidate"]["expected_direction"] == "negative_post_shock_abnormal_return"
    assert charter["candidate"]["shock_threshold"] == 0.05
    assert charter["timestamp_contract"]["signal_anchor"] == "shock_trading_date_close"
    assert charter["coverage_policy"]["missing_coverage"] == "no_view_not_zero_alpha"
    assert "shifted_date_placebo" in charter["hard_falsifiers"]
    assert "stale_price_matched_placebo" in charter["hard_falsifiers"]
    assert charter["downstream_boundaries"]["q1_entry_allowed"] is False

    manifest = json.loads(result.artifacts["d3_charter_manifest"].read_text(encoding="utf-8"))
    assert manifest["candidate_charter_hash"]
    assert manifest["source_freeze_review_hash"]
    assert manifest["source_top_pocket_summary_hash"]
    assert not (tmp_path / "d3" / "measurement_spec.yaml").exists()
    assert not (tmp_path / "d3" / "signal_panel.csv").exists()
    assert not (tmp_path / "d3" / "expected_return_panel.csv").exists()
