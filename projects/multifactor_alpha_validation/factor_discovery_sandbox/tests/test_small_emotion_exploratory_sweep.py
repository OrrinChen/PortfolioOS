from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.small_emotion_exploratory_sweep import (
    run_small_emotion_exploratory_sweep,
)
from test_small_emotion_d2 import _write_benchmark_fixture, _write_delisting_fixture, _write_price_fixture


def test_exploratory_sweep_writes_overfit_candidates_without_downstream_artifacts(tmp_path: Path) -> None:
    prices = _write_price_fixture(tmp_path / "prices.csv")
    benchmark = _write_benchmark_fixture(tmp_path / "benchmark.csv")
    delistings = _write_delisting_fixture(tmp_path / "delistings.csv")

    result = run_small_emotion_exploratory_sweep(
        price_panel_path=prices,
        benchmark_panel_path=benchmark,
        delisting_path=delistings,
        output_dir=tmp_path / "e0",
        shock_thresholds=[0.05, 0.08],
        volume_spike_thresholds=[1.0, 1.5],
        windows=["post_1_5", "post_1_10"],
        mechanisms=["up_shock_continuation", "up_shock_reversal", "down_shock_reversal", "down_shock_continuation"],
        market_cap_buckets=["all_small_cap"],
        liquidity_filters=["all"],
        stale_filters=["medium"],
        adv_min_dollars=[75_000.0],
        min_history_observations=20,
        top_n=5,
    )

    assert result.summary["schema_version"] == "small_emotion_exploratory_sweep_summary.v1"
    assert result.summary["stage"] == "E0-SMALL-EMOTION-02"
    assert result.summary["exploratory_only"] is True
    assert result.summary["overfit_search_allowed"] is True
    assert result.summary["not_alpha_evidence"] is True
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["optimizer_entry_allowed"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["production_approval_claimed"] is False

    grid = pd.read_csv(result.artifacts["parameter_sweep_grid"])
    assert not grid.empty
    assert {
        "mechanism",
        "shock_threshold",
        "volume_spike_threshold",
        "market_cap_bucket",
        "liquidity_filter",
        "stale_filter",
        "adv_min_dollars",
        "window",
        "active_event_count",
        "event_month_count",
        "mean_directional_return",
        "in_sample_rank",
    }.issubset(grid.columns)

    best = pd.read_csv(result.artifacts["best_in_sample_candidates"])
    assert len(best) <= 5
    assert best["in_sample_rank"].is_monotonic_increasing

    overfit = json.loads(result.artifacts["overfit_risk_report"].read_text(encoding="utf-8"))
    assert overfit["selection_bias_risk"] == "high"
    assert overfit["requires_freeze_before_q1"] is True
    assert overfit["exploratory_results_are_not_alpha_evidence"] is True

    freeze_next = json.loads(result.artifacts["candidate_to_freeze_next"].read_text(encoding="utf-8"))
    assert freeze_next["stage"] == "E0-SMALL-EMOTION-02"
    assert freeze_next["measurement_spec_written"] is False
    assert freeze_next["q1_entry_allowed"] is False
    assert freeze_next["q2_entry_allowed"] is False
    assert not (tmp_path / "e0" / "measurement_spec.yaml").exists()
    assert not (tmp_path / "e0" / "expected_return_panel.csv").exists()
