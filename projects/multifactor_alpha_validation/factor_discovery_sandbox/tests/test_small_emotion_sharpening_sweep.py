from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.small_emotion_sharpening_sweep import (
    run_small_emotion_sharpening_sweep,
)
from test_small_emotion_d2 import _write_benchmark_fixture, _write_delisting_fixture, _write_price_fixture


def test_sharpening_sweep_searches_blowoff_filters_without_downstream_artifacts(tmp_path: Path) -> None:
    prices = _write_price_fixture(tmp_path / "prices.csv")
    benchmark = _write_benchmark_fixture(tmp_path / "benchmark.csv")
    delistings = _write_delisting_fixture(tmp_path / "delistings.csv")

    result = run_small_emotion_sharpening_sweep(
        price_panel_path=prices,
        benchmark_panel_path=benchmark,
        delisting_path=delistings,
        output_dir=tmp_path / "e0_sharpen",
        shock_thresholds=[0.05, 0.08],
        volume_spike_thresholds=[1.0, 1.5],
        prior_5d_min_returns=[None, 0.0],
        prior_20d_min_returns=[None, 0.0],
        close_location_filters=["all", "upper_half"],
        low_price_filters=["all", "under_10"],
        market_cap_buckets=["all_small_cap", "micro"],
        liquidity_filters=["all"],
        spread_filters=["all"],
        regime_filters=["all"],
        windows=["post_1_5", "post_6_22", "post_1_22"],
        mechanisms=["up_shock_reversal"],
        adv_min_dollars=[75_000.0],
        min_history_observations=20,
        min_events=1,
        top_n=10,
    )

    assert result.summary["schema_version"] == "small_emotion_sharpening_sweep_summary.v1"
    assert result.summary["stage"] == "E0-SMALL-EMOTION-04"
    assert result.summary["exploratory_only"] is True
    assert result.summary["overfit_search_allowed"] is True
    assert result.summary["not_alpha_evidence"] is True
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["optimizer_entry_allowed"] is False
    assert result.summary["production_approval_claimed"] is False

    grid = pd.read_csv(result.artifacts["sharpening_sweep_grid"])
    assert not grid.empty
    assert {
        "mechanism",
        "shock_threshold",
        "volume_spike_threshold",
        "prior_5d_min_return",
        "prior_20d_min_return",
        "close_location_filter",
        "low_price_filter",
        "window",
        "active_event_count",
        "mean_directional_return",
        "gross_explosive_score",
    }.issubset(grid.columns)

    best = pd.read_csv(result.artifacts["best_explosive_candidates"])
    assert len(best) <= 10
    assert best["gross_rank"].is_monotonic_increasing

    disclosure = json.loads(result.artifacts["overfit_disclosure"].read_text(encoding="utf-8"))
    assert disclosure["selection_bias_risk"] == "very_high"
    assert disclosure["purpose"] == "find_strong_in_sample_pockets_before_freeze"
    assert disclosure["requires_freeze_before_q1"] is True

    freeze_next = json.loads(result.artifacts["candidate_to_freeze_next"].read_text(encoding="utf-8"))
    assert freeze_next["stage"] == "E0-SMALL-EMOTION-04"
    assert freeze_next["measurement_spec_written"] is False
    assert freeze_next["q1_entry_allowed"] is False
    assert freeze_next["q2_entry_allowed"] is False
    assert not (tmp_path / "e0_sharpen" / "measurement_spec.yaml").exists()
    assert not (tmp_path / "e0_sharpen" / "expected_return_panel.csv").exists()
