from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.small_emotion_sharpened_top_pocket_replay import (
    run_small_emotion_sharpened_top_pocket_replay,
)

from test_small_emotion_d2 import _write_benchmark_fixture, _write_delisting_fixture, _write_price_fixture


def test_sharpened_top_pocket_replay_preserves_filters_and_blocks_downstream(tmp_path: Path) -> None:
    chunk_dir = tmp_path / "chunks"
    chunk_dir.mkdir()
    chunk_a = chunk_dir / "chunk_a.csv"
    chunk_b = chunk_dir / "chunk_b.csv"
    price_path = tmp_path / "prices.csv"
    benchmark_path = tmp_path / "benchmark.csv"
    delisting_path = tmp_path / "delisting.csv"
    _write_price_fixture(price_path)
    prices = pd.read_csv(price_path)
    prices.iloc[: len(prices) // 2].to_csv(chunk_a, index=False)
    prices.iloc[len(prices) // 2 :].to_csv(chunk_b, index=False)
    _write_benchmark_fixture(benchmark_path)
    _write_delisting_fixture(delisting_path)

    result = run_small_emotion_sharpened_top_pocket_replay(
        price_chunk_paths=[chunk_a, chunk_b],
        benchmark_panel_path=benchmark_path,
        delisting_path=delisting_path,
        output_dir=tmp_path / "out",
        mechanism="up_shock_reversal",
        shock_threshold=0.05,
        volume_spike_threshold=1.0,
        prior_5d_min_return=None,
        prior_20d_min_return=None,
        close_location_filter="all",
        low_price_filter="all",
        market_cap_bucket="all_small_cap",
        liquidity_filter="all",
        spread_filter="all",
        regime_filter="all",
        adv_min_dollars=75_000.0,
        minimum_observed_chunks=1,
        minimum_positive_chunks=1,
        minimum_aggregate_events=1,
        window="post_1_5",
        refresh=True,
    )

    assert result.summary["schema_version"] == "small_emotion_sharpened_top_pocket_replay_summary.v1"
    assert result.summary["stage"] == "E0-SMALL-EMOTION-04A"
    assert result.summary["prior_5d_min_return"] == ""
    assert result.summary["regime_filter"] == "all"
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["measurement_spec_written"] is False

    review = json.loads(result.artifacts["sharpened_candidate_freeze_review"].read_text(encoding="utf-8"))
    assert review["formula_score_written"] is False
    assert review["expected_return_panel_written"] is False
    assert review["optimizer_entry_allowed"] is False

    metrics = pd.read_csv(result.artifacts["sharpened_top_pocket_chunk_metrics"])
    assert metrics["prior_5d_min_return"].isna().all()
    assert set(metrics["regime_filter"]) == {"all"}
