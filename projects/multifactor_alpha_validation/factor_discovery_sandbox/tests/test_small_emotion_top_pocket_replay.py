from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.small_emotion_top_pocket_replay import (
    run_small_emotion_top_pocket_chunked_replay,
)
from test_small_emotion_d2 import _write_benchmark_fixture, _write_delisting_fixture, _write_price_fixture


def test_top_pocket_chunked_replay_confirms_candidate_without_writing_d3_or_q1(tmp_path: Path) -> None:
    chunk_dir = tmp_path / "chunks"
    chunk_dir.mkdir()
    first = _write_price_fixture(chunk_dir / "prices_0001.csv")
    second = _write_price_fixture(chunk_dir / "prices_0002.csv")
    benchmark = _write_benchmark_fixture(tmp_path / "benchmark.csv")
    delistings = _write_delisting_fixture(tmp_path / "delistings.csv")

    result = run_small_emotion_top_pocket_chunked_replay(
        price_chunk_paths=[first, second],
        benchmark_panel_path=benchmark,
        delisting_path=delistings,
        output_dir=tmp_path / "top_pocket",
        mechanism="up_shock_reversal",
        shock_threshold=0.05,
        volume_spike_threshold=1.0,
        market_cap_bucket="all_small_cap",
        liquidity_filter="all",
        stale_filter="medium",
        adv_min_dollars=75_000.0,
        window="post_1_22",
        min_history_observations=20,
        minimum_observed_chunks=1,
        minimum_positive_chunks=1,
        minimum_aggregate_events=1,
    )

    assert result.summary["schema_version"] == "small_emotion_top_pocket_replay_summary.v1"
    assert result.summary["stage"] == "E0-SMALL-EMOTION-02A"
    assert result.summary["source_mode"] == "chunked_price_panels"
    assert result.summary["exploratory_only"] is True
    assert result.summary["overfit_search_allowed"] is True
    assert result.summary["candidate_can_be_reviewed_for_d3_freeze"] in {True, False}
    assert result.summary["d3_charter_written"] is False
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["optimizer_entry_allowed"] is False
    assert result.summary["production_approval_claimed"] is False

    manifest = pd.read_csv(result.artifacts["top_pocket_chunk_manifest"])
    assert list(manifest["chunk_status"]) == ["computed", "computed"]
    assert set(manifest["chunk_path"]) == {str(first), str(second)}
    assert manifest["q1_entry_allowed"].eq(False).all()

    metrics = pd.read_csv(result.artifacts["top_pocket_chunk_metrics"])
    assert len(metrics) == 2
    assert metrics["mechanism"].eq("up_shock_reversal").all()
    assert metrics["window"].eq("post_1_22").all()
    assert "chunk_positive" in metrics.columns

    freeze_review = json.loads(result.artifacts["candidate_freeze_review"].read_text(encoding="utf-8"))
    assert freeze_review["stage"] == "E0-SMALL-EMOTION-02A"
    assert freeze_review["measurement_spec_written"] is False
    assert freeze_review["q1_entry_allowed"] is False
    assert freeze_review["q2_entry_allowed"] is False

    assert not (tmp_path / "top_pocket" / "measurement_spec.yaml").exists()
    assert not (tmp_path / "top_pocket" / "expected_return_panel.csv").exists()
