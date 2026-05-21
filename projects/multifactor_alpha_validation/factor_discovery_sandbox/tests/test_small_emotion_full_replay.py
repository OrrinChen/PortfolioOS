from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.small_emotion_full_replay import run_small_emotion_chunked_full_replay
from test_small_emotion_d2 import _write_benchmark_fixture, _write_delisting_fixture, _write_price_fixture


def test_chunked_full_replay_writes_resumable_chunk_manifest_and_preserves_boundaries(tmp_path: Path) -> None:
    chunk_dir = tmp_path / "chunks"
    chunk_dir.mkdir()
    first = _write_price_fixture(chunk_dir / "prices_0001.csv")
    second = _write_price_fixture(chunk_dir / "prices_0002.csv")
    benchmark = _write_benchmark_fixture(tmp_path / "benchmark.csv")
    delistings = _write_delisting_fixture(tmp_path / "delistings.csv")

    result = run_small_emotion_chunked_full_replay(
        price_chunk_paths=[first, second],
        benchmark_panel_path=benchmark,
        delisting_path=delistings,
        output_dir=tmp_path / "full_replay",
        minimum_subset_events=3,
        minimum_event_month_count=2,
        minimum_label_coverage_share=0.70,
        min_history_observations=20,
        min_adv_dollars=75_000.0,
        minimum_observable_chunks=1,
    )

    assert result.summary["schema_version"] == "small_emotion_full_replay_summary.v1"
    assert result.summary["stage"] == "D2-SMALL-EMOTION-01A"
    assert result.summary["chunk_count"] == 2
    assert result.summary["source_mode"] == "chunked_price_panels"
    assert result.summary["formula_score_written"] is False
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["optimizer_entry_allowed"] is False
    assert result.summary["portfolio_construction_allowed"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["production_approval_claimed"] is False
    assert len(result.summary["allow_d3_charter_for"]) <= 1

    manifest = pd.read_csv(result.artifacts["chunk_manifest"])
    assert list(manifest["chunk_status"]) == ["computed", "computed"]
    assert set(manifest["chunk_path"]) == {str(first), str(second)}
    assert manifest["q2_entry_allowed"].eq(False).all()

    aggregate = pd.read_csv(result.artifacts["subset_guard_aggregate"])
    assert {
        "panic_overreaction_candidate",
        "fomo_continuation_candidate",
        "liquidity_vacuum_reversal_candidate",
    }.issubset(set(aggregate["event_subset"]))
    assert "subset_guard_passed" in aggregate.columns

    decision = json.loads(result.artifacts["full_replay_decision"].read_text(encoding="utf-8"))
    assert decision["source_mode"] == "chunked_price_panels"
    assert not (tmp_path / "full_replay" / "measurement_spec.yaml").exists()
    assert not (tmp_path / "full_replay" / "expected_return_panel.csv").exists()

    resumed = run_small_emotion_chunked_full_replay(
        price_chunk_paths=[first, second],
        benchmark_panel_path=benchmark,
        delisting_path=delistings,
        output_dir=tmp_path / "full_replay",
        minimum_subset_events=3,
        minimum_event_month_count=2,
        minimum_label_coverage_share=0.70,
        min_history_observations=20,
        min_adv_dollars=75_000.0,
        minimum_observable_chunks=1,
    )
    resumed_manifest = pd.read_csv(resumed.artifacts["chunk_manifest"])
    assert list(resumed_manifest["chunk_status"]) == ["resumed", "resumed"]
