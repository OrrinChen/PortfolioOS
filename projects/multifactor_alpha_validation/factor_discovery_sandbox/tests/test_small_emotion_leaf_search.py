from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.small_emotion_leaf_search import run_small_emotion_leaf_search
from test_small_emotion_d2 import _write_benchmark_fixture, _write_delisting_fixture, _write_price_fixture


def test_leaf_search_finds_strong_paths_without_downstream_artifacts(tmp_path: Path) -> None:
    prices = _write_price_fixture(tmp_path / "prices.csv")
    benchmark = _write_benchmark_fixture(tmp_path / "benchmark.csv")
    delistings = _write_delisting_fixture(tmp_path / "delistings.csv")

    result = run_small_emotion_leaf_search(
        price_panel_path=prices,
        benchmark_panel_path=benchmark,
        delisting_path=delistings,
        output_dir=tmp_path / "leaf",
        mechanisms=["up_shock_reversal", "up_shock_continuation"],
        windows=["post_1_5", "post_1_22"],
        base_shock_threshold=0.01,
        base_volume_spike_threshold=1.0,
        adv_min_dollars=75_000.0,
        max_depth=2,
        beam_width=4,
        min_events=1,
        min_event_months=1,
        min_history_observations=20,
        top_n=5,
    )

    assert result.summary["schema_version"] == "small_emotion_leaf_search_summary.v1"
    assert result.summary["stage"] == "E0-SMALL-EMOTION-05"
    assert result.summary["exploratory_only"] is True
    assert result.summary["overfit_search_allowed"] is True
    assert result.summary["not_alpha_evidence"] is True
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["formula_score_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["optimizer_entry_allowed"] is False
    assert result.summary["production_approval_claimed"] is False

    tree = pd.read_csv(result.artifacts["leaf_search_tree"])
    best = pd.read_csv(result.artifacts["best_leaf_candidates"])
    assert not tree.empty
    assert not best.empty
    assert best["leaf_rank"].is_monotonic_increasing
    assert best["depth"].max() <= 2
    assert best["active_event_count"].min() >= 1
    assert "path_predicates" in best.columns
    assert best["selection_status"].eq("aggressive_leaf_search_only").all()

    disclosure = json.loads(result.artifacts["leaf_overfit_disclosure"].read_text(encoding="utf-8"))
    assert disclosure["selection_bias_risk"] == "extreme"
    assert disclosure["requires_freeze_before_q1"] is True

    freeze_next = json.loads(result.artifacts["leaf_candidate_to_freeze_next"].read_text(encoding="utf-8"))
    assert freeze_next["measurement_spec_written"] is False
    assert freeze_next["q1_entry_allowed"] is False
    assert freeze_next["expected_return_panel_written"] is False

    assert not (tmp_path / "leaf" / "measurement_spec.yaml").exists()
    assert not (tmp_path / "leaf" / "signal_panel.csv").exists()
    assert not (tmp_path / "leaf" / "expected_return_panel.csv").exists()
