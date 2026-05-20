from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.small_emotion_full_market_overfit_lab import (
    run_small_emotion_full_market_overfit_lab,
)
from test_small_emotion_d2 import _write_benchmark_fixture, _write_delisting_fixture, _write_price_fixture


def test_full_market_overfit_lab_writes_exploratory_pockets_without_downstream_artifacts(tmp_path: Path) -> None:
    prices = _write_price_fixture(tmp_path / "prices.csv")
    benchmark = _write_benchmark_fixture(tmp_path / "benchmark.csv")
    delistings = _write_delisting_fixture(tmp_path / "delistings.csv")

    result = run_small_emotion_full_market_overfit_lab(
        price_panel_path=prices,
        benchmark_panel_path=benchmark,
        delisting_path=delistings,
        output_dir=tmp_path / "full_market_overfit",
        shock_thresholds=[0.05, 0.08],
        volume_spike_thresholds=[1.0, 2.0],
        windows=["post_1_5", "post_1_22"],
        mechanisms=["up_shock_reversal", "up_shock_continuation", "down_shock_reversal"],
        min_events=1,
        min_event_months=1,
        min_history_observations=20,
        adv_min_dollars=[50_000.0],
        top_n=10,
        max_rows=None,
    )

    assert result.summary["schema_version"] == "small_emotion_full_market_overfit_lab_summary.v1"
    assert result.summary["stage"] == "E1-SMALL-EMOTION-FULL-MARKET-OVERFIT"
    assert result.summary["universe_scope"] == "full_market_common_stock_research_universe"
    assert result.summary["exploratory_only"] is True
    assert result.summary["overfit_search_allowed"] is True
    assert result.summary["not_alpha_evidence"] is True
    assert result.summary["requires_freeze_before_q1"] is True
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["formula_score_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["optimizer_entry_allowed"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["production_approval_claimed"] is False

    grid = pd.read_csv(result.artifacts["full_market_overfit_grid"])
    top = pd.read_csv(result.artifacts["top_50_overfit_pockets"])
    tail = pd.read_csv(result.artifacts["tail_concentration_audit"])
    cost = pd.read_csv(result.artifacts["cost_liquidity_audit"])
    assert not grid.empty
    assert not top.empty
    assert not tail.empty
    assert not cost.empty
    assert top["pocket_rank"].is_monotonic_increasing
    assert top["selection_status"].eq("exploratory_full_market_overfit_only").all()
    assert {"mean_directional_return", "t_stat", "hit_rate", "tail_concentration_status"}.issubset(top.columns)
    assert not top["tail_concentration_status"].eq("pending_audit").any()
    canonical_paths = top["path_predicates"].fillna("").map(
        lambda value: " & ".join(sorted(part.strip() for part in str(value).split("&") if part.strip()))
    )
    assert not top.assign(canonical_path=canonical_paths).duplicated(
        ["mechanism", "window", "shock_threshold", "volume_spike_threshold", "adv_min_dollars", "canonical_path"]
    ).any()
    assert "event_set_hash" in top.columns
    assert not top["event_set_hash"].duplicated().any()

    draft = json.loads(result.artifacts["best_pocket_spec_draft"].read_text(encoding="utf-8"))
    assert draft["measurement_spec_written"] is False
    assert draft["q1_entry_allowed"] is False
    assert draft["recommendation"] in {"freeze_candidate_before_q1", "no_overfit_pocket_found"}

    report = result.artifacts["full_market_overfit_report"].read_text(encoding="utf-8").lower()
    assert "exploratory overfit lab" in report
    assert "not alpha evidence" in report
    for forbidden in [
        "production approved",
        "paper ready",
        "live trading",
        "broker execution",
        "order generation",
        "alpha passed",
        "q2-ready",
        "tradable alpha",
    ]:
        assert forbidden not in report

    assert not (tmp_path / "full_market_overfit" / "measurement_spec.yaml").exists()
    assert not (tmp_path / "full_market_overfit" / "signal_panel.csv").exists()
    assert not (tmp_path / "full_market_overfit" / "expected_return_panel.csv").exists()


def test_full_market_overfit_lab_can_cache_features_then_replay_from_cache(tmp_path: Path) -> None:
    prices = _write_price_fixture(tmp_path / "prices.csv")
    benchmark = _write_benchmark_fixture(tmp_path / "benchmark.csv")
    delistings = _write_delisting_fixture(tmp_path / "delistings.csv")
    cache_dir = tmp_path / "feature_cache"

    cache_only = run_small_emotion_full_market_overfit_lab(
        price_panel_path=prices,
        benchmark_panel_path=benchmark,
        delisting_path=delistings,
        output_dir=tmp_path / "cache_only",
        feature_cache_dir=cache_dir,
        cache_only=True,
        shock_thresholds=[0.05],
        volume_spike_thresholds=[1.0],
        windows=["post_1_22"],
        mechanisms=["up_shock_continuation"],
        min_events=1,
        min_event_months=1,
        min_history_observations=20,
        adv_min_dollars=[50_000.0],
        top_n=5,
        max_rows=None,
    )

    assert cache_only.summary["feature_cache_status"] == "written_cache_only"
    assert cache_only.summary["cache_only"] is True
    assert cache_only.summary["top_pocket_count"] == 0
    assert (cache_dir / "feature_cache_manifest.json").exists()
    assert (cache_dir / "event_labels_post_1_22.csv").exists()

    replay = run_small_emotion_full_market_overfit_lab(
        price_panel_path=prices,
        benchmark_panel_path=benchmark,
        delisting_path=delistings,
        output_dir=tmp_path / "replay",
        feature_cache_dir=cache_dir,
        shock_thresholds=[0.05],
        volume_spike_thresholds=[1.0],
        windows=["post_1_22"],
        mechanisms=["up_shock_continuation"],
        min_events=1,
        min_event_months=1,
        min_history_observations=20,
        adv_min_dollars=[50_000.0],
        top_n=5,
        max_rows=None,
    )

    assert replay.summary["feature_cache_status"] == "cache_hit"
    assert replay.summary["cache_only"] is False
    assert replay.summary["top_pocket_count"] > 0
    assert replay.summary["q1_entry_allowed"] is False
    assert replay.summary["q2_entry_allowed"] is False

    top = pd.read_csv(replay.artifacts["top_50_overfit_pockets"])
    assert not top.empty
    assert top["selection_status"].eq("exploratory_full_market_overfit_only").all()


def test_full_market_overfit_lab_can_exclude_cost_toxic_predicates(tmp_path: Path) -> None:
    prices = _write_price_fixture(tmp_path / "prices.csv")
    benchmark = _write_benchmark_fixture(tmp_path / "benchmark.csv")
    delistings = _write_delisting_fixture(tmp_path / "delistings.csv")

    result = run_small_emotion_full_market_overfit_lab(
        price_panel_path=prices,
        benchmark_panel_path=benchmark,
        delisting_path=delistings,
        output_dir=tmp_path / "cost_clean_replay",
        shock_thresholds=[0.05, 0.08],
        volume_spike_thresholds=[1.0, 2.0],
        windows=["post_1_5", "post_1_22"],
        mechanisms=["up_shock_reversal", "up_shock_continuation", "down_shock_reversal"],
        min_events=1,
        min_event_months=1,
        min_history_observations=20,
        adv_min_dollars=[50_000.0],
        excluded_predicates=["spread_wide", "price_under_5", "weak_liquidity", "liquidity_low"],
        top_n=10,
        max_rows=None,
    )

    assert result.summary["excluded_predicates"] == ["liquidity_low", "price_under_5", "spread_wide", "weak_liquidity"]
    grid = pd.read_csv(result.artifacts["full_market_overfit_grid"])
    top = pd.read_csv(result.artifacts["top_50_overfit_pockets"])
    assert not grid["added_predicate"].isin({"spread_wide", "price_under_5", "weak_liquidity", "liquidity_low"}).any()
    if not top.empty:
        paths = top["path_predicates"].fillna("").astype(str)
        for forbidden in ["spread_wide", "price_under_5", "weak_liquidity", "liquidity_low"]:
            assert not paths.str.contains(forbidden, regex=False).any()


def test_full_market_overfit_lab_can_filter_stale_price_candidate_events(tmp_path: Path) -> None:
    prices = _write_price_fixture(tmp_path / "prices.csv")
    benchmark = _write_benchmark_fixture(tmp_path / "benchmark.csv")
    delistings = _write_delisting_fixture(tmp_path / "delistings.csv")
    cache_dir = tmp_path / "feature_cache"

    run_small_emotion_full_market_overfit_lab(
        price_panel_path=prices,
        benchmark_panel_path=benchmark,
        delisting_path=delistings,
        output_dir=tmp_path / "cache_only",
        feature_cache_dir=cache_dir,
        cache_only=True,
        shock_thresholds=[0.05],
        volume_spike_thresholds=[1.0],
        windows=["post_1_22"],
        mechanisms=["up_shock_reversal"],
        min_events=1,
        min_event_months=1,
        min_history_observations=20,
        adv_min_dollars=[50_000.0],
        max_rows=None,
    )
    label_path = cache_dir / "event_labels_post_1_22.csv"
    labels = pd.read_csv(label_path)
    assert len(labels) >= 2
    labels.loc[labels.index[0], "stale_roll_5"] = 1
    labels.loc[labels.index[1], "zero_volume"] = True
    labels.to_csv(label_path, index=False)

    result = run_small_emotion_full_market_overfit_lab(
        price_panel_path=prices,
        benchmark_panel_path=benchmark,
        delisting_path=delistings,
        output_dir=tmp_path / "stale_clean_replay",
        feature_cache_dir=cache_dir,
        shock_thresholds=[0.05],
        volume_spike_thresholds=[1.0],
        windows=["post_1_22"],
        mechanisms=["up_shock_reversal"],
        min_events=1,
        min_event_months=1,
        min_history_observations=20,
        adv_min_dollars=[50_000.0],
        top_n=5,
        max_rows=None,
        exclude_stale_price_events=True,
    )

    assert result.summary["exclude_stale_price_events"] is True
    assert result.summary["candidate_event_row_count_before_filter"] == len(labels)
    assert result.summary["candidate_event_row_count_after_filter"] == len(labels) - 2
    assert result.summary["candidate_event_row_count_removed_by_stale_price_filter"] == 2
