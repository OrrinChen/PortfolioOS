from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from factor_discovery_sandbox.small_emotion_freeze_validation import (
    _sweep_adjusted_selection_audit,
    run_small_emotion_freeze_validation,
)


PRIOR_HASH = "eb56b3e27b0e0b397e3143b7a01e0d8e089b25a560dbc53dcf7ee94f51d2b976"


def test_freeze_validation_writes_new_spec_and_locked_audits_without_downstream_paths(tmp_path: Path) -> None:
    cache_dir = _write_feature_cache(tmp_path / "cache")
    top_path = _write_top_pocket(tmp_path / "top_50_overfit_pockets.csv")
    grid_path = _write_search_grid(tmp_path / "full_market_overfit_grid.csv")
    prior_spec = tmp_path / "prior_measurement_spec.yaml"
    prior_spec.write_text("measurement_spec_id: small_cap_sharpened_up_shock_reversal_post_1_22_v0\n", encoding="utf-8")

    result = run_small_emotion_freeze_validation(
        top_pockets_path=top_path,
        search_grid_path=grid_path,
        feature_cache_dir=cache_dir,
        prior_measurement_spec_path=prior_spec,
        prior_measurement_spec_hash=PRIOR_HASH,
        output_dir=tmp_path / "freeze_02",
        random_seed=7,
        min_events=3,
        min_event_months=3,
        excluded_predicates=["spread_wide", "price_under_5"],
    )

    assert result.summary["schema_version"] == "small_emotion_freeze_02_summary.v1"
    assert result.summary["stage"] == "SMALL-EMOTION-FREEZE-02"
    assert result.summary["prior_spec_identical"] is False
    assert result.summary["d3_charter_written"] is True
    assert result.summary["measurement_spec_written"] is True
    assert result.summary["formula_score_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["optimizer_entry_allowed"] is False
    assert result.summary["portfolio_construction_allowed"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["production_approval_claimed"] is False
    assert result.summary["no_view_not_zero_alpha"] is True
    assert result.summary["decision"] in {
        "promote_to_q2_candidate",
        "locked_oos_pass_cost_pending",
        "cost_liquidity_failed",
        "selection_bias_failed",
        "stale_or_bad_print_failed",
        "locked_oos_failed",
    }

    spec = yaml.safe_load(result.artifacts["measurement_spec"].read_text(encoding="utf-8"))
    assert spec["measurement_spec_id"] == "small_emotion_full_market_spread_wide_shock_ge20_up_reversal_post_1_22_v0"
    assert spec["signal_definition"]["mechanism"] == "up_shock_reversal"
    assert spec["signal_definition"]["filters"]["shock_threshold"] == 0.20
    assert spec["signal_definition"]["filters"]["spread_filter"] == "wide"
    assert spec["label_contract"]["primary_window"] == "post_1_22"

    split = pd.read_csv(result.artifacts["temporal_split_metrics"])
    assert {"train", "validation", "test"}.issubset(set(split["split"]))
    assert (split["event_count"] > 0).all()

    sweep = json.loads(result.artifacts["sweep_adjusted_selection_audit"].read_text(encoding="utf-8"))
    assert sweep["searched_grid_row_count"] == 3
    assert "best_placebo_mean_directional_return" in sweep

    placebo = pd.read_csv(result.artifacts["placebo_top_pockets"])
    assert {"same_coverage_random", "large_cap_matched", "shifted_date"}.issubset(set(placebo["placebo_name"]))
    assert not placebo["path_predicates"].fillna("").astype(str).str.contains("spread_wide|price_under_5", regex=True).any()

    cost = pd.read_csv(result.artifacts["cost_liquidity_gate"])
    assert {"next_close_entry", "next_open_entry_proxy"}.issubset(set(cost["entry_assumption"]))
    frontier = pd.read_csv(result.artifacts["capacity_frontier"])
    assert {"10000", "25000", "50000", "100000"}.issubset(set(frontier["notional_usd"].astype(str)))

    report = result.artifacts["freeze_report"].read_text(encoding="utf-8").lower()
    assert "locked validation" in report
    for forbidden in ["expected_return_panel", "paper ready", "live trading", "broker execution", "production approved"]:
        assert forbidden not in report


def test_freeze_validation_applies_full_path_predicates(tmp_path: Path) -> None:
    cache_dir = _write_feature_cache(tmp_path / "cache")
    top_path = _write_top_pocket(
        tmp_path / "top_50_overfit_pockets.csv",
        path_predicates="close_top_quartile & spread_wide & shock_ge_20pct",
    )
    grid_path = _write_search_grid(tmp_path / "full_market_overfit_grid.csv")
    prior_spec = tmp_path / "prior_measurement_spec.yaml"
    prior_spec.write_text("measurement_spec_id: small_cap_sharpened_up_shock_reversal_post_1_22_v0\n", encoding="utf-8")

    result = run_small_emotion_freeze_validation(
        top_pockets_path=top_path,
        search_grid_path=grid_path,
        feature_cache_dir=cache_dir,
        prior_measurement_spec_path=prior_spec,
        prior_measurement_spec_hash=PRIOR_HASH,
        output_dir=tmp_path / "freeze_02",
        random_seed=7,
        min_events=3,
        min_event_months=3,
    )

    locked = pd.read_csv(result.artifacts["locked_event_panel"])
    assert len(locked) == 6
    assert pd.to_numeric(locked["close_location"], errors="coerce").ge(0.75).all()
    assert locked["spread_bucket"].astype(str).eq("wide").all()
    spec = yaml.safe_load(result.artifacts["measurement_spec"].read_text(encoding="utf-8"))
    assert spec["measurement_spec_id"] == (
        "small_emotion_full_market_close_top_quartile_spread_wide_shock_ge20_up_reversal_post_1_22_v0"
    )
    assert spec["signal_definition"]["filters"]["path_predicates"] == "close_top_quartile & spread_wide & shock_ge_20pct"


def test_freeze_validation_filters_stale_price_candidate_events(tmp_path: Path) -> None:
    cache_dir = _write_feature_cache(tmp_path / "cache")
    labels_path = cache_dir / "event_labels_post_1_22.csv"
    labels = pd.read_csv(labels_path)
    selected = labels["spread_bucket"].astype(str).eq("wide")
    selected_indices = labels[selected].head(2).index
    labels.loc[selected_indices[0], "stale_roll_5"] = 1
    labels.loc[selected_indices[1], "zero_volume"] = True
    labels.to_csv(labels_path, index=False)

    top_path = _write_top_pocket(tmp_path / "top_50_overfit_pockets.csv")
    grid_path = _write_search_grid(tmp_path / "full_market_overfit_grid.csv")
    prior_spec = tmp_path / "prior_measurement_spec.yaml"
    prior_spec.write_text("measurement_spec_id: small_cap_sharpened_up_shock_reversal_post_1_22_v0\n", encoding="utf-8")

    result = run_small_emotion_freeze_validation(
        top_pockets_path=top_path,
        search_grid_path=grid_path,
        feature_cache_dir=cache_dir,
        prior_measurement_spec_path=prior_spec,
        prior_measurement_spec_hash=PRIOR_HASH,
        output_dir=tmp_path / "freeze_02",
        random_seed=7,
        min_events=3,
        min_event_months=3,
        exclude_stale_price_events=True,
    )

    locked = pd.read_csv(result.artifacts["locked_event_panel"])
    assert len(locked) == 10
    assert pd.to_numeric(locked["stale_roll_5"], errors="coerce").fillna(0).lt(1).all()
    assert not locked["zero_volume"].astype(str).str.lower().eq("true").any()
    assert result.summary["exclude_stale_price_events"] is True
    assert result.summary["candidate_event_row_count_removed_by_stale_price_filter"] == 2


def test_sweep_audit_uses_profile_gate_not_mean_only() -> None:
    selected = pd.DataFrame(
        {
            "directional_return": [0.20] * 55 + [0.22] * 45,
            "event_month": [f"2021-{(idx % 55) + 1:02d}" for idx in range(100)],
        }
    )
    placebo = pd.DataFrame(
        [
            {
                "placebo_name": "same_coverage_random",
                "mean_directional_return": 0.35,
                "t_stat": 0.98,
                "hit_rate": 0.45,
                "event_month_count": 28,
            },
            {
                "placebo_name": "large_cap_matched",
                "mean_directional_return": 0.15,
                "t_stat": 3.68,
                "hit_rate": 0.78,
                "event_month_count": 28,
            },
        ]
    )
    placebo.attrs["excluded_predicates"] = ["spread_wide"]

    audit = _sweep_adjusted_selection_audit(
        grid=pd.DataFrame({"row": [1, 2, 3]}),
        selected=selected,
        placebo_top=placebo,
        pocket={"mechanism": "up_shock_reversal", "window": "post_1_22", "path_predicates": "prior5_ge_20pct"},
    )

    assert audit["best_placebo_mean_directional_return"] == 0.35
    assert audit["selected_mean_beats_best_placebo_mean"] is False
    assert audit["best_placebo_profile_name"] == "large_cap_matched"
    assert audit["selected_beats_best_placebo"] is True


def test_sweep_audit_blocks_placebo_with_stronger_profile() -> None:
    selected = pd.DataFrame(
        {
            "directional_return": [0.05] * 40 + [-0.02] * 20,
            "event_month": [f"2021-{(idx % 24) + 1:02d}" for idx in range(60)],
        }
    )
    placebo = pd.DataFrame(
        [
            {
                "placebo_name": "stable_placebo",
                "mean_directional_return": 0.08,
                "t_stat": 7.0,
                "hit_rate": 0.70,
                "event_month_count": 36,
            }
        ]
    )

    audit = _sweep_adjusted_selection_audit(
        grid=pd.DataFrame({"row": [1]}),
        selected=selected,
        placebo_top=placebo,
        pocket={"mechanism": "up_shock_reversal", "window": "post_1_22", "path_predicates": "prior5_ge_20pct"},
    )

    assert audit["best_placebo_profile_name"] == "stable_placebo"
    assert audit["selected_beats_best_placebo"] is False


def _write_feature_cache(cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True)
    rows = []
    for idx in range(18):
        month = idx + 1
        is_selected = idx < 12
        rows.append(
            {
                "asset_id": f"A{idx % 8}",
                "ticker": f"T{idx % 8}",
                "date": f"2021-{month:02d}-15" if month <= 12 else f"2022-{month - 12:02d}-15",
                "event_month": f"2021-{month:02d}" if month <= 12 else f"2022-{month - 12:02d}",
                "shock_return": 0.24 if is_selected else 0.08,
                "abs_shock_return": 0.24 if is_selected else 0.08,
                "abnormal_volume": 1.8,
                "prior_5d_return": 0.02,
                "prior_20d_return": 0.05,
                "close_location": 0.8 if idx < 6 else 0.4,
                "open_to_close_return": -0.04,
                "low_price_bucket": "under_10" if idx % 3 == 0 else "above_20",
                "market_cap": 120_000_000 + idx * 1_000_000,
                "full_market_size_bucket": "micro" if idx % 2 == 0 else "small",
                "adv20": 300_000.0 + idx * 10_000.0,
                "liquidity_bucket": "low",
                "weak_liquidity": True,
                "bid_ask_spread": 0.12 if is_selected else 0.03,
                "spread_bucket": "wide" if is_selected else "medium",
                "sector": "tech" if idx % 2 == 0 else "health",
                "industry": "software" if idx % 2 == 0 else "biotech",
                "market_regime": "market_up_20d",
                "stale_roll_5": 0,
                "zero_volume": False,
                "window": "post_1_22",
                "asset_return": -0.22 if is_selected else 0.04,
                "benchmark_return": 0.02,
                "abnormal_return": -0.24 if is_selected else 0.02,
                "label_status": "observed",
            }
        )
    pd.DataFrame(rows).to_csv(cache_dir / "event_labels_post_1_22.csv", index=False)
    (cache_dir / "feature_cache_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "small_emotion_full_market_feature_cache_manifest.v1",
                "coverage": {"data_status": "available", "event_label_row_count": len(rows), "price_row_count": 1000},
                "windows": ["post_1_22"],
                "cache_files": ["event_labels_post_1_22.csv"],
            }
        ),
        encoding="utf-8",
    )
    return cache_dir


def _write_top_pocket(path: Path, *, path_predicates: str = "spread_wide & shock_ge_20pct") -> Path:
    pd.DataFrame(
        [
            {
                "mechanism": "up_shock_reversal",
                "window": "post_1_22",
                "shock_threshold": 0.08,
                "volume_spike_threshold": 1.0,
                "adv_min_dollars": 250000.0,
                "path_predicates": path_predicates,
                "active_event_count": 12,
                "event_month_count": 12,
                "issuer_count": 8,
                "mean_directional_return": 0.24,
                "t_stat": 9.0,
                "hit_rate": 1.0,
                "pocket_rank": 1,
                "tail_concentration_status": "pass",
            }
        ]
    ).to_csv(path, index=False)
    return path


def _write_search_grid(path: Path) -> Path:
    pd.DataFrame(
        [
            {"node_id": 1, "mechanism": "up_shock_reversal", "window": "post_1_22", "path_predicates": "spread_wide & shock_ge_20pct"},
            {"node_id": 2, "mechanism": "up_shock_reversal", "window": "post_1_10", "path_predicates": "spread_wide"},
            {"node_id": 3, "mechanism": "down_shock_continuation", "window": "post_1_22", "path_predicates": "spread_wide"},
        ]
    ).to_csv(path, index=False)
    return path
