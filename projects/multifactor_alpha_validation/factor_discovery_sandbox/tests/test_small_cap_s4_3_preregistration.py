from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from factor_discovery_sandbox.small_cap_s4_3_preregistration import (
    S4_3_FORBIDDEN_SIGNAL_VARIANTS,
    S4_3_WEIGHTING_SCHEMES,
    run_small_cap_s4_3_preregistration,
)


def test_s4_3_closes_slow_signal_branch(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    closeout = json.loads(result.artifacts["slow_signal_closeout"].read_text(encoding="utf-8"))

    assert closeout["s4_2_decision"] == "reject_temporal_noise_confirmed"
    assert closeout["lagged_signal_beats_live_after_capacity_filter"] is False
    assert closeout["smoothed_signal_beats_live_after_capacity_filter"] is False
    assert closeout["slow_signal_branch_closed"] is True
    assert closeout["not_alpha_evidence"] is True


def test_capacity_filter_manifest_locks_live_3m_quarterly_before_evaluation(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    manifest = json.loads(result.artifacts["capacity_filter_preregistration_manifest"].read_text(encoding="utf-8"))

    assert manifest["filter_locked_before_evaluation"] is True
    assert manifest["manifest_written_before_evaluation"] is True
    assert manifest["signal_variant"] == "live_signal"
    assert manifest["target_horizon"] == "3m"
    assert manifest["rebalance_frequency"] == "quarterly"
    assert manifest["primary_weighting_scheme"] == "adv_weight_within_bucket"
    assert manifest["control_weighting_scheme"] == "capacity_capped_equal_weight"
    assert manifest["chosen_from_s4_2_diagnostic"] is True


def test_s4_3_forbids_lagged_and_smoothed_signal_variants(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    oos = pd.read_csv(result.artifacts["capacity_filtered_live_signal_oos"])
    confirmation = pd.read_csv(result.artifacts["fixed_weighting_confirmation"])

    assert set(oos["signal_variant"]) == {"live_signal"}
    assert set(confirmation["signal_variant"]) == {"live_signal"}
    assert S4_3_FORBIDDEN_SIGNAL_VARIANTS == {
        "lag_1m_signal",
        "lag_2m_signal",
        "lag_3m_signal",
        "rolling_3m_mean_signal",
        "rolling_3m_median_signal",
        "stale_signal_carry_forward",
    }


def test_s4_3_forbids_learned_weighting(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    confirmation = pd.read_csv(result.artifacts["fixed_weighting_confirmation"])
    decision = json.loads(result.artifacts["s4_3_decision"].read_text(encoding="utf-8"))

    assert set(confirmation["weighting_scheme"]) == set(S4_3_WEIGHTING_SCHEMES)
    assert confirmation["rolling_icir_used"].eq(False).all()
    assert confirmation["ridge_weighting_used"].eq(False).all()
    assert confirmation["shrunk_icir_used"].eq(False).all()
    assert confirmation["learned_weighting_used"].eq(False).all()
    assert decision["learned_weighting_used"] is False


def test_confirmation_split_required_for_pre_register_decision(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path, months=5)

    split = json.loads(result.artifacts["confirmation_split_manifest"].read_text(encoding="utf-8"))
    decision = json.loads(result.artifacts["s4_3_decision"].read_text(encoding="utf-8"))

    assert split["confirmation_available"] is False
    assert decision["decision_label"] != "pre_register_capacity_filtered_v2"


def test_capacity_filter_excludes_microcap_low_adv_wide_spread_low_price(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    oos = pd.read_csv(result.artifacts["capacity_filtered_live_signal_oos"])

    assert oos["microcap_rows_after_filter"].eq(0).all()
    assert oos["low_adv_rows_after_filter"].eq(0).all()
    assert oos["wide_spread_rows_after_filter"].eq(0).all()
    assert oos["low_price_rows_after_filter"].eq(0).all()


def test_placebo_comparison_includes_same_coverage_capacity_matched_and_rebalance_shifted(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    placebo = pd.read_csv(result.artifacts["placebo_comparison"])

    assert set(placebo["placebo_type"]) == {
        "same_coverage_placebo",
        "capacity_matched_placebo",
        "rebalance_shifted_placebo",
    }


def test_shortability_boundary_blocks_long_short_tradability_claim(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    boundary = json.loads(result.artifacts["shortability_boundary_report"].read_text(encoding="utf-8"))
    decision = json.loads(result.artifacts["s4_3_decision"].read_text(encoding="utf-8"))

    assert boundary["shortability_unknown"] is True
    assert boundary["borrow_data_available"] is False
    assert boundary["long_short_tradability_claimed"] is False
    assert boundary["production_approval_claimed"] is False
    assert decision["decision_label"] != "pre_register_capacity_filtered_v2"


def test_s4_3_decision_blocks_allocator_q1_q2_registry(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    decision = json.loads(result.artifacts["s4_3_decision"].read_text(encoding="utf-8"))

    assert decision["decision_label"] in {
        "reject_capacity_filter_hypothesis",
        "diagnostic_only_replicated",
        "diagnostic_only_cost_blocked",
        "diagnostic_only_shortability_blocked",
        "pre_register_capacity_filtered_v2",
        "close_family",
    }
    assert decision["allocator_entry_allowed"] is False
    assert decision["q1_entry_allowed"] is False
    assert decision["q2_entry_allowed"] is False
    assert decision["alpha_registry_update_allowed"] is False
    assert decision["production_approval_claimed"] is False
    assert decision["direct_q2_entry_allowed"] is False
    assert decision["not_alpha_evidence"] is True


def test_outputs_are_not_alpha_evidence(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    for name, path in result.artifacts.items():
        if path.suffix == ".csv":
            frame = pd.read_csv(path)
            assert "not_alpha_evidence" in frame.columns, name
            assert frame["not_alpha_evidence"].eq(True).all(), name
        elif path.suffix == ".json":
            payload = json.loads(path.read_text(encoding="utf-8"))
            assert payload["not_alpha_evidence"] is True, name


def _run_fixture(tmp_path: Path, months: int = 18):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    signal_path = source_dir / "monthly_signal_panel_cache.csv"
    target_path = source_dir / "forward_target_panel_cache.csv"
    _signal_panel(months=months).to_csv(signal_path, index=False)
    _target_panel(months=months).to_csv(target_path, index=False)
    s4_2_decision_path = source_dir / "s4_2_decision.json"
    s4_2_grid_path = source_dir / "slow_signal_validation_grid.csv"
    s4_2_decision_path.write_text(
        json.dumps(
            {
                "schema_version": "fd_small_cap_s4_2_decision.v1",
                "decision_label": "reject_temporal_noise_confirmed",
                "lag_or_smoothed_beats_live": False,
                "best_cost_adjusted_spread": 0.10115,
                "not_alpha_evidence": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    _s4_2_grid().to_csv(s4_2_grid_path, index=False)
    output_dir = tmp_path / "family_candidates" / "capacity_filtered_live_s4_3"
    return run_small_cap_s4_3_preregistration(
        source_signal_panel_path=signal_path,
        source_target_cache_path=target_path,
        s4_2_decision_path=s4_2_decision_path,
        s4_2_grid_path=s4_2_grid_path,
        output_dir=output_dir,
        report_path=tmp_path / "s4_3_report.md",
    )


def _signal_panel(months: int) -> pd.DataFrame:
    dates = pd.date_range("2021-01-31", periods=months, freq="ME")
    assets = [f"A{i:02d}" for i in range(12)]
    rows = []
    for date_index, date in enumerate(dates):
        for asset_index, asset in enumerate(assets):
            rows.append(
                {
                    "schema_version": "fd_small_cap_signal_panel.v1",
                    "family_id": "small_cap_quality_residual_momentum_v1",
                    "signal_id": "small_cap_quality_residual_momentum_6m_ex1m",
                    "rebalance_date": date.date().isoformat(),
                    "asset_id": asset,
                    "ticker": asset,
                    "sector": "tech" if asset_index % 3 == 0 else "industrial",
                    "universe_tier": "microcap_quarantine" if asset_index == 0 else "small_cap_investable",
                    "coverage_status": "active_view",
                    "score": asset_index + 0.2 * np.sin(date_index / 3.0),
                    "market_cap": 35_000_000 + asset_index * 120_000_000,
                    "adv_3m": 50_000 + asset_index * 500_000,
                    "spread_proxy": 0.018 - asset_index * 0.0012,
                    "beta_6m": 0.65 + asset_index * 0.025,
                    "fixed_single_signal_scoring": True,
                    "learned_weighting_used": False,
                    "rolling_icir_used": False,
                    "ridge_weighting_used": False,
                    "direct_q2_entry_allowed": False,
                    "not_alpha_evidence": True,
                }
            )
    return pd.DataFrame(rows)


def _target_panel(months: int) -> pd.DataFrame:
    dates = pd.date_range("2021-01-31", periods=months, freq="ME")
    assets = [f"A{i:02d}" for i in range(12)]
    rows = []
    for date_index, date in enumerate(dates):
        for asset_index, asset in enumerate(assets):
            if date_index + 3 >= len(dates):
                continue
            forward = 0.004 * asset_index + 0.0007 * date_index
            rows.append(
                {
                    "rebalance_date": date.date().isoformat(),
                    "asset_id": asset,
                    "period": "test",
                    "horizon_months": 3,
                    "forward_asset_return": forward + 0.002,
                    "forward_asset_return_no_delisting": forward + 0.002,
                    "delisting_return_applied": np.nan,
                    "forward_market_relative_return": forward,
                    "forward_market_relative_return_no_delisting": forward,
                    "forward_small_cap_relative_return": forward - 0.001,
                    "forward_smb_adjusted_return": forward - 0.0005,
                    "forward_sector_adjusted_return": forward - 0.0002,
                    "forward_liquidity_adjusted_return": forward - 0.0003,
                    "forward_cost_adjusted_return": forward - 0.002,
                    "market_cap": 35_000_000 + asset_index * 120_000_000,
                    "price": 2.0 + asset_index * 1.4,
                }
            )
    return pd.DataFrame(rows)


def _s4_2_grid() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "signal_variant": "live_signal",
                "horizon_months": 3,
                "rebalance_frequency": "quarterly",
                "weighting_scheme": "adv_weight_within_bucket",
                "cost_adjusted_spread": 0.10115,
                "not_alpha_evidence": True,
            },
            {
                "signal_variant": "lag_1m_signal",
                "horizon_months": 3,
                "rebalance_frequency": "quarterly",
                "weighting_scheme": "adv_weight_within_bucket",
                "cost_adjusted_spread": 0.07138,
                "not_alpha_evidence": True,
            },
        ]
    )
