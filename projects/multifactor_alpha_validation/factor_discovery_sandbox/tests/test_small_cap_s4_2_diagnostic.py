from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from factor_discovery_sandbox.small_cap_s4_2_diagnostic import (
    S4_2_SIGNAL_VARIANTS,
    S4_2_WEIGHTING_SCHEMES,
    run_small_cap_s4_2_diagnostic,
)
from factor_discovery_sandbox.small_cap_target_cache import write_forward_target_cache_from_panel


def test_target_cache_includes_1m_3m_6m_forward_returns(tmp_path: Path) -> None:
    target = _target_panel(include_6m=True)

    artifacts = write_forward_target_cache_from_panel(target, tmp_path / "target_cache")
    cached = pd.read_csv(artifacts["forward_returns"])
    audit = json.loads(artifacts["audit"].read_text(encoding="utf-8"))

    assert {1, 3, 6}.issubset(set(cached["horizon_months"].astype(int)))
    assert {1, 3, 6}.issubset({row["target_horizon"] for row in audit["horizon_audit"]})
    assert audit["all_required_horizons_available"] is True
    assert audit["not_alpha_evidence"] is True


def test_s4_2_blocks_candidate_decision_when_6m_target_unavailable(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path, include_6m=False)

    decision = json.loads(result.artifacts["s4_2_decision"].read_text(encoding="utf-8"))

    assert decision["six_month_target_available"] is False
    assert decision["decision_label"] != "revise_to_pre_registered_v2_candidate"
    assert decision["not_alpha_evidence"] is True


def test_pre_registered_filter_manifest_is_written_before_evaluation(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    manifest = json.loads(result.artifacts["pre_registered_filter_manifest"].read_text(encoding="utf-8"))
    decision = json.loads(result.artifacts["s4_2_decision"].read_text(encoding="utf-8"))

    assert manifest["filter_locked_before_evaluation"] is True
    assert manifest["manifest_written_before_evaluation"] is True
    assert decision["pre_registered_filter_manifest"] == str(result.artifacts["pre_registered_filter_manifest"])


def test_capacity_filter_excludes_microcap_low_adv_wide_spread_low_price(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    oos = pd.read_csv(result.artifacts["capacity_filtered_oos"])

    assert oos["microcap_rows_after_filter"].eq(0).all()
    assert oos["low_adv_rows_after_filter"].eq(0).all()
    assert oos["wide_spread_rows_after_filter"].eq(0).all()
    assert oos["low_price_rows_after_filter"].eq(0).all()


def test_slow_signal_grid_only_contains_live_lag1_rolling_mean_rolling_median(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    grid = pd.read_csv(result.artifacts["slow_signal_validation_grid"])

    assert set(grid["signal_variant"]) == set(S4_2_SIGNAL_VARIANTS)
    assert set(S4_2_SIGNAL_VARIANTS) == {
        "live_signal",
        "lag_1m_signal",
        "rolling_3m_mean_signal",
        "rolling_3m_median_signal",
    }
    assert {1, 3, 6}.issubset(set(grid["horizon_months"].astype(int)))


def test_s4_2_forbids_learned_weighting(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    grid = pd.read_csv(result.artifacts["slow_signal_validation_grid"])
    decision = json.loads(result.artifacts["s4_2_decision"].read_text(encoding="utf-8"))

    assert set(grid["weighting_scheme"]) == set(S4_2_WEIGHTING_SCHEMES)
    assert grid["learned_weighting_used"].eq(False).all()
    assert grid["rolling_icir_used"].eq(False).all()
    assert grid["ridge_weighting_used"].eq(False).all()
    assert decision["learned_weighting_used"] is False


def test_cost_adjusted_survival_reports_gross_net_cost_capacity(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    survival = pd.read_csv(result.artifacts["cost_adjusted_survival"])

    assert {
        "gross_spread",
        "cost_adjusted_spread",
        "cost_drag",
        "capacity_penalty",
        "subperiod_survival_rate",
        "placebo_status",
    }.issubset(survival.columns)


def test_s4_2_decision_blocks_allocator_q1_q2_registry(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    decision = json.loads(result.artifacts["s4_2_decision"].read_text(encoding="utf-8"))

    assert decision["decision_label"] in {
        "reject_temporal_noise_confirmed",
        "reject_capacity_filter_not_enough",
        "diagnostic_only_cost_blocked",
        "diagnostic_only_shortability_blocked",
        "revise_to_pre_registered_v2_candidate",
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


def _run_fixture(tmp_path: Path, include_6m: bool = True):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    signal_path = source_dir / "monthly_signal_panel_cache.csv"
    target_path = source_dir / "forward_target_panel_cache.csv"
    _signal_panel().to_csv(signal_path, index=False)
    _target_panel(include_6m=include_6m).to_csv(target_path, index=False)
    output_dir = tmp_path / "family_candidates" / "quality_residual_momentum_s4_2"
    return run_small_cap_s4_2_diagnostic(
        source_signal_panel_path=signal_path,
        source_target_panel_path=target_path,
        target_cache_output_dir=tmp_path / "target_cache",
        output_dir=output_dir,
        report_path=tmp_path / "s4_2_report.md",
    )


def _signal_panel() -> pd.DataFrame:
    dates = pd.date_range("2021-01-31", periods=12, freq="ME")
    assets = [f"A{i:02d}" for i in range(10)]
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
                    "sector": "tech" if asset_index % 2 == 0 else "industrial",
                    "universe_tier": "microcap_quarantine" if asset_index == 0 else "small_cap_investable",
                    "quality_control_status": "quality_controlled",
                    "evidence_quality": "standard",
                    "coverage_status": "active_view",
                    "score": asset_index + 0.1 * date_index + np.sin(date_index / 2.0),
                    "market_cap": 50_000_000 + asset_index * 80_000_000,
                    "adv_3m": 100_000 + asset_index * 400_000,
                    "spread_proxy": 0.012 - asset_index * 0.0009,
                    "beta_6m": 0.7 + asset_index * 0.03,
                    "fixed_single_signal_scoring": True,
                    "learned_weighting_used": False,
                    "rolling_icir_used": False,
                    "ridge_weighting_used": False,
                    "direct_q2_entry_allowed": False,
                    "not_alpha_evidence": True,
                }
            )
    return pd.DataFrame(rows)


def _target_panel(include_6m: bool) -> pd.DataFrame:
    dates = pd.date_range("2021-01-31", periods=12, freq="ME")
    assets = [f"A{i:02d}" for i in range(10)]
    rows = []
    for date_index, date in enumerate(dates):
        for asset_index, asset in enumerate(assets):
            for horizon in ([1, 3, 6] if include_6m else [1, 3]):
                if date_index + horizon >= len(dates):
                    continue
                forward = 0.003 * asset_index + 0.0005 * date_index - 0.0008 * horizon
                rows.append(
                    {
                        "rebalance_date": date.date().isoformat(),
                        "asset_id": asset,
                        "period": "validation" if date_index < 4 else "test",
                        "horizon_months": horizon,
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
                        "market_cap": 50_000_000 + asset_index * 80_000_000,
                        "price": 2.0 + asset_index * 1.5,
                    }
                )
    return pd.DataFrame(rows)
