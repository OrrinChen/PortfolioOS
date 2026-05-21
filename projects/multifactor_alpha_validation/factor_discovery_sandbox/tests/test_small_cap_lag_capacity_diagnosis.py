from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from factor_discovery_sandbox.small_cap_lag_capacity_diagnosis import (
    FIXED_WEIGHTING_SCHEMES,
    run_small_cap_lag_capacity_diagnosis,
)
from factor_discovery_sandbox.small_cap_temporal_diagnostics import build_signal_variants


def test_lag_construction_uses_past_signal_only(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    audit = pd.read_csv(result.artifacts["lag_construction_audit"])
    lagged = audit[audit["signal_variant"].str.startswith("lag_")]

    assert not lagged.empty
    assert lagged["uses_future_signal"].eq(False).all()
    assert lagged["max_source_rebalance_date"].le(lagged["rebalance_date"]).all()
    assert lagged["not_alpha_evidence"].eq(True).all()


def test_stale_signal_carry_forward_does_not_cross_assets() -> None:
    signal_panel = pd.DataFrame(
        [
            {
                "signal_id": "small_cap_quality_residual_momentum_6m_ex1m",
                "coverage_status": "active_view",
                "rebalance_date": "2021-01-31",
                "asset_id": "A",
                "score": 1.0,
            },
            {
                "signal_id": "small_cap_quality_residual_momentum_6m_ex1m",
                "coverage_status": "active_view",
                "rebalance_date": "2021-02-28",
                "asset_id": "A",
                "score": 2.0,
            },
            {
                "signal_id": "small_cap_quality_residual_momentum_6m_ex1m",
                "coverage_status": "active_view",
                "rebalance_date": "2021-01-31",
                "asset_id": "B",
                "score": 10.0,
            },
        ]
    )

    variants = build_signal_variants(signal_panel)
    stale = variants[variants["signal_variant"] == "stale_signal_carry_forward"]
    first_b = stale[(stale["asset_id"] == "B") & (stale["rebalance_date"] == "2021-01-31")]

    assert first_b["diagnostic_score"].isna().all()


def test_live_minus_lag_update_component_is_reported(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    update = pd.read_csv(result.artifacts["temporal_update_component_diagnostics"])
    decay = pd.read_csv(result.artifacts["lag_decay_grid"])

    assert "live_minus_lag_update_component" in set(decay["signal_variant"])
    assert {"update_component_rank_ic_1m", "update_component_spread_1m", "update_component_status"}.issubset(
        update.columns
    )


def test_signal_decay_grid_contains_live_lag_smoothed_variants(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    decay = pd.read_csv(result.artifacts["lag_decay_grid"])

    assert {
        "live_signal",
        "lag_1m_signal",
        "lag_2m_signal",
        "lag_3m_signal",
        "rolling_3m_mean_signal",
        "rolling_3m_median_signal",
        "stale_signal_carry_forward",
        "live_minus_lag_update_component",
    }.issubset(set(decay["signal_variant"]))
    assert {
        "rank_ic_1m",
        "rank_ic_3m",
        "spread_1m",
        "spread_3m",
        "spread_tstat",
        "ic_tstat",
        "active_count",
        "coverage_loss",
        "turnover",
        "cost_adjusted_spread",
        "subperiod_survival_rate",
        "placebo_status",
    }.issubset(decay.columns)


def test_holding_period_grid_contains_1m_3m_6m_and_monthly_quarterly(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    holding = pd.read_csv(result.artifacts["holding_period_sensitivity"])

    assert {1, 3, 6}.issubset(set(holding["holding_period_months"].astype(int)))
    assert {"monthly", "quarterly"}.issubset(set(holding["rebalance_frequency"]))
    assert {"gross_spread", "net_spread", "turnover", "cost_drag", "capacity_penalty", "rank_ic"}.issubset(
        holding.columns
    )


def test_capacity_bucket_diagnostics_include_mcap_adv_spread_price_buckets(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    buckets = pd.read_csv(result.artifacts["capacity_bucket_diagnostics"])

    assert {"market_cap", "adv", "spread", "price"}.issubset(set(buckets["bucket_type"]))
    assert {
        "equal_weight_spread",
        "value_weight_spread",
        "adv_weight_spread",
        "capacity_weight_spread",
        "gross_spread",
        "net_spread",
        "cost_drag",
        "turnover",
        "active_count",
    }.issubset(buckets.columns)


def test_weighting_scheme_comparison_uses_only_fixed_schemes(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    schemes = pd.read_csv(result.artifacts["weighting_scheme_comparison"])

    assert set(schemes["weighting_scheme"]) == set(FIXED_WEIGHTING_SCHEMES)
    assert schemes["learned_weighting_used"].eq(False).all()
    assert schemes["rolling_icir_used"].eq(False).all()
    assert schemes["ridge_weighting_used"].eq(False).all()


def test_learned_weighting_methods_are_forbidden() -> None:
    forbidden = {"rolling_icir", "ridge", "shrunk_icir", "signed_shrunk_icir"}

    assert forbidden.isdisjoint(set(FIXED_WEIGHTING_SCHEMES))


def test_cost_drag_decomposition_reports_gross_and_net_components(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    cost = pd.read_csv(result.artifacts["cost_drag_decomposition"])

    assert {
        "gross_spread",
        "half_spread_cost",
        "turnover_cost",
        "ADV_capacity_penalty",
        "estimated_impact_cost",
        "borrow_or_short_constraint_proxy",
        "net_spread",
        "spread_proxy_used",
        "cost_evidence_quality",
        "shortability_unknown",
    }.issubset(cost.columns)


def test_dominance_decision_blocks_allocator_q1_q2_registry(tmp_path: Path) -> None:
    result = _run_fixture(tmp_path)

    decision = json.loads(result.artifacts["small_cap_dominance_decision"].read_text(encoding="utf-8"))

    assert decision["decision_label"] in {
        "reject_temporal_noise",
        "revise_to_slow_signal_candidate",
        "revise_to_capacity_filtered_candidate",
        "revise_to_slow_capacity_filtered_candidate",
        "close_family",
        "diagnostic_only",
        "diagnostic_only_cost_blocked",
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


def _run_fixture(tmp_path: Path):
    output_dir = tmp_path / "quality_residual_momentum"
    output_dir.mkdir(parents=True)
    _write_cached_panels(output_dir)
    return run_small_cap_lag_capacity_diagnosis(
        family_output_dir=output_dir,
        report_path=tmp_path / "dominance_report.md",
    )


def _write_cached_panels(output_dir: Path) -> None:
    dates = pd.date_range("2021-01-31", periods=9, freq="ME")
    assets = [f"A{i:02d}" for i in range(8)]
    signal_rows = []
    target_rows = []
    for date_index, date in enumerate(dates):
        for asset_index, asset in enumerate(assets):
            score = asset_index + 0.15 * date_index + np.sin((date_index + asset_index) / 3.0)
            market_cap = 100_000_000 + asset_index * 50_000_000
            adv = 500_000 + asset_index * 250_000
            spread = 0.006 - asset_index * 0.00035
            price = 5.0 + asset_index * 2.5
            signal_rows.append(
                {
                    "schema_version": "fd_small_cap_signal_panel.v1",
                    "family_id": "small_cap_quality_residual_momentum_v1",
                    "signal_id": "small_cap_quality_residual_momentum_6m_ex1m",
                    "rebalance_date": date.date().isoformat(),
                    "asset_id": asset,
                    "ticker": asset,
                    "sector": "tech" if asset_index % 2 == 0 else "industrial",
                    "coverage_status": "active_view",
                    "score": score,
                    "raw_value": score,
                    "market_cap": market_cap,
                    "adv_3m": adv,
                    "spread_proxy": spread,
                    "price": price,
                    "fixed_single_signal_scoring": True,
                    "learned_weighting_used": False,
                    "rolling_icir_used": False,
                    "ridge_weighting_used": False,
                    "direct_q2_entry_allowed": False,
                    "not_alpha_evidence": True,
                }
            )
            for horizon in [1, 3, 6]:
                if date_index + horizon >= len(dates):
                    continue
                forward = 0.002 * asset_index + 0.0004 * date_index - 0.001 * horizon
                target_rows.append(
                    {
                        "rebalance_date": date.date().isoformat(),
                        "asset_id": asset,
                        "period": "test",
                        "horizon_months": horizon,
                        "forward_market_relative_return": forward,
                        "forward_asset_return": forward + 0.003,
                        "forward_market_relative_return_no_delisting": forward,
                        "market_cap": market_cap,
                    }
                )
    pd.DataFrame(signal_rows).to_csv(output_dir / "monthly_signal_panel_cache.csv", index=False)
    pd.DataFrame(target_rows).to_csv(output_dir / "forward_target_panel_cache.csv", index=False)
    (output_dir / "panel_cache_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "fd_small_cap_panel_cache_manifest.v1",
                "signal_panel_cache_status": "hit",
                "target_panel_cache_status": "hit",
                "direct_q2_entry_allowed": False,
                "not_alpha_evidence": True,
            }
        ),
        encoding="utf-8",
    )
