from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.failure_diagnosis import run_real_failure_diagnosis


def test_real_failure_diagnosis_flags_duplicate_residual_and_blocks_allocator(tmp_path: Path) -> None:
    artifacts = _write_failure_artifacts(tmp_path)

    result = run_real_failure_diagnosis(
        factor_panel_path=artifacts["factor_panel"],
        rolling_weights_path=artifacts["weights"],
        oos_score_panel_path=artifacts["scores"],
        placebo_report_path=artifacts["placebo"],
        output_dir=tmp_path / "fd_r5_1",
    )

    assert result.summary["schema_version"] == "fd_real_failure_diagnosis_summary.v1"
    assert result.summary["stage"] == "FD-R5.1"
    assert result.summary["allocator_entry_allowed"] is False
    assert result.summary["recommended_next_action"] == "stop_before_allocator"
    assert result.summary["not_alpha_evidence"] is True
    assert result.summary["failure_flags"] == {
        "data_timestamp_failure": False,
        "coverage_failure": False,
        "factor_definition_failure": True,
        "redundancy_failure": True,
        "rolling_icir_overfit_noise_failure": True,
        "sector_regime_contribution": "partial",
        "allocator_entry": "blocked",
    }

    assert {
        "standalone_factor_oos_diagnostics",
        "family_composite_diagnostics",
        "rolling_weight_failure_attribution",
        "real_factor_redundancy_clusters",
        "candidate_revision_recommendations",
        "factor_failure_diagnosis_report",
    } == set(result.artifacts)

    redundancy = pd.read_csv(result.artifacts["real_factor_redundancy_clusters"])
    residual_rows = redundancy[redundancy["factor_id"] == "residual_momentum_6m"]
    assert not residual_rows.empty
    assert residual_rows["recommended_action"].eq("rewrite_as_true_residual").any()
    assert (
        redundancy["cluster_members"].str.contains("momentum_6m")
        & redundancy["cluster_members"].str.contains("trend_slope_6m")
        & redundancy["cluster_members"].str.contains("residual_momentum_6m")
    ).any()

    family = pd.read_csv(result.artifacts["family_composite_diagnostics"])
    live = family[(family["candidate"] == "rolling_icir_live_composite") & (family["period"] == "test")]
    price_momentum = family[(family["candidate"] == "price_momentum_equal") & (family["period"] == "test")]
    assert float(live["mean_top_bottom_spread"].mean()) < 0.0
    assert float(price_momentum["mean_top_bottom_spread"].mean()) > float(live["mean_top_bottom_spread"].mean())
    assert family["not_alpha_evidence"].eq(True).all()

    attribution = pd.read_csv(result.artifacts["rolling_weight_failure_attribution"])
    assert {"factor_id", "family", "mean_abs_weight", "standalone_mean_top_bottom_spread", "attribution_status"}.issubset(
        attribution.columns
    )
    assert "overweighted_negative_or_noise_factor" in set(attribution["attribution_status"])

    recommendations = json.loads(result.artifacts["candidate_revision_recommendations"].read_text(encoding="utf-8"))
    assert recommendations["recommended_next_action"] == "stop_before_allocator"
    assert recommendations["allocator_entry_allowed"] is False
    assert "residual_momentum_6m_v2" in recommendations["rewrite_required"]
    assert "price_momentum" in recommendations["core_families"]
    assert "risk_volatility" in recommendations["diagnostic_only_families"]
    assert recommendations["archive_factors"] == []

    report = result.artifacts["factor_failure_diagnosis_report"].read_text(encoding="utf-8").lower()
    assert "fd-r5.1 factor failure diagnosis" in report
    assert "data/timestamp failure: no" in report
    assert "rolling icir overfit/noise failure: yes" in report
    assert "allocator entry: blocked" in report
    assert "not alpha evidence" in report


def _write_failure_artifacts(tmp_path: Path) -> dict[str, Path]:
    dates = pd.to_datetime(["2020-01-31", "2020-02-28", "2020-03-31", "2020-04-30"])
    assets = ["A", "B", "C", "D", "E"]
    base_values = {
        "A": 2.0,
        "B": 1.0,
        "C": 0.0,
        "D": -1.0,
        "E": -2.0,
    }
    factor_defs = {
        "momentum_6m": ("price_momentum", 1.0),
        "trend_slope_6m": ("trend_following", 1.0),
        "residual_momentum_6m": ("residual_momentum", 1.0),
        "momentum_3m": ("price_momentum", 0.7),
        "trend_slope_3m": ("trend_following", 0.7),
        "volatility_1m": ("risk_volatility", -1.0),
        "liquidity_dollar_volume_1m": ("liquidity_volume", -0.5),
        "reversal_1m": ("short_reversal", -1.0),
        "price_to_high_12m": ("price_position", 0.4),
        "drawdown_12m": ("price_position", 0.4),
    }
    panel_rows = []
    score_rows = []
    weight_rows = []
    placebo_rows = []
    for date_index, date in enumerate(dates):
        period = "validation" if date_index < 1 else "test"
        for asset in assets:
            base = base_values[asset] + date_index * 0.01
            forward = base * 0.01
            live_score = -base
            score_rows.append(
                {
                    "schema_version": "fd_real_oos_factor_score.v1",
                    "rebalance_date": date.date().isoformat(),
                    "period": period,
                    "horizon_months": 1,
                    "asset_id": asset,
                    "ticker": asset,
                    "score": live_score,
                    "coverage_state": "active_score",
                    "abstain_reason": "",
                    "available_weight_abs": 1.0,
                    "forward_asset_return": forward,
                    "forward_benchmark_return": 0.0,
                    "forward_excess_return": forward,
                    "forward_return_available": True,
                    "signal_timestamp": f"{date.date().isoformat()}T16:00:00",
                    "visibility_timestamp": f"{date.date().isoformat()}T23:00:00",
                    "tradable_timestamp": f"{(date + pd.Timedelta(days=1)).date().isoformat()}T16:00:00",
                    "target_return_visible_timestamp": f"{(date + pd.Timedelta(days=30)).date().isoformat()}T00:00:00",
                    "not_alpha_evidence": True,
                }
            )
            for factor_id, (family, multiplier) in factor_defs.items():
                value = base * multiplier
                panel_rows.append(
                    {
                        "schema_version": "fd_real_factor_panel.v1",
                        "factor_id": factor_id,
                        "date": date.date().isoformat(),
                        "rebalance_date": date.date().isoformat(),
                        "asset_id": asset,
                        "ticker": asset,
                        "raw_value": value,
                        "normalized_value": value,
                        "coverage_status": "active_view",
                        "abstain_reason": "",
                        "signal_timestamp": f"{date.date().isoformat()}T16:00:00",
                        "visibility_timestamp": f"{date.date().isoformat()}T23:00:00",
                        "tradable_timestamp": f"{(date + pd.Timedelta(days=1)).date().isoformat()}T16:00:00",
                        "known_correlation_family": family,
                        "no_view_is_not_zero_alpha": True,
                        "not_alpha_evidence": True,
                    }
                )
        for factor_id, (family, _multiplier) in factor_defs.items():
            weight = -0.5 if family == "risk_volatility" else 0.05
            weight_rows.append(
                {
                    "schema_version": "fd_real_rolling_icir.v1",
                    "rebalance_date": date.date().isoformat(),
                    "period": period,
                    "horizon_months": 1,
                    "estimation_window_start": "2019-01-31",
                    "estimation_window_end": "2019-12-31",
                    "return_visibility_cutoff": "2020-01-15",
                    "factor_id": factor_id,
                    "history_observation_count": 12,
                    "rolling_ic_mean": weight,
                    "rolling_ic_std": 1.0,
                    "rolling_icir": weight,
                    "weight": weight,
                    "weight_status": "active",
                    "uses_full_sample_icir": False,
                    "not_alpha_evidence": True,
                }
            )
        for test_name, spread, rank_ic in [
            ("live_oos_score", -0.02, -0.1),
            ("shuffled_cross_section_placebo", 0.0, 0.0),
            ("random_same_coverage_placebo", 0.01, 0.05),
            ("sector_neutral_placebo", -0.005, -0.02),
            ("future_return_leakage_negative_control", 0.20, 1.0),
        ]:
            placebo_rows.append(
                {
                    "schema_version": "fd_real_placebo_report.v1",
                    "test_name": test_name,
                    "period": period,
                    "horizon_months": 1,
                    "rebalance_count": 3,
                    "mean_rank_ic": rank_ic,
                    "mean_top_bottom_spread": spread,
                    "positive_spread_rate": 0.3,
                    "not_alpha_evidence": True,
                }
            )

    artifacts = {
        "factor_panel": tmp_path / "real_factor_panel.csv",
        "scores": tmp_path / "oos_factor_score_panel_real.csv",
        "weights": tmp_path / "rolling_icir_real.csv",
        "placebo": tmp_path / "placebo_report.csv",
    }
    pd.DataFrame(panel_rows).to_csv(artifacts["factor_panel"], index=False)
    pd.DataFrame(score_rows).to_csv(artifacts["scores"], index=False)
    pd.DataFrame(weight_rows).to_csv(artifacts["weights"], index=False)
    pd.DataFrame(placebo_rows).to_csv(artifacts["placebo"], index=False)
    return artifacts
