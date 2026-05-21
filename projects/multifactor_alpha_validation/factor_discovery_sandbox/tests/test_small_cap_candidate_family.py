from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from factor_discovery_sandbox.small_cap_data_admission import run_small_cap_data_admission
from factor_discovery_sandbox.small_cap_quality_family import (
    _select_decision,
    run_small_cap_quality_residual_momentum,
)
from factor_discovery_sandbox.small_cap_universe import build_small_cap_universe_tiers


def test_small_cap_data_admission_requires_delisting_handling(tmp_path: Path) -> None:
    manifest = _write_small_cap_fixture(tmp_path, include_delisting=False)

    result = run_small_cap_data_admission(manifest, tmp_path / "small_cap")

    report = json.loads(result.artifacts["data_admission_report"].read_text(encoding="utf-8"))
    assert report["small_cap_research_admitted"] is False
    assert report["candidate_family_run_allowed"] is False
    assert report["delisting_handling_status"] == "fail"
    assert report["q1_entry_allowed"] is False
    assert report["q2_entry_allowed"] is False
    assert report["not_alpha_evidence"] is True


def test_microcap_quarantine_is_diagnostic_only(tmp_path: Path) -> None:
    manifest = _write_small_cap_fixture(tmp_path)
    payload = yaml.safe_load(manifest.read_text(encoding="utf-8"))
    prices = pd.read_csv(payload["prices"]["path"])
    universe = pd.read_csv(payload["universe"]["path"])

    tiers = build_small_cap_universe_tiers(prices=prices, universe=universe)
    microcap = tiers[tiers["universe_tier"] == "microcap_quarantine"]

    assert not microcap.empty
    assert microcap["diagnostic_only"].eq(True).all()
    assert microcap["candidate_decision_allowed"].eq(False).all()
    assert microcap["not_alpha_evidence"].eq(True).all()


def test_small_cap_family_uses_fixed_single_signal_not_learned_weighting(tmp_path: Path) -> None:
    manifest = _write_small_cap_fixture(tmp_path)

    result = run_small_cap_quality_residual_momentum(
        manifest_path=manifest,
        output_dir=tmp_path / "small_cap" / "family",
        report_path=tmp_path / "report.md",
    )

    family_manifest = json.loads(result.artifacts["family_manifest"].read_text(encoding="utf-8"))
    design_manifest = json.loads(result.artifacts["candidate_design_manifest"].read_text(encoding="utf-8"))
    decision = json.loads(result.artifacts["family_decision"].read_text(encoding="utf-8"))
    assert family_manifest["family_id"] == "small_cap_quality_residual_momentum_v1"
    assert family_manifest["primary_signal"] == "small_cap_quality_residual_momentum_6m_ex1m"
    assert family_manifest["design_contract_valid"] is True
    assert family_manifest["formula_is_measurement_not_thesis"] is True
    assert "market_pain_point" in family_manifest["design_contract"]
    assert design_manifest["candidate_validation_allowed"] is True
    assert design_manifest["design_contract_valid"] is True
    assert family_manifest["fixed_single_signal_scoring"] is True
    assert family_manifest["rolling_icir_used"] is False
    assert family_manifest["ridge_weighting_used"] is False
    assert family_manifest["learned_weighting_used"] is False
    assert decision["allocator_entry_allowed"] is False
    assert result.summary["not_alpha_evidence"] is True


def test_missing_quality_marks_degraded_or_no_view(tmp_path: Path) -> None:
    manifest = _write_small_cap_fixture(tmp_path, include_quality=False)

    result = run_small_cap_quality_residual_momentum(
        manifest_path=manifest,
        output_dir=tmp_path / "small_cap" / "family",
        report_path=tmp_path / "report.md",
    )

    signal = pd.read_csv(result.artifacts["signal_panel"])
    primary = signal[signal["signal_id"] == "small_cap_quality_residual_momentum_6m_ex1m"]
    assert not primary.empty
    assert set(primary["quality_control_status"]) <= {"no_quality_variant"}
    assert set(primary["evidence_quality"]) <= {"degraded", "no_view"}
    assert primary["no_view_is_not_zero_alpha"].eq(True).all()
    decision = json.loads(result.artifacts["family_decision"].read_text(encoding="utf-8"))
    assert decision["decision_label"] != "candidate_for_phase64_review"


def test_degraded_no_quality_signal_cannot_be_phase64_candidate() -> None:
    signal_panel = pd.DataFrame(
        [
            {
                "signal_id": "small_cap_quality_residual_momentum_6m_ex1m",
                "coverage_status": "active_view",
                "evidence_quality": "degraded",
            },
            {
                "signal_id": "small_cap_quality_residual_momentum_6m_ex1m",
                "coverage_status": "no_view",
                "evidence_quality": "no_view",
            },
        ]
    )
    oos = pd.DataFrame(
        [
            {
                "signal_id": "small_cap_quality_residual_momentum_6m_ex1m",
                "period": "test",
                "return_adjustment": "market_relative",
                "rank_ic": 0.02,
                "top_bottom_spread": 0.01,
            }
        ]
    )
    placebo = pd.DataFrame({"control_beats_live": [False]})
    exposure = pd.DataFrame({"exposure_name": ["liquidity_exposure"], "exposure_value": [0.1]})

    assert _select_decision(signal_panel, oos, placebo, exposure) == "calibration_only"


def test_family_loads_pit_quality_scores_from_manifest_section(tmp_path: Path) -> None:
    manifest = _write_small_cap_fixture(tmp_path, include_quality=False)
    _attach_quality_score_panel(manifest)

    result = run_small_cap_quality_residual_momentum(
        manifest_path=manifest,
        output_dir=tmp_path / "small_cap" / "family",
        report_path=tmp_path / "report.md",
    )

    signal = pd.read_csv(result.artifacts["signal_panel"])
    primary = signal[
        (signal["signal_id"] == "small_cap_quality_residual_momentum_6m_ex1m")
        & (signal["coverage_status"] == "active_view")
    ]
    assert not primary.empty
    assert set(primary["quality_control_status"]) == {"quality_controlled"}
    assert set(primary["evidence_quality"]) == {"standard"}
    assert primary["quality_score"].notna().all()
    assert primary["residualization_controls"].str.contains("quality_score").all()


def test_placebo_controls_are_realized_return_diagnostics_not_scaled_live(tmp_path: Path) -> None:
    manifest = _write_small_cap_fixture(tmp_path)

    result = run_small_cap_quality_residual_momentum(
        manifest_path=manifest,
        output_dir=tmp_path / "small_cap" / "family",
        report_path=tmp_path / "report.md",
    )

    placebo = pd.read_csv(result.artifacts["placebo_comparison"])
    assert "control_method" in placebo.columns
    assert "uses_realized_forward_returns" in placebo.columns
    assert placebo["uses_realized_forward_returns"].eq(True).all()
    methods = set(placebo["control_method"])
    assert {
        "deterministic_permutation",
        "within_size_bucket_permutation",
        "within_sector_permutation",
        "asset_rebalance_lag",
        "without_delisting_adjustment",
        "value_weighted_live",
    }.issubset(methods)


def test_cost_capacity_pre_gate_is_written_and_blocks_approval_paths(tmp_path: Path) -> None:
    manifest = _write_small_cap_fixture(tmp_path)

    result = run_small_cap_quality_residual_momentum(
        manifest_path=manifest,
        output_dir=tmp_path / "small_cap" / "family",
        report_path=tmp_path / "report.md",
    )

    assert "cost_capacity_pre_gate" in result.artifacts
    gate = pd.read_csv(result.artifacts["cost_capacity_pre_gate"])
    assert {
        "pre_gate_status",
        "gross_mean_spread",
        "cost_adjusted_mean_spread",
        "capacity_usd_1pct_adv",
        "not_alpha_evidence",
        "direct_q2_entry_allowed",
    }.issubset(gate.columns)
    assert gate["not_alpha_evidence"].eq(True).all()
    assert gate["direct_q2_entry_allowed"].eq(False).all()


def test_family_writes_and_reuses_monthly_signal_target_cache(tmp_path: Path) -> None:
    manifest = _write_small_cap_fixture(tmp_path)
    output_dir = tmp_path / "small_cap" / "family"

    first = run_small_cap_quality_residual_momentum(
        manifest_path=manifest,
        output_dir=output_dir,
        report_path=tmp_path / "report.md",
    )

    assert {"monthly_signal_panel_cache", "forward_target_panel_cache", "panel_cache_manifest"}.issubset(
        first.artifacts
    )
    first_cache = json.loads(first.artifacts["panel_cache_manifest"].read_text(encoding="utf-8"))
    assert first_cache["signal_panel_cache_status"] == "miss"
    assert first_cache["target_panel_cache_status"] == "miss"
    target_cache = pd.read_csv(first.artifacts["forward_target_panel_cache"])
    assert {"rebalance_date", "asset_id", "forward_market_relative_return"}.issubset(target_cache.columns)

    second = run_small_cap_quality_residual_momentum(
        manifest_path=manifest,
        output_dir=output_dir,
        report_path=tmp_path / "report.md",
    )

    second_cache = json.loads(second.artifacts["panel_cache_manifest"].read_text(encoding="utf-8"))
    assert second_cache["signal_panel_cache_status"] == "hit"
    assert second_cache["target_panel_cache_status"] == "hit"
    assert second_cache["not_alpha_evidence"] is True
    assert second_cache["direct_q2_entry_allowed"] is False


def test_placebo_dominance_diagnosis_explains_lag_and_value_weight_controls(tmp_path: Path) -> None:
    manifest = _write_small_cap_fixture(tmp_path)

    result = run_small_cap_quality_residual_momentum(
        manifest_path=manifest,
        output_dir=tmp_path / "small_cap" / "family",
        report_path=tmp_path / "report.md",
    )

    assert "placebo_dominance_diagnosis" in result.artifacts
    diagnosis = pd.read_csv(result.artifacts["placebo_dominance_diagnosis"])
    assert {
        "control_name",
        "likely_driver",
        "evidence_metric_1_name",
        "evidence_metric_1_value",
        "recommended_action",
        "not_alpha_evidence",
        "direct_q2_entry_allowed",
    }.issubset(diagnosis.columns)
    assert {
        "rebalance_date_shifted_signal",
        "equal_weight_vs_value_weight_comparison",
    }.issubset(set(diagnosis["control_name"]))
    assert diagnosis["recommended_action"].astype(str).str.len().gt(0).all()
    assert diagnosis["not_alpha_evidence"].eq(True).all()
    assert diagnosis["direct_q2_entry_allowed"].eq(False).all()


def test_signal_residualizes_sector_beta_size_liquidity(tmp_path: Path) -> None:
    manifest = _write_small_cap_fixture(tmp_path)

    result = run_small_cap_quality_residual_momentum(
        manifest_path=manifest,
        output_dir=tmp_path / "small_cap" / "family",
        report_path=tmp_path / "report.md",
    )

    signal = pd.read_csv(result.artifacts["signal_panel"])
    primary = signal[
        (signal["signal_id"] == "small_cap_quality_residual_momentum_6m_ex1m")
        & (signal["coverage_status"] == "active_view")
    ]
    assert not primary.empty
    assert primary["residualization_controls"].str.contains("sector").all()
    assert primary["residualization_controls"].str.contains("beta").all()
    assert primary["residualization_controls"].str.contains("log_market_cap").all()
    assert primary["residualization_controls"].str.contains("log_adv_3m").all()
    assert not np.allclose(primary["raw_momentum_6m_ex1m"], primary["score"])


def test_negative_controls_include_size_and_liquidity_matched_placebos(tmp_path: Path) -> None:
    manifest = _write_small_cap_fixture(tmp_path)

    result = run_small_cap_quality_residual_momentum(
        manifest_path=manifest,
        output_dir=tmp_path / "small_cap" / "family",
        report_path=tmp_path / "report.md",
    )

    placebo = pd.read_csv(result.artifacts["placebo_comparison"])
    assert {
        "random_same_mcap_adv_coverage",
        "size_bucket_shuffled_signal",
        "sector_shuffled_signal",
        "rebalance_date_shifted_signal",
        "delisting_return_removed_sensitivity",
        "equal_weight_vs_value_weight_comparison",
    }.issubset(set(placebo["control_name"]))


def test_family_decision_blocks_allocator_q1_q2_registry(tmp_path: Path) -> None:
    manifest = _write_small_cap_fixture(tmp_path)

    result = run_small_cap_quality_residual_momentum(
        manifest_path=manifest,
        output_dir=tmp_path / "small_cap" / "family",
        report_path=tmp_path / "report.md",
    )

    decision = json.loads(result.artifacts["family_decision"].read_text(encoding="utf-8"))
    assert decision["decision_label"] in {
        "reject_data_admission",
        "reject_no_signal",
        "reject_microcap_only",
        "reject_liquidity_exposure",
        "reject_placebo_failure",
        "calibration_only",
        "candidate_for_phase64_review",
    }
    assert decision["allocator_entry_allowed"] is False
    assert decision["q1_entry_allowed"] is False
    assert decision["q2_entry_allowed"] is False
    assert decision["alpha_registry_update_allowed"] is False
    assert decision["production_approval_claimed"] is False
    assert decision["direct_q2_entry_allowed"] is False
    assert decision["not_alpha_evidence"] is True


def test_outputs_are_not_alpha_evidence(tmp_path: Path) -> None:
    manifest = _write_small_cap_fixture(tmp_path)

    result = run_small_cap_quality_residual_momentum(
        manifest_path=manifest,
        output_dir=tmp_path / "small_cap" / "family",
        report_path=tmp_path / "report.md",
    )

    for key in [
        "universe_tiering_report",
        "signal_panel",
        "oos_validation",
        "placebo_comparison",
        "placebo_dominance_diagnosis",
        "exposure_attribution",
        "cost_capacity_pre_gate",
    ]:
        frame = pd.read_csv(result.artifacts[key])
        assert "not_alpha_evidence" in frame.columns
        assert frame["not_alpha_evidence"].eq(True).all()

    report = result.artifacts["family_report"].read_text(encoding="utf-8").lower()
    assert "not alpha evidence" in report
    assert "allocator entry: blocked" in report
    assert "q2 entry: blocked" in report


def _write_small_cap_fixture(
    tmp_path: Path,
    *,
    include_delisting: bool = True,
    include_quality: bool = True,
) -> Path:
    dates = pd.bdate_range("2020-01-02", periods=210)
    assets = [f"A{i:02d}" for i in range(12)]
    sectors = {asset: "tech" if index % 2 == 0 else "industrial" for index, asset in enumerate(assets)}
    price_rows = []
    universe_rows = []
    for asset_index, asset in enumerate(assets):
        base_cap = [50, 70, 90, 130, 170, 240, 320, 450, 700, 950, 1300, 1800][asset_index] * 1_000_000.0
        shares = 10_000_000.0 + asset_index * 500_000.0
        base_price = base_cap / shares
        quality = -1.0 + asset_index / 5.0
        universe_rows.append(
            {
                "asset_id": asset,
                "permno": asset,
                "ticker": asset,
                "membership_start": dates[0].date().isoformat(),
                "membership_end": dates[-1].date().isoformat(),
                "date": dates[0].date().isoformat(),
                "as_of_timestamp": dates[0].date().isoformat(),
                "source_is_pit": True,
                "in_universe": True,
                "sector": sectors[asset],
                "industry": f"{sectors[asset]}_industry",
                "exchange_code": "NYSE",
                "share_code": 10,
                "common_share": True,
            }
        )
        for day_index, date in enumerate(dates):
            drift = 0.0005 * (asset_index - 5.5)
            seasonal = 0.015 * np.sin(day_index / 13.0 + asset_index)
            adjusted_close = base_price * np.exp(drift * day_index + seasonal)
            adjusted_open = adjusted_close * (1.0 - 0.001)
            volume = 60_000 + asset_index * 25_000 + (day_index % 7) * 1000
            row = {
                "asset_id": asset,
                "permno": asset,
                "ticker": asset,
                "date": date.date().isoformat(),
                "raw_open": adjusted_open,
                "raw_close": adjusted_close,
                "adjusted_open": adjusted_open,
                "adjusted_close": adjusted_close,
                "volume": volume,
                "return": drift,
                "shares_outstanding": shares,
                "market_cap": adjusted_close * shares,
                "sector": sectors[asset],
                "exchange_code": "NYSE",
                "share_code": 10,
                "bid_ask_spread": 0.002 + asset_index * 0.0001,
                "adjusted_price_convention": "fixture_adjusted",
            }
            if include_quality:
                row["quality_score"] = quality
            price_rows.append(row)

    benchmark_rows = [
        {
            "date": date.date().isoformat(),
            "benchmark": "IWM",
            "raw_open": 100.0 + index * 0.01,
            "raw_close": 100.1 + index * 0.01,
            "adjusted_open": 100.0 + index * 0.01,
            "adjusted_close": 100.1 + index * 0.01,
            "volume": 1_000_000,
            "return": 0.0001,
            "adjusted_price_convention": "fixture_adjusted",
        }
        for index, date in enumerate(dates)
    ]
    delisting_rows = (
        [
            {
                "asset_id": "A00",
                "permno": "A00",
                "delisting_date": dates[-2].date().isoformat(),
                "delisting_return": -0.35,
                "inactive_reason": "DLST",
                "last_trade_date": dates[-3].date().isoformat(),
            }
        ]
        if include_delisting
        else []
    )

    pd.DataFrame(price_rows).to_csv(tmp_path / "adjusted_price_volume_panel.csv", index=False)
    pd.DataFrame(universe_rows).to_csv(tmp_path / "historical_universe_membership.csv", index=False)
    pd.DataFrame(benchmark_rows).to_csv(tmp_path / "small_cap_benchmark_panel.csv", index=False)
    pd.DataFrame(delisting_rows).to_csv(tmp_path / "delisting_returns.csv", index=False)
    manifest = tmp_path / "research_mode_dataset_manifest.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "schema_version": "research_mode_dataset_manifest.v1",
                "mode": "research_mode",
                "allowed_use_mode": "formal_research",
                "content_hash": "small-cap-fixture",
                "source_provenance": {
                    "provider": "fixture",
                    "as_of_timestamp": "2026-05-08",
                    "license_mode": "local_fixture",
                },
                "universe": {
                    "path": str(tmp_path / "historical_universe_membership.csv"),
                    "constituent_mode": "historical_membership",
                    "source": "fixture",
                    "source_is_pit": True,
                },
                "prices": {
                    "path": str(tmp_path / "adjusted_price_volume_panel.csv"),
                    "source": "fixture",
                    "adjusted": True,
                },
                "benchmark": {
                    "path": str(tmp_path / "small_cap_benchmark_panel.csv"),
                    "benchmark_id": "IWM",
                    "source": "fixture",
                },
                "delisting": {
                    "handling": "explicit_file" if include_delisting else "missing",
                    "path": str(tmp_path / "delisting_returns.csv"),
                },
                "timestamp_policy": {
                    "signal": "month_end_close",
                    "visibility": "after_month_end_close",
                    "tradable": "next_session_close",
                    "allow_same_close_trading": False,
                },
                "non_claims": {
                    "production_approval": False,
                    "live_trading": False,
                    "security_orders": False,
                    "direct_q2_entry": False,
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return manifest


def _attach_quality_score_panel(manifest: Path) -> None:
    payload = yaml.safe_load(manifest.read_text(encoding="utf-8"))
    prices = pd.read_csv(payload["prices"]["path"])
    month_ends = pd.to_datetime(prices["date"]).groupby(pd.to_datetime(prices["date"]).dt.to_period("M")).max()
    rows = []
    for date in month_ends:
        for asset_index, asset_id in enumerate(sorted(prices["asset_id"].astype(str).unique())):
            rows.append(
                {
                    "schema_version": "fd_small_cap_quality_score.v1",
                    "asset_id": asset_id,
                    "date": pd.Timestamp(date).date().isoformat(),
                    "quality_score": -1.0 + asset_index / 3.0,
                    "profitability_roa": 0.05 + asset_index / 100.0,
                    "gross_profitability": 0.2 + asset_index / 50.0,
                    "leverage": 0.4,
                    "visibility_timestamp": "2020-01-15",
                    "tradable_timestamp": "2020-01-16",
                    "source": "fixture_quality",
                    "not_alpha_evidence": True,
                    "direct_q2_entry_allowed": False,
                }
            )
    quality_path = manifest.parent / "quality_score_panel.csv"
    pd.DataFrame(rows).to_csv(quality_path, index=False)
    payload["quality"] = {
        "path": str(quality_path),
        "source": "fixture_quality",
        "score_definition": "fixture_quality_score",
        "pit_safe": True,
    }
    manifest.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
