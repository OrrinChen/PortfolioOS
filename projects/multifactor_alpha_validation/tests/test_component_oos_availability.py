from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from multifactor_alpha_validation.component_oos_availability import (
    run_component_oos_availability_expansion,
)


def test_component_oos_availability_classifies_unavailable_components_without_fabricating_returns(
    tmp_path: Path,
) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    _write_spec(spec_dir, "momentum_12_1", family_id="momentum", data_tier="tier_1_price")
    _write_spec(spec_dir, "liquidity_turnover", family_id="liquidity", data_tier="tier_1_price_volume")
    _write_spec(spec_dir, "value_bm", family_id="value", data_tier="tier_2_fundamental", reporting_lag_days=90)
    _write_spec(spec_dir, "sue_event_reference", family_id="sue", data_tier="tier_3_event", status="reference")

    pool_path = tmp_path / "soft_resurrected_component_pool.csv"
    observations_path = tmp_path / "real_oos_observations.csv"
    portfolio_validation_dir = tmp_path / "portfolio_validation"
    portfolio_validation_dir.mkdir()
    _write_pool(pool_path)
    _write_observations(observations_path)
    (portfolio_validation_dir / "portfolio_assembly_audit.json").write_text(
        json.dumps(
            {
                "reclassified_decision_state": "observed_subset_fails_gross",
                "component_pool_validation_state": "component_pool_unavailable_coverage_gap",
                "coverage_ratio": 0.25,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    result = run_component_oos_availability_expansion(
        spec_dir=spec_dir,
        component_pool_path=pool_path,
        oos_observation_path=observations_path,
        portfolio_validation_dir=portfolio_validation_dir,
        output_dir=tmp_path / "r15_6",
    )

    report = pd.read_csv(result.availability_report_path).set_index("factor_id")
    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    plan = Path(result.enablement_plan_path).read_text(encoding="utf-8").lower()

    assert result.component_pool_validation_state == "component_pool_validation_incomplete"
    assert result.full_pool_decision_allowed is False
    assert summary["component_pool_validation_min_coverage"] == 0.6
    assert summary["coverage_ratio"] == 0.25
    assert summary["observed_component_count"] == 1
    assert summary["unavailable_component_count"] == 3
    assert summary["fabricated_returns"] is False
    assert summary["or_optimizer_unlocked"] is False

    assert report.loc["momentum_12_1", "current_status"] == "observed"
    assert report.loc["liquidity_turnover", "unavailable_reason"] == "missing_signal_panel"
    assert report.loc["value_bm", "unavailable_reason"] == "missing_signal_panel"
    assert bool(report.loc["value_bm", "fundamental_reporting_lag_ok"]) is True
    assert int(report.loc["value_bm", "reporting_lag_days"]) == 90
    assert report.loc["sue_event_reference", "unavailable_reason"] == "missing_event_timestamp"
    assert bool(report.loc["liquidity_turnover", "can_enable_now"]) is False
    assert bool(report.loc["value_bm", "can_enable_now"]) is False

    assert "missing_signal_panel" in summary["unavailable_reason_counts"]
    assert "missing_event_timestamp" in summary["unavailable_reason_counts"]
    assert "not alpha evidence" in plan
    assert "or remains locked" in plan


def test_component_oos_availability_keeps_hard_blocked_components_unavailable(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    _write_spec(spec_dir, "momentum_12_1", family_id="momentum", data_tier="tier_1_price")
    _write_spec(spec_dir, "analyst_revision_disabled", family_id="analyst_revision", data_tier="tier_3_estimates", status="disabled")

    pool_path = tmp_path / "soft_resurrected_component_pool.csv"
    observations_path = tmp_path / "real_oos_observations.csv"
    portfolio_validation_dir = tmp_path / "portfolio_validation"
    portfolio_validation_dir.mkdir()
    pd.DataFrame(
        [
            _pool_row("momentum_12_1", "momentum", "eligible_benchmark_premia_component", True),
            _pool_row(
                "analyst_revision_disabled",
                "analyst_revision",
                "blocked_component",
                False,
                filter_class="hard_excluded",
                hard_reason="missing_pit_estimate_source",
            ),
        ]
    ).to_csv(pool_path, index=False)
    _write_observations(observations_path)

    result = run_component_oos_availability_expansion(
        spec_dir=spec_dir,
        component_pool_path=pool_path,
        oos_observation_path=observations_path,
        portfolio_validation_dir=portfolio_validation_dir,
        output_dir=tmp_path / "r15_6",
    )

    report = pd.read_csv(result.availability_report_path).set_index("factor_id")
    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))

    assert report.loc["analyst_revision_disabled", "current_status"] == "hard_blocked"
    assert report.loc["analyst_revision_disabled", "unavailable_reason"] == "research_mode_blocked"
    assert bool(report.loc["analyst_revision_disabled", "can_enable_now"]) is False
    assert "missing_pit_estimate_source" in report.loc["analyst_revision_disabled", "blocked_reason"]
    assert summary["hard_blocked_component_count"] == 1
    assert summary["eligible_component_count"] == 1
    assert summary["full_pool_decision_allowed"] is True


def _write_spec(
    spec_dir: Path,
    factor_id: str,
    *,
    family_id: str,
    data_tier: str,
    status: str = "enabled",
    reporting_lag_days: int = 0,
) -> None:
    disabled = status == "disabled"
    payload = {
        "schema_version": "factor_spec.v1",
        "factor_id": factor_id,
        "family_id": family_id,
        "display_name": factor_id,
        "mechanism": "test mechanism",
        "mechanism_type": "fixed_horizon",
        "data_tier": data_tier,
        "status": status,
        "data_requirements": {"required_fields": ["adjusted_close", "trading_calendar"]},
        "pit_contract": {
            "signal_timestamp_rule": "close_on_signal_date",
            "visibility_timestamp_rule": "after_market_close",
            "tradable_timestamp_rule": "next_trading_day_open",
            "reporting_lag_days": reporting_lag_days,
            "reject_if_missing_visibility": True,
        },
        "signal_definition": {"lookback_days": 21, "skip_days": 0, "transform": "rank_zscore"},
        "horizon": {"horizon_type": "rebalance_period", "holding_days": 21, "rebalance_frequency": "monthly"},
        "coverage": {"min_assets": 10, "min_history_days": 21, "missing_policy": "explicit_abstain"},
        "neutralization": {"beta": "report", "sector": "report", "size": "report"},
        "cost_sensitivity": {"expected_turnover": "medium", "capacity_risk": "medium"},
        "known_failure_modes": ["test_failure"],
        "no_view_is_not_zero_alpha": True,
        "non_claims": {
            "production_approval": False,
            "live_trading": False,
            "security_orders": False,
            "direct_q2_entry": False,
        },
    }
    if data_tier == "tier_2_fundamental":
        payload["data_requirements"] = {"required_fields": ["filing_date", "book_value", "trading_calendar"]}
        payload["pit_contract"]["signal_timestamp_rule"] = "filing_date_plus_lag"
        payload["pit_contract"]["visibility_timestamp_rule"] = "public_filing_available"
    if data_tier == "tier_3_event":
        payload["data_requirements"] = {"required_fields": ["earnings_announcement_timestamp", "trading_calendar"]}
        payload["pit_contract"]["signal_timestamp_rule"] = "event_available_timestamp"
        payload["pit_contract"]["visibility_timestamp_rule"] = "public_earnings_release_timestamp"
        payload["horizon"] = {"horizon_type": "event_window", "holding_days": 21, "rebalance_frequency": "event_driven"}
    if disabled:
        payload["disabled_reason"] = "missing_pit_estimate_source"
        payload["pit_source_required"] = "wrds_ibescorp_pit"
    (spec_dir / f"{factor_id}.yaml").write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _write_pool(path: Path) -> None:
    pd.DataFrame(
        [
            _pool_row("momentum_12_1", "momentum", "eligible_benchmark_premia_component", True),
            _pool_row("liquidity_turnover", "liquidity", "eligible_component_pending_risk_attribution", True),
            _pool_row("value_bm", "value", "eligible_fundamental_premia_component", True),
            _pool_row("sue_event_reference", "sue", "eligible_reference_component", True),
        ]
    ).to_csv(path, index=False)


def _pool_row(
    factor_id: str,
    family_id: str,
    component_status: str,
    eligible: bool,
    *,
    filter_class: str = "soft_resurrected",
    hard_reason: str = "",
) -> dict[str, object]:
    return {
        "factor_id": factor_id,
        "family_id": family_id,
        "filter_class": filter_class,
        "component_pool_eligible": eligible,
        "component_status": component_status,
        "component_role": "style_premia_return_driver",
        "portfolio_validation_allowed": eligible,
        "hard_exclusion_reason": hard_reason,
        "not_alpha_evidence": True,
        "production_approval": False,
        "live_trading": False,
        "security_orders": False,
        "direct_q2_entry": False,
    }


def _write_observations(path: Path) -> None:
    dates = pd.date_range("2020-01-31", periods=3, freq="ME")
    rows = [
        {
            "factor_id": "momentum_12_1",
            "rebalance_date": date.date().isoformat(),
            "history_cutoff_date": (date - pd.Timedelta(days=1)).date().isoformat(),
            "signal_date": date.date().isoformat(),
            "tradable_date": (date + pd.Timedelta(days=1)).date().isoformat(),
            "full_sample_icir_used": False,
            "prior_history_only": True,
            "gross_spread": 0.001,
            "net_spread": 0.0,
            "qqq_return": 0.002,
            "cost_drag": 0.001,
            "asset_count": 50,
        }
        for date in dates
    ]
    pd.DataFrame(rows).to_csv(path, index=False)
