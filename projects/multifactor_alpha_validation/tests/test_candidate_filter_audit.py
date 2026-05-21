from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from multifactor_alpha_validation.candidate_filter_audit import run_candidate_filter_audit


def test_candidate_filter_audit_resurrects_soft_failures_and_blocks_hard_failures(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    _write_spec(spec_dir, "momentum_12_1", status="enabled", data_tier="tier_1_price")
    _write_spec(spec_dir, "value_bm", status="enabled", data_tier="tier_2_fundamental", reporting_lag_days=90)
    _write_spec(spec_dir, "sue_event_reference", status="reference", data_tier="tier_3_event")
    _write_spec(
        spec_dir,
        "analyst_revision_disabled",
        family_id="analyst_revision",
        status="disabled",
        data_tier="tier_3_estimates",
        disabled_reason="missing_pit_estimate_source",
        pit_source_required="wrds_ibescorp_pit",
    )

    input_dir = tmp_path / "risk_model"
    input_dir.mkdir()
    pd.DataFrame(
        [
            {
                "factor_id": "momentum_12_1",
                "component_status": "eligible_benchmark_premia_component",
                "component_role": "style_premia_return_driver",
                "portfolio_validation_allowed": True,
                "source_closeout_status": "insufficient_residual_evidence",
                "source_dominant_failure_layer": "residual_stability",
            }
        ]
    ).to_csv(input_dir / "component_candidate_table.csv", index=False)
    pd.DataFrame(
        [
            {
                "factor_id": "momentum_12_1",
                "closeout_status": "insufficient_residual_evidence",
                "dominant_failure_layer": "residual_stability",
            }
        ]
    ).to_csv(input_dir / "factor_failure_diagnosis.csv", index=False)

    result = run_candidate_filter_audit(spec_dir, input_dir, tmp_path / "r14_5")

    audit = pd.read_csv(result.candidate_filter_audit_path).set_index("factor_id")
    soft = pd.read_csv(result.soft_resurrected_pool_path)
    hard = pd.read_csv(result.hard_excluded_path)
    manifest = json.loads(Path(result.component_pool_manifest_path).read_text(encoding="utf-8"))

    assert result.total_candidate_count == 4
    assert result.hard_excluded_count == 1
    assert result.component_pool_count == 3
    assert audit.loc["analyst_revision_disabled", "filter_class"] == "hard_excluded"
    assert bool(audit.loc["analyst_revision_disabled", "component_pool_eligible"]) is False
    assert audit.loc["momentum_12_1", "filter_class"] == "soft_resurrected"
    assert audit.loc["momentum_12_1", "resurrection_source"] == "component_gate"
    assert audit.loc["value_bm", "filter_class"] == "soft_resurrected"
    assert audit.loc["value_bm", "resurrection_source"] == "formal_factor_spec_not_yet_risk_attributed"
    assert audit.loc["sue_event_reference", "component_role"] == "reference_event_component"
    assert set(soft["factor_id"]) == {"momentum_12_1", "value_bm", "sue_event_reference"}
    assert set(hard["factor_id"]) == {"analyst_revision_disabled"}
    assert manifest["schema_version"] == "component_pool_manifest.v1"
    assert manifest["component_pool_count"] == 3
    assert manifest["hard_excluded_count"] == 1
    assert manifest["r15_input_path"].endswith("soft_resurrected_component_pool.csv")
    assert manifest["non_claims"]["production_approval"] is False

    report = Path(result.report_path).read_text(encoding="utf-8").lower()
    assert "filter audit" in report
    assert "hard failures remain blocked" in report
    assert "soft failures are restored" in report
    assert "not alpha evidence" in report


def _write_spec(
    spec_dir: Path,
    factor_id: str,
    *,
    family_id: str | None = None,
    status: str,
    data_tier: str,
    reporting_lag_days: int = 0,
    disabled_reason: str | None = None,
    pit_source_required: str | None = None,
) -> None:
    payload = {
        "schema_version": "factor_spec.v1",
        "factor_id": factor_id,
        "family_id": family_id or factor_id.split("_")[0],
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
        "signal_definition": {"lookback_days": 252, "skip_days": 21, "transform": "rank_zscore"},
        "horizon": {"horizon_type": "rebalance_period", "holding_days": 21, "rebalance_frequency": "monthly"},
        "coverage": {"min_assets": 10, "min_history_days": 252, "missing_policy": "explicit_abstain"},
        "neutralization": {"beta": "report", "sector": "report", "size": "report"},
        "cost_sensitivity": {"expected_turnover": "medium", "capacity_risk": "medium"},
        "known_failure_modes": ["test_failure"],
        "no_view_is_not_zero_alpha": True,
        "non_claims": {"production_approval": False, "live_trading": False, "security_orders": False, "direct_q2_entry": False},
    }
    if disabled_reason:
        payload["disabled_reason"] = disabled_reason
    if pit_source_required:
        payload["pit_source_required"] = pit_source_required
    (spec_dir / f"{factor_id}.yaml").write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
