from __future__ import annotations

import json
from pathlib import Path

import pytest

from multifactor_alpha_validation.factor_library import (
    load_factor_specs,
    validate_factor_spec_directory,
)
from multifactor_alpha_validation.schema import FactorSpec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPEC_DIR = PROJECT_ROOT / "factor_specs"


def test_factor_specs_validate_with_required_timestamp_and_abstain_contracts() -> None:
    specs = load_factor_specs(SPEC_DIR)

    assert len(specs) >= 10
    assert sum(spec.status == "enabled" for spec in specs) >= 8
    for spec in specs:
        assert spec.pit_contract.signal_timestamp_rule
        assert spec.pit_contract.visibility_timestamp_rule
        assert spec.pit_contract.tradable_timestamp_rule
        assert spec.coverage.missing_policy == "explicit_abstain"
        assert spec.no_view_is_not_zero_alpha is True


def test_fundamental_specs_require_reporting_lag_and_revision_is_disabled() -> None:
    specs = {spec.factor_id: spec for spec in load_factor_specs(SPEC_DIR)}

    for factor_id in (
        "value_bm",
        "profitability_quality",
        "investment_asset_growth",
        "accruals",
    ):
        assert specs[factor_id].data_tier == "tier_2_fundamental"
        assert specs[factor_id].pit_contract.reporting_lag_days >= 45

    revision = specs["analyst_revision_disabled"]
    assert revision.status == "disabled"
    assert revision.disabled_reason == "missing_pit_estimate_source"
    assert revision.pit_source_required == "wrds_ibescorp_pit"


def test_factor_spec_rejects_missing_pit_contract() -> None:
    payload = {
        "schema_version": "factor_spec.v1",
        "factor_id": "bad_factor",
        "family_id": "bad",
        "display_name": "Bad Factor",
        "mechanism": "missing contract",
        "mechanism_type": "fixed_horizon",
        "data_tier": "tier_1_price",
        "status": "enabled",
        "data_requirements": {"required_fields": ["adjusted_close"]},
        "signal_definition": {"lookback_days": 21, "transform": "rank_zscore"},
        "horizon": {"horizon_type": "rebalance_period", "holding_days": 21, "rebalance_frequency": "monthly"},
        "coverage": {"min_assets": 300, "min_history_days": 21, "missing_policy": "explicit_abstain"},
        "neutralization": {"beta": "report", "sector": "report", "size": "report"},
        "cost_sensitivity": {"expected_turnover": "medium", "capacity_risk": "medium"},
        "known_failure_modes": ["contract_missing"],
        "no_view_is_not_zero_alpha": True,
        "non_claims": {"production_approval": False, "live_trading": False, "security_orders": False},
    }

    with pytest.raises(Exception):
        FactorSpec.model_validate(payload)


def test_spec_validation_report_is_written_without_boundary_violations(tmp_path: Path) -> None:
    report = validate_factor_spec_directory(SPEC_DIR, output_dir=tmp_path)

    report_path = tmp_path / "spec_validation_report.json"
    assert report_path.exists()
    data = json.loads(report_path.read_text())
    assert data["schema_version"] == "factor_spec_validation.v1"
    assert data["all_specs_valid"] is True
    assert data["enabled_factor_count"] >= 8
    assert data["non_claims"] == {
        "production_approval": False,
        "live_trading": False,
        "security_orders": False,
        "direct_q2_entry": False,
    }
    assert report["all_specs_valid"] is True
