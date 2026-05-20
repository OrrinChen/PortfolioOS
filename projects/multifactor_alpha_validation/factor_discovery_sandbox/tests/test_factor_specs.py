from __future__ import annotations

import json
from pathlib import Path

import yaml

from factor_discovery_sandbox.factor_specs import write_price_volume_factor_specs
from factor_discovery_sandbox.teaching_baseline import FACTOR_NAMES


def test_factor_spec_conversion_writes_29_specs_with_timestamp_and_abstain_contracts(tmp_path: Path) -> None:
    spec_dir = tmp_path / "factor_specs" / "price_volume_29"
    validation_path = tmp_path / "factor_spec_validation.json"

    result = write_price_volume_factor_specs(spec_dir=spec_dir, validation_path=validation_path)

    assert result["schema_version"] == "factor_spec_validation.v1"
    assert result["factor_count"] == 29
    assert result["all_specs_valid"] is True
    assert result["no_view_is_not_zero_alpha"] is True
    assert result["insufficient_coverage_policy"] == "explicit_abstain"
    assert len(list(spec_dir.glob("*.yaml"))) == 29

    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert validation == result
    assert sorted(validation["factors"]) == sorted(FACTOR_NAMES)

    sample = yaml.safe_load((spec_dir / "momentum_6m.yaml").read_text(encoding="utf-8"))
    required_fields = {
        "factor_id",
        "formula_version",
        "formula_summary",
        "design_contract",
        "design_review_required",
        "pre_formula_evidence_required",
        "formula_is_measurement_not_thesis",
        "raw_value_definition",
        "oriented_score_definition",
        "required_inputs",
        "fallback_policy",
        "fallback_audit_required",
        "duplicate_cluster_audit_required",
        "not_alpha_evidence",
        "direct_q2_entry_allowed",
        "mechanism",
        "mechanism_family",
        "lookback",
        "skip",
        "direction",
        "signal_timestamp",
        "visibility_timestamp",
        "tradable_timestamp",
        "coverage_rule",
        "expected_horizon",
        "known_correlation_family",
        "known_failure_mode",
        "no_view_is_not_zero_alpha",
    }
    assert required_fields.issubset(sample)
    assert sample["coverage_rule"]["insufficient_coverage"] == "explicit_abstain"
    assert sample["coverage_rule"]["abstain_reason"] == "insufficient_price_volume_history"
    assert sample["signal_timestamp"] == "month_end_close"
    assert sample["visibility_timestamp"] == "after_month_end_close"
    assert sample["tradable_timestamp"] == "next_rebalance_session"
    assert sample["no_view_is_not_zero_alpha"] is True
    assert sample["formula_version"] == "price_volume_29_mechanism_v2"
    assert sample["design_review_required"] is True
    assert sample["pre_formula_evidence_required"] is True
    assert sample["formula_is_measurement_not_thesis"] is True
    assert {
        "market_pain_point",
        "mechanism_hypothesis",
        "investor_constraint_or_behavior",
        "expected_universe",
        "expected_regime",
        "why_not_arbitraged_away",
        "observable_pre_formula_diagnostics",
        "formula_measurement_role",
        "placebo_design",
        "cost_capacity_risks",
        "expected_failure_modes",
    }.issubset(sample["design_contract"])
    assert "price trend" in sample["design_contract"]["market_pain_point"]
    assert sample["fallback_audit_required"] is True
    assert sample["duplicate_cluster_audit_required"] is True
    assert sample["not_alpha_evidence"] is True
    assert sample["direct_q2_entry_allowed"] is False
