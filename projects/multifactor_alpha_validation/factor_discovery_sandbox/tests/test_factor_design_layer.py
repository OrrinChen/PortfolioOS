from __future__ import annotations

import json
from pathlib import Path

from factor_discovery_sandbox.factor_design import (
    REQUIRED_DESIGN_CONTRACT_KEYS,
    build_candidate_design_manifest,
    validate_design_contract,
    write_candidate_design_manifest,
    write_factor_design_layer_spec,
)


def test_design_contract_validator_rejects_formula_only_candidate() -> None:
    spec = {
        "factor_id": "formula_only_candidate",
        "formula_summary": "rank(ret_12m)",
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
    }

    result = validate_design_contract(spec)

    assert result["valid"] is False
    assert "missing_design_contract" in result["failure_reasons"]
    assert result["not_alpha_evidence"] is True
    assert result["direct_q2_entry_allowed"] is False


def test_design_contract_validator_requires_pain_mechanism_placebo_and_capacity() -> None:
    incomplete_contract = {key: "documented" for key in REQUIRED_DESIGN_CONTRACT_KEYS}
    incomplete_contract.pop("market_pain_point")
    incomplete_contract.pop("placebo_design")
    incomplete_contract.pop("cost_capacity_risks")

    result = validate_design_contract(
        {
            "factor_id": "incomplete_candidate",
            "design_contract": incomplete_contract,
            "formula_is_measurement_not_thesis": True,
            "pre_formula_evidence_required": True,
            "design_review_required": True,
            "not_alpha_evidence": True,
            "direct_q2_entry_allowed": False,
        }
    )

    assert result["valid"] is False
    assert "missing_design_fields:cost_capacity_risks,market_pain_point,placebo_design" in result["failure_reasons"]


def test_write_factor_design_layer_spec_records_fd_d0_charter_and_blocks_promotion(tmp_path: Path) -> None:
    result = write_factor_design_layer_spec(
        output_dir=tmp_path / "outputs" / "design_layer",
        report_path=tmp_path / "reports" / "factor_discovery_design_layer_report.md",
    )

    validation = json.loads(result.artifacts["design_contract_validation"].read_text(encoding="utf-8"))
    report = result.artifacts["design_layer_report"].read_text(encoding="utf-8")

    assert result.summary["stage"] == "FD-D0"
    assert validation["schema_version"] == "fd_factor_design_contract_validation.v1"
    assert validation["design_layer_required_before_formula"] is True
    assert validation["required_design_fields"] == list(REQUIRED_DESIGN_CONTRACT_KEYS)
    assert validation["not_alpha_evidence"] is True
    assert validation["direct_q2_entry_allowed"] is False
    assert "market pain point" in report
    assert "formula is measurement, not thesis" in report


def test_candidate_design_manifest_is_valid_before_standalone_validation(tmp_path: Path) -> None:
    manifest = build_candidate_design_manifest(
        candidate_id="small_cap_quality_residual_momentum_6m_ex1m",
        family_id="small_cap_quality_residual_momentum_v1",
        mechanism_family="small_cap_quality_residual_momentum",
    )

    assert manifest["schema_version"] == "fd_candidate_design_manifest.v1"
    assert manifest["design_contract_valid"] is True
    assert manifest["candidate_validation_allowed"] is True
    assert manifest["design_layer_required_before_formula"] is True
    assert manifest["formula_is_measurement_not_thesis"] is True
    assert manifest["not_alpha_evidence"] is True
    assert manifest["direct_q2_entry_allowed"] is False
    assert manifest["design_contract"]["market_pain_point"]

    path = tmp_path / "candidate_design_manifest.json"
    written = write_candidate_design_manifest(
        path=path,
        candidate_id="small_cap_quality_residual_momentum_6m_ex1m",
        family_id="small_cap_quality_residual_momentum_v1",
        mechanism_family="small_cap_quality_residual_momentum",
    )
    assert json.loads(path.read_text(encoding="utf-8")) == written
