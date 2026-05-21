from __future__ import annotations

from pathlib import Path

import pytest

from multifactor_alpha_validation.allocator import build_allocator_result, write_allocator_outputs
from multifactor_alpha_validation.covariance import build_covariance_diagnostics
from multifactor_alpha_validation.factor_library import load_factor_specs
from multifactor_alpha_validation.q1_evidence import build_q1_evidence
from multifactor_alpha_validation.redundancy_gate import build_redundancy_gate
from multifactor_alpha_validation.shrinkage import build_shrinkage_results
from multifactor_alpha_validation.signal_builders import build_signal_panels


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPEC_DIR = PROJECT_ROOT / "factor_specs"


def _build_week6_result():
    specs = load_factor_specs(SPEC_DIR)
    signals = build_signal_panels(specs)
    evidence = build_q1_evidence(specs, signals.signal_panels)
    redundancy = build_redundancy_gate(specs, signals.signal_panels, evidence.factor_evidence_table)
    shrinkage = build_shrinkage_results(specs, evidence.factor_evidence_table, redundancy.marginal_value_decision_table)
    covariance = build_covariance_diagnostics(signals.signal_panels, redundancy.factor_clusters, shrinkage.posterior_mu)
    return build_allocator_result(specs, shrinkage.posterior_mu, covariance.shrunk_covariance, redundancy.marginal_value_decision_table)


def test_allocator_weights_are_nonnegative_normalized_and_factor_level_only() -> None:
    result = _build_week6_result()
    weights = result.factor_weights

    assert (weights["weight"] >= 0).all()
    assert weights["weight"].sum() == pytest.approx(1.0)
    assert "asset_id" not in weights.columns
    assert result.non_claims == {
        "production_approval": False,
        "live_trading": False,
        "security_orders": False,
    }


def test_zero_weight_attribution_requires_allowed_reason() -> None:
    result = _build_week6_result()
    zero_rows = result.zero_weight_attribution

    assert not zero_rows.empty
    allowed = {
        "no_view",
        "insufficient_evidence",
        "low_posterior_alpha",
        "high_redundancy",
        "cluster_dominated",
        "high_cost_drag",
        "high_turnover",
        "capacity_limited",
        "constraint_bound",
        "unstable_covariance",
    }
    assert set(zero_rows["zero_weight_reason"]).issubset(allowed)
    assert zero_rows["zero_weight_reason"].notna().all()


def test_allocator_sanity_checks_cover_sign_flip_scale_and_no_view() -> None:
    result = _build_week6_result()

    assert result.sanity_checks["sign_flip_check_passed"] is True
    assert result.sanity_checks["scale_response_check_passed"] is True
    assert result.sanity_checks["no_view_zero_alpha_distinct"] is True
    assert result.sanity_checks["high_redundancy_compression_passed"] is True


def test_allocator_outputs_are_written(tmp_path: Path) -> None:
    result = _build_week6_result()
    written = write_allocator_outputs(result, tmp_path)

    assert (tmp_path / "factor_allocator_weights.csv").exists()
    assert (tmp_path / "zero_weight_attribution.csv").exists()
    assert (tmp_path / "allocator_diagnostics.json").exists()
    assert (tmp_path / "allocator_sanity_checks.json").exists()
    assert (tmp_path / "allocator_report.md").exists()
    assert "allocator_report.md" in written
