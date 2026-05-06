from __future__ import annotations

from pathlib import Path

from multifactor_alpha_validation.covariance import build_covariance_diagnostics, write_covariance_outputs
from multifactor_alpha_validation.factor_library import load_factor_specs
from multifactor_alpha_validation.q1_evidence import build_q1_evidence
from multifactor_alpha_validation.redundancy_gate import build_redundancy_gate
from multifactor_alpha_validation.shrinkage import build_shrinkage_results, write_shrinkage_outputs
from multifactor_alpha_validation.signal_builders import build_signal_panels


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPEC_DIR = PROJECT_ROOT / "factor_specs"


def _build_week5_inputs():
    specs = load_factor_specs(SPEC_DIR)
    signals = build_signal_panels(specs)
    evidence = build_q1_evidence(specs, signals.signal_panels)
    redundancy = build_redundancy_gate(specs, signals.signal_panels, evidence.factor_evidence_table)
    return specs, signals, evidence, redundancy


def test_shrinkage_reduces_low_stability_and_low_marginal_value_alpha() -> None:
    specs, _signals, evidence, redundancy = _build_week5_inputs()
    result = build_shrinkage_results(specs, evidence.factor_evidence_table, redundancy.marginal_value_decision_table)
    posterior = result.posterior_mu.set_index("factor_id")

    promoted = posterior[posterior["marginal_value_score"] == posterior["marginal_value_score"].max()].iloc[0]
    archived = posterior[posterior["decision"] == "archive_no_marginal_value"].iloc[0]

    assert promoted["shrinkage_intensity"] > archived["shrinkage_intensity"]
    assert abs(archived["posterior_expected_return"]) <= abs(archived["raw_expected_return"])
    assert archived["posterior_expected_return"] <= 0.001
    assert result.summary["parameters_preregistered"] is True


def test_covariance_shrinkage_improves_condition_number_and_reports_duplicates() -> None:
    specs, signals, evidence, redundancy = _build_week5_inputs()
    shrinkage = build_shrinkage_results(specs, evidence.factor_evidence_table, redundancy.marginal_value_decision_table)
    covariance = build_covariance_diagnostics(signals.signal_panels, redundancy.factor_clusters, shrinkage.posterior_mu)

    diagnostics = covariance.diagnostics
    assert diagnostics["condition_number_after"] <= diagnostics["condition_number_before"]
    assert diagnostics["target_type"] == "block_cluster"
    assert diagnostics["near_duplicate_pairs"]
    assert diagnostics["cluster_count"] == int(redundancy.factor_clusters["cluster_id"].nunique())


def test_shrinkage_and_covariance_outputs_are_written(tmp_path: Path) -> None:
    specs, signals, evidence, redundancy = _build_week5_inputs()
    shrinkage = build_shrinkage_results(specs, evidence.factor_evidence_table, redundancy.marginal_value_decision_table)
    covariance = build_covariance_diagnostics(signals.signal_panels, redundancy.factor_clusters, shrinkage.posterior_mu)

    shrinkage_files = write_shrinkage_outputs(shrinkage, tmp_path / "factor_shrinkage")
    covariance_files = write_covariance_outputs(covariance, tmp_path / "factor_covariance")

    assert (tmp_path / "factor_shrinkage" / "factor_posterior_mu.csv").exists()
    assert (tmp_path / "factor_shrinkage" / "shrinkage_summary.json").exists()
    assert (tmp_path / "factor_covariance" / "factor_covariance_sample.csv").exists()
    assert (tmp_path / "factor_covariance" / "factor_covariance_shrunk.csv").exists()
    assert (tmp_path / "factor_covariance" / "covariance_diagnostics.json").exists()
    assert "shrinkage_summary.json" in shrinkage_files
    assert "covariance_diagnostics.json" in covariance_files

