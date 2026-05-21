from __future__ import annotations

from pathlib import Path

from multifactor_alpha_validation.allocator import build_allocator_result
from multifactor_alpha_validation.cost_capacity import build_survival_result, write_survival_outputs
from multifactor_alpha_validation.covariance import build_covariance_diagnostics
from multifactor_alpha_validation.factor_library import load_factor_specs
from multifactor_alpha_validation.q1_evidence import build_q1_evidence
from multifactor_alpha_validation.redundancy_gate import build_redundancy_gate
from multifactor_alpha_validation.shrinkage import build_shrinkage_results
from multifactor_alpha_validation.signal_builders import build_signal_panels


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPEC_DIR = PROJECT_ROOT / "factor_specs"


def _build_week7_result():
    specs = load_factor_specs(SPEC_DIR)
    signals = build_signal_panels(specs)
    evidence = build_q1_evidence(specs, signals.signal_panels)
    redundancy = build_redundancy_gate(specs, signals.signal_panels, evidence.factor_evidence_table)
    shrinkage = build_shrinkage_results(specs, evidence.factor_evidence_table, redundancy.marginal_value_decision_table)
    covariance = build_covariance_diagnostics(signals.signal_panels, redundancy.factor_clusters, shrinkage.posterior_mu)
    allocator = build_allocator_result(
        specs,
        shrinkage.posterior_mu,
        covariance.shrunk_covariance,
        redundancy.marginal_value_decision_table,
    )
    return build_survival_result(specs, evidence.factor_evidence_table, allocator, shrinkage.posterior_mu)


def test_cost_survival_blocks_negative_net_alpha() -> None:
    result = _build_week7_result()
    cost = result.cost_stress_matrix

    killed = cost[cost["net_alpha_after_cost"] < 0]
    assert not killed.empty
    assert set(killed["cost_survival_status"]) == {"cost_killed"}
    assert (cost[cost["net_alpha_after_cost"] >= 0]["cost_survival_status"] == "cost_survived").any()


def test_capacity_frontier_reports_bottlenecks_and_net_alpha_vs_aum() -> None:
    result = _build_week7_result()
    capacity = result.capacity_frontier

    assert {"aum_usd", "net_alpha_after_capacity", "capacity_bottleneck_names"}.issubset(capacity.columns)
    assert capacity["aum_usd"].is_monotonic_increasing
    assert capacity["capacity_bottleneck_names"].str.len().gt(0).all()


def test_benchmark_attribution_and_failure_layers_are_explicit() -> None:
    result = _build_week7_result()
    attribution = result.benchmark_attribution

    assert {"raw_return", "benchmark_relative_return", "beta_adjusted_return", "sector_style_adjusted_return"}.issubset(
        attribution.columns
    )
    assert (attribution["raw_return"] != attribution["beta_adjusted_return"]).any()
    assert result.failure_attribution["analyst_revision_disabled"]["stop_layer"] == "pit_unavailable"
    assert result.failure_attribution["analyst_revision_disabled"]["performance_status"] == "unavailable"


def test_survival_outputs_are_written(tmp_path: Path) -> None:
    result = _build_week7_result()
    written = write_survival_outputs(result, tmp_path)

    assert (tmp_path / "survival_funnel.csv").exists()
    assert (tmp_path / "cost_stress_matrix.csv").exists()
    assert (tmp_path / "capacity_frontier.csv").exists()
    assert (tmp_path / "benchmark_attribution.csv").exists()
    assert (tmp_path / "failure_attribution.json").exists()
    assert (tmp_path / "survival_summary.md").exists()
    assert "survival_summary.md" in written
