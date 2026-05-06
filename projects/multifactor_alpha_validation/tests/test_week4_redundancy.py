from __future__ import annotations

from pathlib import Path

from multifactor_alpha_validation.factor_library import load_factor_specs
from multifactor_alpha_validation.q1_evidence import build_q1_evidence
from multifactor_alpha_validation.redundancy_gate import build_redundancy_gate, write_redundancy_outputs
from multifactor_alpha_validation.signal_builders import build_signal_panels


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPEC_DIR = PROJECT_ROOT / "factor_specs"


def _build_week4_result():
    specs = load_factor_specs(SPEC_DIR)
    signals = build_signal_panels(specs)
    evidence = build_q1_evidence(specs, signals.signal_panels)
    return build_redundancy_gate(specs, signals.signal_panels, evidence.factor_evidence_table)


def test_redundancy_gate_clusters_highly_correlated_factors() -> None:
    result = _build_week4_result()

    matrix = result.factor_correlation_matrix
    assert "momentum_12_1" in matrix.columns
    assert float(matrix.loc["momentum_12_1", "value_bm"]) >= 0.95

    clusters = result.factor_clusters.set_index("factor_id")
    assert clusters.loc["momentum_12_1", "cluster_id"] == clusters.loc["value_bm", "cluster_id"]
    assert result.factor_clusters["cluster_id"].nunique() < len(result.factor_clusters)


def test_marginal_value_report_contains_residual_net_and_decisions() -> None:
    result = _build_week4_result()
    decisions = result.marginal_value_decision_table

    for column in (
        "residual_ic_after_baseline",
        "incremental_gross_spread",
        "incremental_net_spread",
        "incremental_turnover",
        "incremental_cost_drag",
        "marginal_value_score",
        "decision",
        "decision_reason",
    ):
        assert column in decisions.columns

    redundant = decisions[decisions["max_pairwise_correlation"] >= 0.95]
    assert not redundant.empty
    assert set(redundant["decision"]).issubset(
        {"promote_to_allocator", "real_but_redundant", "archive_no_marginal_value", "diagnostic_only"}
    )
    low_residual = redundant[redundant["residual_ic_after_baseline"] < 0.05]
    assert set(low_residual["decision"]).isdisjoint({"promote_to_allocator"})


def test_redundancy_outputs_are_written(tmp_path: Path) -> None:
    result = _build_week4_result()
    written = write_redundancy_outputs(result, tmp_path)

    assert (tmp_path / "factor_correlation_matrix.csv").exists()
    assert (tmp_path / "factor_clusters.csv").exists()
    assert (tmp_path / "marginal_value_report.json").exists()
    assert (tmp_path / "marginal_value_decision_table.csv").exists()
    assert (tmp_path / "redundancy_report.md").exists()
    assert "redundancy_report.md" in written

