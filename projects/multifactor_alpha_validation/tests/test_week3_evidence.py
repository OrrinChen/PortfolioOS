from __future__ import annotations

from pathlib import Path

from multifactor_alpha_validation.factor_library import load_factor_specs
from multifactor_alpha_validation.q1_evidence import build_q1_evidence, write_q1_evidence_outputs
from multifactor_alpha_validation.signal_builders import build_signal_panels


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPEC_DIR = PROJECT_ROOT / "factor_specs"


def test_q1_evidence_separates_raw_neutralized_and_benchmark_readouts() -> None:
    specs = load_factor_specs(SPEC_DIR)
    signals = build_signal_panels(specs)
    result = build_q1_evidence(specs, signals.signal_panels)

    evidence = result.factor_evidence_table
    assert not evidence.empty
    for column in (
        "raw_rank_ic_mean",
        "raw_rank_ic_t",
        "neutralized_rank_ic_mean",
        "neutralized_rank_ic_t",
        "top_bottom_spread",
        "beta_adjusted_spread",
        "sector_neutral_spread",
        "style_adjusted_spread",
        "benchmark_relative_spread",
    ):
        assert column in evidence.columns

    assert (evidence["raw_rank_ic_mean"] != evidence["neutralized_rank_ic_mean"]).any()
    assert set(evidence["q1_decision"]).isdisjoint({"promote_to_allocator"})


def test_q1_evidence_reports_coverage_turnover_decay_and_exposures() -> None:
    specs = load_factor_specs(SPEC_DIR)
    signals = build_signal_panels(specs)
    result = build_q1_evidence(specs, signals.signal_panels)

    evidence = result.factor_evidence_table
    assert evidence["coverage_ratio"].between(0, 1).all()
    assert (evidence["turnover_estimate"] > 0).all()
    assert evidence["decay_profile"].str.contains("21d").all()

    exposure = result.neutralization_report
    assert {"beta_exposure", "sector_exposure", "size_exposure", "liquidity_exposure"}.issubset(exposure.columns)
    assert set(exposure["adjustment_status"]) == {"reported"}


def test_q1_evidence_outputs_are_written(tmp_path: Path) -> None:
    specs = load_factor_specs(SPEC_DIR)
    signals = build_signal_panels(specs)
    result = build_q1_evidence(specs, signals.signal_panels)

    written = write_q1_evidence_outputs(result, tmp_path)

    assert (tmp_path / "factor_evidence_table.csv").exists()
    assert (tmp_path / "neutralization_report.csv").exists()
    assert (tmp_path / "q1_summary.md").exists()
    assert (tmp_path / "factor_evidence_momentum_12_1.json").exists()
    assert "q1_summary.md" in written

