from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.factor_design_d1 import (
    REQUIRED_LEDGER_COLUMNS,
    run_factor_design_d1,
    validate_design_ledger,
)


def test_design_d1_writes_pain_point_map_ledger_and_backlog(tmp_path: Path) -> None:
    result = run_factor_design_d1(output_dir=tmp_path / "outputs" / "factor_discovery" / "design_layer" / "d1")

    assert result.summary["schema_version"] == "fd_factor_design_d1_summary.v1"
    assert result.summary["stage"] == "FD-D1"
    assert result.summary["design_layer_required_before_formula"] is True
    assert result.summary["formula_first_candidates_blocked"] is True
    assert result.summary["not_alpha_evidence"] is True
    assert result.summary["direct_q2_entry_allowed"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["alpha_registry_update_allowed"] is False

    assert set(result.artifacts) == {
        "factor_pain_point_map",
        "factor_design_ledger",
        "candidate_family_backlog",
        "factor_design_d1_summary",
    }
    assert {path.name for path in result.artifacts.values()} == {
        "factor_pain_point_map.md",
        "factor_design_ledger.csv",
        "candidate_family_backlog.json",
        "factor_design_d1_summary.json",
    }

    ledger = pd.read_csv(result.artifacts["factor_design_ledger"])
    assert set(REQUIRED_LEDGER_COLUMNS).issubset(ledger.columns)
    assert len(ledger) >= 6
    assert ledger["not_alpha_evidence"].eq(True).all()
    assert ledger["direct_q2_entry_allowed"].eq(False).all()
    assert ledger["formula_measurement_role"].str.contains("measurement", case=False).all()
    assert ledger["formula_measurement_role"].str.contains("not thesis", case=False).all()
    assert {
        "small_cap_quality_residual_momentum_v1",
        "revision_confirmed_earnings_underreaction",
        "momentum_12m_ex1m_low_vol_3m",
    }.issubset(set(ledger["candidate_family_id"]))

    backlog = json.loads(result.artifacts["candidate_family_backlog"].read_text(encoding="utf-8"))
    assert backlog["schema_version"] == "fd_candidate_family_backlog.v1"
    assert backlog["stage"] == "FD-D1"
    assert backlog["not_alpha_evidence"] is True
    assert backlog["direct_q2_entry_allowed"] is False
    assert backlog["candidate_family_count"] == len(backlog["candidate_families"])
    assert all(item["design_status"] != "promotable_to_q1" for item in backlog["candidate_families"])
    prior = {item["candidate_family_id"]: item for item in backlog["candidate_families"]}
    assert prior["revision_confirmed_earnings_underreaction"]["prior_result_label"] == "insufficient_support"
    assert prior["small_cap_quality_residual_momentum_v1"]["prior_result_label"] == "reject_placebo_failure"

    report = result.artifacts["factor_pain_point_map"].read_text(encoding="utf-8").lower()
    assert "market pain point" in report
    assert "formula is measurement, not thesis" in report
    assert "not alpha evidence" in report
    assert "direct q2 entry: not allowed" in report


def test_design_ledger_validation_rejects_formula_first_rows() -> None:
    valid = _valid_ledger_row()
    invalid = dict(valid)
    invalid["market_pain_point"] = ""
    invalid["placebo_design"] = ""
    invalid["cost_capacity_risks"] = ""

    result = validate_design_ledger(pd.DataFrame([valid, invalid]))

    assert result["valid"] is False
    assert result["row_count"] == 2
    assert result["invalid_row_count"] == 1
    assert "missing_design_fields" in result["failure_reasons"][0]
    assert result["not_alpha_evidence"] is True
    assert result["direct_q2_entry_allowed"] is False


def test_design_d1_has_no_downstream_import_surface() -> None:
    module_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "factor_discovery_sandbox"
        / "factor_design_d1.py"
    )
    module_source = module_path.read_text(encoding="utf-8")
    import_surface = "\n".join(
        line for line in module_source.splitlines() if line.startswith(("import ", "from "))
    )
    forbidden_import_fragments = [
        "agentic_alpha_triage",
        "execution_aware_optimizer",
        "promotion_gate",
        "alpha_registry",
        "portfolio_os.alpha.projection",
        "typed_alpha_pilot",
    ]
    for fragment in forbidden_import_fragments:
        assert fragment not in import_surface


def _valid_ledger_row() -> dict[str, object]:
    return {
        "pain_point_id": "test_pain",
        "candidate_family_id": "test_family",
        "market_pain_point": "documented pain point",
        "mechanism_hypothesis": "documented mechanism",
        "investor_constraint_or_behavior": "documented behavior",
        "expected_universe": "documented universe",
        "expected_regime": "documented regime",
        "why_not_arbitraged_away": "documented friction",
        "observable_pre_formula_diagnostics": "diagnostic one; diagnostic two",
        "formula_measurement_role": "Formula is measurement, not thesis.",
        "placebo_design": "same coverage placebo",
        "cost_capacity_risks": "spread; ADV",
        "expected_failure_modes": "placebo dominance",
        "prior_result_label": "no_prior_result",
        "design_priority": "watchlist",
        "d1_status": "design_backlog_only",
        "next_action": "pre_formula_diagnostics",
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
    }
