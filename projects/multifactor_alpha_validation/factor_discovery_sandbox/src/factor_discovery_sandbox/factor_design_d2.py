"""FD-D2 pre-formula diagnostics for mechanism-first factor discovery."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import pandas as pd

from .factor_design import DESIGN_GUARDS
from .factor_design_d1 import build_design_ledger


SUMMARY_SCHEMA_VERSION = "fd_factor_design_d2_summary.v1"
DIAGNOSTIC_SCHEMA_VERSION = "fd_pre_formula_diagnostics.v1"
DECISIONS_SCHEMA_VERSION = "fd_candidate_family_d2_decisions.v1"

REQUIRED_D2_COLUMNS = (
    "schema_version",
    "stage",
    "pain_point_id",
    "candidate_family_id",
    "d1_ledger_source",
    "market_pain_point",
    "mechanism_hypothesis",
    "coverage_diagnostic",
    "pit_timestamp_diagnostic",
    "placebo_design_diagnostic",
    "exposure_contamination_diagnostic",
    "cost_capacity_diagnostic",
    "prior_result_label",
    "d2_decision_label",
    "d2_decision_reason",
    "ready_for_d3_charter",
    "formula_validation_allowed",
    "formula_validation_ran",
    "q1_candidate_review_eligible",
    "next_action",
    "not_alpha_evidence",
    "direct_q2_entry_allowed",
)


@dataclass(frozen=True)
class FDFactorDesignD2Result:
    """Artifacts and summary for FD-D2."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_factor_design_d2(
    output_dir: str | Path,
    d1_ledger_path: str | Path | None = None,
) -> FDFactorDesignD2Result:
    """Write FD-D2 pre-formula diagnostics without running validation."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    ledger_source = str(d1_ledger_path) if d1_ledger_path is not None else "built_in_fd_d1_ledger"
    ledger = pd.read_csv(d1_ledger_path) if d1_ledger_path is not None else build_design_ledger()
    diagnostics = build_pre_formula_diagnostics(ledger, d1_ledger_source=ledger_source)
    validation = validate_pre_formula_diagnostics(diagnostics)
    decisions = build_d2_decisions(diagnostics, validation)
    summary = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "stage": "FD-D2",
        "d1_ledger_source": ledger_source,
        "pre_formula_diagnostics_only": True,
        "formula_validation_ran": False,
        "diagnostic_row_count": int(len(diagnostics)),
        "d2_decision_count": int(len(decisions["candidate_families"])),
        "ready_for_d3_count": int(decisions["ready_for_d3_count"]),
        "formula_validation_allowed_count": int(decisions["formula_validation_allowed_count"]),
        "diagnostics_valid": bool(validation["valid"]),
        "diagnostics_validation": validation,
        **DESIGN_GUARDS,
    }
    artifacts = {
        "pre_formula_diagnostics": output_path / "pre_formula_diagnostics.csv",
        "candidate_family_d2_decisions": output_path / "candidate_family_d2_decisions.json",
        "pre_formula_diagnostic_summary": output_path / "pre_formula_diagnostic_summary.json",
        "pre_formula_diagnostic_report": output_path / "pre_formula_diagnostic_report.md",
    }
    diagnostics.to_csv(artifacts["pre_formula_diagnostics"], index=False)
    artifacts["candidate_family_d2_decisions"].write_text(
        json.dumps(decisions, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["pre_formula_diagnostic_summary"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["pre_formula_diagnostic_report"].write_text(
        render_pre_formula_report(summary, diagnostics, decisions),
        encoding="utf-8",
    )
    return FDFactorDesignD2Result(summary=summary, artifacts=artifacts)


def build_pre_formula_diagnostics(ledger: pd.DataFrame, d1_ledger_source: str) -> pd.DataFrame:
    """Build deterministic pre-formula diagnostics from the FD-D1 ledger."""

    rows = []
    for row in ledger.to_dict(orient="records"):
        profile = _diagnostic_profile(row)
        rows.append(
            {
                "schema_version": DIAGNOSTIC_SCHEMA_VERSION,
                "stage": "FD-D2",
                "pain_point_id": row["pain_point_id"],
                "candidate_family_id": row["candidate_family_id"],
                "d1_ledger_source": d1_ledger_source,
                "market_pain_point": row["market_pain_point"],
                "mechanism_hypothesis": row["mechanism_hypothesis"],
                "coverage_diagnostic": profile["coverage_diagnostic"],
                "pit_timestamp_diagnostic": profile["pit_timestamp_diagnostic"],
                "placebo_design_diagnostic": profile["placebo_design_diagnostic"],
                "exposure_contamination_diagnostic": profile["exposure_contamination_diagnostic"],
                "cost_capacity_diagnostic": profile["cost_capacity_diagnostic"],
                "prior_result_label": row["prior_result_label"],
                "d2_decision_label": profile["d2_decision_label"],
                "d2_decision_reason": profile["d2_decision_reason"],
                "ready_for_d3_charter": bool(profile["d2_decision_label"] == "ready_for_d3_charter"),
                "formula_validation_allowed": False,
                "formula_validation_ran": False,
                "q1_candidate_review_eligible": False,
                "next_action": profile["next_action"],
                "not_alpha_evidence": True,
                "direct_q2_entry_allowed": False,
            },
        )
    return pd.DataFrame(rows, columns=REQUIRED_D2_COLUMNS)


def validate_pre_formula_diagnostics(diagnostics: pd.DataFrame) -> dict[str, object]:
    """Validate that FD-D2 remained pre-formula and sandbox-only."""

    missing_columns = [column for column in REQUIRED_D2_COLUMNS if column not in diagnostics.columns]
    invalid_rows: list[int] = []
    failure_reasons: list[str] = []
    if missing_columns:
        failure_reasons.append(f"missing_columns:{','.join(missing_columns)}")
        invalid_rows = list(range(len(diagnostics)))
    else:
        for index, row in diagnostics.iterrows():
            reasons = []
            if row.get("formula_validation_ran") is not False:
                reasons.append("formula_validation_ran_in_d2")
            if row.get("formula_validation_allowed") is not False:
                reasons.append("formula_validation_allowed_in_d2")
            if row.get("q1_candidate_review_eligible") is not False:
                reasons.append("q1_candidate_review_eligible_in_d2")
            if row.get("not_alpha_evidence") is not True or row.get("direct_q2_entry_allowed") is not False:
                reasons.append("sandbox_guard_missing")
            required_text_fields = [
                "coverage_diagnostic",
                "pit_timestamp_diagnostic",
                "placebo_design_diagnostic",
                "exposure_contamination_diagnostic",
                "cost_capacity_diagnostic",
                "d2_decision_label",
                "d2_decision_reason",
            ]
            missing_text = [field for field in required_text_fields if not _nonempty(row.get(field))]
            if missing_text:
                reasons.append(f"missing_diagnostic_fields:{','.join(missing_text)}")
            if reasons:
                invalid_rows.append(int(index))
                failure_reasons.append(f"row_{index}:{';'.join(reasons)}")
    return {
        "schema_version": f"{DIAGNOSTIC_SCHEMA_VERSION}.validation",
        "stage": "FD-D2",
        "valid": bool(not failure_reasons),
        "row_count": int(len(diagnostics)),
        "invalid_row_count": int(len(set(invalid_rows))),
        "failure_reasons": failure_reasons,
        "pre_formula_diagnostics_only": True,
        "formula_validation_ran": False,
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
    }


def build_d2_decisions(diagnostics: pd.DataFrame, validation: Mapping[str, object]) -> dict[str, object]:
    """Build D2 decisions while blocking formula validation."""

    families = []
    for row in diagnostics.to_dict(orient="records"):
        families.append(
            {
                "candidate_family_id": row["candidate_family_id"],
                "pain_point_id": row["pain_point_id"],
                "d2_decision_label": row["d2_decision_label"],
                "d2_decision_reason": row["d2_decision_reason"],
                "ready_for_d3_charter": bool(row["ready_for_d3_charter"]),
                "formula_validation_allowed": False,
                "q1_candidate_review_eligible": False,
                "next_action": row["next_action"],
                "not_alpha_evidence": True,
                "direct_q2_entry_allowed": False,
            },
        )
    return {
        "schema_version": DECISIONS_SCHEMA_VERSION,
        "stage": "FD-D2",
        "candidate_family_count": len(families),
        "ready_for_d3_count": int(sum(1 for item in families if item["ready_for_d3_charter"])),
        "formula_validation_allowed_count": 0,
        "diagnostics_valid": bool(validation["valid"]),
        "candidate_families": families,
        "pre_formula_diagnostics_only": True,
        **DESIGN_GUARDS,
    }


def render_pre_formula_report(
    summary: Mapping[str, object],
    diagnostics: pd.DataFrame,
    decisions: Mapping[str, object],
) -> str:
    """Render the FD-D2 report."""

    lines = [
        "# FD-D2 Pre-Formula Diagnostics",
        "",
        "not alpha evidence",
        "direct Q2 entry: not allowed",
        "formula validation ran: false",
        "allocator entry: blocked",
        "Q1 entry: blocked",
        "Q2 entry: blocked",
        "Alpha Registry update: blocked",
        "",
        "FD-D2 checks whether each FD-D1 pain point has enough pre-formula support to draft a D3 candidate-family charter.",
        "It does not score securities, run OOS validation, or create alpha evidence.",
        "",
        "## Summary",
        "",
        f"- diagnostic rows: {summary['diagnostic_row_count']}",
        f"- ready for D3 charter: {summary['ready_for_d3_count']}",
        f"- formula validation allowed: {summary['formula_validation_allowed_count']}",
        "",
        "## Decisions",
        "",
    ]
    for row in diagnostics.itertuples(index=False):
        lines.extend(
            [
                f"### {row.candidate_family_id}",
                "",
                f"- decision: `{row.d2_decision_label}`",
                f"- reason: {row.d2_decision_reason}",
                f"- coverage: {row.coverage_diagnostic}",
                f"- PIT/timestamp: {row.pit_timestamp_diagnostic}",
                f"- placebo: {row.placebo_design_diagnostic}",
                f"- exposure: {row.exposure_contamination_diagnostic}",
                f"- cost/capacity: {row.cost_capacity_diagnostic}",
                f"- next action: {row.next_action}",
                "",
            ],
        )
    lines.extend(
        [
            "## Boundary",
            "",
            f"Ready-for-D3 count in decision payload: {decisions['ready_for_d3_count']}",
            "No FD-D2 row is formula-validation allowed or Q1 candidate-review eligible.",
            "",
        ],
    )
    return "\n".join(lines)


def _diagnostic_profile(row: Mapping[str, object]) -> dict[str, object]:
    candidate = str(row["candidate_family_id"])
    prior = str(row.get("prior_result_label", ""))
    if candidate == "revision_confirmed_earnings_underreaction":
        return {
            "coverage_diagnostic": "blocked: expanded SUE/revision universe does not match default Nasdaq100 daily ADV coverage",
            "pit_timestamp_diagnostic": "pass with caveat: estimate snapshots are PIT but sparse and require coverage-aligned price panel",
            "placebo_design_diagnostic": "blocked: shifted-event and random same-coverage placebos were stronger in prior diagnostic",
            "exposure_contamination_diagnostic": "needs industry and event-coverage exposure attribution after coverage repair",
            "cost_capacity_diagnostic": "needs broad ADV coverage before capacity filter can be trusted",
            "d2_decision_label": "blocked_coverage_alignment",
            "d2_decision_reason": "Coverage mismatch explains too much of the prior result to draft a D3 formula charter.",
            "next_action": "build coverage-aligned price/ADV panel before any formula redesign",
        }
    if candidate == "sue_event_timing_and_timestamp_repair":
        return {
            "coverage_diagnostic": "partial: event and CRSP coverage improved, but timestamp-source proof remains controlling",
            "pit_timestamp_diagnostic": "blocked: no auditable earlier exact public-availability timestamp was found",
            "placebo_design_diagnostic": "blocked: shifted-event placebo cannot be used as tradable evidence",
            "exposure_contamination_diagnostic": "not reached until timestamp anchor is fixed",
            "cost_capacity_diagnostic": "not reached until timestamp anchor is fixed",
            "d2_decision_label": "blocked_timestamp_observability",
            "d2_decision_reason": "The market pain point cannot be measured until the tradable earnings timestamp is auditable.",
            "next_action": "keep SUE formula work blocked unless exact public release timestamps become available",
        }
    if candidate == "small_cap_quality_residual_momentum_v1":
        return {
            "coverage_diagnostic": "pass: small-cap data admission and quality coverage are available",
            "pit_timestamp_diagnostic": "pass with caveat: quality fallback lag must remain explicit",
            "placebo_design_diagnostic": "blocked: same-coverage and capacity-matched placebos beat prior live reads",
            "exposure_contamination_diagnostic": "blocked: size/liquidity payoff concentration remains unresolved",
            "cost_capacity_diagnostic": "blocked: cost-adjusted and capacity diagnostics explain the prior failure",
            "d2_decision_label": "blocked_placebo_prior",
            "d2_decision_reason": "Prior diagnostics show placebo and capacity explanations before any formula redesign.",
            "next_action": "separate tradeability pain from alpha mechanism before writing a new candidate formula",
        }
    if candidate == "momentum_12m_ex1m_low_vol_3m":
        return {
            "coverage_diagnostic": "pass: WRDS daily PIT bundle has enough price-volume history",
            "pit_timestamp_diagnostic": "pass: price-volume timestamps are compatible with next-session tradability",
            "placebo_design_diagnostic": "mixed: strongest placebo gate blocked industry-neutral and capacity variants",
            "exposure_contamination_diagnostic": "needs trend-quality and industry exposure diagnostics before formula changes",
            "cost_capacity_diagnostic": "mixed: capacity-filtered prior variant failed",
            "d2_decision_label": "diagnostic_only_prior_mixed",
            "d2_decision_reason": "The pain point is plausible but prior mixed evidence requires non-formula diagnostics first.",
            "next_action": "run low-vol-winner versus high-vol-winner pre-formula diagnostic by regime and capacity bucket",
        }
    if candidate == "sector_neutral_residual_momentum":
        return {
            "coverage_diagnostic": "pass for design: PIT sector labels and daily price histories are available in the daily bundle",
            "pit_timestamp_diagnostic": "pass for design: sector membership and price timestamps can be audited before scoring",
            "placebo_design_diagnostic": "ready: sector-shuffle, raw momentum control, and same-sector random controls are defined",
            "exposure_contamination_diagnostic": "ready: D3 charter must prove residual signal is not raw momentum or sector beta",
            "cost_capacity_diagnostic": "ready with caveat: D3 charter must include turnover and ADV bucket pre-gates",
            "d2_decision_label": "ready_for_d3_charter",
            "d2_decision_reason": "The pain point has defined diagnostics and no prior blocking result, so a D3 charter is the next step.",
            "next_action": "draft D3 charter for within-sector residual momentum without running formula validation yet",
        }
    if candidate == "liquidity_activity_shock":
        return {
            "coverage_diagnostic": "partial: volume/ADV exists, but attention-event labels are not defined",
            "pit_timestamp_diagnostic": "pass for price-volume; unknown for external attention/event labels",
            "placebo_design_diagnostic": "needs design: capacity-matched and event-contamination controls must be specified in detail",
            "exposure_contamination_diagnostic": "blocked for now: size and liquidity exposure can fully explain activity signals",
            "cost_capacity_diagnostic": "needs pre-formula diagnostics: activity shocks can be pure capacity illusion",
            "d2_decision_label": "needs_pre_formula_data_diagnostics",
            "d2_decision_reason": "The pain point is plausible but D2 needs activity persistence and event-contamination diagnostics.",
            "next_action": "measure ADV shock persistence and size/liquidity exposure before D3 charter",
        }
    return {
        "coverage_diagnostic": "needs review",
        "pit_timestamp_diagnostic": "needs review",
        "placebo_design_diagnostic": "needs review",
        "exposure_contamination_diagnostic": "needs review",
        "cost_capacity_diagnostic": "needs review",
        "d2_decision_label": "needs_pre_formula_data_diagnostics",
        "d2_decision_reason": f"No explicit D2 profile exists for prior result {prior}.",
        "next_action": "write a specific D2 diagnostic profile before D3",
    }


def _nonempty(value: object) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return value is not None
