"""FD-D1 market pain-point map for mechanism-first factor discovery."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import pandas as pd

from .factor_design import DESIGN_GUARDS, REQUIRED_DESIGN_CONTRACT_KEYS, build_design_contract


SCHEMA_VERSION = "fd_factor_design_d1_summary.v1"
LEDGER_SCHEMA_VERSION = "fd_factor_design_ledger.v1"
BACKLOG_SCHEMA_VERSION = "fd_candidate_family_backlog.v1"

REQUIRED_LEDGER_COLUMNS = (
    "pain_point_id",
    "candidate_family_id",
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
    "prior_result_label",
    "design_priority",
    "d1_status",
    "next_action",
    "not_alpha_evidence",
    "direct_q2_entry_allowed",
)


@dataclass(frozen=True)
class FDFactorDesignD1Result:
    """Artifacts and summary for FD-D1."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_factor_design_d1(output_dir: str | Path) -> FDFactorDesignD1Result:
    """Write the FD-D1 pain-point map, design ledger, and backlog artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    ledger = build_design_ledger()
    validation = validate_design_ledger(ledger)
    backlog = build_candidate_family_backlog(ledger, validation)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "stage": "FD-D1",
        "design_layer_required_before_formula": True,
        "formula_first_candidates_blocked": True,
        "ledger_row_count": int(len(ledger)),
        "candidate_family_count": int(len(backlog["candidate_families"])),
        "ledger_valid": bool(validation["valid"]),
        "ledger_validation": validation,
        **DESIGN_GUARDS,
    }

    artifacts = {
        "factor_pain_point_map": output_path / "factor_pain_point_map.md",
        "factor_design_ledger": output_path / "factor_design_ledger.csv",
        "candidate_family_backlog": output_path / "candidate_family_backlog.json",
        "factor_design_d1_summary": output_path / "factor_design_d1_summary.json",
    }
    ledger.to_csv(artifacts["factor_design_ledger"], index=False)
    artifacts["candidate_family_backlog"].write_text(
        json.dumps(backlog, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["factor_design_d1_summary"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["factor_pain_point_map"].write_text(render_pain_point_map(summary, ledger, backlog), encoding="utf-8")
    return FDFactorDesignD1Result(summary=summary, artifacts=artifacts)


def build_design_ledger() -> pd.DataFrame:
    """Build a deterministic FD-D1 ledger of pain points and candidate families."""

    rows = [
        _ledger_row(
            pain_point_id="slow_information_diffusion_in_trends",
            candidate_family_id="momentum_12m_ex1m_low_vol_3m",
            mechanism_family="momentum_low_vol",
            prior_result_label="mixed_initial_diagnostic_gate",
            design_priority="calibration_prior",
            d1_status="prior_diagnostic_only_reframe_under_design_layer",
            next_action="run_pre_formula_trend_quality_diagnostics_before_any_formula_change",
        ),
        _ledger_row(
            pain_point_id="small_cap_capacity_quality_constraint",
            candidate_family_id="small_cap_quality_residual_momentum_v1",
            mechanism_family="small_cap_quality_residual_momentum",
            prior_result_label="reject_placebo_failure",
            design_priority="high_learning_value",
            d1_status="prior_diagnostic_only_reframe_under_design_layer",
            next_action="separate_capacity_tradeability_pain_from_alpha_mechanism_before_new_formula",
        ),
        _ledger_row(
            pain_point_id="post_earnings_revision_underreaction",
            candidate_family_id="revision_confirmed_earnings_underreaction",
            mechanism_family="revision_confirmed_earnings_underreaction",
            prior_result_label="insufficient_support",
            design_priority="blocked_until_coverage_alignment",
            d1_status="prior_diagnostic_only_reframe_under_design_layer",
            next_action="align_price_volume_adv_coverage_to_expanded_sue_events_before_d2",
        ),
        _ledger_row(
            pain_point_id="earnings_timestamp_observability_gap",
            candidate_family_id="sue_event_timing_and_timestamp_repair",
            mechanism_family="sue_event_timing",
            prior_result_label="timestamp_enrichment_no_repair_sue_blocked",
            design_priority="data_boundary_first",
            d1_status="blocked_before_formula",
            next_action="find_auditable_exact_public_availability_timestamps_or_keep_sue_blocked",
        ),
        _ledger_row(
            pain_point_id="within_sector_specific_underreaction",
            candidate_family_id="sector_neutral_residual_momentum",
            mechanism_family="sector_neutral_residual_momentum",
            prior_result_label="no_standalone_candidate_yet",
            design_priority="candidate_backlog",
            d1_status="design_backlog_only",
            next_action="measure_within_sector_dispersion_and_raw_momentum_duplicate_risk",
        ),
        _ledger_row(
            pain_point_id="activity_attention_capacity_confusion",
            candidate_family_id="liquidity_activity_shock",
            mechanism_family="liquidity_shock",
            prior_result_label="no_standalone_candidate_yet",
            design_priority="watchlist",
            d1_status="design_backlog_only",
            next_action="separate_attention_shock_from_adv_capacity_and_size_exposure",
        ),
    ]
    return pd.DataFrame(rows, columns=REQUIRED_LEDGER_COLUMNS)


def validate_design_ledger(ledger: pd.DataFrame) -> dict[str, object]:
    """Validate that each D1 ledger row is mechanism-first and sandbox-only."""

    missing_columns = [column for column in REQUIRED_LEDGER_COLUMNS if column not in ledger.columns]
    failure_reasons: list[str] = []
    invalid_indices: list[int] = []
    if missing_columns:
        failure_reasons.append(f"missing_columns:{','.join(missing_columns)}")
        invalid_indices = list(range(len(ledger)))
    else:
        design_columns = [
            column
            for column in REQUIRED_DESIGN_CONTRACT_KEYS
            if column in REQUIRED_LEDGER_COLUMNS and column != "formula_measurement_role"
        ]
        design_columns.append("formula_measurement_role")
        for index, row in ledger.iterrows():
            missing_fields = [column for column in design_columns if not _nonempty(row.get(column))]
            formula_role = str(row.get("formula_measurement_role", "")).lower()
            guard_failure = row.get("not_alpha_evidence") is not True or row.get("direct_q2_entry_allowed") is not False
            if missing_fields or "measurement" not in formula_role or "not thesis" not in formula_role or guard_failure:
                invalid_indices.append(int(index))
                reason_parts = []
                if missing_fields:
                    reason_parts.append(f"missing_design_fields:{','.join(sorted(missing_fields))}")
                if "measurement" not in formula_role or "not thesis" not in formula_role:
                    reason_parts.append("formula_measurement_boundary_missing")
                if guard_failure:
                    reason_parts.append("sandbox_guard_missing")
                failure_reasons.append(f"row_{index}:{';'.join(reason_parts)}")

    return {
        "schema_version": f"{LEDGER_SCHEMA_VERSION}.validation",
        "stage": "FD-D1",
        "valid": bool(not failure_reasons),
        "row_count": int(len(ledger)),
        "invalid_row_count": int(len(set(invalid_indices))),
        "failure_reasons": failure_reasons,
        "required_columns": list(REQUIRED_LEDGER_COLUMNS),
        "design_layer_required_before_formula": True,
        "formula_first_candidates_blocked": True,
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
    }


def build_candidate_family_backlog(ledger: pd.DataFrame, validation: Mapping[str, object]) -> dict[str, object]:
    """Build a backlog that preserves old diagnostics without promoting them."""

    families = []
    for row in ledger.to_dict(orient="records"):
        families.append(
            {
                "candidate_family_id": row["candidate_family_id"],
                "pain_point_id": row["pain_point_id"],
                "design_status": row["d1_status"],
                "design_priority": row["design_priority"],
                "prior_result_label": row["prior_result_label"],
                "next_action": row["next_action"],
                "candidate_validation_allowed": bool(row["d1_status"] == "design_backlog_only"),
                "q1_candidate_review_eligible": False,
                "not_alpha_evidence": True,
                "direct_q2_entry_allowed": False,
            },
        )
    return {
        "schema_version": BACKLOG_SCHEMA_VERSION,
        "stage": "FD-D1",
        "candidate_family_count": len(families),
        "candidate_families": families,
        "ledger_valid": bool(validation["valid"]),
        "design_layer_required_before_formula": True,
        "formula_first_candidates_blocked": True,
        **DESIGN_GUARDS,
    }


def render_pain_point_map(
    summary: Mapping[str, object],
    ledger: pd.DataFrame,
    backlog: Mapping[str, object],
) -> str:
    """Render a concise mechanism-first D1 report."""

    lines = [
        "# FD-D1 Factor Pain Point Map",
        "",
        "not alpha evidence",
        "direct Q2 entry: not allowed",
        "allocator entry: blocked",
        "Q1 entry: blocked",
        "Q2 entry: blocked",
        "Alpha Registry update: blocked",
        "",
        "Formula is measurement, not thesis.",
        "",
        "## Operating Rule",
        "",
        (
            "FD-D1 maps market pain point -> mechanism hypothesis -> observable pre-formula diagnostics "
            "before any candidate formula can be treated as a validation target."
        ),
        "",
        "## Market Pain Point Ledger",
        "",
    ]
    for row in ledger.itertuples(index=False):
        lines.extend(
            [
                f"### {row.pain_point_id}",
                "",
                f"- candidate family: `{row.candidate_family_id}`",
                f"- market pain point: {row.market_pain_point}",
                f"- mechanism hypothesis: {row.mechanism_hypothesis}",
                f"- investor constraint or behavior: {row.investor_constraint_or_behavior}",
                f"- observable diagnostics: {row.observable_pre_formula_diagnostics}",
                f"- placebo design: {row.placebo_design}",
                f"- cost/capacity risks: {row.cost_capacity_risks}",
                f"- prior result label: `{row.prior_result_label}`",
                f"- D1 status: `{row.d1_status}`",
                f"- next action: {row.next_action}",
                "",
            ],
        )
    lines.extend(
        [
            "## Backlog Boundary",
            "",
            f"Candidate family count: {backlog['candidate_family_count']}",
            f"Ledger valid: {str(bool(summary['ledger_valid'])).lower()}",
            "",
            "No FD-D1 row is Q1 candidate review eligible. Prior diagnostics are preserved as diagnostic history only.",
            "",
        ],
    )
    return "\n".join(lines)


def _ledger_row(
    pain_point_id: str,
    candidate_family_id: str,
    mechanism_family: str,
    prior_result_label: str,
    design_priority: str,
    d1_status: str,
    next_action: str,
) -> dict[str, object]:
    contract = build_design_contract(candidate_family_id, mechanism_family)
    return {
        "pain_point_id": pain_point_id,
        "candidate_family_id": candidate_family_id,
        "market_pain_point": _as_text(contract["market_pain_point"]),
        "mechanism_hypothesis": _as_text(contract["mechanism_hypothesis"]),
        "investor_constraint_or_behavior": _as_text(contract["investor_constraint_or_behavior"]),
        "expected_universe": _as_text(contract["expected_universe"]),
        "expected_regime": _as_text(contract["expected_regime"]),
        "why_not_arbitraged_away": _as_text(contract["why_not_arbitraged_away"]),
        "observable_pre_formula_diagnostics": _as_text(contract["observable_pre_formula_diagnostics"]),
        "formula_measurement_role": "Formula is measurement, not thesis.",
        "placebo_design": _as_text(contract["placebo_design"]),
        "cost_capacity_risks": _as_text(contract["cost_capacity_risks"]),
        "expected_failure_modes": _as_text(contract["expected_failure_modes"]),
        "prior_result_label": prior_result_label,
        "design_priority": design_priority,
        "d1_status": d1_status,
        "next_action": next_action,
        "not_alpha_evidence": True,
        "direct_q2_entry_allowed": False,
    }


def _as_text(value: object) -> str:
    if isinstance(value, (list, tuple, set)):
        return "; ".join(str(item) for item in value)
    return str(value)


def _nonempty(value: object) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return value is not None
