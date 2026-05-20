from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from multifactor_alpha_validation.factor_library import load_factor_specs
from multifactor_alpha_validation.schema import FactorSpec


@dataclass(frozen=True)
class CandidateFilterAuditResult:
    candidate_filter_audit_path: str
    hard_excluded_path: str
    soft_resurrected_pool_path: str
    component_pool_manifest_path: str
    report_path: str
    total_candidate_count: int
    hard_excluded_count: int
    component_pool_count: int
    production_approval: bool
    live_trading: bool
    direct_q2_entry: bool
    not_alpha_evidence: bool


def run_candidate_filter_audit(
    spec_dir: Path,
    input_dir: Path,
    output_dir: Path,
) -> CandidateFilterAuditResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    specs = load_factor_specs(spec_dir)
    component_table = _read_csv(input_dir / "component_candidate_table.csv")
    failure_diagnosis = _read_csv(input_dir / "factor_failure_diagnosis.csv")

    audit = _build_audit_table(specs, component_table, failure_diagnosis)
    hard = audit.loc[audit["filter_class"].astype(str).eq("hard_excluded")].copy()
    soft = audit.loc[audit["component_pool_eligible"].map(_as_bool)].copy()

    manifest = _build_manifest(audit, soft, hard, output_dir / "soft_resurrected_component_pool.csv")

    audit_path = output_dir / "candidate_filter_audit.csv"
    hard_path = output_dir / "hard_excluded_candidates.csv"
    soft_path = output_dir / "soft_resurrected_component_pool.csv"
    manifest_path = output_dir / "component_pool_manifest.json"
    report_path = output_dir / "filter_audit_report.md"

    audit.to_csv(audit_path, index=False)
    hard.to_csv(hard_path, index=False)
    soft.to_csv(soft_path, index=False)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_path.write_text(_render_report(audit, soft, hard, manifest), encoding="utf-8")

    return CandidateFilterAuditResult(
        candidate_filter_audit_path=str(audit_path),
        hard_excluded_path=str(hard_path),
        soft_resurrected_pool_path=str(soft_path),
        component_pool_manifest_path=str(manifest_path),
        report_path=str(report_path),
        total_candidate_count=int(len(audit)),
        hard_excluded_count=int(len(hard)),
        component_pool_count=int(len(soft)),
        production_approval=False,
        live_trading=False,
        direct_q2_entry=False,
        not_alpha_evidence=True,
    )


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _build_audit_table(
    specs: list[FactorSpec],
    component_table: pd.DataFrame,
    failure_diagnosis: pd.DataFrame,
) -> pd.DataFrame:
    component_by_factor = _index_by_factor(component_table)
    diagnosis_by_factor = _index_by_factor(failure_diagnosis)
    rows: list[dict[str, Any]] = []
    for spec in sorted(specs, key=lambda item: item.factor_id):
        component_row = component_by_factor.get(spec.factor_id, {})
        diagnosis_row = diagnosis_by_factor.get(spec.factor_id, {})
        rows.append(_build_candidate_row(spec, component_row, diagnosis_row))
    return pd.DataFrame(rows, columns=_audit_columns())


def _index_by_factor(frame: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if frame.empty or "factor_id" not in frame.columns:
        return {}
    indexed: dict[str, dict[str, Any]] = {}
    for _, row in frame.iterrows():
        factor_id = str(row.get("factor_id", ""))
        if factor_id:
            indexed[factor_id] = row.to_dict()
    return indexed


def _build_candidate_row(
    spec: FactorSpec,
    component_row: dict[str, Any],
    diagnosis_row: dict[str, Any],
) -> dict[str, Any]:
    hard_reason = _hard_exclusion_reason(spec, diagnosis_row)
    if hard_reason:
        filter_class = "hard_excluded"
        component_pool_eligible = False
        component_status = "blocked_component"
        component_role = "hard_blocked_component"
        resurrection_source = "not_resurrected_hard_failure"
        eligibility_reason = hard_reason
        allowed_next_layer = "none"
    elif component_row:
        filter_class = "soft_resurrected"
        component_pool_eligible = True
        component_status = _string(component_row.get("component_status"), "eligible_component")
        component_role = _string(component_row.get("component_role"), _default_component_role(spec))
        resurrection_source = "component_gate"
        eligibility_reason = _string(
            component_row.get("eligibility_reason"),
            "Soft evidence label is retained for portfolio-level validation.",
        )
        allowed_next_layer = "portfolio_level_oos_ensemble_validation"
    else:
        filter_class = "soft_resurrected"
        component_pool_eligible = True
        component_status = _default_component_status(spec)
        component_role = _default_component_role(spec)
        resurrection_source = (
            "formal_reference_spec_not_yet_portfolio_validated"
            if spec.status == "reference"
            else "formal_factor_spec_not_yet_risk_attributed"
        )
        eligibility_reason = (
            "Formal FactorSpec has no hard validity failure; absence of clean standalone residual evidence "
            "is not a portfolio pre-screen kill condition."
        )
        allowed_next_layer = "portfolio_level_oos_ensemble_validation"

    return {
        "schema_version": "candidate_filter_audit.v1",
        "factor_id": spec.factor_id,
        "family_id": spec.family_id,
        "display_name": spec.display_name,
        "status": spec.status,
        "data_tier": spec.data_tier,
        "filter_class": filter_class,
        "component_pool_eligible": component_pool_eligible,
        "resurrection_source": resurrection_source,
        "component_status": component_status,
        "component_role": component_role,
        "eligibility_reason": eligibility_reason,
        "hard_exclusion_reason": hard_reason,
        "soft_failure_labels": _soft_failure_labels(component_row, diagnosis_row),
        "source_closeout_status": _string(
            component_row.get("source_closeout_status"),
            _string(diagnosis_row.get("closeout_status"), "not_yet_risk_attributed"),
        ),
        "source_dominant_failure_layer": _string(
            component_row.get("source_dominant_failure_layer"),
            _string(diagnosis_row.get("dominant_failure_layer"), "not_yet_risk_attributed"),
        ),
        "standalone_alpha_claim_allowed": _as_bool(component_row.get("standalone_alpha_claim_allowed")),
        "alpha_claim_allowed": False,
        "portfolio_validation_allowed": component_pool_eligible,
        "portfolio_validation_mode": "diagnostic_ensemble_only" if component_pool_eligible else "blocked",
        "allowed_next_layer": allowed_next_layer,
        "redundancy_gate_allowed": False,
        "allocator_entry_allowed": False,
        "not_alpha_evidence": True,
        "production_approval": False,
        "paper_canary": False,
        "live_trading": False,
        "security_orders": False,
        "direct_q2_entry": False,
    }


def _hard_exclusion_reason(spec: FactorSpec, diagnosis_row: dict[str, Any]) -> str:
    if spec.status == "disabled":
        if spec.disabled_reason:
            return spec.disabled_reason
        return "disabled_factor_spec"
    closeout_status = _string(diagnosis_row.get("closeout_status"))
    dominant_layer = _string(diagnosis_row.get("dominant_failure_layer"))
    primary_blocker = _string(diagnosis_row.get("primary_blocker"))
    hard_markers = {
        "blocked_pit_failure",
        "blocked_timestamp_failure",
        "pit_failure",
        "timestamp_failure",
        "lookahead",
        "survivorship_bias",
        "same_close_trading",
        "forward_return_leakage",
    }
    if closeout_status in hard_markers:
        return closeout_status
    if dominant_layer in {"pit_timestamp", "lookahead", "survivorship_bias", "forward_return_leakage"}:
        return dominant_layer
    if any(marker in primary_blocker for marker in ["same_close", "lookahead", "survivorship", "forward_return"]):
        return primary_blocker
    return ""


def _default_component_status(spec: FactorSpec) -> str:
    if spec.status == "reference":
        return "eligible_reference_component"
    if spec.data_tier == "tier_2_fundamental":
        return "eligible_fundamental_premia_component"
    if spec.data_tier == "tier_3_event":
        return "eligible_event_reference_component"
    return "eligible_component_pending_risk_attribution"


def _default_component_role(spec: FactorSpec) -> str:
    if spec.status == "reference" or spec.data_tier == "tier_3_event":
        return "reference_event_component"
    if spec.data_tier == "tier_2_fundamental":
        return "fundamental_premia_component"
    if spec.family_id in {"low_vol", "liquidity"}:
        return "hedge_or_diversifier_component"
    if spec.family_id in {"momentum", "reversal"}:
        return "style_premia_return_driver"
    return "price_volume_premia_component"


def _soft_failure_labels(component_row: dict[str, Any], diagnosis_row: dict[str, Any]) -> str:
    labels = [
        _string(component_row.get("source_closeout_status")),
        _string(component_row.get("source_dominant_failure_layer")),
        _string(diagnosis_row.get("closeout_status")),
        _string(diagnosis_row.get("dominant_failure_layer")),
    ]
    filtered = sorted({label for label in labels if label and label != "nan"})
    return "|".join(filtered)


def _build_manifest(
    audit: pd.DataFrame,
    soft: pd.DataFrame,
    hard: pd.DataFrame,
    r15_input_path: Path,
) -> dict[str, Any]:
    return {
        "schema_version": "component_pool_manifest.v1",
        "total_candidate_count": int(len(audit)),
        "component_pool_count": int(len(soft)),
        "hard_excluded_count": int(len(hard)),
        "r15_input_path": str(r15_input_path),
        "portfolio_validation_mode": "diagnostic_ensemble_only" if len(soft) else "blocked",
        "principles": [
            "Hard validity gates happen before portfolio construction.",
            "Soft evidence labels do not kill candidates before portfolio-level validation.",
            "Single-factor residual evidence is diagnostic, not a mandatory promotion gate.",
            "Final factor inclusion is decided by portfolio-level OOS, ablation, cost, and capacity contribution.",
        ],
        "hard_failure_classes": [
            "pit_failure",
            "timestamp_failure",
            "lookahead",
            "survivorship_bias",
            "same_close_trading",
            "missing_required_data",
            "forward_return_leakage",
        ],
        "soft_resurrection_classes": [
            "weak_standalone_ic",
            "insufficient_residual_evidence",
            "style_proxy_conflict",
            "benchmark_exposure",
            "high_correlation",
            "high_turnover_warning",
            "unstable_alone_plausible_diversifier",
        ],
        "non_claims": _non_claims(),
    }


def _render_report(
    audit: pd.DataFrame,
    soft: pd.DataFrame,
    hard: pd.DataFrame,
    manifest: dict[str, Any],
) -> str:
    lines = [
        "# Candidate Filter Audit",
        "",
        "This filter audit separates hard validity failures from soft single-factor evidence labels.",
        "Hard failures remain blocked. Soft failures are restored to the component pool for portfolio-level OOS validation.",
        "",
        "This is not alpha evidence, not allocator approval, not Q2 entry, and not production approval.",
        "",
        f"Total candidates audited: `{len(audit)}`",
        f"Soft resurrected component pool: `{len(soft)}`",
        f"Hard excluded candidates: `{len(hard)}`",
        f"R15 input: `{manifest['r15_input_path']}`",
        "",
        "## Hard Exclusions",
        "",
    ]
    if hard.empty:
        lines.append("No hard exclusions were found.")
    else:
        for row in hard.itertuples(index=False):
            lines.append(f"- `{row.factor_id}`: `{row.hard_exclusion_reason}`")
    lines.extend(["", "## Component Pool", ""])
    if soft.empty:
        lines.append("No component candidates were restored.")
    else:
        for row in soft.itertuples(index=False):
            lines.append(
                f"- `{row.factor_id}`: `{row.component_status}` as `{row.component_role}` "
                f"from `{row.resurrection_source}`."
            )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "R15 must use the resurrected component pool rather than only the factors that already survived R13/R14.",
            "Single-factor residual conflicts are labels for attribution and ablation, not pre-portfolio deletion rules.",
            "",
        ]
    )
    return "\n".join(lines)


def _audit_columns() -> list[str]:
    return [
        "schema_version",
        "factor_id",
        "family_id",
        "display_name",
        "status",
        "data_tier",
        "filter_class",
        "component_pool_eligible",
        "resurrection_source",
        "component_status",
        "component_role",
        "eligibility_reason",
        "hard_exclusion_reason",
        "soft_failure_labels",
        "source_closeout_status",
        "source_dominant_failure_layer",
        "standalone_alpha_claim_allowed",
        "alpha_claim_allowed",
        "portfolio_validation_allowed",
        "portfolio_validation_mode",
        "allowed_next_layer",
        "redundancy_gate_allowed",
        "allocator_entry_allowed",
        "not_alpha_evidence",
        "production_approval",
        "paper_canary",
        "live_trading",
        "security_orders",
        "direct_q2_entry",
    ]


def _non_claims() -> dict[str, bool]:
    return {
        "not_alpha_evidence": True,
        "production_approval": False,
        "paper_canary": False,
        "live_trading": False,
        "security_orders": False,
        "direct_q2_entry": False,
        "allocator_entry": False,
    }


def _string(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    if text.lower() == "nan" or text == "":
        return default
    return text


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}
