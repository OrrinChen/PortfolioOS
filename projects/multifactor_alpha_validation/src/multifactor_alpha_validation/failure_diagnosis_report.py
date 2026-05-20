from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class FailureDiagnosisReportResult:
    factor_diagnosis_path: str
    qqq_guard_review_path: str
    report_path: str
    factor_count: int
    qqq_guard_hard_gate_recommended: bool
    production_approval: bool
    live_trading: bool
    direct_q2_entry: bool
    not_alpha_evidence: bool


MIN_RESIDUAL_POSITIVE_RATE = 0.55


def run_failure_diagnosis_report(input_dir: Path, output_dir: Path) -> FailureDiagnosisReportResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    decisions = _read_csv(input_dir / "strict_residual_closeout_decision_table.csv")
    by_period = _read_csv(input_dir / "factor_attribution_waterfall_by_period.csv")

    diagnosis = _build_factor_diagnosis(decisions, by_period)
    qqq_review = _build_qqq_guard_review(diagnosis)

    factor_diagnosis_path = output_dir / "factor_failure_diagnosis.csv"
    qqq_guard_review_path = output_dir / "qqq_relative_guard_review.json"
    report_path = output_dir / "factor_failure_diagnosis_report.md"
    diagnosis.to_csv(factor_diagnosis_path, index=False)
    qqq_guard_review_path.write_text(json.dumps(qqq_review, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_path.write_text(_render_report(diagnosis, qqq_review), encoding="utf-8")

    return FailureDiagnosisReportResult(
        factor_diagnosis_path=str(factor_diagnosis_path),
        qqq_guard_review_path=str(qqq_guard_review_path),
        report_path=str(report_path),
        factor_count=int(len(diagnosis)),
        qqq_guard_hard_gate_recommended=bool(qqq_review["hard_gate_recommended_for_long_short_spread"]),
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


def _build_factor_diagnosis(decisions: pd.DataFrame, by_period: pd.DataFrame) -> pd.DataFrame:
    if decisions.empty:
        return pd.DataFrame(columns=_diagnosis_columns())
    rows: list[dict[str, Any]] = []
    for _, row in decisions.iterrows():
        factor_id = str(row.get("factor_id", ""))
        factor_periods = (
            by_period[by_period["factor_id"].astype(str).eq(factor_id)].copy()
            if not by_period.empty and "factor_id" in by_period.columns
            else pd.DataFrame()
        )
        period_stats = _period_stats(factor_periods)
        dominant_layer = _dominant_failure_layer(row, period_stats)
        would_pass_without_qqq = _would_pass_without_qqq_guard(row, period_stats)
        rows.append(
            {
                "schema_version": "factor_failure_diagnosis.v1",
                "factor_id": factor_id,
                "stop_layer": str(row.get("stop_layer", "strict_residual_closeout") or "strict_residual_closeout"),
                "closeout_status": str(row.get("closeout_status", "diagnostic_only")),
                "dominant_failure_layer": dominant_layer,
                "primary_blocker": str(row.get("primary_blocker", "")),
                "decision_reason": str(row.get("decision_reason", "")),
                "period_count": int(_numeric(row.get("period_count"), default=period_stats["period_count"])),
                "gross_spread_mean": _round(row.get("gross_spread_mean")),
                "qqq_relative_spread_mean": _round(row.get("qqq_relative_spread_mean")),
                "beta_adjusted_spread_mean": _round(row.get("beta_adjusted_spread_mean")),
                "industry_adjusted_spread_mean": _round(row.get("industry_adjusted_spread_mean")),
                "style_proxy_adjusted_spread_mean": _round(row.get("style_proxy_adjusted_spread_mean")),
                "full_residual_spread_mean": _round(row.get("full_residual_spread_mean")),
                "residual_positive_rate": _round(row.get("residual_positive_rate")),
                "period_residual_positive_rate": _round(period_stats["residual_positive_rate"]),
                "style_proxy_conflict_count": int(_numeric(row.get("style_proxy_conflict_count"), default=0)),
                "period_style_proxy_conflict_count": int(period_stats["style_proxy_conflict_count"]),
                "qqq_guard_flagged": _numeric(row.get("qqq_relative_spread_mean")) < 0.0,
                "qqq_guard_assessment": _qqq_guard_assessment(row, would_pass_without_qqq),
                "would_pass_without_qqq_guard": would_pass_without_qqq,
                "recommended_action": _recommended_action(dominant_layer, would_pass_without_qqq),
                "redundancy_gate_allowed": False,
                "allocator_entry_allowed": False,
                "not_alpha_evidence": True,
                "production_approval": False,
                "direct_q2_entry": False,
            }
        )
    return pd.DataFrame(rows, columns=_diagnosis_columns())


def _period_stats(factor_periods: pd.DataFrame) -> dict[str, float | int]:
    if factor_periods.empty:
        return {"period_count": 0, "residual_positive_rate": 0.0, "style_proxy_conflict_count": 0}
    residual = pd.to_numeric(factor_periods.get("full_residual_spread", pd.Series(dtype=float)), errors="coerce")
    status = factor_periods.get("waterfall_status", pd.Series(dtype=str)).astype(str)
    return {
        "period_count": int(len(factor_periods)),
        "residual_positive_rate": float(residual.gt(0.0).mean()) if len(residual) else 0.0,
        "style_proxy_conflict_count": int(status.eq("style_proxy_conflict").sum()),
    }


def _dominant_failure_layer(row: pd.Series, period_stats: dict[str, float | int]) -> str:
    status = str(row.get("closeout_status", ""))
    primary = str(row.get("primary_blocker", ""))
    qqq_relative = _numeric(row.get("qqq_relative_spread_mean"))
    beta_adjusted = _numeric(row.get("beta_adjusted_spread_mean"))
    industry_adjusted = _numeric(row.get("industry_adjusted_spread_mean"))
    full_residual = _numeric(row.get("full_residual_spread_mean"))
    residual_positive_rate = _numeric(row.get("residual_positive_rate"), default=period_stats["residual_positive_rate"])

    if status == "insufficient_coverage":
        return "coverage"
    if "same_close" in primary or status == "blocked_pit_failure":
        return "pit_timestamp"
    if full_residual <= 0.0 or residual_positive_rate < MIN_RESIDUAL_POSITIVE_RATE:
        return "residual_stability"
    if qqq_relative < 0.0 and beta_adjusted >= 0.0 and industry_adjusted >= 0.0 and full_residual > 0.0:
        return "qqq_relative_guard"
    if beta_adjusted < 0.0:
        return "beta_exposure"
    if industry_adjusted < 0.0:
        return "industry_exposure"
    if status == "style_proxy_conflict":
        return "style_proxy_conflict"
    return "mixed_attribution"


def _would_pass_without_qqq_guard(row: pd.Series, period_stats: dict[str, float | int]) -> bool:
    gross = _numeric(row.get("gross_spread_mean"))
    beta_adjusted = _numeric(row.get("beta_adjusted_spread_mean"))
    industry_adjusted = _numeric(row.get("industry_adjusted_spread_mean"))
    style_adjusted = _numeric(row.get("style_proxy_adjusted_spread_mean"))
    full_residual = _numeric(row.get("full_residual_spread_mean"))
    residual_positive_rate = _numeric(row.get("residual_positive_rate"), default=period_stats["residual_positive_rate"])
    return (
        gross > 0.0
        and beta_adjusted >= 0.0
        and industry_adjusted >= 0.0
        and style_adjusted >= 0.0
        and full_residual > 0.0
        and residual_positive_rate >= MIN_RESIDUAL_POSITIVE_RATE
    )


def _qqq_guard_assessment(row: pd.Series, would_pass_without_qqq_guard: bool) -> str:
    qqq_relative = _numeric(row.get("qqq_relative_spread_mean"))
    if qqq_relative >= 0.0:
        return "not_binding"
    if would_pass_without_qqq_guard:
        return "over_strict_as_long_short_hard_gate"
    return "diagnostic_context_not_sole_blocker"


def _recommended_action(dominant_layer: str, would_pass_without_qqq_guard: bool) -> str:
    if would_pass_without_qqq_guard and dominant_layer == "qqq_relative_guard":
        return "review_qqq_guard_not_promote"
    if dominant_layer == "residual_stability":
        return "stop_residual_not_stable"
    if dominant_layer in {"beta_exposure", "industry_exposure", "style_proxy_conflict"}:
        return "stop_exposure_conflict"
    return "diagnostic_only"


def _build_qqq_guard_review(diagnosis: pd.DataFrame) -> dict[str, Any]:
    if diagnosis.empty:
        qqq_negative_count = 0
        rescued = []
        sole_blockers = []
    else:
        qqq_negative = diagnosis["qqq_guard_flagged"].map(_as_bool)
        rescued_mask = diagnosis["would_pass_without_qqq_guard"].map(_as_bool)
        sole_mask = diagnosis["dominant_failure_layer"].astype(str).eq("qqq_relative_guard")
        qqq_negative_count = int(qqq_negative.sum())
        rescued = diagnosis.loc[rescued_mask, "factor_id"].astype(str).tolist()
        sole_blockers = diagnosis.loc[sole_mask, "factor_id"].astype(str).tolist()
    return {
        "schema_version": "qqq_relative_guard_review.v1",
        "guard_scope": "diagnostic_context_for_long_short_factor_spreads",
        "hard_gate_recommended_for_long_short_spread": False,
        "over_strict_as_hard_gate": True,
        "reason": (
            "A benchmark return is not the same unit as a long-short factor sleeve spread; "
            "QQQ-relative readouts are useful context, but beta-adjusted and residual evidence "
            "should carry the hard-gate decision."
        ),
        "qqq_negative_factor_count": qqq_negative_count,
        "rescued_by_softening_count": int(len(rescued)),
        "rescued_by_softening_factors": rescued,
        "qqq_as_sole_blocker_factors": sole_blockers,
        "does_not_promote": True,
        "non_claims": _non_claims(),
    }


def _render_report(diagnosis: pd.DataFrame, qqq_review: dict[str, Any]) -> str:
    lines = [
        "# Factor Failure Diagnosis Report",
        "",
        "This fixed report explains why the current factors stopped at MF-R13.",
        "It is diagnostic only and does not promote factors into redundancy, allocator, Q2, paper/live, or production workflows.",
        "",
        "## Stop Layer",
        "",
    ]
    if diagnosis.empty:
        lines.extend(["No factor diagnosis rows were produced.", ""])
    else:
        for row in diagnosis.itertuples(index=False):
            lines.append(
                f"- `{row.factor_id}`: stop layer `{row.stop_layer}`, status `{row.closeout_status}`, "
                f"dominant failure `{row.dominant_failure_layer}`, action `{row.recommended_action}`."
            )
        lines.append("")
    lines.extend(
        [
            "## QQQ-Relative Guard Review",
            "",
            (
                "The QQQ-relative guard is over-strict as a hard gate for long-short factor spreads. "
                "A benchmark return is not the same unit as a long-short top-minus-bottom sleeve spread."
            ),
            (
                "This review does not promote any factor. It only says QQQ-relative should be treated as "
                "diagnostic context while beta-adjusted, industry-adjusted, style-proxy, and full-residual evidence carry the hard gate."
            ),
            "",
            f"- QQQ-negative factor count: `{qqq_review['qqq_negative_factor_count']}`",
            f"- Factors that would pass if only the QQQ guard were softened: `{qqq_review['rescued_by_softening_count']}`",
            "",
            "No production approval, no paper canary, no live trading, no security orders, no allocator entry, and no direct Q2 entry.",
            "",
        ]
    )
    return "\n".join(lines)


def _diagnosis_columns() -> list[str]:
    return [
        "schema_version",
        "factor_id",
        "stop_layer",
        "closeout_status",
        "dominant_failure_layer",
        "primary_blocker",
        "decision_reason",
        "period_count",
        "gross_spread_mean",
        "qqq_relative_spread_mean",
        "beta_adjusted_spread_mean",
        "industry_adjusted_spread_mean",
        "style_proxy_adjusted_spread_mean",
        "full_residual_spread_mean",
        "residual_positive_rate",
        "period_residual_positive_rate",
        "style_proxy_conflict_count",
        "period_style_proxy_conflict_count",
        "qqq_guard_flagged",
        "qqq_guard_assessment",
        "would_pass_without_qqq_guard",
        "recommended_action",
        "redundancy_gate_allowed",
        "allocator_entry_allowed",
        "not_alpha_evidence",
        "production_approval",
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


def _numeric(value: object, default: float | int = 0.0) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return float(default)
    return float(numeric)


def _round(value: object) -> float:
    return round(_numeric(value), 10)


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}
