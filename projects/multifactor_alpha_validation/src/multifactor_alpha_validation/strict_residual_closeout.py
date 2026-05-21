from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


@dataclass(frozen=True)
class StrictResidualCloseoutResult:
    decision_table_path: str
    report_path: str
    registry_update_path: str
    diagnostics_path: str
    factor_count: int
    ready_for_redundancy_count: int
    production_approval: bool
    live_trading: bool
    direct_q2_entry: bool
    not_alpha_evidence: bool


ALLOWED_CLOSEOUT_STATUSES = {
    "ready_for_redundancy_gate",
    "diagnostic_only",
    "benchmark_exposure_only",
    "industry_exposure_only",
    "style_proxy_conflict",
    "insufficient_residual_evidence",
    "insufficient_coverage",
    "blocked_pit_failure",
}
MIN_PERIODS = 24
MIN_RESIDUAL_POSITIVE_RATE = 0.55


def run_strict_residual_closeout(
    waterfall_input_dir: Path,
    output_dir: Path,
    *,
    min_periods: int = MIN_PERIODS,
    min_residual_positive_rate: float = MIN_RESIDUAL_POSITIVE_RATE,
) -> StrictResidualCloseoutResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    waterfall_path = waterfall_input_dir / "factor_attribution_waterfall.csv"
    by_period_path = waterfall_input_dir / "factor_attribution_waterfall_by_period.csv"
    decision_table_path = output_dir / "strict_residual_closeout_decision_table.csv"
    report_path = output_dir / "strict_residual_closeout_report.md"
    registry_update_path = output_dir / "factor_registry_risk_model_update.yaml"
    diagnostics_path = output_dir / "strict_residual_closeout_diagnostics.json"

    waterfall = _read_csv(waterfall_path)
    by_period = _read_csv(by_period_path)
    decisions = _build_decision_table(
        waterfall=waterfall,
        by_period=by_period,
        min_periods=min_periods,
        min_residual_positive_rate=min_residual_positive_rate,
    )
    diagnostics = _diagnostics(
        decisions=decisions,
        waterfall_input_dir=waterfall_input_dir,
        min_periods=min_periods,
        min_residual_positive_rate=min_residual_positive_rate,
    )
    registry_update = _registry_update(decisions)

    decisions.to_csv(decision_table_path, index=False)
    diagnostics_path.write_text(json.dumps(diagnostics, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    registry_update_path.write_text(yaml.safe_dump(registry_update, sort_keys=False), encoding="utf-8")
    report_path.write_text(_render_report(decisions, diagnostics), encoding="utf-8")

    return StrictResidualCloseoutResult(
        decision_table_path=str(decision_table_path),
        report_path=str(report_path),
        registry_update_path=str(registry_update_path),
        diagnostics_path=str(diagnostics_path),
        factor_count=int(len(decisions)),
        ready_for_redundancy_count=int(decisions["redundancy_gate_allowed"].sum()) if not decisions.empty else 0,
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


def _build_decision_table(
    *,
    waterfall: pd.DataFrame,
    by_period: pd.DataFrame,
    min_periods: int,
    min_residual_positive_rate: float,
) -> pd.DataFrame:
    if waterfall.empty:
        return pd.DataFrame(columns=_decision_columns())
    rows: list[dict[str, Any]] = []
    for _, row in waterfall.iterrows():
        factor_id = str(row.get("factor_id", ""))
        factor_periods = (
            by_period[by_period["factor_id"].astype(str).eq(factor_id)].copy()
            if not by_period.empty and "factor_id" in by_period.columns
            else pd.DataFrame()
        )
        metrics = _factor_metrics(row, factor_periods)
        status, reason, blockers = _decide_factor(metrics, min_periods, min_residual_positive_rate)
        redundancy_allowed = status == "ready_for_redundancy_gate"
        rows.append(
            {
                "schema_version": "strict_residual_closeout.v1",
                "factor_id": factor_id,
                "closeout_status": status,
                "decision_reason": reason,
                "primary_blocker": blockers[0] if blockers else "",
                "blockers": "|".join(blockers),
                "period_count": metrics["period_count"],
                "residual_positive_rate": _round(metrics["residual_positive_rate"]),
                "gross_spread_mean": _round(metrics["gross_spread_mean"]),
                "qqq_relative_spread_mean": _round(metrics["qqq_relative_spread_mean"]),
                "beta_adjusted_spread_mean": _round(metrics["beta_adjusted_spread_mean"]),
                "industry_adjusted_spread_mean": _round(metrics["industry_adjusted_spread_mean"]),
                "style_proxy_adjusted_spread_mean": _round(metrics["style_proxy_adjusted_spread_mean"]),
                "full_residual_spread_mean": _round(metrics["full_residual_spread_mean"]),
                "style_proxy_conflict_count": int(metrics["style_proxy_conflict_count"]),
                "waterfall_status": metrics["waterfall_status"],
                "redundancy_gate_allowed": redundancy_allowed,
                "allocator_entry_allowed": False,
                "registry_status": status,
                "stop_layer": "strict_residual_closeout" if not redundancy_allowed else "",
                "allowed_next_layer": "redundancy_gate" if redundancy_allowed else "diagnostic_only_registry_update",
                "not_style_neutral_alpha": True,
                "not_alpha_evidence": True,
                "production_approval": False,
                "paper_canary": False,
                "live_trading": False,
                "security_orders": False,
                "direct_q2_entry": False,
            }
        )
    return pd.DataFrame(rows, columns=_decision_columns())


def _factor_metrics(row: pd.Series, factor_periods: pd.DataFrame) -> dict[str, Any]:
    period_count = int(_numeric(row.get("period_count"), default=len(factor_periods)))
    if factor_periods.empty or "full_residual_spread" not in factor_periods.columns:
        residual_positive_rate = 0.0
    else:
        residual_positive_rate = float(pd.to_numeric(factor_periods["full_residual_spread"], errors="coerce").gt(0.0).mean())
    conflict_count = int(_numeric(row.get("style_proxy_conflict_count"), default=0))
    if not factor_periods.empty and "waterfall_status" in factor_periods.columns:
        conflict_count = max(conflict_count, int(factor_periods["waterfall_status"].astype(str).eq("style_proxy_conflict").sum()))
    same_close_trading_used = False
    if not factor_periods.empty and "same_close_trading_used" in factor_periods.columns:
        same_close_trading_used = bool(factor_periods["same_close_trading_used"].map(_as_bool).any())
    return {
        "period_count": period_count,
        "residual_positive_rate": residual_positive_rate,
        "gross_spread_mean": _numeric(row.get("gross_spread_mean", row.get("raw_spread_mean"))),
        "qqq_relative_spread_mean": _numeric(row.get("qqq_relative_spread_mean")),
        "beta_adjusted_spread_mean": _numeric(row.get("beta_adjusted_spread_mean")),
        "industry_adjusted_spread_mean": _numeric(row.get("industry_adjusted_spread_mean")),
        "style_proxy_adjusted_spread_mean": _numeric(row.get("style_proxy_adjusted_spread_mean")),
        "full_residual_spread_mean": _numeric(row.get("full_residual_spread_mean")),
        "style_proxy_conflict_count": conflict_count,
        "waterfall_status": str(row.get("waterfall_status", "diagnostic_only")),
        "same_close_trading_used": same_close_trading_used,
    }


def _decide_factor(
    metrics: dict[str, Any],
    min_periods: int,
    min_residual_positive_rate: float,
) -> tuple[str, str, list[str]]:
    blockers: list[str] = []
    if metrics["same_close_trading_used"]:
        return (
            "blocked_pit_failure",
            "same-close trading was detected in the waterfall input, so the factor cannot enter redundancy.",
            ["same_close_trading_used"],
        )
    if metrics["period_count"] < min_periods:
        return (
            "insufficient_coverage",
            f"Only {metrics['period_count']} periods were available; at least {min_periods} are required.",
            ["insufficient_period_count"],
        )

    gross = float(metrics["gross_spread_mean"])
    qqq_relative = float(metrics["qqq_relative_spread_mean"])
    beta_adjusted = float(metrics["beta_adjusted_spread_mean"])
    industry_adjusted = float(metrics["industry_adjusted_spread_mean"])
    style_proxy_adjusted = float(metrics["style_proxy_adjusted_spread_mean"])
    full_residual = float(metrics["full_residual_spread_mean"])
    residual_positive_rate = float(metrics["residual_positive_rate"])
    conflict_count = int(metrics["style_proxy_conflict_count"])

    has_negative_benchmark_or_beta = qqq_relative < 0.0 or beta_adjusted < 0.0
    has_positive_proxy_residual = style_proxy_adjusted > 0.0 or full_residual > 0.0
    if has_negative_benchmark_or_beta and has_positive_proxy_residual:
        return (
            "style_proxy_conflict",
            (
                "A positive proxy residual is present, but benchmark-relative or beta-adjusted readouts are negative; "
                "the proxy residual is diagnostic only and cannot enter redundancy."
            ),
            ["benchmark_or_beta_negative", "positive_proxy_residual"],
        )
    if conflict_count > 0 and has_positive_proxy_residual:
        return (
            "style_proxy_conflict",
            (
                "Waterfall periods contain benchmark/beta/style proxy conflicts; positive proxy residual periods "
                "remain diagnostic and redundancy gate remains blocked."
            ),
            ["style_proxy_conflict_periods"],
        )
    if full_residual <= 0.0 or residual_positive_rate < min_residual_positive_rate:
        blockers.append("residual_not_stable_positive")
        return (
            "insufficient_residual_evidence",
            (
                f"Full residual evidence is not stable enough after proxy controls "
                f"(mean={full_residual:.6f}, positive_rate={residual_positive_rate:.3f})."
            ),
            blockers,
        )
    if qqq_relative < 0.0 and beta_adjusted < 0.0:
        return (
            "benchmark_exposure_only",
            "QQQ-relative and beta-adjusted spreads are both negative, so apparent evidence is benchmark/beta exposure only.",
            ["benchmark_and_beta_adjusted_negative"],
        )
    if beta_adjusted > 0.0 and industry_adjusted <= 0.0:
        return (
            "industry_exposure_only",
            "The factor loses evidence after industry adjustment, so it cannot enter redundancy.",
            ["industry_adjusted_non_positive"],
        )

    clean = (
        gross > 0.0
        and qqq_relative >= 0.0
        and beta_adjusted >= 0.0
        and industry_adjusted >= 0.0
        and style_proxy_adjusted >= 0.0
        and full_residual > 0.0
        and residual_positive_rate >= min_residual_positive_rate
        and conflict_count == 0
        and str(metrics["waterfall_status"]) != "style_proxy_conflict"
    )
    if clean:
        return (
            "ready_for_redundancy_gate",
            "Raw, benchmark-relative, beta-adjusted, industry-adjusted, and proxy residual readouts are stable enough.",
            [],
        )
    return (
        "diagnostic_only",
        "The factor has mixed attribution evidence and remains diagnostic-only before redundancy.",
        ["mixed_attribution_evidence"],
    )


def _diagnostics(
    *,
    decisions: pd.DataFrame,
    waterfall_input_dir: Path,
    min_periods: int,
    min_residual_positive_rate: float,
) -> dict[str, Any]:
    if decisions.empty:
        ready_factors: list[str] = []
        blocked_factors: list[str] = []
        status_counts: dict[str, int] = {}
    else:
        ready_factors = decisions.loc[decisions["redundancy_gate_allowed"], "factor_id"].astype(str).tolist()
        blocked_factors = decisions.loc[~decisions["redundancy_gate_allowed"], "factor_id"].astype(str).tolist()
        status_counts = {str(key): int(value) for key, value in decisions["closeout_status"].value_counts().to_dict().items()}
    return {
        "schema_version": "strict_residual_closeout_diagnostics.v1",
        "model_use": "strict_residual_evidence_gate_only",
        "waterfall_input_dir": str(waterfall_input_dir),
        "factor_count": int(len(decisions)),
        "ready_for_redundancy_count": int(len(ready_factors)),
        "blocked_factor_count": int(len(blocked_factors)),
        "ready_factors": ready_factors,
        "blocked_factors": blocked_factors,
        "status_counts": status_counts,
        "thresholds": {
            "min_periods": int(min_periods),
            "min_residual_positive_rate": float(min_residual_positive_rate),
        },
        "terminology": {
            "proxy_residual": "residual after configured proxy risk-model controls only",
            "forbidden_claim": "proxy residual is not style-neutral alpha",
        },
        "non_claims": _non_claims(),
    }


def _registry_update(decisions: pd.DataFrame) -> dict[str, Any]:
    factors = []
    for row in decisions.itertuples(index=False):
        factors.append(
            {
                "factor_id": str(row.factor_id),
                "registry_status": str(row.registry_status),
                "stop_layer": str(row.stop_layer or "none"),
                "redundancy_gate_allowed": bool(row.redundancy_gate_allowed),
                "allocator_entry_allowed": bool(row.allocator_entry_allowed),
                "decision_reason": str(row.decision_reason),
                "not_alpha_evidence": True,
            }
        )
    return {
        "schema_version": "factor_registry_risk_model_update.v1",
        "source_layer": "strict_residual_closeout",
        "factors": factors,
        "non_claims": _non_claims(),
    }


def _render_report(decisions: pd.DataFrame, diagnostics: dict[str, Any]) -> str:
    lines = [
        "# Strict Residual Evidence Closeout",
        "",
        "This gate converts the MF-R12 attribution waterfall into factor-level stop-layer decisions.",
        "Configured proxy residuals are not style-neutral alpha and are not tradeable predictions.",
        "",
        "No production approval, no paper canary, no live trading, no security orders, no allocator entry, and no direct Q2 entry.",
        "",
    ]
    if decisions.empty:
        lines.extend(["No factor decisions were produced. Redundancy gate remains blocked.", ""])
        return "\n".join(lines)
    lines.extend(["## Decision Table", ""])
    for row in decisions.itertuples(index=False):
        gate_text = "allowed" if bool(row.redundancy_gate_allowed) else "blocked"
        lines.append(
            f"- `{row.factor_id}`: `{row.closeout_status}`; redundancy gate {gate_text}; "
            f"gross={row.gross_spread_mean:.6f}, QQQ-relative={row.qqq_relative_spread_mean:.6f}, "
            f"beta-adjusted={row.beta_adjusted_spread_mean:.6f}, industry-adjusted={row.industry_adjusted_spread_mean:.6f}, "
            f"proxy residual={row.full_residual_spread_mean:.6f}. {row.decision_reason}"
        )
    if diagnostics.get("blocked_factor_count", 0):
        lines.extend(
            [
                "",
                "For blocked factors, the redundancy gate remains blocked. Positive proxy residual readouts do not override negative benchmark, beta, industry, or unstable full-residual evidence.",
                "",
            ]
        )
    lines.append("The registry update records these as research closeout states only, not alpha approval.")
    lines.append("")
    return "\n".join(lines)


def _decision_columns() -> list[str]:
    return [
        "schema_version",
        "factor_id",
        "closeout_status",
        "decision_reason",
        "primary_blocker",
        "blockers",
        "period_count",
        "residual_positive_rate",
        "gross_spread_mean",
        "qqq_relative_spread_mean",
        "beta_adjusted_spread_mean",
        "industry_adjusted_spread_mean",
        "style_proxy_adjusted_spread_mean",
        "full_residual_spread_mean",
        "style_proxy_conflict_count",
        "waterfall_status",
        "redundancy_gate_allowed",
        "allocator_entry_allowed",
        "registry_status",
        "stop_layer",
        "allowed_next_layer",
        "not_style_neutral_alpha",
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


def _numeric(value: object, default: float | int = 0.0) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return float(default)
    return float(numeric)


def _round(value: object) -> float:
    numeric = _numeric(value)
    return round(float(numeric), 10)


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}
