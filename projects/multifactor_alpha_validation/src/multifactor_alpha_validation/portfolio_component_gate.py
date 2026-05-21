from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class PortfolioComponentGateResult:
    component_table_path: str
    summary_path: str
    report_path: str
    factor_count: int
    component_candidate_count: int
    standalone_clean_alpha_count: int
    portfolio_validation_mode: str
    production_approval: bool
    live_trading: bool
    direct_q2_entry: bool
    not_alpha_evidence: bool


def run_portfolio_component_gate(input_dir: Path, output_dir: Path) -> PortfolioComponentGateResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    diagnosis = _read_csv(input_dir / "factor_failure_diagnosis.csv")
    qqq_review = _read_json(input_dir / "qqq_relative_guard_review.json")
    component_table = _build_component_table(diagnosis)
    summary = _build_summary(component_table, qqq_review)

    component_table_path = output_dir / "component_candidate_table.csv"
    summary_path = output_dir / "portfolio_component_gate_summary.json"
    report_path = output_dir / "portfolio_component_gate_report.md"
    component_table.to_csv(component_table_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_path.write_text(_render_report(component_table, summary), encoding="utf-8")

    return PortfolioComponentGateResult(
        component_table_path=str(component_table_path),
        summary_path=str(summary_path),
        report_path=str(report_path),
        factor_count=int(len(component_table)),
        component_candidate_count=int(summary["component_candidate_count"]),
        standalone_clean_alpha_count=int(summary["standalone_clean_alpha_count"]),
        portfolio_validation_mode=str(summary["portfolio_validation_mode"]),
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


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _build_component_table(diagnosis: pd.DataFrame) -> pd.DataFrame:
    if diagnosis.empty:
        return pd.DataFrame(columns=_component_columns())
    rows: list[dict[str, Any]] = []
    for _, row in diagnosis.iterrows():
        status, role, reason = _classify_component(row)
        allowed = status not in {"blocked_component", "diagnostic_only"}
        rows.append(
            {
                "schema_version": "portfolio_component_gate.v1",
                "factor_id": str(row.get("factor_id", "")),
                "component_status": status,
                "component_role": role,
                "eligibility_reason": reason,
                "source_closeout_status": str(row.get("closeout_status", "")),
                "source_dominant_failure_layer": str(row.get("dominant_failure_layer", "")),
                "gross_spread_mean": _round(row.get("gross_spread_mean")),
                "qqq_relative_spread_mean": _round(row.get("qqq_relative_spread_mean")),
                "beta_adjusted_spread_mean": _round(row.get("beta_adjusted_spread_mean")),
                "industry_adjusted_spread_mean": _round(row.get("industry_adjusted_spread_mean")),
                "style_proxy_adjusted_spread_mean": _round(row.get("style_proxy_adjusted_spread_mean")),
                "full_residual_spread_mean": _round(row.get("full_residual_spread_mean")),
                "residual_positive_rate": _round(row.get("residual_positive_rate")),
                "standalone_alpha_claim_allowed": status == "standalone_clean_alpha",
                "alpha_claim_allowed": False,
                "portfolio_validation_allowed": allowed,
                "portfolio_validation_mode": "diagnostic_ensemble_only" if allowed else "blocked",
                "allowed_next_layer": "portfolio_level_oos_ensemble_validation" if allowed else "none",
                "redundancy_gate_allowed": False,
                "allocator_entry_allowed": False,
                "not_alpha_evidence": True,
                "production_approval": False,
                "paper_canary": False,
                "live_trading": False,
                "security_orders": False,
                "direct_q2_entry": False,
            }
        )
    return pd.DataFrame(rows, columns=_component_columns())


def _classify_component(row: pd.Series) -> tuple[str, str, str]:
    closeout_status = str(row.get("closeout_status", ""))
    failure_layer = str(row.get("dominant_failure_layer", ""))
    gross = _numeric(row.get("gross_spread_mean"))
    beta_adjusted = _numeric(row.get("beta_adjusted_spread_mean"))
    industry_adjusted = _numeric(row.get("industry_adjusted_spread_mean"))
    full_residual = _numeric(row.get("full_residual_spread_mean"))
    residual_positive_rate = _numeric(row.get("residual_positive_rate"))

    if closeout_status in {"blocked_pit_failure", "blocked_timestamp_failure", "insufficient_coverage"}:
        return (
            "blocked_component",
            "invalid_research_component",
            "Hard validity failure blocks portfolio component use.",
        )
    if failure_layer in {"pit_timestamp", "coverage"}:
        return (
            "blocked_component",
            "invalid_research_component",
            "PIT/timestamp or coverage failure cannot be rescued by portfolio construction.",
        )
    if closeout_status == "ready_for_redundancy_gate":
        return (
            "standalone_clean_alpha",
            "standalone_alpha_candidate",
            "The factor has clean enough residual evidence for normal downstream gates.",
        )
    if gross > 0.0 and beta_adjusted >= 0.0 and industry_adjusted >= 0.0:
        return (
            "eligible_benchmark_premia_component",
            "style_premia_return_driver",
            "The factor is not clean standalone residual alpha, but it may act as a documented style/premia component.",
        )
    if full_residual > 0.0 and residual_positive_rate >= 0.5 and failure_layer in {"beta_exposure", "style_proxy_conflict"}:
        return (
            "eligible_hedge_component",
            "hedge_or_diversifier_component",
            "The factor has exposure conflicts but may still be useful as a hedge or diversifying component in diagnostic ensemble validation.",
        )
    if full_residual > 0.0 and residual_positive_rate >= 0.45:
        return (
            "eligible_component",
            "regime_or_diversifier_component",
            "The factor is weak as a standalone alpha but has enough directional residual evidence for diagnostic portfolio validation.",
        )
    return (
        "diagnostic_only",
        "not_portfolio_ready_component",
        "The factor lacks enough standalone or component-level evidence for portfolio validation.",
    )


def _build_summary(component_table: pd.DataFrame, qqq_review: dict[str, Any]) -> dict[str, Any]:
    if component_table.empty:
        component_candidate_count = 0
        standalone_count = 0
        blocked_count = 0
        status_counts: dict[str, int] = {}
    else:
        candidate_mask = component_table["portfolio_validation_allowed"].map(_as_bool)
        component_candidate_count = int(candidate_mask.sum())
        standalone_count = int(component_table["component_status"].astype(str).eq("standalone_clean_alpha").sum())
        blocked_count = int(component_table["component_status"].astype(str).eq("blocked_component").sum())
        status_counts = {str(key): int(value) for key, value in component_table["component_status"].value_counts().to_dict().items()}
    return {
        "schema_version": "portfolio_component_gate_summary.v1",
        "factor_count": int(len(component_table)),
        "standalone_clean_alpha_count": standalone_count,
        "component_candidate_count": component_candidate_count,
        "blocked_component_count": blocked_count,
        "portfolio_validation_allowed": component_candidate_count > 0,
        "portfolio_validation_mode": "diagnostic_ensemble_only" if component_candidate_count > 0 else "blocked",
        "status_counts": status_counts,
        "qqq_relative_guard_hard_gate_recommended": bool(
            qqq_review.get("hard_gate_recommended_for_long_short_spread", False)
        ),
        "qqq_relative_guard_scope": "diagnostic_context_not_hard_component_gate",
        "non_claims": _non_claims(),
    }


def _render_report(component_table: pd.DataFrame, summary: dict[str, Any]) -> str:
    lines = [
        "# Portfolio Component Gate",
        "",
        "This gate converts risk-attributed factor closeout rows into portfolio component roles.",
        "It does not require standalone clean residual alpha before diagnostic ensemble validation.",
        "",
        "It does not promote factors into production, Q2, paper/live, broker/order, or unrestricted allocator workflows.",
        "",
        f"Portfolio validation mode: `{summary['portfolio_validation_mode']}`",
        "",
        "## Components",
        "",
    ]
    if component_table.empty:
        lines.append("No component rows were produced.")
    else:
        for row in component_table.itertuples(index=False):
            allowed = "allowed" if bool(row.portfolio_validation_allowed) else "blocked"
            lines.append(
                f"- `{row.factor_id}`: `{row.component_status}` as `{row.component_role}`; "
                f"diagnostic ensemble {allowed}. {row.eligibility_reason}"
            )
    lines.extend(
        [
            "",
            "The next layer is portfolio-level OOS ensemble validation with ablation. Component eligibility is not an alpha claim.",
            "",
        ]
    )
    return "\n".join(lines)


def _component_columns() -> list[str]:
    return [
        "schema_version",
        "factor_id",
        "component_status",
        "component_role",
        "eligibility_reason",
        "source_closeout_status",
        "source_dominant_failure_layer",
        "gross_spread_mean",
        "qqq_relative_spread_mean",
        "beta_adjusted_spread_mean",
        "industry_adjusted_spread_mean",
        "style_proxy_adjusted_spread_mean",
        "full_residual_spread_mean",
        "residual_positive_rate",
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
