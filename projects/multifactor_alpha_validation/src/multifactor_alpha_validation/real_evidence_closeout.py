from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class RealEvidenceCloseoutResult:
    decision: str
    decision_path: str
    report_path: str
    conflict_diagnostics_path: str
    production_approval: bool
    live_trading: bool
    direct_q2_entry: bool


_ALLOWED_DECISIONS = {"ready_for_redundancy_gate", "needs_data_fix", "diagnostic_only", "blocked"}
_CONFLICT_COLUMNS = [
    "factor_id",
    "conflict_type",
    "qqq_relative_spread_mean",
    "beta_adjusted_spread_mean",
    "style_adjusted_spread_mean",
    "style_adjusted_net_spread_mean",
    "style_adjusted_status",
    "style_model_scope",
    "interpretation",
    "closeout_effect",
]


def run_real_evidence_closeout(real_oos_output_dir: Path, output_dir: Path) -> RealEvidenceCloseoutResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = real_oos_output_dir / "real_oos_summary.json"
    if not summary_path.exists():
        decision = "blocked"
        reasons = ["missing_real_oos_summary"]
        summary = {"oos_status": "missing", "dataset_frequency": "unknown"}
        evidence = pd.DataFrame()
        neutralization = pd.DataFrame()
        conflicts = _empty_conflicts()
    else:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        evidence = _read_csv(real_oos_output_dir / "real_oos_factor_evidence.csv")
        neutralization = _read_csv(real_oos_output_dir / "real_oos_neutralization_report.csv")
        conflicts = _build_conflict_diagnostics(evidence, neutralization)
        decision, reasons = _decide(summary, evidence, neutralization, conflicts)
    if decision not in _ALLOWED_DECISIONS:
        raise ValueError(f"invalid real evidence closeout decision: {decision}")

    conflict_path = output_dir / "real_evidence_conflict_diagnostics.csv"
    conflicts.to_csv(conflict_path, index=False)
    payload = {
        "schema_version": "real_evidence_closeout.v1",
        "decision": decision,
        "decision_reasons": reasons,
        "dataset_frequency": summary.get("dataset_frequency", "unknown"),
        "oos_status": summary.get("oos_status", "unknown"),
        "factor_count": int(len(evidence)) if not evidence.empty else 0,
        "conflict_count": int(len(conflicts)),
        "conflict_factors": conflicts["factor_id"].tolist() if not conflicts.empty else [],
        "conflict_diagnostics_path": str(conflict_path),
        "production_approval": False,
        "live_trading": False,
        "security_orders": False,
        "paper_canary": False,
        "direct_q2_entry": False,
        "allocator_entry_allowed": False,
        "not_alpha_evidence": True,
    }
    decision_path = output_dir / "real_evidence_closeout_decision.json"
    report_path = output_dir / "real_evidence_closeout_report.md"
    decision_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_path.write_text(_render_report(payload, conflicts), encoding="utf-8")
    return RealEvidenceCloseoutResult(
        decision=decision,
        decision_path=str(decision_path),
        report_path=str(report_path),
        conflict_diagnostics_path=str(conflict_path),
        production_approval=False,
        live_trading=False,
        direct_q2_entry=False,
    )


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _decide(
    summary: dict[str, object],
    evidence: pd.DataFrame,
    neutralization: pd.DataFrame,
    conflicts: pd.DataFrame,
) -> tuple[str, list[str]]:
    status = str(summary.get("oos_status", ""))
    if status == "needs_daily_price_volume":
        return "needs_data_fix", ["daily_price_volume_required"]
    if status != "evidence_ready":
        return "blocked", [str(summary.get("decision_blocker") or "real_oos_not_ready")]
    if evidence.empty:
        return "blocked", ["missing_real_oos_evidence"]
    if bool(evidence.get("full_sample_icir_used", pd.Series([False])).astype(bool).any()):
        return "blocked", ["full_sample_icir_used"]

    reasons: list[str] = []
    if not neutralization.empty and "sector_adjusted_status" in neutralization.columns:
        if not _statuses_observed(neutralization["sector_adjusted_status"]):
            reasons.append("sector_attribution_unavailable")
    else:
        reasons.append("sector_attribution_unavailable")
    if not neutralization.empty and "style_adjusted_status" in neutralization.columns:
        if not _statuses_observed(neutralization["style_adjusted_status"]):
            reasons.append("style_attribution_unavailable")
        elif neutralization["style_adjusted_status"].astype(str).str.contains("proxy").any():
            reasons.append("style_proxy_only")
    else:
        reasons.append("style_attribution_unavailable")
    if "style_adjusted_net_spread_mean" in evidence.columns:
        adjusted_net_column = "style_adjusted_net_spread_mean"
    else:
        adjusted_net_column = "net_spread_mean"
    if adjusted_net_column in evidence.columns and not evidence[adjusted_net_column].gt(0).any():
        reasons.append("no_positive_adjusted_net_oos_evidence")
    if not conflicts.empty:
        reasons.append("benchmark_beta_style_conflict")

    if reasons:
        return "diagnostic_only", list(dict.fromkeys(reasons))
    return "ready_for_redundancy_gate", ["clean_oos_evidence_and_attribution"]


def _statuses_observed(series: pd.Series) -> bool:
    statuses = series.dropna().astype(str)
    return not statuses.empty and statuses.str.startswith("observed").all()


def _build_conflict_diagnostics(evidence: pd.DataFrame, neutralization: pd.DataFrame) -> pd.DataFrame:
    if evidence.empty or "factor_id" not in evidence.columns:
        return _empty_conflicts()

    neutralization_by_factor = (
        neutralization.set_index("factor_id", drop=False)
        if not neutralization.empty and "factor_id" in neutralization.columns
        else pd.DataFrame()
    )
    rows: list[dict[str, object]] = []
    for _, evidence_row in evidence.iterrows():
        factor_id = str(evidence_row.get("factor_id", ""))
        neutralization_row = (
            neutralization_by_factor.loc[factor_id]
            if factor_id in neutralization_by_factor.index
            else pd.Series(dtype=object)
        )
        qqq_relative = _first_numeric(evidence_row, neutralization_row, "qqq_relative_spread_mean")
        beta_adjusted = _first_numeric(evidence_row, neutralization_row, "beta_adjusted_spread_mean")
        style_adjusted = _first_numeric(evidence_row, neutralization_row, "style_adjusted_spread_mean")
        style_adjusted_net = _first_numeric(evidence_row, neutralization_row, "style_adjusted_net_spread_mean")
        if qqq_relative < 0 and beta_adjusted < 0 and style_adjusted_net > 0:
            style_status = _first_text(evidence_row, neutralization_row, "style_adjusted_status")
            style_scope = _first_text(evidence_row, neutralization_row, "style_model_scope")
            rows.append(
                {
                    "factor_id": factor_id,
                    "conflict_type": "benchmark_beta_negative_style_positive",
                    "qqq_relative_spread_mean": qqq_relative,
                    "beta_adjusted_spread_mean": beta_adjusted,
                    "style_adjusted_spread_mean": style_adjusted,
                    "style_adjusted_net_spread_mean": style_adjusted_net,
                    "style_adjusted_status": style_status,
                    "style_model_scope": style_scope,
                    "interpretation": (
                        "The style-adjusted proxy residual is positive, but benchmark-relative and beta-adjusted "
                        "readouts are negative; the positive style-adjusted result does not override the "
                        "benchmark/beta failure."
                    ),
                    "closeout_effect": "diagnostic_only_block_redundancy_gate",
                }
            )
    if not rows:
        return _empty_conflicts()
    return pd.DataFrame(rows, columns=_CONFLICT_COLUMNS)


def _empty_conflicts() -> pd.DataFrame:
    return pd.DataFrame(columns=_CONFLICT_COLUMNS)


def _first_numeric(primary: pd.Series, secondary: pd.Series, column: str) -> float:
    value = primary.get(column)
    if pd.isna(value) and not secondary.empty:
        value = secondary.get(column)
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return 0.0
    return float(numeric)


def _first_text(primary: pd.Series, secondary: pd.Series, column: str) -> str:
    value = primary.get(column)
    if pd.isna(value) and not secondary.empty:
        value = secondary.get(column)
    if pd.isna(value):
        return "unavailable"
    return str(value)


def _render_report(payload: dict[str, object], conflicts: pd.DataFrame) -> str:
    reasons = payload.get("decision_reasons", [])
    reason_lines = "\n".join(f"- {reason}" for reason in reasons)
    lines = [
        "# Real Evidence Closeout Gate",
        "",
        f"Decision: `{payload['decision']}`",
        "",
        "Reasons:",
        reason_lines,
        "",
    ]
    if not conflicts.empty:
        lines.extend(
            [
                "## Benchmark / Style Conflicts",
                "",
                (
                    "A positive style-adjusted proxy residual is diagnostic only when QQQ-relative and "
                    "beta-adjusted readouts are negative. It does not override benchmark/beta failure."
                ),
                "",
            ]
        )
        for _, row in conflicts.iterrows():
            lines.extend(
                [
                    (
                        f"- `{row['factor_id']}`: QQQ-relative={row['qqq_relative_spread_mean']:.6f}, "
                        f"beta-adjusted={row['beta_adjusted_spread_mean']:.6f}, "
                        f"style-adjusted net={row['style_adjusted_net_spread_mean']:.6f}; "
                        f"{row['interpretation']}"
                    ),
                ]
            )
        lines.append("")
    lines.extend(
        [
            "No production approval, no paper canary, no live trading, no security orders, and no direct Q2 entry.",
            "",
            "Diagnostic-only outputs do not enter allocator or redundancy gates.",
            "",
        ]
    )
    return "\n".join(lines)
