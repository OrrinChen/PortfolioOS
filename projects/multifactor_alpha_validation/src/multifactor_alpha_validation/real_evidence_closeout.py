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
    production_approval: bool
    live_trading: bool
    direct_q2_entry: bool


_ALLOWED_DECISIONS = {"ready_for_redundancy_gate", "needs_data_fix", "diagnostic_only", "blocked"}


def run_real_evidence_closeout(real_oos_output_dir: Path, output_dir: Path) -> RealEvidenceCloseoutResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = real_oos_output_dir / "real_oos_summary.json"
    if not summary_path.exists():
        decision = "blocked"
        reasons = ["missing_real_oos_summary"]
        summary = {"oos_status": "missing", "dataset_frequency": "unknown"}
        evidence = pd.DataFrame()
        neutralization = pd.DataFrame()
    else:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        evidence = _read_csv(real_oos_output_dir / "real_oos_factor_evidence.csv")
        neutralization = _read_csv(real_oos_output_dir / "real_oos_neutralization_report.csv")
        decision, reasons = _decide(summary, evidence, neutralization)
    if decision not in _ALLOWED_DECISIONS:
        raise ValueError(f"invalid real evidence closeout decision: {decision}")

    payload = {
        "schema_version": "real_evidence_closeout.v1",
        "decision": decision,
        "decision_reasons": reasons,
        "dataset_frequency": summary.get("dataset_frequency", "unknown"),
        "oos_status": summary.get("oos_status", "unknown"),
        "factor_count": int(len(evidence)) if not evidence.empty else 0,
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
    report_path.write_text(_render_report(payload), encoding="utf-8")
    return RealEvidenceCloseoutResult(
        decision=decision,
        decision_path=str(decision_path),
        report_path=str(report_path),
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
    adjusted_net_column = "style_adjusted_net_spread_mean" if "style_adjusted_net_spread_mean" in evidence.columns else "net_spread_mean"
    if adjusted_net_column in evidence.columns and not evidence[adjusted_net_column].gt(0).any():
        reasons.append("no_positive_adjusted_net_oos_evidence")

    if reasons:
        return "diagnostic_only", reasons
    return "ready_for_redundancy_gate", ["clean_oos_evidence_and_attribution"]


def _statuses_observed(series: pd.Series) -> bool:
    statuses = series.dropna().astype(str)
    return not statuses.empty and statuses.str.startswith("observed").all()


def _render_report(payload: dict[str, object]) -> str:
    reasons = payload.get("decision_reasons", [])
    reason_lines = "\n".join(f"- {reason}" for reason in reasons)
    return "\n".join(
        [
            "# Real Evidence Closeout Gate",
            "",
            f"Decision: `{payload['decision']}`",
            "",
            "Reasons:",
            reason_lines,
            "",
            "No production approval, no paper canary, no live trading, no security orders, and no direct Q2 entry.",
            "",
            "Diagnostic-only outputs do not enter allocator or redundancy gates.",
            "",
        ]
    )
