"""Finding construction and summarization helpers."""

from __future__ import annotations

from collections import Counter
from typing import Any

from portfolio_os.domain.enums import FindingCategory, FindingSeverity, RepairStatus
from portfolio_os.domain.models import ComplianceFinding


def build_finding(
    code: str,
    category: FindingCategory,
    severity: FindingSeverity,
    message: str,
    *,
    ticker: str | None = None,
    rule_source: str,
    blocking: bool = False,
    repair_status: RepairStatus = RepairStatus.NOT_NEEDED,
    details: dict[str, Any] | None = None,
) -> ComplianceFinding:
    """Create a structured finding."""

    return ComplianceFinding(
        code=code,
        category=category,
        severity=severity,
        ticker=ticker,
        message=message,
        rule_source=rule_source,
        blocking=blocking,
        repair_status=repair_status,
        details=details or {},
    )


def summarize_findings(findings: list[ComplianceFinding]) -> dict[str, object]:
    """Build reusable counts for reports, benchmarks, and exports."""

    rule_counts = Counter(finding.code for finding in findings)
    severity_counts = Counter(finding.severity.value for finding in findings)
    category_counts = Counter(finding.category.value for finding in findings)
    repair_status_counts = Counter(finding.repair_status.value for finding in findings)
    blocking_count = sum(1 for finding in findings if finding.blocking)
    unresolved_blocking_count = sum(
        1 for finding in findings if finding.blocking and finding.repair_status == RepairStatus.UNRESOLVED
    )
    return {
        "total": len(findings),
        "blocked_trade_count": int(rule_counts.get("trade_blocked", 0)),
        "warning_count": int(severity_counts.get(FindingSeverity.WARNING.value, 0)),
        "breach_count": int(severity_counts.get(FindingSeverity.BREACH.value, 0)),
        "info_count": int(severity_counts.get(FindingSeverity.INFO.value, 0)),
        "blocking_count": int(blocking_count),
        "unresolved_blocking_count": int(unresolved_blocking_count),
        "rule_counts": dict(rule_counts),
        "severity_counts": dict(severity_counts),
        "category_counts": dict(category_counts),
        "repair_status_counts": dict(repair_status_counts),
    }


def suggest_blocking_action(finding: ComplianceFinding) -> str:
    """Return a normalized suggested action for blocking findings."""

    code = finding.code
    if code in {"trade_blocked", "no_order_due_to_constraint"}:
        return "restrict"
    if code in {"single_name_limit", "industry_limit_breach", "cash_repair_failed"}:
        return "reduce"
    return "waive"


def aggregate_findings_for_reporting(findings: list[ComplianceFinding]) -> list[dict[str, Any]]:
    """Aggregate repetitive findings for human-facing summaries."""

    grouped: dict[tuple[str, str | None, str, str, bool, str], list[ComplianceFinding]] = {}
    for finding in findings:
        key = (
            finding.code,
            finding.ticker,
            finding.severity.value,
            finding.category.value,
            bool(finding.blocking),
            finding.repair_status.value,
        )
        grouped.setdefault(key, []).append(finding)

    aggregated: list[dict[str, Any]] = []
    for key, bucket in grouped.items():
        code, ticker, severity, category, blocking, repair_status = key
        first = bucket[0]
        aggregated.append(
            {
                "code": code,
                "ticker": ticker,
                "severity": severity,
                "category": category,
                "blocking": blocking,
                "repair_status": repair_status,
                "message": first.message,
                "count": len(bucket),
                "suggested_action": suggest_blocking_action(first) if blocking else "monitor",
            }
        )
    aggregated.sort(
        key=lambda item: (
            0 if item["blocking"] else 1,
            0 if item["severity"] == FindingSeverity.BREACH.value else 1,
            -int(item["count"]),
            str(item["code"]),
            str(item["ticker"] or ""),
        )
    )
    return aggregated
