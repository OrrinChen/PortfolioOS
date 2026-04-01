"""Export-readiness helpers built from structured findings."""

from __future__ import annotations

from portfolio_os.domain.models import ComplianceFinding, Order


def blocking_checks_cleared_for_ticker(
    *,
    ticker: str,
    findings: list[ComplianceFinding],
) -> bool:
    """Return whether a ticker has any unresolved blocking findings."""

    for finding in findings:
        if not finding.blocking:
            continue
        if finding.repair_status == "repaired":
            continue
        if finding.ticker is None or finding.ticker == ticker:
            return False
    return True


def build_order_release_status(
    orders: list[Order],
    findings: list[ComplianceFinding],
) -> dict[str, bool]:
    """Return per-ticker export readiness for OMS-style outputs."""

    return {
        order.ticker: blocking_checks_cleared_for_ticker(ticker=order.ticker, findings=findings)
        for order in orders
    }


def run_posttrade_checks(
    orders: list[Order],
    findings: list[ComplianceFinding],
) -> dict[str, object]:
    """Return a lightweight post-trade/export readiness summary."""

    release_status = build_order_release_status(orders, findings)
    return {
        "order_count": len(orders),
        "ready_order_count": sum(1 for cleared in release_status.values() if cleared),
        "blocked_order_count": sum(1 for cleared in release_status.values() if not cleared),
        "release_status": release_status,
    }
