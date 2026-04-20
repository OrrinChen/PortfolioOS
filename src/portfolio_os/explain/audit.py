"""Structured audit package generation."""

from __future__ import annotations

from typing import Any

from portfolio_os.domain.models import Basket, ComplianceFinding, ConstraintSnapshot
from portfolio_os.storage.snapshots import file_metadata
from portfolio_os.utils.config import AppConfig


def build_audit_payload(
    *,
    input_paths: dict[str, str],
    config: AppConfig,
    constraint_snapshot: ConstraintSnapshot,
    findings: list[ComplianceFinding],
    basket: Basket,
    summary: dict[str, Any],
    run_id: str,
    created_at: str,
    export_readiness: dict[str, Any] | None = None,
    optimization_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the audit JSON payload."""

    return {
        "run_id": run_id,
        "created_at": created_at,
        "disclaimer": config.project.disclaimer,
        "inputs": {name: file_metadata(path) for name, path in input_paths.items()},
        "parameters": config.model_dump(mode="json"),
        "constraints_snapshot": constraint_snapshot.model_dump(mode="json"),
        "findings": [finding.model_dump(mode="json") for finding in findings],
        "orders": [order.model_dump(mode="json") for order in basket.orders],
        "summary": summary,
        "optimization_metadata": optimization_metadata or {},
        "export_readiness": export_readiness or {},
    }
