"""Execution adapter result models."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class OrderExecutionRecord:
    """One broker-side order execution outcome."""

    ticker: str
    direction: str
    requested_qty: float
    filled_qty: float
    avg_fill_price: float | None
    status: str
    reject_reason: str | None = None
    order_id: str | None = None
    submitted_at_utc: str | None = None
    terminal_at_utc: str | None = None
    poll_count: int = 0
    timeout_cancelled: bool = False
    cancel_requested: bool = False
    cancel_acknowledged: bool = False
    broker_order_id: str | None = None
    status_history: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ExecutionResult:
    """Batch execution outcome for one submitted basket."""

    orders: list[OrderExecutionRecord] = field(default_factory=list)
    submitted_count: int = 0
    filled_count: int = 0
    partial_count: int = 0
    unfilled_count: int = 0
    rejected_count: int = 0
    timeout_cancelled_count: int = 0
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["orders"] = [asdict(item) for item in self.orders]
        return payload


@dataclass
class ReconciliationDetail:
    """Ticker-level position reconciliation detail."""

    ticker: str
    expected_quantity: float | None
    actual_quantity: float | None
    quantity_diff: float
    expected_value: float | None
    actual_value: float | None
    value_diff: float


@dataclass
class ReconciliationReport:
    """Aggregate reconciliation report for expected vs broker positions."""

    matched_count: int
    mismatched_count: int
    missing_in_broker: list[str]
    missing_in_system: list[str]
    details: list[ReconciliationDetail] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["details"] = [asdict(item) for item in self.details]
        return payload
