from __future__ import annotations

import argparse
import ast
import csv
import json
import math
import os
import subprocess
import sys
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest, StockLatestTradeRequest

from portfolio_os.execution.alpaca_adapter import AlpacaAdapter
from portfolio_os.execution import fill_collection, fill_collection_campaign
from portfolio_os.execution import slippage_calibration
from portfolio_os.execution.models import ExecutionResult


ROOT = Path(__file__).resolve().parents[1]
OUTPUTS_DIR = ROOT / "outputs"
TRACKING_DIR = OUTPUTS_DIR / "pilot_tracking"
WEEKLY_DIR = TRACKING_DIR / "weekly"
BROKER_STATE_INSPECTION_DIR = OUTPUTS_DIR / "broker_state_inspection"
PRE_SUBMISSION_CHECK_DIR = OUTPUTS_DIR / "pre_submission_checks"
OFF_HOURS_PREP_DIR = OUTPUTS_DIR / "off_hours_prep"
FILL_COLLECTION_BATCH_DIR = OUTPUTS_DIR / "fill_collection_batches"
SLIPPAGE_CALIBRATION_PREP_DIR = slippage_calibration.DEFAULT_OUTPUT_ROOT / "prep"
TEMPLATE_DIR = ROOT / "data" / "templates" / "pilot"
MINIMAL_BUY_VALIDATION_PREFERRED_TICKERS = ["SPY", "AAPL"]
DEFAULT_FILL_COLLECTION_PARTICIPATION_BUCKETS = "0.01%,0.1%,1%,5%"

DASHBOARD_PATH = TRACKING_DIR / "pilot_dashboard.csv"
INCIDENT_PATH = TRACKING_DIR / "incident_register.csv"
GO_NOGO_PATH = TRACKING_DIR / "go_nogo_decision.md"

STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_INSUFFICIENT = "INSUFFICIENT_DATA"
STATUS_WAIVE = "WAIVE"
COMPARISON_ELIGIBILITY_ELIGIBLE = "ELIGIBLE"
COMPARISON_ELIGIBILITY_INELIGIBLE = "INELIGIBLE"
COMPARISON_ELIGIBILITY_INVALID = "INVALID"
COMPARISON_ELIGIBILITY_NOT_AVAILABLE = "NOT_AVAILABLE"

ELIGIBILITY_REASON_MISSING_REQUIRED = "comparison_eligibility_missing_required_for_ab_gate"
ELIGIBILITY_REASON_INVALID_JSON = "comparison_eligibility_json_invalid"
ELIGIBILITY_REASON_PAYLOAD_NOT_MAPPING = "comparison_eligibility_payload_not_mapping"
ELIGIBILITY_REASON_INVALID_ELIGIBLE_FIELD = "comparison_eligibility_eligible_field_invalid"
ELIGIBILITY_REASON_FALSE_WITHOUT_REASON = "comparison_eligibility_false_without_reason"

EXIT_CODE_ELIGIBILITY_INELIGIBLE = 21
EXIT_CODE_ELIGIBILITY_INVALID = 22
EXIT_CODE_ELIGIBILITY_MISSING = 23

ELIGIBILITY_LOG_INELIGIBLE = "eligibility_gate: INELIGIBLE; blocking decision flow."
ELIGIBILITY_LOG_INVALID = "eligibility_gate: INVALID; blocking decision flow."
ELIGIBILITY_LOG_MISSING = "eligibility_gate: MISSING; blocking decision flow in required A/B gate mode."

DASHBOARD_HEADERS = [
    "date",
    "phase",
    "mode",
    "run_root",
    "as_of_date",
    "nightly_status",
    "release_status",
    "release_gate_passed",
    "rebalance_triggered",
    "artifact_chain_complete",
    "override_count",
    "cost_better_ratio",
    "primary_feed_success",
    "fallback_activated",
    "solver_primary",
    "blocked_untradeable_count",
    "static_count",
    "real_count",
    "full_chain_success_static",
    "full_chain_success_real",
    "override_used_static",
    "score_gap_ge_001_static",
    "cost_better_ratio_static",
    "solver_fallback_used_static",
    "solver_sample_count_static",
    "mean_order_reasonableness_static",
    "mean_findings_explainability_static",
    "mean_execution_credibility_static",
    "execution_residual_risk_consistent",
    "provider_blockers_count",
    "comparison_eligibility_status",
    "comparison_eligibility_reason_count",
    "incident_id",
    "notes",
]


def _resolve_tracking_paths(output_dir: Path | None = None) -> tuple[Path, Path, Path, Path]:
    base_dir = (Path(output_dir).resolve() if output_dir is not None else TRACKING_DIR).resolve()
    dashboard_path = base_dir / "pilot_dashboard.csv"
    incident_path = base_dir / "incident_register.csv"
    go_nogo_path = base_dir / "go_nogo_decision.md"
    weekly_dir = base_dir / "weekly"
    return dashboard_path, incident_path, go_nogo_path, weekly_dir


def _safe_float(raw: Any, default: float = 0.0) -> float:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _maybe_float(raw: Any) -> float | None:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _safe_int(raw: Any, default: int = 0) -> int:
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return default


def _preflight_alpaca_credentials() -> None:
    api_key = str(os.getenv("ALPACA_API_KEY", "")).strip()
    secret_key = str(os.getenv("ALPACA_SECRET_KEY", "")).strip()
    if not api_key or not secret_key:
        raise RuntimeError(
            "Missing Alpaca credentials in the current process environment. "
            "Set ALPACA_API_KEY and ALPACA_SECRET_KEY before running collect-fills, collect-fills-campaign, or live pre-submit-check."
        )


def _collect_alpaca_broker_state_snapshot(adapter_factory: Any = AlpacaAdapter) -> dict[str, Any]:
    adapter = adapter_factory()
    account = adapter.query_account()
    positions = adapter.query_positions()
    open_orders = adapter.query_open_orders()
    return {
        "captured_at_utc": datetime.now().astimezone().isoformat(),
        "account": account,
        "positions": positions,
        "open_orders": open_orders,
    }


def _safe_position_float(row: dict[str, Any], key: str, default: float = 0.0) -> float:
    value = row.get(key)
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return float(default)
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    if not math.isfinite(parsed):
        return float(default)
    return float(parsed)


def _safe_timestamp(raw: Any) -> pd.Timestamp | None:
    if raw is None:
        return None
    try:
        timestamp = pd.to_datetime(raw, utc=True, errors="coerce")
    except Exception:
        return None
    if pd.isna(timestamp):
        return None
    return pd.Timestamp(timestamp)


def _classify_open_orders(open_orders: list[dict[str, Any]]) -> dict[str, Any]:
    now_utc = pd.Timestamp.now(tz="UTC")
    audited_orders: list[dict[str, Any]] = []
    recommended_cancellations: list[dict[str, Any]] = []
    locked_buying_power_count = 0
    locked_inventory_count = 0
    stale_count = 0
    long_lived_count = 0
    manual_confirmation_count = 0
    side_counts: Counter[str] = Counter()
    order_type_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()

    for row in open_orders:
        ticker = str(row.get("ticker", "")).strip().upper()
        direction = str(row.get("direction", "")).strip().lower()
        order_type = str(row.get("order_type", "")).strip().lower()
        status = str(row.get("status", "")).strip().lower()
        time_in_force = str(row.get("time_in_force", "")).strip().lower()
        quantity = _safe_float(row.get("quantity"), 0.0)
        filled_qty = _safe_float(row.get("filled_qty"), 0.0)
        submitted_ts = _safe_timestamp(row.get("submitted_at") or row.get("created_at") or row.get("updated_at"))
        age_minutes = None
        if submitted_ts is not None:
            age_minutes = max(0.0, float((now_utc - submitted_ts).total_seconds() / 60.0))
        locks_buying_power = direction == "buy"
        locks_inventory = direction == "sell"
        long_lived = bool(age_minutes is not None and age_minutes >= 60.0 and status in {"open", "new", "accepted", "pending_new"})
        stale = bool(age_minutes is not None and age_minutes >= 120.0 and status in {"open", "new", "accepted", "pending_new"})
        needs_manual_confirmation = bool(age_minutes is not None and age_minutes >= 240.0)
        suggested_action = "keep"
        if stale or needs_manual_confirmation:
            suggested_action = "consider_cancel"
        impact: list[str] = []
        if locks_buying_power:
            impact.append("locks buying power")
            locked_buying_power_count += 1
        if locks_inventory:
            impact.append("locks inventory")
            locked_inventory_count += 1
        if stale:
            impact.append("stale")
            stale_count += 1
        if long_lived:
            long_lived_count += 1
        if needs_manual_confirmation:
            impact.append("manual confirmation recommended")
            manual_confirmation_count += 1
        if suggested_action == "consider_cancel":
            recommended_cancellations.append(
                {
                    "order_id": row.get("order_id", ""),
                    "ticker": ticker,
                    "direction": direction,
                    "reason": "; ".join(impact) or "open order is stale or long-lived",
                    "impact": "buying power" if locks_buying_power else "inventory" if locks_inventory else "unknown",
                    "age_minutes": age_minutes,
                }
            )
        audited_orders.append(
            {
                "order_id": str(row.get("order_id", "")).strip(),
                "client_order_id": str(row.get("client_order_id", "")).strip(),
                "ticker": ticker,
                "direction": direction,
                "order_type": order_type,
                "time_in_force": time_in_force,
                "status": status,
                "quantity": quantity,
                "filled_qty": filled_qty,
                "limit_price": row.get("limit_price"),
                "stop_price": row.get("stop_price"),
                "extended_hours": bool(row.get("extended_hours", False)),
                "submitted_at": row.get("submitted_at"),
                "created_at": row.get("created_at"),
                "updated_at": row.get("updated_at"),
                "filled_at": row.get("filled_at"),
                "age_minutes": age_minutes,
                "locks_buying_power": locks_buying_power,
                "locks_inventory": locks_inventory,
                "long_lived": long_lived,
                "stale": stale,
                "needs_manual_confirmation": needs_manual_confirmation,
                "suggested_action": suggested_action,
                "impact_reason": "; ".join(impact) or "",
            }
        )
        side_counts[direction or "unknown"] += 1
        order_type_counts[order_type or "unknown"] += 1
        status_counts[status or "unknown"] += 1

    audited_orders.sort(
        key=lambda item: (
            -(float(item.get("age_minutes") or 0.0) if item.get("age_minutes") is not None else 0.0),
            str(item.get("ticker", "")),
            str(item.get("order_id", "")),
        )
    )
    recommended_cancellations.sort(
        key=lambda item: (
            -(float(item.get("age_minutes") or 0.0) if item.get("age_minutes") is not None else 0.0),
            str(item.get("ticker", "")),
            str(item.get("order_id", "")),
        )
    )
    return {
        "open_orders_count": int(len(audited_orders)),
        "open_order_side_counts": {str(key): int(value) for key, value in side_counts.items()},
        "open_order_type_counts": {str(key): int(value) for key, value in order_type_counts.items()},
        "open_order_status_counts": {str(key): int(value) for key, value in status_counts.items()},
        "open_order_tickers": sorted({item["ticker"] for item in audited_orders if item.get("ticker")}),
        "open_orders": audited_orders,
        "recommended_cancellations": recommended_cancellations,
        "locked_buying_power_count": int(locked_buying_power_count),
        "locked_inventory_count": int(locked_inventory_count),
        "stale_open_order_count": int(stale_count),
        "long_lived_open_order_count": int(long_lived_count),
        "manual_confirmation_open_order_count": int(manual_confirmation_count),
    }


def _build_broker_state_inspection_payload(snapshot: dict[str, Any], *, notes: str = "") -> dict[str, Any]:
    summary = fill_collection_campaign._broker_state_snapshot_summary(snapshot)
    account = dict((snapshot or {}).get("account") or {})
    positions = list(summary.get("positions") or [])
    open_orders = list(summary.get("open_orders") or [])
    open_orders_audit = _classify_open_orders(open_orders)
    buying_power = _safe_float(summary.get("buying_power"), 0.0) if summary.get("buying_power") is not None else 0.0
    cash = _safe_float(summary.get("cash"), 0.0) if summary.get("cash") is not None else 0.0
    equity_value = summary.get("equity")
    if equity_value is None:
        equity_value = summary.get("portfolio_value")
    equity = _safe_float(equity_value, 0.0) if equity_value is not None else 0.0
    positions_count = int(summary.get("positions_count", len(positions)) or 0)
    position_tickers = list(summary.get("position_tickers", []) or [])
    total_market_value = sum(_safe_position_float(row, "market_value", 0.0) for row in positions)
    total_unrealized_pnl = sum(_safe_position_float(row, "unrealized_pnl", 0.0) for row in positions)
    sellable_positions = [row for row in positions if _safe_position_float(row, "quantity", 0.0) > 0]
    reduction_candidates = sorted(
        sellable_positions,
        key=lambda row: (
            -_safe_position_float(row, "market_value", 0.0),
            _safe_position_float(row, "unrealized_pnl", 0.0),
            str(row.get("ticker", "")),
        ),
    )
    ranked_reduction_candidates = []
    for index, row in enumerate(reduction_candidates, start=1):
        ranked_reduction_candidates.append(
            {
                "rank": index,
                "ticker": str(row.get("ticker", "")).strip(),
                "quantity": _safe_position_float(row, "quantity", 0.0),
                "market_value": _safe_position_float(row, "market_value", 0.0),
                "avg_entry_price": _safe_position_float(row, "avg_entry_price", 0.0),
                "current_price": _safe_position_float(row, "current_price", 0.0),
                "unrealized_pnl": _safe_position_float(row, "unrealized_pnl", 0.0),
            }
        )

    account_type = str(account.get("account_type", "")).strip().lower()
    reset_account_feasible = account_type == "paper"
    buy_only_campaign_feasible = buying_power > 0.0
    sell_only_campaign_feasible = len(sellable_positions) > 0
    reduce_positions_feasible = len(sellable_positions) > 0
    preopen_action = "review open orders"
    preopen_action_reason = "No open orders need attention."
    if open_orders_audit["recommended_cancellations"]:
        preopen_action = "review open orders before opening bell"
        preopen_action_reason = "One or more open orders are stale or long-lived and may lock buying power or inventory."
    elif open_orders_audit["open_orders_count"] > 0:
        preopen_action = "monitor open orders"
        preopen_action_reason = "Open orders exist but none are clearly stale."

    if positions_count <= 0 and buying_power <= 0.0:
        recommended_next_action = "reset account"
        recommendation_reason = "No positions and no buying power; reset is the cleanest path."
    elif buying_power <= 0.0 and positions_count > 0:
        recommended_next_action = "reduce positions"
        recommendation_reason = "Buying power is zero and positions exist; reducing positions is the fastest path to release cash."
    elif buy_only_campaign_feasible and not sell_only_campaign_feasible:
        recommended_next_action = "buy-only campaign"
        recommendation_reason = "Buying power exists and no sell inventory is available."
    elif sell_only_campaign_feasible:
        recommended_next_action = "sell-only campaign"
        recommendation_reason = "Positions exist, so sell-side collection is feasible."
    else:
        recommended_next_action = "reset account"
        recommendation_reason = "No feasible route is currently available."

    return {
        "inspected_at_utc": summary.get("captured_at_utc"),
        "notes": notes,
        "broker": "alpaca",
        "market": "us",
        "account_snapshot": account,
        "broker_state_snapshot": summary,
        "positions": positions,
        "positions_summary": {
            "positions_count": positions_count,
            "position_tickers": position_tickers,
            "total_market_value": float(total_market_value),
            "total_unrealized_pnl": float(total_unrealized_pnl),
        },
        "open_orders_summary": open_orders_audit,
        "feasible_routes": {
            "reset_account": reset_account_feasible,
            "reduce_positions": reduce_positions_feasible,
            "sell_only_campaign": sell_only_campaign_feasible,
            "buy_only_campaign": buy_only_campaign_feasible,
        },
        "preopen_action": preopen_action,
        "preopen_action_reason": preopen_action_reason,
        "recommended_next_action": recommended_next_action,
        "recommendation_reason": recommendation_reason,
        "buy_only_not_feasible_reason": (
            "buying_power is zero" if buying_power <= 0.0 else ""
        ),
        "sell_only_feasible": sell_only_campaign_feasible,
        "buy_only_feasible": buy_only_campaign_feasible,
        "reduce_positions_candidates": ranked_reduction_candidates,
        "open_orders": open_orders_audit["open_orders"],
        "recommended_cancellations": open_orders_audit["recommended_cancellations"],
    }


def _build_broker_state_transition_payload(
    *,
    before_snapshot: dict[str, Any],
    after_snapshot: dict[str, Any],
    campaign_manifest: dict[str, Any],
    notes: str = "",
) -> dict[str, Any]:
    before_summary = fill_collection_campaign._broker_state_snapshot_summary(before_snapshot)
    after_summary = fill_collection_campaign._broker_state_snapshot_summary(after_snapshot)
    before_account = dict((before_snapshot or {}).get("account") or {})
    after_account = dict((after_snapshot or {}).get("account") or {})
    before_positions = list(before_summary.get("positions") or [])
    after_positions = list(after_summary.get("positions") or [])
    sold_tickers = list(campaign_manifest.get("reduction_selected_tickers") or [])
    sold_quantities = [float(value) for value in list(campaign_manifest.get("reduction_selected_quantities") or [])]
    sold_notionals = [float(value) for value in list(campaign_manifest.get("reduction_selected_notionals") or [])]
    submitted_sell_order_count = _safe_int(campaign_manifest.get("submitted_sell_order_count"), 0)
    filled_sell_order_count = _safe_int(campaign_manifest.get("filled_sell_order_count"), 0)
    cash_before = _safe_float(before_summary.get("cash"), 0.0) if before_summary.get("cash") is not None else 0.0
    cash_after = _safe_float(after_summary.get("cash"), 0.0) if after_summary.get("cash") is not None else 0.0
    buying_power_before = _safe_float(before_summary.get("buying_power"), 0.0) if before_summary.get("buying_power") is not None else 0.0
    buying_power_after = _safe_float(after_summary.get("buying_power"), 0.0) if after_summary.get("buying_power") is not None else 0.0
    equity_before = _safe_float(before_summary.get("equity"), 0.0) if before_summary.get("equity") is not None else 0.0
    equity_after = _safe_float(after_summary.get("equity"), 0.0) if after_summary.get("equity") is not None else 0.0
    sold_notional = float(sum(sold_notionals))
    return {
        "inspected_at_utc": datetime.now().astimezone().isoformat(),
        "notes": notes,
        "broker": str(campaign_manifest.get("broker", "alpaca")),
        "market": str(campaign_manifest.get("market", "us")),
        "campaign_run_id": campaign_manifest.get("campaign_run_id", ""),
        "campaign_preset": campaign_manifest.get("campaign_preset", ""),
        "side_scope": campaign_manifest.get("side_scope", ""),
        "before_account_snapshot": before_account,
        "after_account_snapshot": after_account,
        "before_broker_state_snapshot": before_summary,
        "after_broker_state_snapshot": after_summary,
        "before_positions": before_positions,
        "after_positions": after_positions,
        "positions_before_count": int(before_summary.get("positions_count", len(before_positions)) or 0),
        "positions_after_count": int(after_summary.get("positions_count", len(after_positions)) or 0),
        "cash_before": float(cash_before),
        "cash_after": float(cash_after),
        "cash_delta": float(cash_after - cash_before),
        "buying_power_before": float(buying_power_before),
        "buying_power_after": float(buying_power_after),
        "buying_power_delta": float(buying_power_after - buying_power_before),
        "equity_before": float(equity_before),
        "equity_after": float(equity_after),
        "equity_delta": float(equity_after - equity_before),
        "submitted_sell_order_count": int(submitted_sell_order_count),
        "filled_sell_order_count": int(filled_sell_order_count),
        "sold_tickers": sold_tickers,
        "sold_quantities": sold_quantities,
        "sold_notionals": sold_notionals,
        "sold_notional_total": float(sold_notional),
        "broker_state_recommendation": (
            "buy-only campaign after cash release"
            if buying_power_after > 0.0 and cash_after >= 0.0
            else "reduce positions"
        ),
        "recommended_next_action": (
            "buy-only campaign after cash release"
            if buying_power_after > 0.0 and cash_after >= 0.0
            else "reduce positions"
        ),
        "reduction_result": (
            "reduction successful"
            if submitted_sell_order_count > 0 and filled_sell_order_count > 0 and buying_power_after > buying_power_before
            else "still not buy-ready"
        ),
        "still_negative_cash": bool(cash_after < 0.0),
        "ready_for_buy_only": bool(buying_power_after > 0.0 and cash_after >= 0.0),
    }


def _render_broker_state_transition_report(payload: dict[str, Any]) -> str:
    before = payload.get("before_broker_state_snapshot") or {}
    after = payload.get("after_broker_state_snapshot") or {}
    lines = [
        "# Broker State Transition",
        "",
        "## Summary",
        "",
        f"- inspected_at_utc: {payload.get('inspected_at_utc', '')}",
        f"- broker: {payload.get('broker', '')}",
        f"- market: {payload.get('market', '')}",
        f"- campaign_run_id: {payload.get('campaign_run_id', '')}",
        f"- campaign_preset: {payload.get('campaign_preset', '')}",
        f"- side_scope: {payload.get('side_scope', '')}",
        f"- notes: {payload.get('notes', '')}",
        f"- reduction_result: {payload.get('reduction_result', '')}",
        f"- recommended_next_action: {payload.get('recommended_next_action', '')}",
        f"- ready_for_buy_only: {str(bool(payload.get('ready_for_buy_only', False))).lower()}",
        f"- still_negative_cash: {str(bool(payload.get('still_negative_cash', False))).lower()}",
        "",
        "## Before",
        "",
        f"- cash: {payload.get('cash_before', None)}",
        f"- buying_power: {payload.get('buying_power_before', None)}",
        f"- equity: {payload.get('equity_before', None)}",
        f"- positions_count: {payload.get('positions_before_count', 0)}",
        f"- position_tickers: {before.get('position_tickers', [])}",
        "",
        "## After",
        "",
        f"- cash: {payload.get('cash_after', None)}",
        f"- buying_power: {payload.get('buying_power_after', None)}",
        f"- equity: {payload.get('equity_after', None)}",
        f"- positions_count: {payload.get('positions_after_count', 0)}",
        f"- position_tickers: {after.get('position_tickers', [])}",
        "",
        "## Delta",
        "",
        f"- cash_delta: {payload.get('cash_delta', None)}",
        f"- buying_power_delta: {payload.get('buying_power_delta', None)}",
        f"- equity_delta: {payload.get('equity_delta', None)}",
        f"- sold_tickers: {payload.get('sold_tickers', [])}",
        f"- sold_quantities: {payload.get('sold_quantities', [])}",
        f"- sold_notionals: {payload.get('sold_notionals', [])}",
        f"- sold_notional_total: {payload.get('sold_notional_total', 0.0)}",
        f"- submitted_sell_order_count: {payload.get('submitted_sell_order_count', 0)}",
        f"- filled_sell_order_count: {payload.get('filled_sell_order_count', 0)}",
    ]
    return "\n".join(lines) + "\n"


def _render_broker_state_report(payload: dict[str, Any]) -> str:
    summary = payload.get("broker_state_snapshot") or {}
    account = payload.get("account_snapshot") or {}
    positions = payload.get("reduce_positions_candidates") or []
    routes = payload.get("feasible_routes") or {}
    open_orders_summary = payload.get("open_orders_summary") or {}
    open_orders = payload.get("open_orders") or []
    recommended_cancellations = payload.get("recommended_cancellations") or []
    lines = [
        "# Broker State Inspection",
        "",
        "## Summary",
        "",
        f"- inspected_at_utc: {payload.get('inspected_at_utc', '')}",
        f"- broker: {payload.get('broker', '')}",
        f"- market: {payload.get('market', '')}",
        f"- recommended_next_action: {payload.get('recommended_next_action', '')}",
        f"- recommendation_reason: {payload.get('recommendation_reason', '')}",
        f"- buy_only_not_feasible_reason: {payload.get('buy_only_not_feasible_reason', '')}",
        f"- sell_only_feasible: {str(bool(payload.get('sell_only_feasible', False))).lower()}",
        f"- reduce_positions_feasible: {str(bool(routes.get('reduce_positions', False))).lower()}",
        f"- notes: {payload.get('notes', '')}",
        "",
        "## Account Snapshot",
        "",
        f"- buying_power: {summary.get('buying_power', account.get('buying_power'))}",
        f"- cash: {summary.get('cash', account.get('cash'))}",
        f"- equity: {summary.get('equity', account.get('equity', account.get('portfolio_value')))}",
        f"- portfolio_value: {summary.get('portfolio_value', account.get('portfolio_value'))}",
        f"- positions_count: {summary.get('positions_count', 0)}",
        f"- position_tickers: {summary.get('position_tickers', [])}",
        f"- total_market_value: {payload.get('positions_summary', {}).get('total_market_value', 0.0)}",
        f"- total_unrealized_pnl: {payload.get('positions_summary', {}).get('total_unrealized_pnl', 0.0)}",
        "",
        "## Feasible Routes",
        "",
        "| route | feasible |",
        "| --- | --- |",
    ]
    for route_name in ["reset_account", "reduce_positions", "sell_only_campaign", "buy_only_campaign"]:
        lines.append(f"| {route_name} | {str(bool(routes.get(route_name, False))).lower()} |")
    lines.extend(
        [
        "",
        "## Positions",
        "",
        "| rank | ticker | quantity | market_value | unrealized_pnl | avg_entry_price | current_price |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    if positions:
        for row in positions:
            lines.append(
                "| {rank} | {ticker} | {quantity:.0f} | {market_value:.2f} | {unrealized_pnl:.2f} | {avg_entry_price:.2f} | {current_price:.2f} |".format(
                    rank=int(row.get("rank", 0) or 0),
                    ticker=str(row.get("ticker", "")),
                    quantity=_safe_position_float(row, "quantity", 0.0),
                    market_value=_safe_position_float(row, "market_value", 0.0),
                    unrealized_pnl=_safe_position_float(row, "unrealized_pnl", 0.0),
                    avg_entry_price=_safe_position_float(row, "avg_entry_price", 0.0),
                    current_price=_safe_position_float(row, "current_price", 0.0),
                )
            )
    else:
        lines.append("|  | none | 0 | 0.00 | 0.00 | 0.00 | 0.00 |")
    lines.extend(
        [
            "",
            "## Open Orders",
            "",
            f"- open_orders_count: {open_orders_summary.get('open_orders_count', len(open_orders))}",
            f"- locked_buying_power_count: {open_orders_summary.get('locked_buying_power_count', 0)}",
            f"- locked_inventory_count: {open_orders_summary.get('locked_inventory_count', 0)}",
            f"- stale_open_order_count: {open_orders_summary.get('stale_open_order_count', 0)}",
            f"- long_lived_open_order_count: {open_orders_summary.get('long_lived_open_order_count', 0)}",
            "",
            "| order_id | ticker | direction | order_type | tif | status | age_minutes | locks_bp | locks_inv | suggested_action |",
            "| --- | --- | --- | --- | --- | --- | ---: | --- | --- | --- |",
        ]
    )
    if open_orders:
        for row in open_orders:
            lines.append(
                "| {order_id} | {ticker} | {direction} | {order_type} | {time_in_force} | {status} | {age_minutes} | {locks_buying_power} | {locks_inventory} | {suggested_action} |".format(
                    order_id=str(row.get("order_id", "")),
                    ticker=str(row.get("ticker", "")),
                    direction=str(row.get("direction", "")),
                    order_type=str(row.get("order_type", "")),
                    time_in_force=str(row.get("time_in_force", "")),
                    status=str(row.get("status", "")),
                    age_minutes=("N/A" if row.get("age_minutes") is None else f"{float(row.get('age_minutes', 0.0)):.1f}"),
                    locks_buying_power=str(bool(row.get("locks_buying_power", False))).lower(),
                    locks_inventory=str(bool(row.get("locks_inventory", False))).lower(),
                    suggested_action=str(row.get("suggested_action", "")),
                )
            )
    else:
        lines.append("|  | none |  |  |  |  |  |  |  |  |")
    lines.extend(
        [
            "",
            "## Recommended Cancellations",
            "",
        ]
    )
    if recommended_cancellations:
        for row in recommended_cancellations:
            lines.append(
                f"- {row.get('order_id', '')} {row.get('ticker', '')} {row.get('direction', '')}: {row.get('reason', '')} "
                f"(impact={row.get('impact', '')}, age_minutes={row.get('age_minutes', None)})"
            )
    else:
        lines.append("- none")
    lines.extend(["", "## Next Step", "", f"- {payload.get('recommended_next_action', '')}"])
    return "\n".join(lines) + "\n"


def _write_broker_state_inspection_artifacts(*, output_dir: Path, snapshot: dict[str, Any], notes: str = "") -> Path:
    run_id = fill_collection.generate_run_id(source_type="broker_state_inspection")
    run_dir = output_dir.resolve() / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = _build_broker_state_inspection_payload(snapshot, notes=notes)
    (run_dir / "broker_state_report.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (run_dir / "broker_state_report.md").write_text(_render_broker_state_report(payload), encoding="utf-8")
    return run_dir


def _load_broker_state_snapshot_file(snapshot_path: Path) -> dict[str, Any]:
    payload = json.loads(Path(snapshot_path).resolve().read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError(f"broker state snapshot must be a JSON object: {snapshot_path}")
    if isinstance(payload.get("broker_state_inspection"), dict):
        payload = dict(payload["broker_state_inspection"])
    summary = payload.get("broker_state_snapshot") if isinstance(payload.get("broker_state_snapshot"), dict) else {}
    if "account" in payload:
        snapshot = dict(payload)
    else:
        snapshot = {
            "captured_at_utc": (
                payload.get("captured_at_utc")
                or payload.get("inspected_at_utc")
                or payload.get("generated_at_utc")
                or summary.get("captured_at_utc")
            ),
            "account": payload.get("account_snapshot") or payload.get("account") or {},
            "positions": payload.get("positions") or summary.get("positions") or [],
            "open_orders": payload.get("open_orders") or summary.get("open_orders") or [],
        }
    snapshot.setdefault("account", {})
    snapshot.setdefault("positions", [])
    snapshot.setdefault("open_orders", [])
    return snapshot


def _price_for_notional(row: dict[str, Any]) -> float:
    price = _to_float(row.get("price_limit"), 0.0)
    if price <= 0:
        price = _to_float(row.get("estimated_price"), 0.0)
    return float(max(price, 0.0))


def _notional_summary_from_orders(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty:
        return {"total_notional": 0.0, "buy_notional": 0.0, "sell_notional": 0.0}
    total_notional = 0.0
    buy_notional = 0.0
    sell_notional = 0.0
    for row in frame.to_dict(orient="records"):
        quantity = max(0.0, _to_float(row.get("quantity"), 0.0))
        notional = quantity * _price_for_notional(row)
        total_notional += notional
        direction = str(row.get("direction", "")).strip().lower()
        if direction == "buy":
            buy_notional += notional
        elif direction == "sell":
            sell_notional += notional
    return {
        "total_notional": float(total_notional),
        "buy_notional": float(buy_notional),
        "sell_notional": float(sell_notional),
    }


def _reason_counter(rows: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        reason = str(row.get("reason", "")).strip()
        if reason:
            counter[reason] += 1
    return {str(key): int(value) for key, value in counter.items()}


def _pre_submission_recommendation(
    *,
    account_payload: dict[str, Any],
    open_orders_summary: dict[str, Any],
    precheck: dict[str, Any],
) -> tuple[str, str]:
    status = str(account_payload.get("status", "")).strip().upper()
    if status and status != "ACTIVE":
        return "blocked_account", f"account_status={status}"
    if bool(account_payload.get("trading_blocked", False)):
        return "blocked_account", "trading_blocked=true"
    if bool(account_payload.get("transfers_blocked", False)):
        return "blocked_account", "transfers_blocked=true"
    if int(open_orders_summary.get("open_orders_count", 0) or 0) > 0:
        return "review_open_orders_before_submit", "open_orders_present"
    if int(precheck.get("submitted_count", 0) or 0) <= 0:
        return "blocked_no_submittable_orders", "precheck_submitted_count=0"
    if int(precheck.get("skipped_count", 0) or 0) > 0 or int(precheck.get("clipped_count", 0) or 0) > 0:
        return "ready_with_adjustments", "precheck_clipped_or_skipped_orders_present"
    return "ready_to_submit", "all_orders_submittable"


def _build_pre_submission_check_payload(
    *,
    sources: list[fill_collection.FillSourceBasket],
    broker_state_snapshot: dict[str, Any],
    snapshot_source: str,
    snapshot_path: Path | None,
    notes: str = "",
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    inspection_payload = _build_broker_state_inspection_payload(broker_state_snapshot, notes=notes)
    account_payload = dict((broker_state_snapshot or {}).get("account") or {})
    raw_positions = (broker_state_snapshot or {}).get("positions")
    if isinstance(raw_positions, pd.DataFrame):
        positions_df = raw_positions.copy()
    else:
        positions_df = pd.DataFrame(list(raw_positions or []))
    basket_checks: list[dict[str, Any]] = []
    prepared_frames: dict[str, pd.DataFrame] = {}

    for index, source in enumerate(sources, start=1):
        orders_df = _source_orders_for_precheck(source)
        prepared_orders_df, precheck = _prepare_orders_for_alpaca(
            orders_df=orders_df,
            broker_positions_before=positions_df,
            account_payload=account_payload,
        )
        requested_notionals = _notional_summary_from_orders(orders_df)
        submitted_notionals = _notional_summary_from_orders(prepared_orders_df)
        recommendation, recommendation_reason = _pre_submission_recommendation(
            account_payload=account_payload,
            open_orders_summary=inspection_payload.get("open_orders_summary", {}),
            precheck=precheck,
        )
        sample_id = str(source.sample_id or f"basket_{index:02d}").strip() or f"basket_{index:02d}"
        artifact_id = sample_id if sample_id not in prepared_frames else f"{sample_id}_{index:02d}"
        prepared_frames[artifact_id] = prepared_orders_df.copy()
        basket_checks.append(
            {
                "artifact_id": artifact_id,
                "sample_id": sample_id,
                "source_path": str(source.source_path),
                "input_order_count": int(len(orders_df)),
                "input_buy_count": int((orders_df.get("direction", pd.Series(dtype="object")).astype(str).str.lower() == "buy").sum()),
                "input_sell_count": int((orders_df.get("direction", pd.Series(dtype="object")).astype(str).str.lower() == "sell").sum()),
                "submitted_order_count": int(precheck.get("submitted_count", 0) or 0),
                "submitted_buy_count": int((prepared_orders_df.get("direction", pd.Series(dtype="object")).astype(str).str.lower() == "buy").sum()),
                "submitted_sell_count": int((prepared_orders_df.get("direction", pd.Series(dtype="object")).astype(str).str.lower() == "sell").sum()),
                "skipped_order_count": int(precheck.get("skipped_count", 0) or 0),
                "clipped_order_count": int(precheck.get("clipped_count", 0) or 0),
                "requested_total_notional": requested_notionals["total_notional"],
                "requested_buy_notional": requested_notionals["buy_notional"],
                "requested_sell_notional": requested_notionals["sell_notional"],
                "submitted_total_notional": submitted_notionals["total_notional"],
                "submitted_buy_notional": submitted_notionals["buy_notional"],
                "submitted_sell_notional": submitted_notionals["sell_notional"],
                "buying_power": _to_float(precheck.get("buying_power"), 0.0),
                "buy_budget_80pct": _to_float(precheck.get("buy_budget_80pct"), 0.0),
                "buy_budget_remaining": _to_float(precheck.get("buy_budget_remaining"), 0.0),
                "blocked_reason_counts": _reason_counter(list(precheck.get("skipped_orders") or [])),
                "clipped_reason_counts": _reason_counter(list(precheck.get("clipped_orders") or [])),
                "skipped_orders": list(precheck.get("skipped_orders") or []),
                "clipped_orders": list(precheck.get("clipped_orders") or []),
                "input_tickers": sorted({str(value).strip() for value in orders_df.get("ticker", pd.Series(dtype="object")).astype(str).tolist() if str(value).strip()}),
                "submitted_tickers": sorted({str(value).strip() for value in prepared_orders_df.get("ticker", pd.Series(dtype="object")).astype(str).tolist() if str(value).strip()}),
                "recommendation": recommendation,
                "recommendation_reason": recommendation_reason,
                "prepared_orders_preview": prepared_orders_df.head(10).to_dict(orient="records"),
            }
        )

    if not basket_checks:
        overall_recommendation = "no_sources_loaded"
        overall_reason = "no_orders_found_for_requested_inputs"
    else:
        recommendations = {item["recommendation"] for item in basket_checks}
        if recommendations == {"ready_to_submit"}:
            overall_recommendation = "ready_to_submit"
            overall_reason = "all_baskets_ready"
        elif any(item.startswith("blocked_") for item in recommendations):
            overall_recommendation = "mixed_or_blocked"
            overall_reason = "one_or_more_baskets_blocked"
        else:
            overall_recommendation = "review_required"
            overall_reason = "open_orders_or_precheck_adjustments_present"

    payload = {
        "generated_at_utc": datetime.now().astimezone().isoformat(),
        "broker": "alpaca",
        "market": "us",
        "notes": notes,
        "snapshot_source": snapshot_source,
        "snapshot_path": str(snapshot_path.resolve()) if snapshot_path is not None else None,
        "overall_recommendation": overall_recommendation,
        "overall_reason": overall_reason,
        "basket_count": int(len(basket_checks)),
        "broker_state_inspection": inspection_payload,
        "account_snapshot": inspection_payload.get("account_snapshot"),
        "positions_summary": inspection_payload.get("positions_summary"),
        "open_orders_summary": inspection_payload.get("open_orders_summary"),
        "basket_checks": basket_checks,
    }
    return payload, prepared_frames


def _render_pre_submission_check_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Pre-Submission Check",
        "",
        "## Summary",
        "",
        f"- generated_at_utc: {payload.get('generated_at_utc', '')}",
        f"- broker: {payload.get('broker', '')}",
        f"- market: {payload.get('market', '')}",
        f"- snapshot_source: {payload.get('snapshot_source', '')}",
        f"- snapshot_path: {payload.get('snapshot_path', '')}",
        f"- overall_recommendation: {payload.get('overall_recommendation', '')}",
        f"- overall_reason: {payload.get('overall_reason', '')}",
        f"- basket_count: {payload.get('basket_count', 0)}",
        "",
        "## Broker State",
        "",
        f"- buying_power: {((payload.get('account_snapshot') or {}).get('buying_power'))}",
        f"- cash: {((payload.get('account_snapshot') or {}).get('cash'))}",
        f"- account_status: {((payload.get('account_snapshot') or {}).get('status', ''))}",
        f"- positions_count: {((payload.get('positions_summary') or {}).get('positions_count', 0))}",
        f"- open_orders_count: {((payload.get('open_orders_summary') or {}).get('open_orders_count', 0))}",
        f"- recommended_cancellations: {len((payload.get('broker_state_inspection') or {}).get('recommended_cancellations', []))}",
        "",
        "## Basket Checks",
        "",
    ]
    for basket in payload.get("basket_checks", []):
        lines.extend(
            [
                f"### {basket.get('sample_id', '')}",
                "",
                f"- source_path: {basket.get('source_path', '')}",
                f"- recommendation: {basket.get('recommendation', '')}",
                f"- recommendation_reason: {basket.get('recommendation_reason', '')}",
                f"- input_order_count: {basket.get('input_order_count', 0)}",
                f"- submitted_order_count: {basket.get('submitted_order_count', 0)}",
                f"- skipped_order_count: {basket.get('skipped_order_count', 0)}",
                f"- clipped_order_count: {basket.get('clipped_order_count', 0)}",
                f"- requested_total_notional: {basket.get('requested_total_notional', 0.0)}",
                f"- submitted_total_notional: {basket.get('submitted_total_notional', 0.0)}",
                f"- blocked_reason_counts: {basket.get('blocked_reason_counts', {})}",
                f"- clipped_reason_counts: {basket.get('clipped_reason_counts', {})}",
                f"- input_tickers: {basket.get('input_tickers', [])}",
                f"- submitted_tickers: {basket.get('submitted_tickers', [])}",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def _write_pre_submission_check_artifacts(
    *,
    output_dir: Path,
    payload: dict[str, Any],
    prepared_frames: dict[str, pd.DataFrame],
) -> Path:
    run_id = fill_collection.generate_run_id(source_type="pre_submission_check")
    run_dir = output_dir.resolve() / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    prepared_dir = run_dir / "prepared_orders"
    prepared_dir.mkdir(parents=True, exist_ok=True)

    (run_dir / "pre_submission_check.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (run_dir / "pre_submission_check.md").write_text(
        _render_pre_submission_check_report(payload),
        encoding="utf-8",
    )

    summary_rows: list[dict[str, Any]] = []
    for basket in payload.get("basket_checks", []):
        summary_rows.append(
            {
                "artifact_id": basket.get("artifact_id", ""),
                "sample_id": basket.get("sample_id", ""),
                "source_path": basket.get("source_path", ""),
                "recommendation": basket.get("recommendation", ""),
                "recommendation_reason": basket.get("recommendation_reason", ""),
                "input_order_count": basket.get("input_order_count", 0),
                "submitted_order_count": basket.get("submitted_order_count", 0),
                "skipped_order_count": basket.get("skipped_order_count", 0),
                "clipped_order_count": basket.get("clipped_order_count", 0),
                "requested_total_notional": basket.get("requested_total_notional", 0.0),
                "submitted_total_notional": basket.get("submitted_total_notional", 0.0),
                "blocked_reason_counts_json": json.dumps(basket.get("blocked_reason_counts", {}), ensure_ascii=False, sort_keys=True),
                "clipped_reason_counts_json": json.dumps(basket.get("clipped_reason_counts", {}), ensure_ascii=False, sort_keys=True),
            }
        )
        artifact_id = str(basket.get("artifact_id", "")).strip()
        frame = prepared_frames.get(artifact_id)
        if frame is not None:
            frame.to_csv(prepared_dir / f"{artifact_id}_prepared_orders.csv", index=False, encoding="utf-8")
    pd.DataFrame(summary_rows).to_csv(run_dir / "basket_precheck_summary.csv", index=False, encoding="utf-8")
    return run_dir


def _latest_matching_path(root: Path, pattern: str) -> Path | None:
    candidates = [path for path in root.glob(pattern) if path.is_file()]
    if not candidates:
        return None
    return sorted(candidates, key=lambda path: (path.stat().st_mtime, path.name))[-1]


def _resolve_latest_bucket_plan_file(bucket_plan_file: Path | None = None) -> Path:
    if bucket_plan_file is not None:
        return Path(bucket_plan_file).resolve()
    latest = _latest_matching_path(OUTPUTS_DIR / "alpaca_fill_campaign", "bucket_plan_*.yaml")
    if latest is None:
        raise RuntimeError(
            "Unable to locate a bucket plan file under outputs/alpaca_fill_campaign. "
            "Pass --bucket-plan-file explicitly."
        )
    return latest


def _build_tomorrow_minimal_validation_plan(
    *,
    bucket_plan_file: Path,
    output_dir: Path,
    max_seed_notional: float,
    max_seed_orders: int,
    market_is_open: bool,
    notes: str,
) -> dict[str, Any]:
    recommended_command = [
        sys.executable,
        str(ROOT / "scripts" / "pilot_ops.py"),
        "collect-fills-minimal-buy",
        "--output-dir",
        str(OUTPUTS_DIR / "alpaca_fill_collection"),
        "--timeout-seconds",
        "60",
        "--poll-interval-seconds",
        "1",
    ]
    return {
        "generated_at_utc": datetime.now().astimezone().isoformat(),
        "execution_window": "regular market hours",
        "market_is_open_at_prep_time": bool(market_is_open),
        "campaign_preset": "minimal-buy-validation",
        "side_scope": "buy-only",
        "max_seed_notional": float(max_seed_notional),
        "max_seed_orders": int(max_seed_orders),
        "force_outside_market_hours": False,
        "bucket_plan_file": str(Path(bucket_plan_file).resolve()),
        "plan_file": str((Path(output_dir).resolve() / "tomorrow_minimal_validation_plan.yaml").resolve()),
        "output_dir": str(Path(output_dir).resolve()),
        "objective": "Minimal buy validation only; submit exactly one deterministic market buy order.",
        "notes": notes,
        "recommended_command": recommended_command,
        "recommended_next_action": "Run the command during regular market hours and confirm submitted_order_count=1.",
    }


def _resolve_latest_validation_plan_file() -> Path | None:
    latest = _latest_matching_path(OFF_HOURS_PREP_DIR, "off_hours_prep_*/tomorrow_minimal_validation_plan.yaml")
    return latest


def _estimate_minimal_buy_validation_price(ticker: str) -> tuple[float, str]:
    api_key = str(os.getenv("ALPACA_API_KEY", "")).strip()
    secret_key = str(os.getenv("ALPACA_SECRET_KEY", "")).strip()
    client = StockHistoricalDataClient(api_key=api_key, secret_key=secret_key, sandbox=False)

    def _extract_price(payload: Any, *, fields: list[str]) -> float | None:
        if payload is None:
            return None
        candidates = [payload]
        if isinstance(payload, dict):
            candidates.append(payload.get(ticker))
        for candidate in candidates:
            if candidate is None:
                continue
            for field in fields:
                value = getattr(candidate, field, None)
                if value is None and isinstance(candidate, dict):
                    value = candidate.get(field)
                if value is None:
                    continue
                try:
                    parsed = float(value)
                except (TypeError, ValueError):
                    continue
                if math.isfinite(parsed) and parsed > 0:
                    return float(parsed)
        return None

    try:
        quote_payload = client.get_stock_latest_quote(StockLatestQuoteRequest(symbol_or_symbols=ticker))
        quote_price = _extract_price(quote_payload, fields=["ask_price", "bid_price", "price", "last"])
        if quote_price is not None:
            return float(quote_price), "latest_quote"
    except Exception:
        pass

    try:
        trade_payload = client.get_stock_latest_trade(StockLatestTradeRequest(symbol_or_symbols=ticker))
        trade_price = _extract_price(trade_payload, fields=["price", "trade_price", "last"])
        if trade_price is not None:
            return float(trade_price), "latest_trade"
    except Exception:
        pass

    return 1.0, "fallback_minimum"


def _select_minimal_buy_validation_candidate(
    *,
    account_payload: dict[str, Any],
    preferred_tickers: list[str] | None = None,
) -> dict[str, Any]:
    preferred = [str(ticker).strip().upper() for ticker in (preferred_tickers or MINIMAL_BUY_VALIDATION_PREFERRED_TICKERS)]
    tradable = fill_collection_campaign._collect_tradable_alpaca_symbols(preferred)
    buying_power = _safe_float(account_payload.get("buying_power"), 0.0)
    cash = _safe_float(account_payload.get("cash"), 0.0)
    if buying_power <= 0.0 or cash <= 0.0:
        raise RuntimeError("buying_power_budget_exhausted")

    candidate_audit: list[dict[str, Any]] = []
    for ticker in preferred:
        if ticker not in tradable:
            candidate_audit.append(
                {
                    "ticker": ticker,
                    "tradable": False,
                    "estimated_price": None,
                    "price_source": "not_tradable",
                    "feasible": False,
                }
            )
            continue
        estimated_price, price_source = _estimate_minimal_buy_validation_price(ticker)
        feasible = estimated_price > 0.0 and estimated_price <= buying_power and estimated_price <= cash
        candidate_audit.append(
            {
                "ticker": ticker,
                "tradable": True,
                "estimated_price": float(estimated_price),
                "price_source": price_source,
                "feasible": bool(feasible),
            }
        )
        if feasible:
            return {
                "ticker": ticker,
                "estimated_price": float(estimated_price),
                "price_source": price_source,
                "candidate_audit": candidate_audit,
                "selected_reason": (
                    "preferred_spy_tradable_and_within_budget" if ticker == "SPY" else "aapl_fallback_within_budget"
                ),
            }
    raise RuntimeError("no_buy_candidate_for_minimal_validation")


def _validate_minimal_buy_validation_precheck(
    *,
    account_payload: dict[str, Any],
    positions: pd.DataFrame,
    open_orders: pd.DataFrame,
) -> None:
    status = str(account_payload.get("status", "")).strip().upper()
    trading_blocked = bool(account_payload.get("trading_blocked", False))
    transfers_blocked = bool(account_payload.get("transfers_blocked", False))
    buying_power = _safe_float(account_payload.get("buying_power"), 0.0)
    cash = _safe_float(account_payload.get("cash"), 0.0)
    positions_count = int(len(positions)) if positions is not None else 0
    open_orders_count = int(len(open_orders)) if open_orders is not None else 0
    failures: list[str] = []
    if status != "ACTIVE":
        failures.append(f"account_status={status or 'UNKNOWN'}")
    if trading_blocked:
        failures.append("trading_blocked=true")
    if transfers_blocked:
        failures.append("transfers_blocked=true")
    if buying_power <= 0.0:
        failures.append("buying_power<=0")
    if cash <= 0.0:
        failures.append("cash<=0")
    if positions_count > 0:
        failures.append(f"positions_count={positions_count}")
    if open_orders_count > 0:
        failures.append(f"open_orders_count={open_orders_count}")
    if failures:
        raise RuntimeError("minimal_buy_validation_precheck_failed: " + ", ".join(failures))


def _build_minimal_buy_validation_orders_oms(*, output_dir: Path, ticker: str, estimated_price: float) -> Path:
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    orders_oms_path = output_dir / "minimal_buy_validation_orders_oms.csv"
    orders_frame = pd.DataFrame(
        [
            {
                "sample_id": "minimal_buy_validation",
                "ticker": str(ticker).strip().upper(),
                "direction": "buy",
                "quantity": 1,
                "estimated_price": float(estimated_price),
                "price_limit": "",
                "extended_hours": False,
            }
        ]
    )
    orders_frame.to_csv(orders_oms_path, index=False, encoding="utf-8")
    return orders_oms_path


def _parse_participation_bucket_values(raw: str | None) -> list[float]:
    if raw is None or not str(raw).strip():
        raw = DEFAULT_FILL_COLLECTION_PARTICIPATION_BUCKETS
    values: list[float] = []
    seen: set[float] = set()
    for token in str(raw).split(","):
        normalized = str(token).strip()
        if not normalized:
            continue
        if normalized.endswith("%"):
            value = float(normalized[:-1].strip()) / 100.0
        else:
            value = float(normalized)
        if value <= 0.0:
            raise ValueError(f"Participation bucket must be positive: {normalized!r}")
        if value in seen:
            continue
        seen.add(value)
        values.append(float(value))
    if not values:
        raise ValueError("At least one participation bucket is required.")
    return values


def _format_participation_bucket_label(value: float) -> str:
    percent_value = float(value) * 100.0
    rendered = f"{percent_value:.6f}".rstrip("0").rstrip(".")
    return f"{rendered}%"


def _normalize_fill_collection_batch_market_frame(market_frame: pd.DataFrame) -> pd.DataFrame:
    if market_frame.empty:
        return pd.DataFrame(columns=["ticker", "estimated_price", "adv_shares", "tradable"])
    work = market_frame.copy()
    columns = {str(column).strip().lower(): column for column in work.columns}
    ticker_col = next((columns[key] for key in ("ticker", "symbol") if key in columns), None)
    price_col = next((columns[key] for key in ("close", "vwap", "price", "last", "estimated_price") if key in columns), None)
    adv_col = next((columns[key] for key in ("adv_shares", "average_daily_volume", "avg_daily_volume", "daily_volume", "volume", "adv") if key in columns), None)
    tradable_col = next((columns[key] for key in ("tradable", "is_tradable") if key in columns), None)
    if ticker_col is None or price_col is None or adv_col is None:
        raise ValueError("market_file must include ticker/symbol, price (close/vwap), and adv_shares columns.")

    normalized = pd.DataFrame(
        {
            "ticker": work[ticker_col].astype(str).str.strip().str.upper(),
            "estimated_price": pd.to_numeric(work[price_col], errors="coerce"),
            "adv_shares": pd.to_numeric(work[adv_col], errors="coerce"),
        }
    )
    if tradable_col is None:
        normalized["tradable"] = True
    else:
        normalized["tradable"] = work[tradable_col].apply(
            lambda value: False
            if pd.isna(value)
            else str(value).strip().lower() in {"true", "1", "yes", "y", "on"}
        )
    normalized = normalized.loc[normalized["ticker"].astype(bool)].copy()
    normalized = normalized.sort_values(by=["ticker"], kind="mergesort", ignore_index=True)
    normalized = normalized.drop_duplicates(subset=["ticker"], keep="first").reset_index(drop=True)
    return normalized


def _load_fill_collection_batch_market_frame(market_file: Path) -> pd.DataFrame:
    resolved = Path(market_file).resolve()
    frame = pd.read_csv(resolved, encoding="utf-8-sig")
    return _normalize_fill_collection_batch_market_frame(frame)


def _normalize_broker_positions_frame(positions_frame: pd.DataFrame) -> pd.DataFrame:
    if positions_frame.empty:
        return pd.DataFrame(columns=["ticker", "shares"])
    work = positions_frame.copy()
    columns = {str(column).strip().lower(): column for column in work.columns}
    ticker_col = next((columns[key] for key in ("ticker", "symbol") if key in columns), None)
    shares_col = next((columns[key] for key in ("shares", "quantity", "qty") if key in columns), None)
    if ticker_col is None or shares_col is None:
        raise ValueError("broker_positions_file must include ticker/symbol and shares/quantity columns.")
    normalized = pd.DataFrame(
        {
            "ticker": work[ticker_col].astype(str).str.strip().str.upper(),
            "shares": pd.to_numeric(work[shares_col], errors="coerce").fillna(0.0),
        }
    )
    normalized = normalized.loc[normalized["ticker"].astype(bool)].copy()
    normalized = (
        normalized.groupby("ticker", as_index=False)["shares"].sum()
        .sort_values(by=["ticker"], kind="mergesort", ignore_index=True)
    )
    return normalized


def _load_broker_positions_frame(positions_file: Path) -> pd.DataFrame:
    resolved = Path(positions_file).resolve()
    frame = pd.read_csv(resolved, encoding="utf-8-sig")
    return _normalize_broker_positions_frame(frame)


def _side_scope_values(side_scope: str) -> list[str]:
    normalized = str(side_scope).strip().lower()
    if normalized == "buy-only":
        return ["buy"]
    if normalized == "sell-only":
        return ["sell"]
    if normalized == "both":
        return ["buy", "sell"]
    raise ValueError(f"Unsupported side_scope: {side_scope!r}")


def _generate_fill_collection_batch_plan(
    *,
    market_frame: pd.DataFrame,
    participation_buckets: list[float],
    orders_per_bucket: int,
    side_scope: str,
    sample_id: str,
    broker_positions_frame: pd.DataFrame | None = None,
    buying_power: float | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if int(orders_per_bucket) <= 0:
        raise ValueError("orders_per_bucket must be >= 1.")

    normalized_market = _normalize_fill_collection_batch_market_frame(market_frame)
    normalized_positions = _normalize_broker_positions_frame(broker_positions_frame) if broker_positions_frame is not None else pd.DataFrame(columns=["ticker", "shares"])
    sides = _side_scope_values(side_scope)
    skipped_untradable_count = int((~normalized_market["tradable"].astype(bool)).sum()) if not normalized_market.empty else 0
    tradable_market = normalized_market.loc[
        normalized_market["tradable"].astype(bool)
        & pd.to_numeric(normalized_market["estimated_price"], errors="coerce").fillna(0.0).gt(0.0)
        & pd.to_numeric(normalized_market["adv_shares"], errors="coerce").fillna(0.0).gt(0.0)
    ].copy()
    positions_map = {
        str(row["ticker"]).strip().upper(): float(row["shares"])
        for row in normalized_positions.to_dict(orient="records")
        if float(row.get("shares", 0.0) or 0.0) > 0.0
    }
    sorted_buckets = sorted(float(value) for value in participation_buckets)
    buying_power_budget = None if buying_power is None else max(0.0, float(buying_power))
    buy_budget_remaining = buying_power_budget
    budget_exhausted = False

    selected_rows: list[dict[str, Any]] = []
    insufficient_targets: list[dict[str, Any]] = []
    by_bucket_side: dict[str, dict[str, dict[str, float | int]]] = {}
    used_tickers_by_side: dict[str, set[str]] = {side: set() for side in sides}

    for target_participation in sorted_buckets:
        bucket_label = _format_participation_bucket_label(float(target_participation))
        candidate_rows: list[dict[str, Any]] = []
        for row in tradable_market.to_dict(orient="records"):
            ticker = str(row["ticker"]).strip().upper()
            adv_shares = float(row["adv_shares"])
            estimated_price = float(row["estimated_price"])
            quantity = int(math.floor(adv_shares * float(target_participation)))
            if quantity < 1:
                continue
            actual_participation = float(quantity / adv_shares)
            estimated_notional = float(quantity * estimated_price)
            candidate_rows.append(
                {
                    "ticker": ticker,
                    "estimated_price": estimated_price,
                    "reference_price": estimated_price,
                    "adv_shares": adv_shares,
                    "quantity": int(quantity),
                    "estimated_notional": estimated_notional,
                    "target_participation": float(target_participation),
                    "target_participation_bucket": bucket_label,
                    "actual_participation": actual_participation,
                    "participation_gap": abs(float(target_participation) - actual_participation),
                    "selection_reason": "lowest_estimated_notional_for_target_bucket",
                }
            )
        candidates = pd.DataFrame(candidate_rows)
        if not candidates.empty:
            candidates = candidates.sort_values(
                by=["estimated_notional", "participation_gap", "ticker"],
                kind="mergesort",
                ignore_index=True,
            )
        for side in sides:
            side_candidates = candidates.copy()
            if side == "sell" and broker_positions_frame is not None:
                if side_candidates.empty:
                    side_candidates = candidates.copy()
                if not side_candidates.empty:
                    side_candidates["available_shares"] = side_candidates["ticker"].map(positions_map).fillna(0.0)
                    side_candidates = side_candidates.loc[side_candidates["available_shares"] > 0.0].copy()
                    if not side_candidates.empty:
                        side_candidates["quantity"] = side_candidates.apply(
                            lambda row: int(min(int(row["quantity"]), math.floor(float(row["available_shares"])))),
                            axis=1,
                        )
                        side_candidates = side_candidates.loc[side_candidates["quantity"] > 0].copy()
                        side_candidates["actual_participation"] = side_candidates["quantity"] / side_candidates["adv_shares"]
                        side_candidates["estimated_notional"] = side_candidates["quantity"] * side_candidates["estimated_price"]
                        side_candidates["participation_gap"] = (side_candidates["target_participation"] - side_candidates["actual_participation"]).abs()
                        side_candidates = side_candidates.sort_values(
                            by=["participation_gap", "estimated_notional", "ticker"],
                            kind="mergesort",
                            ignore_index=True,
                        )
            if side == "buy" and buying_power_budget is not None and budget_exhausted:
                insufficient_targets.append(
                    {
                        "target_participation_bucket": bucket_label,
                        "target_participation": float(target_participation),
                        "direction": side,
                        "requested_order_count": int(orders_per_bucket),
                        "generated_order_count": 0,
                        "reason": "buying_power_budget_exhausted",
                    }
                )
                by_bucket_side.setdefault(bucket_label, {})[side] = {
                    "generated_order_count": 0,
                    "estimated_notional": 0.0,
                }
                continue
            if candidates.empty:
                bucket_rows = side_candidates.copy()
            else:
                unused_candidates = side_candidates.loc[~side_candidates["ticker"].astype(str).isin(used_tickers_by_side.get(side, set()))].copy()
                if len(unused_candidates) >= int(orders_per_bucket):
                    bucket_rows = unused_candidates.head(int(orders_per_bucket)).copy()
                else:
                    reused_candidates = side_candidates.loc[side_candidates["ticker"].astype(str).isin(used_tickers_by_side.get(side, set()))].copy()
                    bucket_rows = pd.concat([unused_candidates, reused_candidates], ignore_index=True).head(int(orders_per_bucket)).copy()
            if side == "buy" and buying_power_budget is not None and not bucket_rows.empty:
                affordable_rows: list[dict[str, Any]] = []
                for row in bucket_rows.to_dict(orient="records"):
                    estimated_notional = float(row.get("estimated_notional", 0.0) or 0.0)
                    if buy_budget_remaining is not None and estimated_notional <= buy_budget_remaining + 1e-9:
                        affordable_rows.append(row)
                        buy_budget_remaining = max(0.0, float(buy_budget_remaining - estimated_notional))
                    else:
                        budget_exhausted = True
                        break
                bucket_rows = pd.DataFrame(affordable_rows) if affordable_rows else pd.DataFrame(columns=bucket_rows.columns)
            if len(bucket_rows) < int(orders_per_bucket):
                reason = "insufficient_tradable_candidates_for_bucket"
                if side == "sell" and broker_positions_frame is not None:
                    reason = "insufficient_broker_position_for_sell_bucket"
                if side == "buy" and buying_power_budget is not None and budget_exhausted:
                    reason = "buying_power_budget_exhausted"
                insufficient_targets.append(
                    {
                        "target_participation_bucket": bucket_label,
                        "target_participation": float(target_participation),
                        "direction": side,
                        "requested_order_count": int(orders_per_bucket),
                        "generated_order_count": int(len(bucket_rows)),
                        "reason": reason,
                    }
                )
            if bucket_rows.empty:
                by_bucket_side.setdefault(bucket_label, {})[side] = {
                    "generated_order_count": 0,
                    "estimated_notional": 0.0,
                }
                continue
            bucket_rows["direction"] = side
            bucket_rows["sample_id"] = str(sample_id).strip() or "fill_collection_batch"
            bucket_rows["price_limit"] = ""
            bucket_rows["extended_hours"] = False
            bucket_rows["selection_rank"] = range(1, len(bucket_rows) + 1)
            selected_rows.extend(bucket_rows.to_dict(orient="records"))
            used_tickers_by_side.setdefault(side, set()).update(bucket_rows["ticker"].astype(str).tolist())
            by_bucket_side.setdefault(bucket_label, {})[side] = {
                "generated_order_count": int(len(bucket_rows)),
                "estimated_notional": float(pd.to_numeric(bucket_rows["estimated_notional"], errors="coerce").fillna(0.0).sum()),
            }

    batch_frame = pd.DataFrame(selected_rows)
    if batch_frame.empty:
        batch_frame = pd.DataFrame(
            columns=[
                "sample_id",
                "ticker",
                "direction",
                "quantity",
                "reference_price",
                "estimated_price",
                "price_limit",
                "extended_hours",
                "adv_shares",
                "estimated_notional",
                "target_participation",
                "target_participation_bucket",
                "actual_participation",
                "participation_gap",
                "selection_rank",
                "selection_reason",
            ]
        )
    else:
        batch_frame["quantity"] = pd.to_numeric(batch_frame["quantity"], errors="coerce").fillna(0).astype(int)
        batch_frame["extended_hours"] = batch_frame["extended_hours"].astype(bool)
        batch_frame = batch_frame[
            [
                "sample_id",
                "ticker",
                "direction",
                "quantity",
                "reference_price",
                "estimated_price",
                "price_limit",
                "extended_hours",
                "adv_shares",
                "estimated_notional",
                "target_participation",
                "target_participation_bucket",
                "actual_participation",
                "participation_gap",
                "selection_rank",
                "selection_reason",
            ]
        ].sort_values(
            by=["target_participation", "direction", "selection_rank", "ticker"],
            kind="mergesort",
            ignore_index=True,
        )

    orders_frame = batch_frame[
        [
            "sample_id",
            "ticker",
            "direction",
            "quantity",
            "reference_price",
            "estimated_price",
            "price_limit",
            "extended_hours",
            "target_participation_bucket",
            "actual_participation",
        ]
    ].copy()

    generated_by_side = {
        side: int((batch_frame["direction"].astype(str).str.lower() == side).sum()) if not batch_frame.empty else 0
        for side in sides
    }
    summary = {
        "sample_id": str(sample_id).strip() or "fill_collection_batch",
        "side_scope": str(side_scope).strip().lower(),
        "orders_per_bucket": int(orders_per_bucket),
        "participation_buckets": [
            {
                "value": float(value),
                "label": _format_participation_bucket_label(float(value)),
            }
            for value in participation_buckets
        ],
        "generated_order_count": int(len(batch_frame)),
        "generated_by_side": generated_by_side,
        "skipped_untradable_count": int(skipped_untradable_count),
        "broker_positions_source": "provided" if broker_positions_frame is not None else "none",
        "broker_positions_ticker_count": int(len(positions_map)),
        "buying_power_budget": buying_power_budget,
        "buy_budget_remaining": buy_budget_remaining,
        "budget_exhausted": bool(budget_exhausted),
        "insufficient_target_count": int(len(insufficient_targets)),
        "insufficient_targets": insufficient_targets,
        "bucket_side_summary": by_bucket_side,
        "total_estimated_notional": float(pd.to_numeric(batch_frame.get("estimated_notional"), errors="coerce").fillna(0.0).sum()) if not batch_frame.empty else 0.0,
    }
    return batch_frame, orders_frame, summary


def _build_fill_collection_batch_report(
    *,
    manifest: dict[str, Any],
    batch_frame: pd.DataFrame,
) -> str:
    lines = [
        "# Fill Collection Batch Plan",
        "",
        f"- generated_at: {manifest.get('generated_at', '')}",
        f"- market_file: {manifest.get('market_file', '')}",
        f"- sample_id: {manifest.get('sample_id', '')}",
        f"- side_scope: {manifest.get('side_scope', '')}",
        f"- orders_per_bucket: {manifest.get('orders_per_bucket', 0)}",
        f"- generated_order_count: {manifest.get('generated_order_count', 0)}",
        f"- total_estimated_notional: {manifest.get('total_estimated_notional', 0.0):.2f}",
        f"- skipped_untradable_count: {manifest.get('skipped_untradable_count', 0)}",
        f"- broker_positions_source: {manifest.get('broker_positions_source', '')}",
        f"- broker_positions_ticker_count: {manifest.get('broker_positions_ticker_count', 0)}",
        f"- buying_power_budget: {manifest.get('buying_power_budget', None)}",
        f"- buy_budget_remaining: {manifest.get('buy_budget_remaining', None)}",
        f"- budget_exhausted: {manifest.get('budget_exhausted', False)}",
        f"- insufficient_target_count: {manifest.get('insufficient_target_count', 0)}",
        "",
        "## Bucket Summary",
    ]
    for bucket in manifest.get("participation_buckets", []):
        label = str(bucket.get("label", ""))
        side_map = dict((manifest.get("bucket_side_summary") or {}).get(label, {}))
        for side in _side_scope_values(str(manifest.get("side_scope", "both"))):
            side_summary = dict(side_map.get(side, {}))
            lines.append(
                f"- {label} {side}: generated_order_count={int(side_summary.get('generated_order_count', 0))}, "
                f"estimated_notional={float(side_summary.get('estimated_notional', 0.0)):.2f}"
            )
    if manifest.get("insufficient_targets"):
        lines.extend(["", "## Insufficient Targets"])
        for item in manifest.get("insufficient_targets", []):
            lines.append(
                f"- {item.get('target_participation_bucket', '')} {item.get('direction', '')}: "
                f"generated={item.get('generated_order_count', 0)} / requested={item.get('requested_order_count', 0)} "
                f"({item.get('reason', '')})"
            )
    if not batch_frame.empty:
        lines.extend(["", "## Orders"])
        for row in batch_frame.to_dict(orient="records"):
            lines.append(
                f"- {row.get('target_participation_bucket', '')} {row.get('direction', '')} {row.get('ticker', '')}: "
                f"qty={int(row.get('quantity', 0) or 0)}, actual_participation={float(row.get('actual_participation', 0.0) or 0.0):.8f}, "
                f"estimated_notional={float(row.get('estimated_notional', 0.0) or 0.0):.2f}"
            )
    return "\n".join(lines) + "\n"


def _write_fill_collection_batch_artifacts(
    *,
    output_dir: Path,
    market_file: Path,
    batch_frame: pd.DataFrame,
    orders_frame: pd.DataFrame,
    summary: dict[str, Any],
    notes: str = "",
) -> dict[str, Path]:
    run_id = fill_collection.generate_run_id(source_type="fill_collection_batch")
    run_dir = Path(output_dir).resolve() / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    batch_path = run_dir / "fill_collection_batch.csv"
    orders_path = run_dir / "orders_oms.csv"
    manifest_path = run_dir / "fill_collection_batch_manifest.json"
    report_path = run_dir / "fill_collection_batch_report.md"

    manifest = {
        **dict(summary),
        "generated_at": datetime.now().astimezone().isoformat(),
        "market_file": str(Path(market_file).resolve()),
        "notes": str(notes),
    }
    batch_frame.to_csv(batch_path, index=False, encoding="utf-8")
    orders_frame.to_csv(orders_path, index=False, encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    report_path.write_text(_build_fill_collection_batch_report(manifest=manifest, batch_frame=batch_frame), encoding="utf-8")
    return {
        "run_dir": run_dir,
        "fill_collection_batch_csv": batch_path,
        "orders_oms_csv": orders_path,
        "fill_collection_batch_manifest_json": manifest_path,
        "fill_collection_batch_report_md": report_path,
    }


def _build_minimal_buy_validation_payload(
    *,
    run_dir: Path,
    account_before: dict[str, Any],
    account_after: dict[str, Any],
    positions_before: pd.DataFrame,
    positions_after: pd.DataFrame,
    open_orders_before: pd.DataFrame,
    open_orders_after: pd.DataFrame,
    manifest: dict[str, Any],
    orders_frame: pd.DataFrame,
    events_frame: pd.DataFrame,
    selection_audit: dict[str, Any],
    validation_plan_path: Path | None,
    slippage_prep_checklist_path: Path | None,
) -> dict[str, Any]:
    order_row = orders_frame.iloc[0].to_dict() if not orders_frame.empty else {}
    selected_ticker = str(order_row.get("ticker", "")).strip().upper()
    selected_status = str(order_row.get("status", "")).strip().lower()
    selected_qty = _safe_float(order_row.get("requested_qty"), 0.0)
    filled_qty = _safe_float(order_row.get("filled_qty"), 0.0)
    avg_fill_price = _safe_float(order_row.get("avg_fill_price"), 0.0)
    latency_seconds = _safe_float(order_row.get("latency_seconds"), 0.0)
    order_timeline = (
        events_frame.loc[events_frame["ticker"].astype(str).str.upper() == selected_ticker].to_dict(orient="records")
        if not events_frame.empty and "ticker" in events_frame.columns
        else []
    )
    buying_power_before = _safe_float(account_before.get("buying_power"), 0.0)
    buying_power_after = _safe_float(account_after.get("buying_power"), 0.0)
    cash_before = _safe_float(account_before.get("cash"), 0.0)
    cash_after = _safe_float(account_after.get("cash"), 0.0)
    final_status = selected_status or str(manifest.get("status_mix", {})).lower()
    if filled_qty > 0 and selected_status == "filled":
        recommendation = "minimal_buy_validation_successful"
    elif manifest.get("submitted_count", 0) > 0:
        recommendation = "minimal_buy_validation_submitted_but_not_filled"
    else:
        recommendation = "minimal_buy_validation_blocked_by_buying_power"
    payload = {
        "generated_at_utc": datetime.now().astimezone().isoformat(),
        "run_dir": str(run_dir.resolve()),
        "campaign_preset": "minimal-buy-validation",
        "side_scope": "buy-only",
        "order_type": "market",
        "time_in_force": "day",
        "quantity": 1,
        "selected_ticker": selected_ticker,
        "selected_reason": selection_audit.get("selected_reason", ""),
        "estimated_price": _safe_float(order_row.get("estimated_price"), 0.0),
        "estimated_price_source": selection_audit.get("price_source", ""),
        "selected_candidate_audit": selection_audit.get("candidate_audit", []),
        "validation_plan_file": str(validation_plan_path.resolve()) if validation_plan_path else "",
        "slippage_calibration_prep_checklist": str(slippage_prep_checklist_path.resolve()) if slippage_prep_checklist_path else "",
        "account_snapshot_before": account_before,
        "account_snapshot_after": account_after,
        "positions_before": positions_before.to_dict(orient="records"),
        "positions_after": positions_after.to_dict(orient="records"),
        "open_orders_before": open_orders_before.to_dict(orient="records"),
        "open_orders_after": open_orders_after.to_dict(orient="records"),
        "submitted_order_count": int(manifest.get("submitted_count", 0) or 0),
        "filled_order_count": int(manifest.get("filled_count", 0) or 0),
        "filled_qty": float(filled_qty),
        "avg_fill_price": float(avg_fill_price) if avg_fill_price > 0 else None,
        "latency_seconds": float(latency_seconds) if latency_seconds > 0 else None,
        "final_status": final_status,
        "buying_power_before": float(buying_power_before),
        "buying_power_after": float(buying_power_after),
        "buying_power_delta": float(buying_power_after - buying_power_before),
        "cash_before": float(cash_before),
        "cash_after": float(cash_after),
        "cash_delta": float(cash_after - cash_before),
        "suitable_for_slippage_calibration": bool(int(manifest.get("filled_count", 0) or 0) > 0),
        "recommendation": recommendation,
        "blocked_by_reason": manifest.get("broker_precheck_summary", {}).get("reason_counts", {}),
        "event_timeline": order_timeline,
        "manifest": manifest,
        "order_row": order_row,
    }
    return payload


def _render_minimal_buy_validation_report(payload: dict[str, Any]) -> str:
    account_before = payload.get("account_snapshot_before") or {}
    account_after = payload.get("account_snapshot_after") or {}
    lines = [
        "# Minimal Buy Validation",
        "",
        "## Summary",
        "",
        f"- generated_at_utc: {payload.get('generated_at_utc', '')}",
        f"- run_dir: {payload.get('run_dir', '')}",
        f"- campaign_preset: {payload.get('campaign_preset', '')}",
        f"- side_scope: {payload.get('side_scope', '')}",
        f"- order_type: {payload.get('order_type', '')}",
        f"- time_in_force: {payload.get('time_in_force', '')}",
        f"- quantity: {payload.get('quantity', '')}",
        f"- selected_ticker: {payload.get('selected_ticker', '')}",
        f"- selected_reason: {payload.get('selected_reason', '')}",
        f"- estimated_price: {payload.get('estimated_price', None)}",
        f"- estimated_price_source: {payload.get('estimated_price_source', '')}",
        f"- validation_plan_file: {payload.get('validation_plan_file', '')}",
        f"- slippage_calibration_prep_checklist: {payload.get('slippage_calibration_prep_checklist', '')}",
        f"- submitted_order_count: {payload.get('submitted_order_count', 0)}",
        f"- filled_order_count: {payload.get('filled_order_count', 0)}",
        f"- filled_qty: {payload.get('filled_qty', 0.0)}",
        f"- avg_fill_price: {payload.get('avg_fill_price', None)}",
        f"- latency_seconds: {payload.get('latency_seconds', None)}",
        f"- final_status: {payload.get('final_status', '')}",
        f"- recommendation: {payload.get('recommendation', '')}",
        f"- suitable_for_slippage_calibration: {str(bool(payload.get('suitable_for_slippage_calibration', False))).lower()}",
        "",
        "## Before",
        "",
        f"- account_status: {account_before.get('status', '')}",
        f"- cash: {account_before.get('cash', None)}",
        f"- buying_power: {account_before.get('buying_power', None)}",
        f"- positions_count: {len(payload.get('positions_before') or [])}",
        f"- open_orders_count: {len(payload.get('open_orders_before') or [])}",
        "",
        "## After",
        "",
        f"- account_status: {account_after.get('status', '')}",
        f"- cash: {account_after.get('cash', None)}",
        f"- buying_power: {account_after.get('buying_power', None)}",
        f"- positions_count: {len(payload.get('positions_after') or [])}",
        f"- open_orders_count: {len(payload.get('open_orders_after') or [])}",
        "",
        "## Event Timeline",
        "",
    ]
    event_timeline = payload.get("event_timeline") or []
    if not event_timeline:
        lines.append("- no_events_recorded")
    else:
        for event in event_timeline:
            lines.append(
                "- {event_at_utc} | {status} | filled_qty={filled_qty} | filled_avg_price={filled_avg_price} | reject_reason={reject_reason}".format(
                    event_at_utc=event.get("event_at_utc", ""),
                    status=event.get("status", ""),
                    filled_qty=event.get("filled_qty", ""),
                    filled_avg_price=event.get("filled_avg_price", ""),
                    reject_reason=event.get("reject_reason", ""),
                )
            )
    lines.extend(
        [
            "",
            "## Blocked By Reason",
            "",
            f"- {payload.get('blocked_by_reason', {})}",
        ]
    )
    return "\n".join(lines)


def _write_minimal_buy_validation_artifacts(*, run_dir: Path, payload: dict[str, Any]) -> dict[str, Path]:
    run_dir = run_dir.resolve()
    report_json = run_dir / "minimal_buy_validation_report.json"
    report_md = run_dir / "minimal_buy_validation_report.md"
    plan_yaml = run_dir / "minimal_buy_validation_plan.yaml"
    plan_json = run_dir / "minimal_buy_validation_plan.json"
    open_orders_before_csv = run_dir / "broker_open_orders_before.csv"
    open_orders_after_csv = run_dir / "broker_open_orders_after.csv"
    open_orders_before_json = run_dir / "broker_open_orders_before.json"
    open_orders_after_json = run_dir / "broker_open_orders_after.json"
    plan_payload = {
        "generated_at_utc": payload.get("generated_at_utc"),
        "campaign_preset": payload.get("campaign_preset"),
        "side_scope": payload.get("side_scope"),
        "order_type": payload.get("order_type"),
        "time_in_force": payload.get("time_in_force"),
        "quantity": payload.get("quantity"),
        "selected_ticker": payload.get("selected_ticker"),
        "estimated_price": payload.get("estimated_price"),
        "validation_plan_file": payload.get("validation_plan_file"),
        "slippage_calibration_prep_checklist": payload.get("slippage_calibration_prep_checklist"),
        "recommended_next_action": "Use this run as the first slippage calibration input if filled_order_count > 0.",
    }
    report_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    report_md.write_text(_render_minimal_buy_validation_report(payload), encoding="utf-8")
    plan_yaml.write_text(yaml.safe_dump(plan_payload, sort_keys=False, allow_unicode=False), encoding="utf-8")
    plan_json.write_text(json.dumps(plan_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(list(payload.get("open_orders_before") or [])).to_csv(open_orders_before_csv, index=False, encoding="utf-8")
    pd.DataFrame(list(payload.get("open_orders_after") or [])).to_csv(open_orders_after_csv, index=False, encoding="utf-8")
    open_orders_before_json.write_text(
        json.dumps(list(payload.get("open_orders_before") or []), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    open_orders_after_json.write_text(
        json.dumps(list(payload.get("open_orders_after") or []), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return {
        "minimal_buy_validation_report_json": report_json,
        "minimal_buy_validation_report_md": report_md,
        "minimal_buy_validation_plan_yaml": plan_yaml,
        "minimal_buy_validation_plan_json": plan_json,
        "broker_open_orders_before_csv": open_orders_before_csv,
        "broker_open_orders_after_csv": open_orders_after_csv,
        "broker_open_orders_before_json": open_orders_before_json,
        "broker_open_orders_after_json": open_orders_after_json,
    }


def _render_open_orders_table(open_orders: list[dict[str, Any]]) -> str:
    lines = [
        "| order_id | ticker | direction | order_type | tif | status | quantity | filled_qty | age_minutes | locks_bp | locks_inv | suggested_action |",
        "| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | --- | --- | --- |",
    ]
    if not open_orders:
        lines.append("|  | none |  |  |  |  | 0 | 0 | 0 | false | false |  |")
        return "\n".join(lines)
    for row in open_orders:
        lines.append(
            "| {order_id} | {ticker} | {direction} | {order_type} | {time_in_force} | {status} | {quantity:.0f} | {filled_qty:.0f} | {age_minutes} | {locks_buying_power} | {locks_inventory} | {suggested_action} |".format(
                order_id=str(row.get("order_id", "")),
                ticker=str(row.get("ticker", "")),
                direction=str(row.get("direction", "")),
                order_type=str(row.get("order_type", "")),
                time_in_force=str(row.get("time_in_force", "")),
                status=str(row.get("status", "")),
                quantity=_safe_float(row.get("quantity"), 0.0),
                filled_qty=_safe_float(row.get("filled_qty"), 0.0),
                age_minutes=("N/A" if row.get("age_minutes") is None else f"{float(row.get('age_minutes', 0.0)):.1f}"),
                locks_buying_power=str(bool(row.get("locks_buying_power", False))).lower(),
                locks_inventory=str(bool(row.get("locks_inventory", False))).lower(),
                suggested_action=str(row.get("suggested_action", "")),
            )
        )
    return "\n".join(lines)


def _build_off_hours_prep_payload(
    snapshot: dict[str, Any],
    *,
    notes: str,
    bucket_plan_file: Path,
    max_seed_notional: float,
    max_seed_orders: int,
    output_dir: Path,
) -> dict[str, Any]:
    broker_state_payload = _build_broker_state_inspection_payload(snapshot, notes=notes)
    market_is_open = fill_collection.is_us_market_open()
    tomorrow_plan = _build_tomorrow_minimal_validation_plan(
        bucket_plan_file=bucket_plan_file,
        output_dir=output_dir,
        max_seed_notional=max_seed_notional,
        max_seed_orders=max_seed_orders,
        market_is_open=market_is_open,
        notes=notes,
    )
    slippage_prep_paths = slippage_calibration.prepare_slippage_calibration_prep()
    offline_checks = {
        "pytest_targets": [
            "tests/test_pilot_ops_script.py",
            "tests/test_fill_collection_campaign.py",
            "tests/test_fill_collection.py",
            "tests/test_slippage_calibration.py",
            "tests/test_risk_ab_comparison.py",
            "tests/test_risk_regime_diagnostic.py",
        ],
        "historical_replay_targets": [
            "scripts/pilot_historical_replay.py",
            "tests/test_risk_ab_comparison.py",
            "tests/test_risk_regime_diagnostic.py",
        ],
        "status": "pending",
        "notes": "Run offline validation and historical replay after prep if needed.",
    }
    return {
        "generated_at_utc": datetime.now().astimezone().isoformat(),
        "prep_mode": "read_only_off_hours_prep",
        "notes": notes,
        "broker": "alpaca",
        "market": "us",
        "broker_state_inspection": broker_state_payload,
        "account_snapshot": broker_state_payload["account_snapshot"],
        "broker_state_snapshot": broker_state_payload["broker_state_snapshot"],
        "positions": broker_state_payload["positions"],
        "positions_summary": broker_state_payload["positions_summary"],
        "open_orders_summary": broker_state_payload["open_orders_summary"],
        "open_orders": broker_state_payload["open_orders"],
        "recommended_cancellations": broker_state_payload["recommended_cancellations"],
        "preopen_action": broker_state_payload["preopen_action"],
        "preopen_action_reason": broker_state_payload["preopen_action_reason"],
        "recommended_next_action": broker_state_payload["recommended_next_action"],
        "recommendation_reason": broker_state_payload["recommendation_reason"],
        "tomorrow_minimal_validation_plan": tomorrow_plan,
        "slippage_calibration_prep": {key: str(value) for key, value in slippage_prep_paths.items()},
        "offline_checks": offline_checks,
        "next_recommended_action": "Review the open orders above, then run the minimal buy validation during regular market hours.",
    }


def _render_off_hours_prep_report(payload: dict[str, Any]) -> str:
    summary = payload.get("broker_state_snapshot") or {}
    open_orders = payload.get("open_orders") or []
    open_orders_summary = payload.get("open_orders_summary") or {}
    tomorrow_plan = payload.get("tomorrow_minimal_validation_plan") or {}
    slippage_prep = payload.get("slippage_calibration_prep") or {}
    offline_checks = payload.get("offline_checks") or {}
    lines = [
        "# Off-Hours Prep Report",
        "",
        "## Summary",
        "",
        f"- generated_at_utc: {payload.get('generated_at_utc', '')}",
        f"- prep_mode: {payload.get('prep_mode', '')}",
        f"- broker: {payload.get('broker', '')}",
        f"- market: {payload.get('market', '')}",
        f"- recommended_next_action: {payload.get('recommended_next_action', '')}",
        f"- recommendation_reason: {payload.get('recommendation_reason', '')}",
        f"- preopen_action: {payload.get('preopen_action', '')}",
        f"- preopen_action_reason: {payload.get('preopen_action_reason', '')}",
        "",
        "## Account Snapshot",
        "",
        f"- buying_power: {summary.get('buying_power', None)}",
        f"- cash: {summary.get('cash', None)}",
        f"- equity: {summary.get('equity', None)}",
        f"- positions_count: {summary.get('positions_count', 0)}",
        f"- open_orders_count: {open_orders_summary.get('open_orders_count', len(open_orders))}",
        f"- open_order_tickers: {summary.get('open_order_tickers', [])}",
        "",
        "## Open Orders Audit",
        "",
        f"- locked_buying_power_count: {open_orders_summary.get('locked_buying_power_count', 0)}",
        f"- locked_inventory_count: {open_orders_summary.get('locked_inventory_count', 0)}",
        f"- stale_open_order_count: {open_orders_summary.get('stale_open_order_count', 0)}",
        f"- long_lived_open_order_count: {open_orders_summary.get('long_lived_open_order_count', 0)}",
        "",
        _render_open_orders_table(open_orders),
        "",
        "## Recommended Cancellations",
        "",
    ]
    recommended_cancellations = payload.get("recommended_cancellations") or []
    if recommended_cancellations:
        for row in recommended_cancellations:
            lines.append(
                f"- {row.get('order_id', '')} {row.get('ticker', '')} {row.get('direction', '')}: {row.get('reason', '')} "
                f"(impact={row.get('impact', '')}, age_minutes={row.get('age_minutes', None)})"
            )
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Tomorrow Minimal Validation Plan",
            "",
            f"- campaign_preset: {tomorrow_plan.get('campaign_preset', '')}",
            f"- side_scope: {tomorrow_plan.get('side_scope', '')}",
        f"- max_seed_notional: {tomorrow_plan.get('max_seed_notional', None)}",
        f"- max_seed_orders: {tomorrow_plan.get('max_seed_orders', None)}",
        f"- execution_window: {tomorrow_plan.get('execution_window', '')}",
        f"- bucket_plan_file: {tomorrow_plan.get('bucket_plan_file', '')}",
        f"- plan_file: {tomorrow_plan.get('plan_file', '')}",
        f"- recommended_command: {' '.join(str(part) for part in tomorrow_plan.get('recommended_command', []))}",
        f"- recommended_next_action: {tomorrow_plan.get('recommended_next_action', '')}",
            "",
            "## Slippage Calibration Prep",
            "",
            f"- prep_root: {slippage_prep.get('prep_root', '')}",
            f"- checklist: {slippage_prep.get('slippage_calibration_prep_checklist', '')}",
            f"- manifest: {slippage_prep.get('slippage_calibration_prep_manifest', '')}",
            f"- fill_collection_dir: {slippage_prep.get('fill_collection_dir', '')}",
            f"- dataset_dir: {slippage_prep.get('dataset_dir', '')}",
            f"- residuals_dir: {slippage_prep.get('residuals_dir', '')}",
            f"- candidate_overlay_dir: {slippage_prep.get('candidate_overlay_dir', '')}",
            f"- diagnostics_dir: {slippage_prep.get('diagnostics_dir', '')}",
            "",
            "## Offline Validation",
            "",
            f"- status: {offline_checks.get('status', 'pending')}",
        ]
    )
    for target in offline_checks.get("pytest_targets", []):
        lines.append(f"- pytest_target: {target}")
    for target in offline_checks.get("historical_replay_targets", []):
        lines.append(f"- historical_replay_target: {target}")
    lines.extend(["", "## Next Step", "", f"- {payload.get('next_recommended_action', '')}"])
    return "\n".join(lines) + "\n"


def _write_off_hours_prep_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> Path:
    run_id = fill_collection.generate_run_id(source_type="off_hours_prep")
    run_dir = output_dir.resolve() / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "broker_state_report.json").write_text(
        json.dumps(
            {
                "generated_at_utc": payload.get("generated_at_utc"),
                "broker": payload.get("broker"),
                "market": payload.get("market"),
                "account_snapshot": payload.get("account_snapshot"),
                "broker_state_snapshot": payload.get("broker_state_snapshot"),
                "positions": payload.get("positions"),
                "positions_summary": payload.get("positions_summary"),
                "open_orders_summary": payload.get("open_orders_summary"),
                "open_orders": payload.get("open_orders"),
                "recommended_cancellations": payload.get("recommended_cancellations"),
                "recommended_next_action": payload.get("recommended_next_action"),
                "preopen_action": payload.get("preopen_action"),
                "preopen_action_reason": payload.get("preopen_action_reason"),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (run_dir / "broker_state_report.md").write_text(
        _render_broker_state_report(
            payload.get("broker_state_inspection", {})
        ),
        encoding="utf-8",
    )
    (run_dir / "off_hours_prep_manifest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (run_dir / "off_hours_prep_report.md").write_text(_render_off_hours_prep_report(payload), encoding="utf-8")
    tomorrow_plan = payload.get("tomorrow_minimal_validation_plan") or {}
    (run_dir / "tomorrow_minimal_validation_plan.yaml").write_text(
        yaml.safe_dump(tomorrow_plan, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    (run_dir / "tomorrow_minimal_validation_plan.json").write_text(
        json.dumps(tomorrow_plan, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return run_dir


def _write_broker_state_transition_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> Path:
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "broker_state_report.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "broker_state_report.md").write_text(_render_broker_state_transition_report(payload), encoding="utf-8")
    return output_dir


def _maybe_int(raw: Any) -> int | None:
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return None


def _to_bool(raw: Any) -> bool | None:
    if raw is None:
        return None
    text = str(raw).strip().lower()
    if text in {"true", "1", "yes", "y", "pass", "passed"}:
        return True
    if text in {"false", "0", "no", "n", "fail", "failed"}:
        return False
    return None


def _parse_list_field(raw: Any) -> list[str]:
    if raw is None:
        return []
    text = str(raw).strip()
    if not text:
        return []
    try:
        value = ast.literal_eval(text)
    except (ValueError, SyntaxError):
        if "|" in text:
            return [part.strip() for part in text.split("|") if part.strip()]
        return [text]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def _copy_if_missing(src: Path, dst: Path) -> None:
    if dst.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")


def _ensure_dashboard_schema(path: Path) -> None:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=DASHBOARD_HEADERS)
            writer.writeheader()
        return

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        existing_headers = list(reader.fieldnames or [])
        existing_rows = list(reader)

    if existing_headers == DASHBOARD_HEADERS:
        return

    normalized_rows: list[dict[str, str]] = []
    for row in existing_rows:
        normalized: dict[str, str] = {}
        for header in DASHBOARD_HEADERS:
            normalized[header] = str(row.get(header, "") or "")
        normalized_rows.append(normalized)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=DASHBOARD_HEADERS)
        writer.writeheader()
        writer.writerows(normalized_rows)


def init_tracking(force: bool = False, output_dir: Path | None = None) -> None:
    dashboard_path, incident_path, go_nogo_path, weekly_dir = _resolve_tracking_paths(output_dir)
    tracking_dir = dashboard_path.parent
    tracking_dir.mkdir(parents=True, exist_ok=True)
    weekly_dir.mkdir(parents=True, exist_ok=True)

    dashboard_template = TEMPLATE_DIR / "pilot_dashboard_template.csv"
    incident_template = TEMPLATE_DIR / "incident_register_template.csv"
    go_nogo_template = TEMPLATE_DIR / "go_nogo_decision_template.md"
    weekly_template = TEMPLATE_DIR / "weekly_summary_template.md"

    if force:
        dashboard_path.unlink(missing_ok=True)
        incident_path.unlink(missing_ok=True)
        go_nogo_path.unlink(missing_ok=True)

    _copy_if_missing(dashboard_template, dashboard_path)
    _copy_if_missing(incident_template, incident_path)
    _copy_if_missing(go_nogo_template, go_nogo_path)
    _copy_if_missing(weekly_template, weekly_dir / "week_00_summary_template.md")
    _ensure_dashboard_schema(dashboard_path)

    print(f"tracking_dir: {tracking_dir}")
    print(f"dashboard: {dashboard_path}")
    print(f"incident_register: {incident_path}")
    print(f"go_nogo_template: {go_nogo_path}")


def _run_validation(
    mode: str,
    reviewer_input: Path | None,
    real_sample: bool,
    market: str = "cn",
    config_overlay: Path | None = None,
    as_of_date: str | None = None,
) -> tuple[int, Path]:
    command = [
        sys.executable,
        str(ROOT / "scripts" / "run_pilot_validation.py"),
        "--mode",
        mode,
        "--market",
        market,
    ]
    if reviewer_input is not None:
        command.extend(["--reviewer-input", str(reviewer_input)])
    if real_sample:
        command.append("--real-sample")
    if as_of_date is not None:
        command.extend(["--real-feed-as-of-date", as_of_date])
    if config_overlay is not None:
        command.extend(["--config-overlay", str(config_overlay)])

    completed = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)

    run_root = _extract_run_root(completed.stdout)
    if run_root is None:
        run_root = _latest_validation_run()
    if run_root is None:
        raise RuntimeError("Unable to locate pilot_validation output directory.")
    return completed.returncode, run_root


def _extract_run_root(stdout: str) -> Path | None:
    for line in stdout.splitlines():
        marker = "Validation completed. Root:"
        if marker in line:
            path_text = line.split(marker, 1)[1].strip()
            candidate = Path(path_text)
            if candidate.exists():
                return candidate
    return None


def _latest_validation_run() -> Path | None:
    candidates = sorted(OUTPUTS_DIR.glob("pilot_validation_*"))
    if not candidates:
        return None
    return candidates[-1]


def _load_sample_rows(run_root: Path) -> list[dict[str, str]]:
    sample_csv = run_root / "evaluation" / "sample_assessment.csv"
    if not sample_csv.exists():
        return []
    with sample_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _load_provider_report(run_root: Path) -> dict[str, Any]:
    report_path = run_root / "evaluation" / "provider_capability_report.json"
    if not report_path.exists():
        return {}
    return json.loads(report_path.read_text(encoding="utf-8"))


def _load_comparison_eligibility(run_root: Path) -> tuple[dict[str, Any] | None, str | None]:
    candidates = [
        run_root / "evaluation" / "comparison_eligibility.json",
        run_root / "comparison_eligibility.json",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None, ELIGIBILITY_REASON_INVALID_JSON
        if not isinstance(payload, dict):
            return None, ELIGIBILITY_REASON_PAYLOAD_NOT_MAPPING
        return payload, None
    return None, None


def _to_text_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _resolve_comparison_eligibility(
    payload: dict[str, Any] | None,
    *,
    load_error: str | None,
    ab_flow: bool,
    require_eligibility_gate: bool,
) -> tuple[str, list[str], list[str]]:
    if load_error is not None:
        return COMPARISON_ELIGIBILITY_INVALID, [load_error], []
    if payload is None:
        reasons = [ELIGIBILITY_REASON_MISSING_REQUIRED] if (ab_flow and require_eligibility_gate) else []
        return COMPARISON_ELIGIBILITY_NOT_AVAILABLE, reasons, []
    reasons = _to_text_list(payload.get("reasons"))
    baseline_quality_flags = _to_text_list(payload.get("baseline_quality_flags"))
    eligible_raw = payload.get("eligible")
    if isinstance(eligible_raw, bool):
        if eligible_raw:
            return COMPARISON_ELIGIBILITY_ELIGIBLE, reasons, baseline_quality_flags
        if not reasons:
            reasons = [ELIGIBILITY_REASON_FALSE_WITHOUT_REASON]
        return COMPARISON_ELIGIBILITY_INELIGIBLE, reasons, baseline_quality_flags
    reasons = reasons + [ELIGIBILITY_REASON_INVALID_ELIGIBLE_FIELD]
    return COMPARISON_ELIGIBILITY_INVALID, reasons, baseline_quality_flags


def _eligibility_gate_outcome(
    *,
    status: str,
    ab_flow: bool,
    require_eligibility_gate: bool,
) -> tuple[int | None, str | None]:
    if status == COMPARISON_ELIGIBILITY_INELIGIBLE:
        return EXIT_CODE_ELIGIBILITY_INELIGIBLE, ELIGIBILITY_LOG_INELIGIBLE
    if status == COMPARISON_ELIGIBILITY_INVALID:
        return EXIT_CODE_ELIGIBILITY_INVALID, ELIGIBILITY_LOG_INVALID
    if status == COMPARISON_ELIGIBILITY_NOT_AVAILABLE and ab_flow and require_eligibility_gate:
        return EXIT_CODE_ELIGIBILITY_MISSING, ELIGIBILITY_LOG_MISSING
    return None, None


def _row_success(row: dict[str, str], field: str) -> bool:
    return bool(_to_bool(row.get(field)))


def _artifact_chain_complete_for_row(row: dict[str, str]) -> bool:
    required_paths = [
        "benchmark_json_path",
        "execution_report_path",
        "scenario_comparison_path",
        "approval_record_path",
        "main_audit_path",
    ]
    if not (
        _row_success(row, "market_builder_success")
        and _row_success(row, "reference_builder_success")
        and _row_success(row, "main_flow_success")
        and _row_success(row, "scenario_success")
        and _row_success(row, "approval_success")
        and _row_success(row, "execution_success")
    ):
        return False
    return all(str(row.get(path_key, "")).strip() for path_key in required_paths)


def _derive_row(
    *,
    mode: str,
    phase: str,
    return_code: int,
    run_root: Path,
    rows: list[dict[str, str]],
    provider_report: dict[str, Any],
    rebalance_triggered: bool,
    incident_id: str,
    notes: str,
    as_of_date: str | None,
    ab_flow: bool,
    require_eligibility_gate: bool,
) -> dict[str, str]:
    sample_count = max(1, len(rows))
    override_count = sum(
        1
        for row in rows
        if str(row.get("override_used", "")).strip().lower() == "true"
    )
    cost_better_count = sum(1 for row in rows if _safe_float(row.get("cost_difference")) > 0.0)
    cost_better_ratio = cost_better_count / sample_count
    blocked_untradeable_count = sum(_safe_int(row.get("single_name_blocked_untradeable_count")) for row in rows)

    fallback_sources: set[str] = set()
    for row in rows:
        for source in _parse_list_field(row.get("data_source_mix_market")) + _parse_list_field(
            row.get("data_source_mix_reference")
        ):
            lowered = source.lower()
            if lowered and lowered not in {"mock", "tushare"}:
                fallback_sources.add(lowered)

    solver_counter: Counter[str] = Counter()
    for row in rows:
        solver = str(row.get("solver_used", "")).strip()
        if solver and solver.upper() != "N/A":
            solver_counter.update([solver.upper()])
    solver_primary = solver_counter.most_common(1)[0][0] if solver_counter else ""

    blockers = list(provider_report.get("provider_capability_blockers") or [])
    comparison_payload, comparison_load_error = _load_comparison_eligibility(run_root)
    comparison_status, comparison_reasons, _comparison_flags = _resolve_comparison_eligibility(
        comparison_payload,
        load_error=comparison_load_error,
        ab_flow=ab_flow,
        require_eligibility_gate=require_eligibility_gate,
    )
    token_available = bool(provider_report.get("token_available", False))
    market_ok = bool(provider_report.get("market_builder_success", False))
    reference_ok = bool(provider_report.get("reference_builder_success", False))
    if not token_available:
        primary_feed_success = "true"
    elif (not market_ok) and (not reference_ok) and not blockers:
        primary_feed_success = "true"
    else:
        primary_feed_success = "true" if (market_ok and reference_ok and not blockers) else "false"

    nightly_status = ""
    release_status = ""
    release_gate_passed = ""
    if mode == "nightly":
        nightly_status = "pass" if return_code == 0 else "fail"
    else:
        nightly_status = "pass"
        release_status = "passed" if return_code == 0 else "failed"
        release_gate_passed = "true" if return_code == 0 else "false"
    if comparison_status in {COMPARISON_ELIGIBILITY_INELIGIBLE, COMPARISON_ELIGIBILITY_INVALID}:
        nightly_status = "ineligible"
        release_status = "ineligible"
        release_gate_passed = "false"
    if comparison_status == COMPARISON_ELIGIBILITY_NOT_AVAILABLE and ab_flow and require_eligibility_gate:
        nightly_status = "ineligible"
        release_status = "ineligible"
        release_gate_passed = "false"

    static_rows = [item for item in rows if not str(item.get("sample_id", "")).startswith("real_sample_")]
    real_rows = [item for item in rows if str(item.get("sample_id", "")).startswith("real_sample_")]

    full_chain_success_static = sum(1 for item in static_rows if _artifact_chain_complete_for_row(item))
    full_chain_success_real = sum(1 for item in real_rows if _artifact_chain_complete_for_row(item))
    static_override_count = sum(
        1 for item in static_rows if str(item.get("override_used", "")).strip().lower() == "true"
    )
    static_gap_ok_count = sum(1 for item in static_rows if _safe_float(item.get("scenario_score_gap")) >= 0.01)
    static_cost_better_count = sum(1 for item in static_rows if _safe_float(item.get("cost_difference")) > 0.0)
    static_count = len(static_rows)
    static_cost_better_ratio = (static_cost_better_count / static_count) if static_count else 0.0
    static_solver_fallback_count = sum(
        1 for item in static_rows if str(item.get("solver_fallback_used", "")).strip().lower() == "true"
    )
    mean_order = (
        sum(_safe_float(item.get("order_reasonableness_score")) for item in static_rows) / static_count
        if static_count
        else 0.0
    )
    mean_findings = (
        sum(_safe_float(item.get("findings_explainability_score")) for item in static_rows) / static_count
        if static_count
        else 0.0
    )
    mean_execution = (
        sum(_safe_float(item.get("execution_credibility_score")) for item in static_rows) / static_count
        if static_count
        else 0.0
    )

    artifact_chain_complete = "true" if all(_artifact_chain_complete_for_row(item) for item in rows) else "false"

    residual_candidates = [
        item
        for item in rows
        if str(item.get("sample_feature", "")).strip() in {"low_liquidity_stress", "real_tushare_snapshot"}
    ]
    if residual_candidates:
        residual_ok = all(
            _safe_int(item.get("stress_partial_fill_count")) > 0
            or _safe_int(item.get("stress_unfilled_count")) > 0
            or _safe_float(item.get("stress_total_unfilled_notional")) > 0.0
            for item in residual_candidates
        )
        execution_residual_risk_consistent = "true" if residual_ok else "false"
    else:
        execution_residual_risk_consistent = ""

    derived_as_of_date = (
        as_of_date
        or str(provider_report.get("effective_trade_date", "")).strip()
        or datetime.now().strftime("%Y-%m-%d")
    )

    return {
        "date": derived_as_of_date,
        "phase": phase,
        "mode": mode,
        "run_root": str(run_root),
        "as_of_date": derived_as_of_date,
        "nightly_status": nightly_status,
        "release_status": release_status,
        "release_gate_passed": release_gate_passed,
        "rebalance_triggered": "true" if rebalance_triggered else "false",
        "artifact_chain_complete": artifact_chain_complete,
        "override_count": str(override_count),
        "cost_better_ratio": f"{cost_better_ratio:.4f}",
        "primary_feed_success": primary_feed_success,
        "fallback_activated": "|".join(sorted(fallback_sources)),
        "solver_primary": solver_primary,
        "blocked_untradeable_count": str(blocked_untradeable_count),
        "static_count": str(static_count),
        "real_count": str(len(real_rows)),
        "full_chain_success_static": str(full_chain_success_static),
        "full_chain_success_real": str(full_chain_success_real),
        "override_used_static": str(static_override_count),
        "score_gap_ge_001_static": str(static_gap_ok_count),
        "cost_better_ratio_static": f"{static_cost_better_ratio:.4f}",
        "solver_fallback_used_static": str(static_solver_fallback_count),
        "solver_sample_count_static": str(static_count),
        "mean_order_reasonableness_static": f"{mean_order:.4f}",
        "mean_findings_explainability_static": f"{mean_findings:.4f}",
        "mean_execution_credibility_static": f"{mean_execution:.4f}",
        "execution_residual_risk_consistent": execution_residual_risk_consistent,
        "provider_blockers_count": str(len(blockers)),
        "comparison_eligibility_status": comparison_status,
        "comparison_eligibility_reason_count": str(len(comparison_reasons)),
        "incident_id": incident_id,
        "notes": notes,
    }


def _append_dashboard_row(row: dict[str, str], path: Path = DASHBOARD_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _ensure_dashboard_schema(path)
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=DASHBOARD_HEADERS)
        writer.writerow({header: str(row.get(header, "") or "") for header in DASHBOARD_HEADERS})


def _load_orders_for_broker(sample_dir: Path) -> pd.DataFrame:
    """Load frozen OMS orders and convert them for broker adapter usage."""

    orders_path = sample_dir / "approval" / "final_orders_oms.csv"
    if not orders_path.exists():
        return pd.DataFrame(columns=["ticker", "direction", "quantity", "price_limit"])
    frame = pd.read_csv(orders_path)
    if frame.empty:
        return pd.DataFrame(columns=["ticker", "direction", "quantity", "price_limit"])
    frame = frame.copy()
    frame["ticker"] = frame["ticker"].astype(str).str.strip()
    frame["direction"] = frame["side"].astype(str).str.strip().str.lower()
    frame["quantity"] = pd.to_numeric(frame["quantity"], errors="coerce").fillna(0.0)
    frame["price_limit"] = pd.to_numeric(frame.get("limit_price"), errors="coerce")
    frame["estimated_price"] = pd.to_numeric(frame.get("estimated_price"), errors="coerce")
    if "extended_hours" in frame.columns:
        frame["extended_hours"] = frame["extended_hours"].astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y", "on"})
    else:
        frame["extended_hours"] = False
    frame = frame.loc[frame["quantity"] > 0].copy()
    return frame[["ticker", "direction", "quantity", "price_limit", "estimated_price", "extended_hours"]]


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
        if not math.isfinite(parsed):
            return default
        return parsed
    except (TypeError, ValueError):
        return default


def _prepare_orders_for_alpaca(
    *,
    orders_df: pd.DataFrame,
    broker_positions_before: pd.DataFrame,
    account_payload: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Apply broker-side guardrails before Alpaca submission.

    Rules:
    - never submit sell quantity above available broker position (avoid accidental shorting);
    - clip buys to a buying-power budget with a reserve (20%).
    """

    if orders_df.empty:
        return orders_df.copy(), {"input_count": 0, "submitted_count": 0, "notes": ["empty_orders"]}

    work = orders_df.copy()
    work["quantity"] = pd.to_numeric(work["quantity"], errors="coerce").fillna(0.0)
    work["price_limit"] = pd.to_numeric(work["price_limit"], errors="coerce")
    work["estimated_price"] = pd.to_numeric(work["estimated_price"], errors="coerce")
    if "reference_price" not in work.columns:
        work["reference_price"] = work["estimated_price"]
    work["reference_price"] = pd.to_numeric(work["reference_price"], errors="coerce")
    if "extended_hours" in work.columns:
        work["extended_hours"] = work["extended_hours"].astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y", "on"})
    else:
        work["extended_hours"] = False
    work["direction"] = work["direction"].astype(str).str.strip().str.lower()

    position_map = {
        str(row["ticker"]).strip(): _to_float(row["quantity"], 0.0)
        for row in broker_positions_before.to_dict(orient="records")
    }
    buying_power = _to_float(
        account_payload.get("buying_power")
        or account_payload.get("cash")
        or account_payload.get("regt_buying_power"),
        0.0,
    )
    buy_budget = max(0.0, buying_power * 0.8)
    buy_budget_remaining = buy_budget

    adjusted_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    clipped_rows: list[dict[str, Any]] = []
    records = list(work.to_dict(orient="records"))

    # Process sells first so no-short clipping is deterministic and separated.
    for row in records:
        ticker = str(row["ticker"]).strip()
        direction = str(row["direction"]).strip().lower()
        requested_qty = max(0.0, _to_float(row["quantity"], 0.0))
        if requested_qty <= 0:
            continue
        if direction != "sell":
            continue
        available_qty = max(0.0, _to_float(position_map.get(ticker), 0.0))
        if available_qty <= 0:
            skipped_rows.append(
                {
                    "ticker": ticker,
                    "direction": direction,
                    "requested_qty": requested_qty,
                    "reason": "no_broker_position_for_sell",
                }
            )
            continue
        final_qty = requested_qty
        if final_qty > available_qty:
            final_qty = available_qty
            clipped_rows.append(
                {
                    "ticker": ticker,
                    "direction": direction,
                    "requested_qty": requested_qty,
                    "submitted_qty": final_qty,
                    "reason": "clipped_to_available_position",
                }
            )
        position_map[ticker] = max(0.0, available_qty - final_qty)
        if final_qty <= 0:
            continue
        adjusted_row = dict(row)
        adjusted_row["quantity"] = float(final_qty)
        adjusted_rows.append(adjusted_row)

    buy_candidates: list[dict[str, Any]] = []
    for row in records:
        ticker = str(row["ticker"]).strip()
        direction = str(row["direction"]).strip().lower()
        requested_qty = max(0.0, _to_float(row["quantity"], 0.0))
        if requested_qty <= 0:
            continue
        if direction not in {"buy", "sell"}:
            skipped_rows.append(
                {
                    "ticker": ticker,
                    "direction": direction,
                    "requested_qty": requested_qty,
                    "reason": "unsupported_direction",
                }
            )
            continue
        if direction != "buy":
            continue
        price = _to_float(row.get("price_limit"), 0.0)
        if price <= 0:
            price = _to_float(row.get("estimated_price"), 0.0)
        if price <= 0:
            skipped_rows.append(
                {
                    "ticker": ticker,
                    "direction": direction,
                    "requested_qty": requested_qty,
                    "reason": "missing_or_invalid_price",
                }
            )
            continue
        buy_candidates.append(
            {
                "row": row,
                "ticker": ticker,
                "requested_qty": requested_qty,
                "price": price,
                "requested_notional": requested_qty * price,
            }
        )

    total_buy_notional = sum(float(item["requested_notional"]) for item in buy_candidates)
    buy_scale = 1.0
    if total_buy_notional > 0:
        buy_scale = min(1.0, buy_budget_remaining / total_buy_notional)

    for item in buy_candidates:
        row = dict(item["row"])
        ticker = str(item["ticker"])
        requested_qty = float(item["requested_qty"])
        price = float(item["price"])
        if buy_budget_remaining <= 0:
            skipped_rows.append(
                {
                    "ticker": ticker,
                    "direction": "buy",
                    "requested_qty": requested_qty,
                    "reason": "buying_power_budget_exhausted",
                }
            )
            continue
        max_affordable_qty = int(buy_budget_remaining / price) if price > 0 else 0
        if max_affordable_qty <= 0:
            skipped_rows.append(
                {
                    "ticker": ticker,
                    "direction": "buy",
                    "requested_qty": requested_qty,
                    "reason": "buying_power_budget_exhausted",
                }
            )
            continue
        scaled_target_qty = int(requested_qty * buy_scale)
        if scaled_target_qty <= 0 and max_affordable_qty >= 1:
            scaled_target_qty = 1
        final_qty = float(min(max_affordable_qty, scaled_target_qty))
        if final_qty <= 0:
            skipped_rows.append(
                {
                    "ticker": ticker,
                    "direction": "buy",
                    "requested_qty": requested_qty,
                    "reason": "buying_power_budget_exhausted",
                }
            )
            continue
        if final_qty < requested_qty:
            clipped_rows.append(
                {
                    "ticker": ticker,
                    "direction": "buy",
                    "requested_qty": requested_qty,
                    "submitted_qty": final_qty,
                    "reason": "clipped_to_buying_power_budget",
                }
            )
        buy_budget_remaining = max(0.0, buy_budget_remaining - (final_qty * price))
        row["quantity"] = final_qty
        adjusted_rows.append(row)

    prepared = pd.DataFrame(
        adjusted_rows,
        columns=["ticker", "direction", "quantity", "price_limit", "estimated_price", "reference_price", "extended_hours"],
    )
    precheck = {
        "input_count": int(len(work)),
        "submitted_count": int(len(prepared)),
        "skipped_count": int(len(skipped_rows)),
        "clipped_count": int(len(clipped_rows)),
        "buying_power": buying_power,
        "buy_budget_80pct": buy_budget,
        "buy_budget_remaining": buy_budget_remaining,
        "skipped_orders": skipped_rows,
        "clipped_orders": clipped_rows,
    }
    return prepared, precheck


def _expected_positions_after_orders(
    *,
    current_positions: pd.DataFrame,
    orders_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build expected post-trade position quantities from current broker positions and signed orders."""

    base = current_positions.copy()
    if base.empty:
        base = pd.DataFrame(columns=["ticker", "quantity", "market_value"])
    base["ticker"] = base["ticker"].astype(str).str.strip()
    quantity_column = "quantity" if "quantity" in base.columns else "expected_quantity" if "expected_quantity" in base.columns else None
    if quantity_column is None:
        base["quantity"] = 0.0
    else:
        base["quantity"] = pd.to_numeric(base[quantity_column], errors="coerce").fillna(0.0)
    signed = orders_df.copy()
    signed["signed_qty"] = signed.apply(
        lambda row: float(row["quantity"]) if str(row["direction"]).lower() == "buy" else -float(row["quantity"]),
        axis=1,
    )
    deltas = signed.groupby("ticker", as_index=False)["signed_qty"].sum()
    merged = base[["ticker", "quantity"]].merge(deltas, on="ticker", how="outer")
    merged["quantity"] = merged["quantity"].fillna(0.0)
    merged["signed_qty"] = merged["signed_qty"].fillna(0.0)
    merged["expected_quantity"] = merged["quantity"] + merged["signed_qty"]
    return merged[["ticker", "expected_quantity"]]


def _run_alpaca_execution_cycle(
    *,
    run_root: Path,
    rows: list[dict[str, str]],
) -> None:
    """Submit frozen real-sample orders to Alpaca paper trading and persist outputs."""

    target_rows = [
        row
        for row in rows
        if str(row.get("sample_id", "")).startswith("real_sample_")
        and _to_bool(row.get("approval_success")) is True
        and _to_bool(row.get("execution_success")) is True
    ]
    if not target_rows:
        return
    adapter = AlpacaAdapter()
    for row in target_rows:
        sample_id = str(row["sample_id"])
        sample_dir = run_root / "samples" / sample_id
        execution_dir = sample_dir / "execution"
        execution_dir.mkdir(parents=True, exist_ok=True)
        orders_df = _load_orders_for_broker(sample_dir)
        if orders_df.empty:
            continue

        try:
            broker_positions_before = adapter.query_positions()
            account_payload = adapter.query_account()
            prepared_orders_df, precheck = _prepare_orders_for_alpaca(
                orders_df=orders_df,
                broker_positions_before=broker_positions_before,
                account_payload=account_payload,
            )
            (execution_dir / "execution_broker_precheck.json").write_text(
                json.dumps(precheck, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            if prepared_orders_df.empty:
                expected_positions = _expected_positions_after_orders(
                    current_positions=broker_positions_before,
                    orders_df=prepared_orders_df,
                )
                reconciliation_report = adapter.reconcile(expected_positions)
                (execution_dir / "execution_result.json").write_text(
                    json.dumps(
                        {
                            "orders": [],
                            "submitted_count": 0,
                            "filled_count": 0,
                            "partial_count": 0,
                            "unfilled_count": 0,
                            "rejected_count": 0,
                            "note": "No broker-submittable orders after precheck clipping.",
                        },
                        indent=2,
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )
                (execution_dir / "reconciliation_report.json").write_text(
                    json.dumps(reconciliation_report.to_dict(), indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
                continue
            expected_positions = _expected_positions_after_orders(
                current_positions=broker_positions_before,
                orders_df=prepared_orders_df,
            )
            execution_result = adapter.submit_orders(prepared_orders_df)
            reconciliation_report = adapter.reconcile(expected_positions)

            (execution_dir / "execution_result.json").write_text(
                json.dumps(execution_result.to_dict(), indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            (execution_dir / "reconciliation_report.json").write_text(
                json.dumps(reconciliation_report.to_dict(), indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            AlpacaAdapter.execution_result_to_frame(execution_result).to_csv(
                execution_dir / "execution_result.csv",
                index=False,
            )
        except Exception as exc:
            (execution_dir / "execution_broker_error.json").write_text(
                json.dumps(
                    {
                        "broker": "alpaca",
                        "sample_id": sample_id,
                        "error": str(exc),
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )


def _write_weekly_summary(run_root: Path, release_passed: bool, weekly_dir: Path = WEEKLY_DIR) -> Path:
    weekly_dir.mkdir(parents=True, exist_ok=True)
    existing = sorted(weekly_dir.glob("week_*_summary.md"))
    next_index = len(
        [
            path
            for path in existing
            if path.name.startswith("week_")
            and path.name.endswith("_summary.md")
            and "template" not in path.name
        ]
    ) + 1
    target = weekly_dir / f"week_{next_index:02d}_summary.md"
    summary_path = run_root / "pilot_validation_summary.md"
    payload = [
        f"# Weekly Pilot Summary (Week {next_index:02d})",
        "",
        f"- generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"- release_gate_passed: {'true' if release_passed else 'false'}",
        f"- run_root: {run_root}",
        f"- validation_summary: {summary_path}",
        "",
        "## Notes",
        "",
        "- Update this file with weekly trend analysis and incident linkage.",
    ]
    target.write_text("\n".join(payload) + "\n", encoding="utf-8")
    return target


def _load_fill_sources(*, run_root: Path | None, orders_oms: Path | None) -> tuple[list[fill_collection.FillSourceBasket], str, str]:
    if orders_oms is not None:
        source_path = Path(orders_oms).resolve()
        if not source_path.exists():
            raise FileNotFoundError(f"--orders-oms path does not exist: {source_path}")
        return fill_collection.load_orders_from_orders_oms(source_path), "orders_oms", str(source_path)
    if run_root is not None:
        source_path = Path(run_root).resolve()
        if not source_path.exists():
            raise FileNotFoundError(f"--run-root path does not exist: {source_path}")
        return fill_collection.load_orders_from_run_root(source_path), "run_root", str(source_path)
    return [], "run_root", ""


def _source_orders_for_precheck(source: fill_collection.FillSourceBasket) -> pd.DataFrame:
    if source.orders.empty:
        return pd.DataFrame(columns=["ticker", "direction", "quantity", "price_limit", "estimated_price", "reference_price"])
    frame = source.orders.rename(columns={"requested_qty": "quantity"}).copy()
    for column in ("ticker", "direction", "quantity", "price_limit", "estimated_price", "reference_price"):
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame[["ticker", "direction", "quantity", "price_limit", "estimated_price", "reference_price"]]


def _latency_seconds(submitted_at_utc: str | None, terminal_at_utc: str | None) -> float | None:
    if not submitted_at_utc or not terminal_at_utc:
        return None
    try:
        submitted = pd.Timestamp(submitted_at_utc)
        terminal = pd.Timestamp(terminal_at_utc)
    except Exception:
        return None
    if submitted.tzinfo is None:
        submitted = submitted.tz_localize("UTC")
    else:
        submitted = submitted.tz_convert("UTC")
    if terminal.tzinfo is None:
        terminal = terminal.tz_localize("UTC")
    else:
        terminal = terminal.tz_convert("UTC")
    delta = (terminal - submitted).total_seconds()
    return float(delta)


def _build_fill_order_row(
    *,
    sample_id: str,
    source_path: Path,
    source_row: dict[str, Any],
    execution_record: Any,
) -> dict[str, Any]:
    requested_qty = _to_float(source_row.get("quantity"), 0.0)
    reference_price = _maybe_float(source_row.get("reference_price"))
    estimated_price = _maybe_float(source_row.get("estimated_price"))
    if reference_price is None:
        reference_price = estimated_price
    if estimated_price is None:
        estimated_price = _maybe_float(source_row.get("price_limit"))
    if reference_price is None:
        reference_price = estimated_price
    avg_fill_price = _maybe_float(getattr(execution_record, "avg_fill_price", None))
    avg_fill_price_fallback_used = False
    if avg_fill_price is None:
        avg_fill_price = reference_price
        avg_fill_price_fallback_used = avg_fill_price is not None
    filled_qty = _to_float(getattr(execution_record, "filled_qty", 0.0), 0.0)
    requested_notional = requested_qty * estimated_price if estimated_price is not None else None
    filled_notional = filled_qty * avg_fill_price if avg_fill_price is not None else None
    fill_ratio = (filled_qty / requested_qty) if requested_qty > 0 else None
    submitted_at_utc = getattr(execution_record, "submitted_at_utc", None)
    terminal_at_utc = getattr(execution_record, "terminal_at_utc", None)
    status_history = [dict(item) for item in list(getattr(execution_record, "status_history", []) or [])]
    row = {
        "sample_id": str(sample_id),
        "source_path": str(source_path),
        "ticker": str(source_row.get("ticker", "")).strip(),
        "direction": str(source_row.get("direction", "")).strip().lower(),
        "requested_qty": requested_qty,
        "filled_qty": filled_qty,
        "avg_fill_price": avg_fill_price,
        "reference_price": reference_price,
        "estimated_price": estimated_price,
        "requested_notional": requested_notional,
        "filled_notional": filled_notional,
        "fill_ratio": fill_ratio,
        "status": str(getattr(execution_record, "status", "")).strip().lower(),
        "reject_reason": getattr(execution_record, "reject_reason", None),
        "broker_order_id": str(
            getattr(execution_record, "broker_order_id", None)
            or getattr(execution_record, "order_id", None)
            or ""
        ).strip()
        or None,
        "submitted_at_utc": submitted_at_utc,
        "terminal_at_utc": terminal_at_utc,
        "latency_seconds": _latency_seconds(submitted_at_utc, terminal_at_utc),
        "poll_count": int(getattr(execution_record, "poll_count", 0) or 0),
        "timeout_cancelled": bool(getattr(execution_record, "timeout_cancelled", False)),
        "cancel_requested": bool(getattr(execution_record, "cancel_requested", False)),
        "cancel_acknowledged": bool(getattr(execution_record, "cancel_acknowledged", False)),
        "avg_fill_price_fallback_used": bool(avg_fill_price_fallback_used),
        "status_history": status_history,
    }
    return row


def _build_aggregate_execution_result(records: list[Any]) -> Any:
    execution_result = {
        "orders": records,
        "submitted_count": len(records),
        "filled_count": 0,
        "partial_count": 0,
        "unfilled_count": 0,
        "rejected_count": 0,
        "timeout_cancelled_count": 0,
    }
    for record in records:
        status = str(getattr(record, "status", "")).strip().lower()
        filled_qty = _to_float(getattr(record, "filled_qty", 0.0), 0.0)
        requested_qty = _to_float(getattr(record, "requested_qty", 0.0), 0.0)
        if status == "filled":
            execution_result["filled_count"] += 1
        elif status == "partially_filled" or (filled_qty > 0 and filled_qty < requested_qty):
            execution_result["partial_count"] += 1
        elif status == "timeout_cancelled":
            execution_result["timeout_cancelled_count"] += 1
            execution_result["unfilled_count"] += 1
        elif status == "rejected":
            execution_result["rejected_count"] += 1
            execution_result["unfilled_count"] += 1
        elif filled_qty <= 0:
            execution_result["unfilled_count"] += 1
        else:
            execution_result["partial_count"] += 1
    return execution_result


def _run_alpaca_fill_collection(
    *,
    run_root: Path | None,
    orders_oms: Path | None,
    output_dir: Path,
    market: str,
    broker: str,
    timeout_seconds: float,
    poll_interval_seconds: float,
    notes: str,
    force_outside_market_hours: bool,
    adapter_factory: Any = AlpacaAdapter,
) -> Path:
    _preflight_alpaca_credentials()
    if str(market).strip().lower() != "us":
        raise ValueError("--market must be us for Alpaca fill collection.")
    if str(broker).strip().lower() != "alpaca":
        raise ValueError("--broker must be alpaca for Alpaca fill collection.")

    sources, source_type, source_path = _load_fill_sources(run_root=run_root, orders_oms=orders_oms)
    run_id = fill_collection.generate_run_id(source_type=source_type)
    run_output_dir = Path(output_dir).resolve() / run_id
    run_output_dir.mkdir(parents=True, exist_ok=True)

    adapter = adapter_factory(
        poll_interval_seconds=float(poll_interval_seconds),
        timeout_seconds=float(timeout_seconds),
    )
    clock_payload = {}
    try:
        clock_payload = adapter.query_clock()
    except Exception:
        clock_payload = {}
    if not force_outside_market_hours and not fill_collection.is_us_market_open(clock_payload=clock_payload):
        raise RuntimeError(
            "US market is closed; rerun with --force-outside-market-hours only if you intend to collect outside session hours."
        )

    account_before = adapter.query_account()
    positions_before = adapter.query_positions()
    current_expected_positions = positions_before.copy()

    requested_order_rows: list[dict[str, Any]] = []
    fill_order_rows: list[dict[str, Any]] = []
    all_execution_records: list[Any] = []
    avg_fill_price_fallback_used = False
    latency_values: list[float] = []
    broker_precheck_rows: list[dict[str, Any]] = []

    for source in sources:
        orders_for_precheck = _source_orders_for_precheck(source)
        for source_row in orders_for_precheck.to_dict(orient="records"):
            requested_price = _maybe_float(source_row.get("estimated_price"))
            if requested_price is None:
                requested_price = _maybe_float(source_row.get("price_limit"))
            reference_price = _maybe_float(source_row.get("reference_price"))
            if reference_price is None:
                reference_price = requested_price
            requested_qty = _to_float(source_row.get("quantity"), 0.0)
            requested_order_rows.append(
                {
                    "sample_id": source.sample_id,
                    "source_path": str(source.source_path),
                    "ticker": str(source_row.get("ticker", "")).strip(),
                    "direction": str(source_row.get("direction", "")).strip().lower(),
                    "requested_qty": requested_qty,
                    "reference_price": reference_price,
                    "estimated_price": requested_price,
                    "requested_notional": (requested_qty * requested_price) if requested_price is not None else None,
                }
            )
        prepared_orders_df, _precheck = _prepare_orders_for_alpaca(
            orders_df=orders_for_precheck,
            broker_positions_before=adapter.query_positions(),
            account_payload=adapter.query_account(),
        )
        precheck_reason_counts = Counter(
            str(item.get("reason", "")).strip()
            for item in list(_precheck.get("skipped_orders") or []) + list(_precheck.get("clipped_orders") or [])
            if str(item.get("reason", "")).strip()
        )
        broker_precheck_rows.append(
            {
                "sample_id": source.sample_id,
                "source_path": str(source.source_path),
                "input_count": int(_precheck.get("input_count", 0) or 0),
                "submitted_count": int(_precheck.get("submitted_count", 0) or 0),
                "skipped_count": int(_precheck.get("skipped_count", 0) or 0),
                "clipped_count": int(_precheck.get("clipped_count", 0) or 0),
                "buying_power": float(_precheck.get("buying_power", 0.0) or 0.0),
                "buy_budget_80pct": float(_precheck.get("buy_budget_80pct", 0.0) or 0.0),
                "buy_budget_remaining": float(_precheck.get("buy_budget_remaining", 0.0) or 0.0),
                "reason_counts_json": json.dumps(dict(precheck_reason_counts), ensure_ascii=False, sort_keys=True),
            }
        )
        if prepared_orders_df.empty:
            continue

        expected_positions = _expected_positions_after_orders(
            current_positions=current_expected_positions,
            orders_df=prepared_orders_df,
        )
        execution_result = adapter.submit_orders_with_telemetry(prepared_orders_df)
        all_execution_records.extend(list(execution_result.orders))
        prepared_rows = prepared_orders_df.to_dict(orient="records")
        for source_row, execution_record in zip(prepared_rows, execution_result.orders):
            row = _build_fill_order_row(
                sample_id=source.sample_id,
                source_path=source.source_path,
                source_row=source_row,
                execution_record=execution_record,
            )
            fill_order_rows.append(row)
            latency = row.get("latency_seconds")
            if latency is not None and not pd.isna(latency):
                latency_values.append(float(latency))
            avg_fill_price_fallback_used = avg_fill_price_fallback_used or bool(row.get("avg_fill_price_fallback_used"))
        current_expected_positions = expected_positions

    account_after = adapter.query_account()
    positions_after = adapter.query_positions()
    fill_order_frame = fill_collection.build_fill_orders_frame(fill_order_rows)
    fill_event_frame = fill_collection.build_fill_events_frame(fill_order_rows)
    execution_result_payload = _build_aggregate_execution_result(all_execution_records)
    execution_result = ExecutionResult(
        orders=all_execution_records,
        submitted_count=int(execution_result_payload["submitted_count"]),
        filled_count=int(execution_result_payload["filled_count"]),
        partial_count=int(execution_result_payload["partial_count"]),
        unfilled_count=int(execution_result_payload["unfilled_count"]),
        rejected_count=int(execution_result_payload["rejected_count"]),
        timeout_cancelled_count=int(execution_result_payload["timeout_cancelled_count"]),
    )
    reconciliation_report = adapter.reconcile(current_expected_positions)
    broker_precheck_summary = {
        "source_count": int(len(broker_precheck_rows)),
        "input_order_count": int(sum(int(row["input_count"]) for row in broker_precheck_rows)),
        "submitted_order_count": int(sum(int(row["submitted_count"]) for row in broker_precheck_rows)),
        "skipped_order_count": int(sum(int(row["skipped_count"]) for row in broker_precheck_rows)),
        "clipped_order_count": int(sum(int(row["clipped_count"]) for row in broker_precheck_rows)),
        "buying_power": float(_to_float(account_before.get("buying_power"), 0.0)),
        "cash": float(_to_float(account_before.get("cash"), 0.0)),
        "buy_budget_80pct": float(_to_float(broker_precheck_rows[0]["buy_budget_80pct"], 0.0)) if broker_precheck_rows else 0.0,
        "buy_budget_remaining": float(_to_float(broker_precheck_rows[-1]["buy_budget_remaining"], 0.0)) if broker_precheck_rows else 0.0,
        "reason_counts": dict(
            Counter(
                reason
                for row in broker_precheck_rows
                for reason, count in json.loads(row["reason_counts_json"]).items()
                for _ in range(int(count))
            )
        ),
        "source_prechecks": broker_precheck_rows,
    }
    manifest = fill_collection.build_fill_manifest(
        run_id=run_id,
        created_at=datetime.now().astimezone().isoformat(),
        market=str(market).strip().lower(),
        broker=str(broker).strip().lower(),
        notes=str(notes),
        source_type=source_type,
        source_path=source_path,
        requested_order_rows=fill_collection.build_fill_orders_frame(requested_order_rows),
        filled_order_rows=fill_order_frame,
        execution_result=execution_result,
        latency_values=latency_values,
        avg_fill_price_fallback_used=avg_fill_price_fallback_used,
        event_granularity="polled_history",
    )
    manifest["broker_precheck_summary"] = broker_precheck_summary
    execution_result_frame = AlpacaAdapter.execution_result_to_frame(execution_result)
    summary_markdown = "\n".join(
        [
            "# Alpaca Fill Collection",
            "",
            f"- run_id: {run_id}",
            f"- source_type: {source_type}",
            f"- source_path: {source_path or 'N/A'}",
            f"- order_count: {manifest['order_count']}",
            f"- submitted_count: {manifest['submitted_count']}",
            f"- filled_count: {manifest['filled_count']}",
            f"- partial_count: {manifest['partial_count']}",
            f"- unfilled_count: {manifest['unfilled_count']}",
            f"- rejected_count: {manifest['rejected_count']}",
            f"- timeout_cancelled_count: {manifest['timeout_cancelled_count']}",
            f"- fill_rate: {manifest['fill_rate']}",
            f"- total_requested_notional: {manifest['total_requested_notional']}",
            f"- total_filled_notional: {manifest['total_filled_notional']}",
            f"- avg_fill_price_mean: {manifest['avg_fill_price_mean']}",
            f"- latency_seconds_mean: {manifest['latency_seconds_mean']}",
            f"- latency_seconds_p50: {manifest['latency_seconds_p50']}",
            f"- latency_seconds_p95: {manifest['latency_seconds_p95']}",
            f"- has_any_filled_orders: {manifest['has_any_filled_orders']}",
            f"- broker_precheck_submitted_order_count: {broker_precheck_summary['submitted_order_count']}",
            f"- broker_precheck_reason_counts: {broker_precheck_summary['reason_counts']}",
        ]
    ) + "\n"
    paths = fill_collection.write_fill_collection_artifacts(
        output_dir=run_output_dir,
        manifest=manifest,
        order_rows=fill_order_frame,
        event_rows=fill_event_frame,
        account_before=account_before,
        account_after=account_after,
        positions_before=positions_before,
        positions_after=positions_after,
        execution_result=execution_result,
        reconciliation_report=reconciliation_report,
        execution_result_frame=execution_result_frame,
        summary_markdown=summary_markdown,
    )
    print(f"alpaca_fill_run_dir: {run_output_dir}")
    for name, path in paths.items():
        print(f"{name}: {path}")
    return run_output_dir


def run_ops(
    *,
    mode: str,
    phase: str,
    reviewer_input: Path | None,
    real_sample: bool,
    rebalance_triggered: bool,
    incident_id: str,
    notes: str,
    market: str = "cn",
    broker: str = "none",
    config_overlay: Path | None = None,
    as_of_date: str | None = None,
    output_dir: Path | None = None,
    ab_flow: bool = False,
    require_eligibility_gate: bool = False,
) -> int:
    dashboard_path, _incident_path, _go_nogo_path, weekly_dir = _resolve_tracking_paths(output_dir)

    init_tracking(force=False, output_dir=output_dir)
    return_code, run_root = _run_validation(
        mode=mode,
        reviewer_input=reviewer_input,
        real_sample=real_sample,
        market=market,
        config_overlay=config_overlay,
        as_of_date=as_of_date,
    )
    rows = _load_sample_rows(run_root)
    provider_report = _load_provider_report(run_root)
    if str(market).lower() == "us" and str(broker).lower() == "alpaca":
        _run_alpaca_execution_cycle(run_root=run_root, rows=rows)
    dashboard_row = _derive_row(
        mode=mode,
        phase=phase,
        return_code=return_code,
        run_root=run_root,
        rows=rows,
        provider_report=provider_report,
        rebalance_triggered=rebalance_triggered,
        incident_id=incident_id,
        notes=notes,
        as_of_date=as_of_date,
        ab_flow=ab_flow,
        require_eligibility_gate=require_eligibility_gate,
    )
    effective_return_code = int(return_code)
    eligibility_exit_code, eligibility_log = _eligibility_gate_outcome(
        status=str(dashboard_row.get("comparison_eligibility_status", "")),
        ab_flow=ab_flow,
        require_eligibility_gate=require_eligibility_gate,
    )
    if eligibility_exit_code is not None:
        if eligibility_log:
            print(eligibility_log)
        effective_return_code = int(eligibility_exit_code)
    _append_dashboard_row(dashboard_row, path=dashboard_path)
    print(f"dashboard_appended: {dashboard_path}")
    if mode == "release":
        weekly_path = _write_weekly_summary(run_root, release_passed=(effective_return_code == 0), weekly_dir=weekly_dir)
        print(f"weekly_summary: {weekly_path}")
    return effective_return_code


def _parse_iso_date(raw: Any) -> date | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _load_dashboard_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _load_incident_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _row_mode(row: dict[str, str]) -> str:
    mode = str(row.get("mode", "")).strip().lower()
    if mode in {"nightly", "release"}:
        return mode
    release_status = str(row.get("release_status", "")).strip()
    return "release" if release_status else "nightly"


def _row_business_date(row: dict[str, str]) -> date | None:
    as_of_value = _parse_iso_date(row.get("as_of_date"))
    if as_of_value is not None:
        return as_of_value
    return _parse_iso_date(row.get("date"))


def _rows_for_window(
    rows: list[dict[str, str]],
    *,
    as_of: date,
    window_days: int,
    window_name: str,
) -> tuple[list[dict[str, str]], set[date], str]:
    with_dates: list[tuple[dict[str, str], date]] = []
    for row in rows:
        row_date = _row_business_date(row)
        if row_date is None or row_date > as_of:
            continue
        with_dates.append((row, row_date))
    if not with_dates:
        return [], set(), "N/A"

    unique_dates = sorted({item[1] for item in with_dates})
    if window_name == "rolling":
        kept_dates = set(unique_dates[-window_days:])
    else:
        kept_dates = set(unique_dates)
    filtered_rows = [item[0] for item in with_dates if item[1] in kept_dates]
    kept_sorted = sorted(kept_dates)
    date_range = f"{kept_sorted[0].isoformat()}..{kept_sorted[-1].isoformat()}"
    return filtered_rows, kept_dates, date_range


def _incidents_for_dates(rows: list[dict[str, str]], dates: set[date], as_of: date) -> list[dict[str, str]]:
    if not rows:
        return []
    filtered: list[dict[str, str]] = []
    for row in rows:
        row_date = _parse_iso_date(row.get("date"))
        if row_date is None or row_date > as_of:
            continue
        if dates and row_date not in dates:
            continue
        filtered.append(row)
    return filtered


def _criterion_result(
    *,
    criterion_id: str,
    window: str,
    threshold: str,
    actual: str,
    status: str,
    evidence: str,
) -> dict[str, str]:
    return {
        "criterion_id": criterion_id,
        "window": window,
        "threshold": threshold,
        "actual": actual,
        "status": status,
        "evidence": evidence,
    }


def _status_from_bool(value: bool) -> str:
    return STATUS_PASS if value else STATUS_FAIL


def _evaluate_window(
    *,
    window_label: str,
    rows: list[dict[str, str]],
    incidents: list[dict[str, str]],
    date_range: str,
    incident_source_exists: bool,
) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    scope = f"{window_label} [{date_range}]"

    nightly_rows = [item for item in rows if _row_mode(item) == "nightly"]
    if not nightly_rows:
        results.append(
            _criterion_result(
                criterion_id="C01_nightly_completion_rate",
                window=scope,
                threshold=">=95%",
                actual="N/A",
                status=STATUS_INSUFFICIENT,
                evidence="nightly_status",
            )
        )
    else:
        nightly_pass = sum(1 for item in nightly_rows if str(item.get("nightly_status", "")).strip() == "pass")
        nightly_rate = nightly_pass / len(nightly_rows)
        results.append(
            _criterion_result(
                criterion_id="C01_nightly_completion_rate",
                window=scope,
                threshold=">=95%",
                actual=f"{nightly_rate:.2%} ({nightly_pass}/{len(nightly_rows)})",
                status=_status_from_bool(nightly_rate >= 0.95),
                evidence="nightly_status",
            )
        )

    release_rows = [item for item in rows if _row_mode(item) == "release"]
    if not release_rows:
        results.append(
            _criterion_result(
                criterion_id="C02_release_pass_rate",
                window=scope,
                threshold="=100%",
                actual="N/A",
                status=STATUS_INSUFFICIENT,
                evidence="release_status/release_gate_passed",
            )
        )
    else:
        release_pass = sum(
            1
            for item in release_rows
            if str(item.get("release_status", "")).strip() == "passed"
            or _to_bool(item.get("release_gate_passed")) is True
        )
        release_rate = release_pass / len(release_rows)
        results.append(
            _criterion_result(
                criterion_id="C02_release_pass_rate",
                window=scope,
                threshold="=100%",
                actual=f"{release_rate:.2%} ({release_pass}/{len(release_rows)})",
                status=_status_from_bool(release_rate == 1.0),
                evidence="release_status/release_gate_passed",
            )
        )

    rebalance_rows = [item for item in rows if _to_bool(item.get("rebalance_triggered")) is True]
    override_values: list[int] = []
    for item in rebalance_rows:
        value = _maybe_int(item.get("override_used_static"))
        if value is None:
            value = _maybe_int(item.get("override_count"))
        if value is not None:
            override_values.append(value)
    if not override_values:
        results.append(
            _criterion_result(
                criterion_id="C03_override_rate_per_rebalance",
                window=scope,
                threshold="<=2/5 per rebalance",
                actual="N/A",
                status=STATUS_INSUFFICIENT,
                evidence="override_used_static|override_count",
            )
        )
    else:
        max_override = max(override_values)
        mean_override = sum(override_values) / len(override_values)
        results.append(
            _criterion_result(
                criterion_id="C03_override_rate_per_rebalance",
                window=scope,
                threshold="<=2/5 per rebalance",
                actual=f"max={max_override}/5, mean={mean_override:.2f}/5, runs={len(override_values)}",
                status=_status_from_bool(max_override <= 2),
                evidence="override_used_static|override_count",
            )
        )

    data_integrity_incidents = [
        item
        for item in incidents
        if str(item.get("severity", "")).strip().upper() == "P1"
        and "data_integrity" in str(item.get("category", "")).strip().lower()
    ]
    if not incident_source_exists:
        incident_status = STATUS_INSUFFICIENT
        incident_actual = "N/A"
    else:
        incident_status = _status_from_bool(len(data_integrity_incidents) == 0)
        incident_actual = f"{len(data_integrity_incidents)} incidents"
    results.append(
        _criterion_result(
            criterion_id="C04_p1_data_integrity_incidents",
            window=scope,
            threshold="=0",
            actual=incident_actual,
            status=incident_status,
            evidence="incident_register(severity=P1,category=data_integrity)",
        )
    )

    artifact_values = [_to_bool(item.get("artifact_chain_complete")) for item in rebalance_rows]
    artifact_valid = [value for value in artifact_values if value is not None]
    if not artifact_valid:
        results.append(
            _criterion_result(
                criterion_id="C05_artifact_chain_completeness",
                window=scope,
                threshold="=100%",
                actual="N/A",
                status=STATUS_INSUFFICIENT,
                evidence="artifact_chain_complete",
            )
        )
    else:
        artifact_pass = sum(1 for value in artifact_valid if value)
        artifact_rate = artifact_pass / len(artifact_valid)
        results.append(
            _criterion_result(
                criterion_id="C05_artifact_chain_completeness",
                window=scope,
                threshold="=100%",
                actual=f"{artifact_rate:.2%} ({artifact_pass}/{len(artifact_valid)})",
                status=_status_from_bool(artifact_rate == 1.0),
                evidence="artifact_chain_complete",
            )
        )

    total_solver_fallback = 0
    total_solver_samples = 0
    for item in rows:
        used = _maybe_int(item.get("solver_fallback_used_static"))
        samples = _maybe_int(item.get("solver_sample_count_static"))
        if used is None or samples is None or samples <= 0:
            continue
        total_solver_fallback += used
        total_solver_samples += samples
    if total_solver_samples <= 0:
        results.append(
            _criterion_result(
                criterion_id="C06_solver_fallback_rate",
                window=scope,
                threshold="<=5%",
                actual="N/A",
                status=STATUS_INSUFFICIENT,
                evidence="solver_fallback_used_static/solver_sample_count_static",
            )
        )
    else:
        solver_rate = total_solver_fallback / total_solver_samples
        results.append(
            _criterion_result(
                criterion_id="C06_solver_fallback_rate",
                window=scope,
                threshold="<=5%",
                actual=f"{solver_rate:.2%} ({total_solver_fallback}/{total_solver_samples})",
                status=_status_from_bool(solver_rate <= 0.05),
                evidence="solver_fallback_used_static/solver_sample_count_static",
            )
        )

    weighted_num = 0.0
    weighted_den = 0.0
    for item in rows:
        ratio = _maybe_float(item.get("cost_better_ratio_static"))
        weight = _maybe_int(item.get("static_count"))
        if ratio is None:
            ratio = _maybe_float(item.get("cost_better_ratio"))
            weight = 1 if ratio is not None else None
        if ratio is None or weight is None or weight <= 0:
            continue
        weighted_num += ratio * float(weight)
        weighted_den += float(weight)
    if weighted_den <= 0:
        results.append(
            _criterion_result(
                criterion_id="C07_cost_advantage_coverage",
                window=scope,
                threshold=">=80%",
                actual="N/A",
                status=STATUS_INSUFFICIENT,
                evidence="cost_better_ratio_static|cost_better_ratio",
            )
        )
    else:
        weighted_ratio = weighted_num / weighted_den
        results.append(
            _criterion_result(
                criterion_id="C07_cost_advantage_coverage",
                window=scope,
                threshold=">=80%",
                actual=f"{weighted_ratio:.2%}",
                status=_status_from_bool(weighted_ratio >= 0.8),
                evidence="cost_better_ratio_static|cost_better_ratio",
            )
        )

    fallback_series: list[tuple[date, bool]] = []
    for item in rows:
        row_date = _row_business_date(item)
        if row_date is None:
            continue
        has_fallback = bool(str(item.get("fallback_activated", "")).strip())
        fallback_series.append((row_date, has_fallback))
    fallback_series.sort(key=lambda x: x[0])
    if len(fallback_series) < 4:
        results.append(
            _criterion_result(
                criterion_id="C08_fallback_activation_trend",
                window=scope,
                threshold="second_half_rate <= first_half_rate",
                actual="N/A",
                status=STATUS_INSUFFICIENT,
                evidence="fallback_activated (first_half vs second_half)",
            )
        )
    else:
        split = len(fallback_series) // 2
        first = fallback_series[:split]
        second = fallback_series[split:]
        first_rate = sum(1 for _d, active in first if active) / len(first)
        second_rate = sum(1 for _d, active in second if active) / len(second)
        results.append(
            _criterion_result(
                criterion_id="C08_fallback_activation_trend",
                window=scope,
                threshold="second_half_rate <= first_half_rate",
                actual=f"first={first_rate:.2%}, second={second_rate:.2%}, n={len(fallback_series)}",
                status=_status_from_bool(second_rate <= first_rate),
                evidence="fallback_activated (first_half vs second_half)",
            )
        )

    residual_values = [_to_bool(item.get("execution_residual_risk_consistent")) for item in rows]
    residual_valid = [value for value in residual_values if value is not None]
    if not residual_valid:
        results.append(
            _criterion_result(
                criterion_id="C09_execution_residual_risk_consistency",
                window=scope,
                threshold=">=80% consistent",
                actual="N/A",
                status=STATUS_INSUFFICIENT,
                evidence="execution_residual_risk_consistent",
            )
        )
    else:
        residual_pass = sum(1 for value in residual_valid if value)
        residual_rate = residual_pass / len(residual_valid)
        results.append(
            _criterion_result(
                criterion_id="C09_execution_residual_risk_consistency",
                window=scope,
                threshold=">=80% consistent",
                actual=f"{residual_rate:.2%} ({residual_pass}/{len(residual_valid)})",
                status=_status_from_bool(residual_rate >= 0.8),
                evidence="execution_residual_risk_consistent",
            )
        )

    reviewer_weighted = {"order": 0.0, "findings": 0.0, "execution": 0.0}
    reviewer_weight_den = 0.0
    for item in rows:
        order_score = _maybe_float(item.get("mean_order_reasonableness_static"))
        findings_score = _maybe_float(item.get("mean_findings_explainability_static"))
        execution_score = _maybe_float(item.get("mean_execution_credibility_static"))
        weight = _maybe_int(item.get("static_count"))
        if (
            order_score is None
            or findings_score is None
            or execution_score is None
            or weight is None
            or weight <= 0
        ):
            continue
        reviewer_weighted["order"] += order_score * float(weight)
        reviewer_weighted["findings"] += findings_score * float(weight)
        reviewer_weighted["execution"] += execution_score * float(weight)
        reviewer_weight_den += float(weight)
    if reviewer_weight_den <= 0:
        results.append(
            _criterion_result(
                criterion_id="C10_reviewer_mean_scores",
                window=scope,
                threshold="all >=3.5/5",
                actual="N/A",
                status=STATUS_INSUFFICIENT,
                evidence="mean_order/findings/execution_static",
            )
        )
    else:
        order_mean = reviewer_weighted["order"] / reviewer_weight_den
        findings_mean = reviewer_weighted["findings"] / reviewer_weight_den
        execution_mean = reviewer_weighted["execution"] / reviewer_weight_den
        pass_all = order_mean >= 3.5 and findings_mean >= 3.5 and execution_mean >= 3.5
        results.append(
            _criterion_result(
                criterion_id="C10_reviewer_mean_scores",
                window=scope,
                threshold="all >=3.5/5",
                actual=f"order={order_mean:.2f}, findings={findings_mean:.2f}, execution={execution_mean:.2f}",
                status=_status_from_bool(pass_all),
                evidence="mean_order/findings/execution_static",
            )
        )

    eligibility_rows = [
        item
        for item in rows
        if str(item.get("comparison_eligibility_status", "")).strip()
    ]
    if not eligibility_rows:
        results.append(
            _criterion_result(
                criterion_id="C11_comparison_eligibility_gate",
                window=scope,
                threshold="ineligible_count = 0",
                actual="N/A",
                status=STATUS_INSUFFICIENT,
                evidence="comparison_eligibility_status/comparison_eligibility_reason_count",
            )
        )
    else:
        eligible_count = sum(
            1
            for item in eligibility_rows
            if str(item.get("comparison_eligibility_status", "")).strip().upper()
            == COMPARISON_ELIGIBILITY_ELIGIBLE
        )
        ineligible_count = sum(
            1
            for item in eligibility_rows
            if str(item.get("comparison_eligibility_status", "")).strip().upper()
            == COMPARISON_ELIGIBILITY_INELIGIBLE
        )
        invalid_count = sum(
            1
            for item in eligibility_rows
            if str(item.get("comparison_eligibility_status", "")).strip().upper()
            == COMPARISON_ELIGIBILITY_INVALID
        )
        not_available_count = sum(
            1
            for item in eligibility_rows
            if str(item.get("comparison_eligibility_status", "")).strip().upper()
            == COMPARISON_ELIGIBILITY_NOT_AVAILABLE
        )
        reasons_total = sum(_safe_int(item.get("comparison_eligibility_reason_count"), 0) for item in eligibility_rows)
        if ineligible_count > 0 or invalid_count > 0:
            criterion_status = STATUS_FAIL
        elif eligible_count == 0 and not_available_count > 0:
            criterion_status = STATUS_WAIVE
        else:
            criterion_status = STATUS_PASS
        results.append(
            _criterion_result(
                criterion_id="C11_comparison_eligibility_gate",
                window=scope,
                threshold="invalid_count=0 and ineligible_count=0",
                actual=(
                    f"eligible={eligible_count}/{len(eligibility_rows)}, "
                    f"ineligible={ineligible_count}/{len(eligibility_rows)}, "
                    f"invalid={invalid_count}/{len(eligibility_rows)}, "
                    f"not_available={not_available_count}/{len(eligibility_rows)}, "
                    f"reasons_total={reasons_total}"
                ),
                status=criterion_status,
                evidence="comparison_eligibility_status/comparison_eligibility_reason_count",
            )
        )

    return results


def _evaluate_go_nogo(
    *,
    dashboard_rows: list[dict[str, str]],
    incident_rows: list[dict[str, str]],
    as_of: date,
    window_trading_days: int,
    incident_source_exists: bool,
) -> dict[str, Any]:
    rolling_rows, rolling_dates, rolling_range = _rows_for_window(
        dashboard_rows,
        as_of=as_of,
        window_days=window_trading_days,
        window_name="rolling",
    )
    pilot_rows, pilot_dates, pilot_range = _rows_for_window(
        dashboard_rows,
        as_of=as_of,
        window_days=window_trading_days,
        window_name="pilot",
    )

    rolling_incidents = _incidents_for_dates(incident_rows, rolling_dates, as_of)
    pilot_incidents = _incidents_for_dates(incident_rows, pilot_dates, as_of)

    rolling_results = _evaluate_window(
        window_label=f"rolling_{window_trading_days}",
        rows=rolling_rows,
        incidents=rolling_incidents,
        date_range=rolling_range,
        incident_source_exists=incident_source_exists,
    )
    pilot_results = _evaluate_window(
        window_label="pilot_to_date",
        rows=pilot_rows,
        incidents=pilot_incidents,
        date_range=pilot_range,
        incident_source_exists=incident_source_exists,
    )

    return {
        "as_of_date": as_of.isoformat(),
        "window_trading_days": window_trading_days,
        "windows": [
            {"name": f"rolling_{window_trading_days}", "date_range": rolling_range, "results": rolling_results},
            {"name": "pilot_to_date", "date_range": pilot_range, "results": pilot_results},
        ],
    }


def _render_go_nogo_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Go/No-Go Status Report",
        "",
        f"- generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"- as_of_date: {payload['as_of_date']}",
        f"- rolling_window_trading_days: {payload['window_trading_days']}",
        "",
        "Decision policy: this report is decision support only; final sign-off remains manual.",
        "",
    ]
    for window in payload.get("windows", []):
        lines.extend(
            [
                f"## Window: {window['name']}",
                f"- range: {window.get('date_range', 'N/A')}",
                "",
                "| criterion_id | window | threshold | actual | status | evidence |",
                "|---|---|---|---|---|---|",
            ]
        )
        status_counter: Counter[str] = Counter()
        for item in window.get("results", []):
            status_counter.update([str(item.get("status", ""))])
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(item.get("criterion_id", "")),
                        str(item.get("window", "")),
                        str(item.get("threshold", "")),
                        str(item.get("actual", "")),
                        str(item.get("status", "")),
                        str(item.get("evidence", "")),
                    ]
                )
                + " |"
            )
        lines.extend(
            [
                "",
                (
                    f"Summary: PASS={status_counter.get(STATUS_PASS, 0)}, "
                    f"FAIL={status_counter.get(STATUS_FAIL, 0)}, "
                    f"WAIVE={status_counter.get(STATUS_WAIVE, 0)}, "
                    f"INSUFFICIENT_DATA={status_counter.get(STATUS_INSUFFICIENT, 0)}"
                ),
                "",
            ]
        )
    return "\n".join(lines) + "\n"


def generate_go_nogo_report(
    *,
    dashboard_path: Path,
    incident_path: Path,
    output_path: Path,
    as_of_date: str,
    window_trading_days: int,
) -> None:
    dashboard_rows = _load_dashboard_rows(dashboard_path)
    incident_rows = _load_incident_rows(incident_path)
    parsed_as_of = _parse_iso_date(as_of_date)
    if parsed_as_of is None:
        raise ValueError(f"Invalid --as-of-date value: {as_of_date}. Expected YYYY-MM-DD.")
    payload = _evaluate_go_nogo(
        dashboard_rows=dashboard_rows,
        incident_rows=incident_rows,
        as_of=parsed_as_of,
        window_trading_days=window_trading_days,
        incident_source_exists=incident_path.exists(),
    )
    markdown = _render_go_nogo_markdown(payload)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8")
    print(f"go_nogo_report: {output_path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pilot operations helper for nightly/weekly routines.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Initialize pilot tracking files under outputs/pilot_tracking.")
    init_parser.add_argument("--force", action="store_true", help="Recreate tracking files from templates.")
    init_parser.add_argument("--market", choices=["cn", "us"], default="cn", help="Reserved for init compatibility.")
    init_parser.add_argument(
        "--output-dir",
        type=Path,
        default=TRACKING_DIR,
        help="Tracking output directory (default: outputs/pilot_tracking).",
    )

    nightly_parser = subparsers.add_parser("nightly", help="Run nightly validation and append dashboard row.")
    nightly_parser.add_argument("--phase", default="phase_1", help="Pilot phase label for dashboard row.")
    nightly_parser.add_argument("--market", choices=["cn", "us"], default="cn", help="Validation market mode.")
    nightly_parser.add_argument("--broker", choices=["none", "alpaca"], default="none", help="Optional broker execution adapter.")
    nightly_parser.add_argument("--ab-flow", action="store_true", help="Mark this run as A/B eligibility-aware flow.")
    nightly_parser.add_argument(
        "--require-eligibility-gate",
        action="store_true",
        help="Block decision flow when A/B comparison eligibility is missing/ineligible/invalid.",
    )
    nightly_parser.add_argument("--real-sample", action="store_true", help="Include real sample in nightly run.")
    nightly_parser.add_argument(
        "--as-of-date",
        default=None,
        help="Business date recorded in dashboard and forwarded to validation (YYYY-MM-DD).",
    )
    nightly_parser.add_argument(
        "--config-overlay",
        type=Path,
        default=None,
        help="Optional YAML config overlay passed through to run_pilot_validation.py.",
    )
    nightly_parser.add_argument("--rebalance-triggered", action="store_true", help="Set rebalance_triggered=true in dashboard row.")
    nightly_parser.add_argument("--incident-id", default="", help="Optional incident ID to link this row.")
    nightly_parser.add_argument("--notes", default="", help="Optional notes for dashboard row.")
    nightly_parser.add_argument(
        "--output-dir",
        type=Path,
        default=TRACKING_DIR,
        help="Tracking output directory (default: outputs/pilot_tracking).",
    )

    weekly_parser = subparsers.add_parser("weekly", help="Run weekly release gate and append dashboard row.")
    weekly_parser.add_argument("--phase", default="phase_2", help="Pilot phase label for dashboard row.")
    weekly_parser.add_argument("--market", choices=["cn", "us"], default="cn", help="Validation market mode.")
    weekly_parser.add_argument("--broker", choices=["none", "alpaca"], default="none", help="Optional broker execution adapter.")
    weekly_parser.add_argument("--ab-flow", action="store_true", help="Mark this run as A/B eligibility-aware flow.")
    weekly_parser.add_argument(
        "--require-eligibility-gate",
        action="store_true",
        help="Block decision flow when A/B comparison eligibility is missing/ineligible/invalid.",
    )
    weekly_parser.add_argument("--reviewer-input", type=Path, required=True, help="Reviewer CSV path for release mode.")
    weekly_parser.add_argument("--real-sample", action="store_true", help="Include real sample in weekly release run.")
    weekly_parser.add_argument(
        "--config-overlay",
        type=Path,
        default=None,
        help="Optional YAML config overlay passed through to run_pilot_validation.py.",
    )
    weekly_parser.add_argument("--rebalance-triggered", action="store_true", help="Set rebalance_triggered=true in dashboard row.")
    weekly_parser.add_argument("--incident-id", default="", help="Optional incident ID to link this row.")
    weekly_parser.add_argument("--notes", default="", help="Optional notes for dashboard row.")
    weekly_parser.add_argument(
        "--output-dir",
        type=Path,
        default=TRACKING_DIR,
        help="Tracking output directory (default: outputs/pilot_tracking).",
    )

    collect_parser = subparsers.add_parser("collect-fills", help="Collect Alpaca paper-trading fills and telemetry.")
    collect_parser.add_argument("--run-root", type=Path, default=None, help="Existing validation run root containing samples/*/approval/final_orders_oms.csv.")
    collect_parser.add_argument("--orders-oms", type=Path, default=None, help="Direct OMS basket input CSV path.")
    collect_parser.add_argument("--market", choices=["us"], default="us", help="Collection market (fixed to us).")
    collect_parser.add_argument("--broker", choices=["alpaca"], default="alpaca", help="Broker (fixed to alpaca).")
    collect_parser.add_argument("--timeout-seconds", type=float, default=300.0, help="Order polling timeout in seconds.")
    collect_parser.add_argument("--poll-interval-seconds", type=float, default=1.0, help="Order status poll interval in seconds.")
    collect_parser.add_argument("--notes", default="", help="Optional notes for the collection manifest.")
    collect_parser.add_argument(
        "--force-outside-market-hours",
        action="store_true",
        help="Allow collection when the US market is not open.",
    )
    collect_parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUTS_DIR / "alpaca_fill_collection",
        help="Parent output directory for alpaca fill collection runs.",
    )

    minimal_buy_parser = subparsers.add_parser(
        "collect-fills-minimal-buy",
        help="Submit one deterministic market buy order (SPY, fallback AAPL) during US regular market hours.",
    )
    minimal_buy_parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUTS_DIR / "alpaca_fill_collection",
        help="Parent output directory for the minimal buy validation run.",
    )
    minimal_buy_parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=60.0,
        help="Order polling timeout in seconds.",
    )
    minimal_buy_parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=1.0,
        help="Order status poll interval in seconds.",
    )
    minimal_buy_parser.add_argument("--notes", default="", help="Optional notes for the validation manifest.")

    batch_plan_parser = subparsers.add_parser(
        "generate-fill-collection-batch",
        help="Generate an auditable fill-collection batch plus orders_oms.csv from market ADV buckets.",
    )
    batch_plan_parser.add_argument(
        "--market-file",
        type=Path,
        required=True,
        help="Market snapshot CSV with ticker, price, adv_shares, and optional tradable columns.",
    )
    batch_plan_parser.add_argument(
        "--output-dir",
        type=Path,
        default=FILL_COLLECTION_BATCH_DIR,
        help="Parent output directory for generated batch artifacts.",
    )
    batch_plan_parser.add_argument(
        "--participation-buckets",
        default=DEFAULT_FILL_COLLECTION_PARTICIPATION_BUCKETS,
        help="Comma-separated participation targets as ratios or percentages, for example 0.01%,0.1%,1%,5%.",
    )
    batch_plan_parser.add_argument(
        "--orders-per-bucket",
        type=int,
        default=3,
        help="How many deterministic tickers to generate for each bucket-side combination.",
    )
    batch_plan_parser.add_argument(
        "--side-scope",
        choices=["buy-only", "sell-only", "both"],
        default="both",
        help="Which sides to generate. both is the default for bucket-coverage batch planning.",
    )
    batch_plan_parser.add_argument(
        "--broker-positions-file",
        type=Path,
        default=None,
        help="Optional CSV with ticker and shares/quantity columns; sell-side generation is restricted to these positions.",
    )
    batch_plan_parser.add_argument(
        "--buying-power",
        type=float,
        default=None,
        help="Optional buy-side total notional budget; buy bucket generation stops when the budget is exhausted.",
    )
    batch_plan_parser.add_argument(
        "--sample-id",
        default="fill_collection_batch",
        help="Sample id to write into the generated orders_oms.csv.",
    )
    batch_plan_parser.add_argument("--notes", default="", help="Optional notes for the batch manifest.")

    campaign_parser = subparsers.add_parser(
        "collect-fills-campaign",
        help="Run a bucket-targeted Alpaca fill collection campaign for slippage calibration coverage.",
    )
    campaign_parser.add_argument("--bucket-plan-file", type=Path, required=True, help="YAML bucket plan file.")
    campaign_parser.add_argument("--output-dir", type=Path, required=True, help="Campaign output directory.")
    campaign_parser.add_argument("--market", choices=["us"], default="us", help="Collection market (fixed to us).")
    campaign_parser.add_argument("--broker", choices=["alpaca"], default="alpaca", help="Broker (fixed to alpaca).")
    campaign_parser.add_argument(
        "--campaign-preset",
        choices=["coverage", "seed-inventory", "reduce-positions"],
        default="coverage",
        help=(
            "Collection preset. coverage keeps the bucket campaign behavior; seed-inventory forces buy-only, "
            "uses a small deterministic budget, and is meant to create the first real inventory; "
            "reduce-positions forces sell-only and is meant to release buying power from current positions."
        ),
    )
    campaign_parser.add_argument("--run-root", type=Path, default=None, help="Optional candidate basket source run root.")
    campaign_parser.add_argument(
        "--source-orders-oms",
        type=Path,
        nargs="*",
        default=None,
        help="Optional direct OMS basket sources.",
    )
    campaign_parser.add_argument("--timeout-seconds", type=float, default=300.0, help="Order polling timeout in seconds.")
    campaign_parser.add_argument("--poll-interval-seconds", type=float, default=1.0, help="Order status poll interval in seconds.")
    campaign_parser.add_argument("--max-runs", type=int, default=20, help="Maximum number of collection runs.")
    campaign_parser.add_argument(
        "--max-seed-notional",
        type=float,
        default=1000.0,
        help="Upper cap for seed-inventory buy notional before the broker safety factor is applied.",
    )
    campaign_parser.add_argument(
        "--max-seed-orders",
        type=int,
        default=3,
        help="Maximum number of buy orders to keep in seed-inventory mode.",
    )
    campaign_parser.add_argument(
        "--side-scope",
        choices=["buy-only", "sell-only", "both"],
        default="buy-only",
        help=(
            "Which sides to collect. buy-only is the default and is safest for accounts without inventory; "
            "sell-only requires matching positions; both should only be used when current broker state is known. "
            "reduce-positions preset forces sell-only regardless of this flag."
        ),
    )
    campaign_parser.add_argument(
        "--force-outside-market-hours",
        action="store_true",
        help="Allow campaign execution when the US market is not open.",
    )
    campaign_parser.add_argument("--notes", default="", help="Optional notes for the campaign manifest.")

    pre_submit_parser = subparsers.add_parser(
        "pre-submit-check",
        help="Audit one US OMS basket against current or saved Alpaca broker state without submitting orders.",
    )
    pre_submit_parser.add_argument("--run-root", type=Path, default=None, help="Existing validation run root containing samples/*/approval/final_orders_oms.csv.")
    pre_submit_parser.add_argument("--orders-oms", type=Path, default=None, help="Direct OMS basket input CSV path.")
    pre_submit_parser.add_argument("--sample-id", default=None, help="Optional sample_id filter when --run-root contains multiple baskets.")
    pre_submit_parser.add_argument("--market", choices=["us"], default="us", help="Precheck market (fixed to us).")
    pre_submit_parser.add_argument("--broker", choices=["alpaca"], default="alpaca", help="Broker (fixed to alpaca).")
    pre_submit_parser.add_argument(
        "--broker-state-snapshot",
        type=Path,
        default=None,
        help="Optional JSON snapshot from inspect-broker-state or off-hours-prep; skips live broker queries.",
    )
    pre_submit_parser.add_argument(
        "--output-dir",
        type=Path,
        default=PRE_SUBMISSION_CHECK_DIR,
        help="Output directory for pre-submission check runs.",
    )
    pre_submit_parser.add_argument("--notes", default="", help="Optional notes for the pre-submission report.")

    inspect_parser = subparsers.add_parser(
        "inspect-broker-state",
        help="Inspect Alpaca broker state and write an auditable route recommendation report.",
    )
    inspect_parser.add_argument(
        "--output-dir",
        type=Path,
        default=BROKER_STATE_INSPECTION_DIR,
        help="Output directory for broker state inspection runs.",
    )
    inspect_parser.add_argument("--notes", default="", help="Optional notes for the broker state report.")

    off_hours_prep_parser = subparsers.add_parser(
        "off-hours-prep",
        help="Read-only off-hours prep: audit broker state, plan tomorrow's minimal validation, and stage slippage calibration inputs.",
    )
    off_hours_prep_parser.add_argument(
        "--output-dir",
        type=Path,
        default=OFF_HOURS_PREP_DIR,
        help="Output directory for off-hours prep runs.",
    )
    off_hours_prep_parser.add_argument(
        "--bucket-plan-file",
        type=Path,
        default=None,
        help="Optional bucket plan file; defaults to the latest outputs/alpaca_fill_campaign bucket plan.",
    )
    off_hours_prep_parser.add_argument(
        "--max-seed-notional",
        type=float,
        default=1.0,
        help="Recommended minimal seed notional for the next regular-hours validation.",
    )
    off_hours_prep_parser.add_argument(
        "--max-seed-orders",
        type=int,
        default=1,
        help="Recommended maximum number of buy orders for the next regular-hours validation.",
    )
    off_hours_prep_parser.add_argument("--notes", default="", help="Optional notes for the off-hours prep report.")

    calibrate_parser = subparsers.add_parser(
        "calibrate-slippage",
        help="Run the slippage calibration / TCA workflow on an auditable fill-collection root.",
    )
    calibrate_parser.add_argument(
        "--fill-collection-root",
        type=Path,
        required=True,
        help="Root directory containing one or more alpaca fill collection run directories.",
    )
    calibrate_parser.add_argument(
        "--source-run-root",
        type=Path,
        default=None,
        help="Optional frozen source run root used to recover market/audit context.",
    )
    calibrate_parser.add_argument(
        "--output-dir",
        type=Path,
        default=slippage_calibration.DEFAULT_OUTPUT_ROOT,
        help="Output directory for TCA artifacts.",
    )
    calibrate_parser.add_argument(
        "--alpha",
        type=float,
        default=0.6,
        help="Fixed participation exponent used when fitting k.",
    )
    calibrate_parser.add_argument(
        "--min-filled-orders",
        type=int,
        default=20,
        help="Minimum filled-order count required for strong-readiness conclusions.",
    )
    calibrate_parser.add_argument(
        "--min-participation-span",
        type=float,
        default=10.0,
        help="Minimum participation span percentage required for strong-readiness conclusions.",
    )

    go_nogo_parser = subparsers.add_parser("go-nogo", help="Build go/no-go status report from dashboard + incidents.")
    go_nogo_parser.add_argument(
        "--output-dir",
        type=Path,
        default=TRACKING_DIR,
        help="Tracking output directory for default dashboard/incident paths.",
    )
    go_nogo_parser.add_argument("--window-trading-days", type=int, default=20, help="Rolling window size in trading days.")
    go_nogo_parser.add_argument("--as-of-date", default=datetime.now().strftime("%Y-%m-%d"), help="Window end date in YYYY-MM-DD.")
    go_nogo_parser.add_argument("--dashboard-path", type=Path, default=DASHBOARD_PATH, help="Input pilot dashboard CSV path.")
    go_nogo_parser.add_argument("--incident-path", type=Path, default=INCIDENT_PATH, help="Input incident register CSV path.")
    go_nogo_parser.add_argument("--output", type=Path, default=TRACKING_DIR / "go_nogo_status.md", help="Output markdown path.")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "init":
        init_tracking(
            force=bool(args.force),
            output_dir=(Path(args.output_dir).resolve() if args.output_dir is not None else None),
        )
        return 0

    if args.command == "nightly":
        return run_ops(
            mode="nightly",
            phase=str(args.phase),
            reviewer_input=None,
            real_sample=bool(args.real_sample),
            rebalance_triggered=bool(args.rebalance_triggered),
            incident_id=str(args.incident_id),
            notes=str(args.notes),
            market=str(args.market),
            broker=str(args.broker),
            config_overlay=(Path(args.config_overlay).resolve() if args.config_overlay is not None else None),
            as_of_date=(str(args.as_of_date) if args.as_of_date is not None else None),
            output_dir=(Path(args.output_dir).resolve() if args.output_dir is not None else None),
            ab_flow=bool(args.ab_flow),
            require_eligibility_gate=bool(args.require_eligibility_gate),
        )

    if args.command == "weekly":
        reviewer_input = Path(args.reviewer_input).resolve()
        return run_ops(
            mode="release",
            phase=str(args.phase),
            reviewer_input=reviewer_input,
            real_sample=bool(args.real_sample),
            rebalance_triggered=bool(args.rebalance_triggered),
            incident_id=str(args.incident_id),
            notes=str(args.notes),
            market=str(args.market),
            broker=str(args.broker),
            config_overlay=(Path(args.config_overlay).resolve() if args.config_overlay is not None else None),
            as_of_date=None,
            output_dir=(Path(args.output_dir).resolve() if args.output_dir is not None else None),
            ab_flow=bool(args.ab_flow),
            require_eligibility_gate=bool(args.require_eligibility_gate),
        )

    if args.command == "go-nogo":
        default_dashboard_path, default_incident_path, _default_go_nogo_template_path, _weekly_dir = _resolve_tracking_paths(
            Path(args.output_dir).resolve() if args.output_dir is not None else None
        )
        generate_go_nogo_report(
            dashboard_path=(
                default_dashboard_path
                if Path(args.dashboard_path) == DASHBOARD_PATH
                else Path(args.dashboard_path).resolve()
            ),
            incident_path=(
                default_incident_path
                if Path(args.incident_path) == INCIDENT_PATH
                else Path(args.incident_path).resolve()
            ),
            output_path=Path(args.output).resolve(),
            as_of_date=str(args.as_of_date),
            window_trading_days=int(args.window_trading_days),
        )
        return 0

    if args.command == "generate-fill-collection-batch":
        market_file = Path(args.market_file).resolve()
        market_frame = _load_fill_collection_batch_market_frame(market_file)
        broker_positions_frame = (
            _load_broker_positions_frame(Path(args.broker_positions_file).resolve())
            if args.broker_positions_file is not None
            else None
        )
        participation_buckets = _parse_participation_bucket_values(str(args.participation_buckets))
        batch_frame, orders_frame, summary = _generate_fill_collection_batch_plan(
            market_frame=market_frame,
            participation_buckets=participation_buckets,
            orders_per_bucket=int(args.orders_per_bucket),
            side_scope=str(args.side_scope),
            sample_id=str(args.sample_id),
            broker_positions_frame=broker_positions_frame,
            buying_power=(float(args.buying_power) if args.buying_power is not None else None),
        )
        paths = _write_fill_collection_batch_artifacts(
            output_dir=Path(args.output_dir).resolve(),
            market_file=market_file,
            batch_frame=batch_frame,
            orders_frame=orders_frame,
            summary=summary,
            notes=str(args.notes),
        )
        print(f"fill_collection_batch_run_dir: {paths['run_dir']}")
        print(f"fill_collection_batch_csv: {paths['fill_collection_batch_csv']}")
        print(f"orders_oms_csv: {paths['orders_oms_csv']}")
        print(f"fill_collection_batch_manifest_json: {paths['fill_collection_batch_manifest_json']}")
        print(f"fill_collection_batch_report_md: {paths['fill_collection_batch_report_md']}")
        print(f"generated_order_count: {summary['generated_order_count']}")
        print(f"side_scope: {summary['side_scope']}")
        print(f"broker_positions_source: {summary['broker_positions_source']}")
        print(f"buying_power_budget: {summary['buying_power_budget']}")
        return 0

    if args.command == "collect-fills":
        if args.orders_oms is None and args.run_root is None:
            raise ValueError("collect-fills requires either --orders-oms or --run-root.")
        _preflight_alpaca_credentials()
        _run_alpaca_fill_collection(
            run_root=(Path(args.run_root).resolve() if args.run_root is not None else None),
            orders_oms=(Path(args.orders_oms).resolve() if args.orders_oms is not None else None),
            output_dir=Path(args.output_dir).resolve(),
            market=str(args.market),
            broker=str(args.broker),
            timeout_seconds=float(args.timeout_seconds),
            poll_interval_seconds=float(args.poll_interval_seconds),
            notes=str(args.notes),
            force_outside_market_hours=bool(args.force_outside_market_hours),
        )
        return 0

    if args.command == "collect-fills-minimal-buy":
        _preflight_alpaca_credentials()
        account_payload = _collect_alpaca_broker_state_snapshot()
        broker_state_summary_before = fill_collection_campaign._broker_state_snapshot_summary(account_payload)
        account_before = dict(account_payload.get("account") or {})
        positions_before = broker_state_summary_before.get("positions", [])
        positions_before_df = pd.DataFrame(list(positions_before))
        open_orders_before_df = pd.DataFrame(list(broker_state_summary_before.get("open_orders", [])))
        _validate_minimal_buy_validation_precheck(
            account_payload=account_before,
            positions=positions_before_df,
            open_orders=open_orders_before_df,
        )
        selection_audit = _select_minimal_buy_validation_candidate(account_payload=account_before)
        orders_oms_path = _build_minimal_buy_validation_orders_oms(
            output_dir=Path(args.output_dir).resolve() / "_minimal_buy_validation_inputs",
            ticker=str(selection_audit["ticker"]),
            estimated_price=float(selection_audit["estimated_price"]),
        )
        run_dir = _run_alpaca_fill_collection(
            run_root=None,
            orders_oms=orders_oms_path,
            output_dir=Path(args.output_dir).resolve(),
            market="us",
            broker="alpaca",
            timeout_seconds=float(args.timeout_seconds),
            poll_interval_seconds=float(args.poll_interval_seconds),
            notes=str(args.notes),
            force_outside_market_hours=False,
        )
        account_after_payload = _collect_alpaca_broker_state_snapshot()
        broker_state_summary_after = fill_collection_campaign._broker_state_snapshot_summary(account_after_payload)
        account_after = dict(account_after_payload.get("account") or {})
        positions_after = broker_state_summary_after.get("positions", [])
        positions_after_df = pd.DataFrame(list(positions_after))
        open_orders_after_df = pd.DataFrame(list(broker_state_summary_after.get("open_orders", [])))
        manifest = json.loads((run_dir / "alpaca_fill_manifest.json").read_text(encoding="utf-8"))
        orders_frame = pd.read_csv(run_dir / "alpaca_fill_orders.csv")
        events_frame = pd.read_csv(run_dir / "alpaca_fill_events.csv")
        validation_plan_file = _resolve_latest_validation_plan_file()
        slippage_checklist = SLIPPAGE_CALIBRATION_PREP_DIR / "slippage_calibration_prep_checklist.md"
        payload = _build_minimal_buy_validation_payload(
            run_dir=run_dir,
            account_before=account_before,
            account_after=account_after,
            positions_before=positions_before_df,
            positions_after=positions_after_df,
            open_orders_before=open_orders_before_df,
            open_orders_after=open_orders_after_df,
            manifest=manifest,
            orders_frame=orders_frame,
            events_frame=events_frame,
            selection_audit=selection_audit,
            validation_plan_path=validation_plan_file,
            slippage_prep_checklist_path=slippage_checklist if slippage_checklist.exists() else None,
        )
        paths = _write_minimal_buy_validation_artifacts(run_dir=run_dir, payload=payload)
        print(f"alpaca_fill_run_dir: {run_dir}")
        for name, path in paths.items():
            print(f"{name}: {path}")
        print(f"selected_ticker: {payload['selected_ticker']}")
        print(f"submitted_order_count: {payload['submitted_order_count']}")
        print(f"filled_order_count: {payload['filled_order_count']}")
        print(f"recommendation: {payload['recommendation']}")
        return 0

    if args.command == "collect-fills-campaign":
        _preflight_alpaca_credentials()
        campaign_preset = str(args.campaign_preset).strip().lower()
        side_scope = str(args.side_scope).strip().lower()
        if campaign_preset == "seed-inventory" and side_scope != "buy-only":
            raise ValueError("seed-inventory preset requires --side-scope buy-only.")
        broker_state_snapshot = _collect_alpaca_broker_state_snapshot()
        campaign_root = fill_collection_campaign.run_fill_collection_campaign(
            bucket_plan_file=Path(args.bucket_plan_file).resolve(),
            output_dir=Path(args.output_dir).resolve(),
            run_root=(Path(args.run_root).resolve() if args.run_root is not None else None),
            source_orders_oms=[Path(path).resolve() for path in (args.source_orders_oms or [])],
            timeout_seconds=float(args.timeout_seconds),
            poll_interval_seconds=float(args.poll_interval_seconds),
            max_runs=int(args.max_runs),
            force_outside_market_hours=bool(args.force_outside_market_hours),
            notes=str(args.notes),
            campaign_preset=campaign_preset,
            side_scope=side_scope,
            broker_state_snapshot=broker_state_snapshot,
            max_seed_notional=float(args.max_seed_notional),
            max_seed_orders=int(args.max_seed_orders),
            market=str(args.market),
            broker=str(args.broker),
            collection_runner=_run_alpaca_fill_collection,
        )
        if campaign_preset == "reduce-positions":
            after_snapshot = _collect_alpaca_broker_state_snapshot()
            manifest_path = campaign_root / "alpaca_fill_campaign_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            transition_payload = _build_broker_state_transition_payload(
                before_snapshot=broker_state_snapshot,
                after_snapshot=after_snapshot,
                campaign_manifest=manifest,
                notes=str(args.notes),
            )
            _write_broker_state_transition_artifacts(output_dir=campaign_root, payload=transition_payload)
            print(f"broker_state_report: {campaign_root / 'broker_state_report.md'}")
            print(f"broker_state_report_json: {campaign_root / 'broker_state_report.json'}")
            print(f"recommended_next_action: {transition_payload['recommended_next_action']}")
        print(f"campaign_run_dir: {campaign_root}")
        return 0

    if args.command == "pre-submit-check":
        if args.orders_oms is None and args.run_root is None:
            raise ValueError("pre-submit-check requires either --orders-oms or --run-root.")
        snapshot_path = Path(args.broker_state_snapshot).resolve() if args.broker_state_snapshot is not None else None
        if snapshot_path is not None:
            broker_state_snapshot = _load_broker_state_snapshot_file(snapshot_path)
            snapshot_source = "file"
        else:
            _preflight_alpaca_credentials()
            broker_state_snapshot = _collect_alpaca_broker_state_snapshot()
            snapshot_source = "live"
        sources, _source_type, _source_path = _load_fill_sources(
            run_root=(Path(args.run_root).resolve() if args.run_root is not None else None),
            orders_oms=(Path(args.orders_oms).resolve() if args.orders_oms is not None else None),
        )
        if args.sample_id is not None:
            requested_sample_id = str(args.sample_id).strip()
            sources = [source for source in sources if str(source.sample_id).strip() == requested_sample_id]
            if not sources:
                raise ValueError(f"No basket matched --sample-id {requested_sample_id!r}.")
        payload, prepared_frames = _build_pre_submission_check_payload(
            sources=sources,
            broker_state_snapshot=broker_state_snapshot,
            snapshot_source=snapshot_source,
            snapshot_path=snapshot_path,
            notes=str(args.notes),
        )
        run_dir = _write_pre_submission_check_artifacts(
            output_dir=Path(args.output_dir).resolve(),
            payload=payload,
            prepared_frames=prepared_frames,
        )
        print(f"pre_submission_check_report: {run_dir / 'pre_submission_check.md'}")
        print(f"pre_submission_check_json: {run_dir / 'pre_submission_check.json'}")
        print(f"basket_precheck_summary: {run_dir / 'basket_precheck_summary.csv'}")
        print(f"prepared_orders_dir: {run_dir / 'prepared_orders'}")
        print(f"overall_recommendation: {payload['overall_recommendation']}")
        return 0

    if args.command == "inspect-broker-state":
        _preflight_alpaca_credentials()
        broker_state_snapshot = _collect_alpaca_broker_state_snapshot()
        run_dir = _write_broker_state_inspection_artifacts(
            output_dir=Path(args.output_dir).resolve(),
            snapshot=broker_state_snapshot,
            notes=str(args.notes),
        )
        payload = _build_broker_state_inspection_payload(broker_state_snapshot, notes=str(args.notes))
        print(f"broker_state_report: {run_dir / 'broker_state_report.md'}")
        print(f"broker_state_report_json: {run_dir / 'broker_state_report.json'}")
        print(f"recommended_next_action: {payload['recommended_next_action']}")
        return 0

    if args.command == "off-hours-prep":
        _preflight_alpaca_credentials()
        broker_state_snapshot = _collect_alpaca_broker_state_snapshot()
        bucket_plan_file = _resolve_latest_bucket_plan_file(
            Path(args.bucket_plan_file).resolve() if args.bucket_plan_file is not None else None
        )
        payload = _build_off_hours_prep_payload(
            broker_state_snapshot,
            notes=str(args.notes),
            bucket_plan_file=bucket_plan_file,
            max_seed_notional=float(args.max_seed_notional),
            max_seed_orders=int(args.max_seed_orders),
            output_dir=Path(args.output_dir).resolve(),
        )
        run_dir = _write_off_hours_prep_artifacts(output_dir=Path(args.output_dir).resolve(), payload=payload)
        print(f"off_hours_prep_report: {run_dir / 'off_hours_prep_report.md'}")
        print(f"off_hours_prep_manifest_json: {run_dir / 'off_hours_prep_manifest.json'}")
        print(f"broker_state_report: {run_dir / 'broker_state_report.md'}")
        print(f"tomorrow_minimal_validation_plan: {run_dir / 'tomorrow_minimal_validation_plan.yaml'}")
        print(f"slippage_calibration_prep: {payload['slippage_calibration_prep'].get('slippage_calibration_prep_checklist', '')}")
        print(f"recommended_next_action: {payload['recommended_next_action']}")
        return 0

    if args.command == "calibrate-slippage":
        result = slippage_calibration.calibrate_slippage(
            fill_collection_root=Path(args.fill_collection_root).resolve(),
            output_dir=Path(args.output_dir).resolve(),
            source_run_root=(Path(args.source_run_root).resolve() if args.source_run_root is not None else None),
            alpha=float(args.alpha),
            min_filled_orders=int(args.min_filled_orders),
            min_participation_span=float(args.min_participation_span),
            update_default_config=False,
        )
        paths = slippage_calibration.write_slippage_calibration_artifacts(
            result=result,
            output_dir=Path(args.output_dir).resolve(),
        )
        for name, path in paths.items():
            print(f"{name}: {path}")
        print(f"overlay_readiness: {result.summary.get('overlay_readiness', '')}")
        print(f"next_recommended_action: {result.summary.get('next_recommended_action', '')}")
        print(f"candidate_k: {result.summary.get('candidate_k', None)}")
        return 0

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
