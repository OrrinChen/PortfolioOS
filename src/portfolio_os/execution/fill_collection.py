"""Utilities for Alpaca fill telemetry collection and artifact writing."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd

from portfolio_os.execution.models import ExecutionResult, ReconciliationReport


ET = ZoneInfo("America/New_York")
DEFAULT_EVENT_GRANULARITY = "polled_history"


@dataclass
class FillSourceBasket:
    """One normalized order basket source."""

    sample_id: str
    source_path: Path
    orders: pd.DataFrame


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(parsed):
        return default
    return float(parsed)


def _normalized_status(raw_status: Any) -> str:
    status = str(raw_status or "").strip().lower()
    if "." in status:
        status = status.split(".")[-1].strip()
    if "partial" in status:
        return "partially_filled"
    if status in {"timeoutcancelled", "timeout_cancelled", "timeout-cancelled"}:
        return "timeout_cancelled"
    if status in {"filled", "partially_filled", "timeout_cancelled", "canceled", "rejected", "expired"}:
        return status
    return "unknown"


def _read_orders_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, encoding="utf-8-sig")


def _normalize_order_source_frame(frame: pd.DataFrame, *, sample_id: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "sample_id",
                "ticker",
                "direction",
                "requested_qty",
                "reference_price",
                "estimated_price",
                "price_limit",
                "source_path",
            ]
        )

    work = frame.copy()
    work["sample_id"] = work.get("sample_id", sample_id)
    work["sample_id"] = work["sample_id"].astype(str).str.strip().replace({"": sample_id})
    if "ticker" not in work.columns and "symbol" in work.columns:
        work["ticker"] = work["symbol"]
    if "direction" not in work.columns and "side" in work.columns:
        work["direction"] = work["side"]
    work["ticker"] = work["ticker"].astype(str).str.strip()
    work["direction"] = work["direction"].astype(str).str.strip().str.lower()
    if "quantity" in work.columns and "requested_qty" not in work.columns:
        work["requested_qty"] = work["quantity"]
    if "requested_qty" not in work.columns:
        work["requested_qty"] = pd.NA
    if "reference_price" not in work.columns:
        work["reference_price"] = work.get("estimated_price", pd.NA)
    if "estimated_price" not in work.columns:
        work["estimated_price"] = pd.NA
    if "price_limit" not in work.columns:
        work["price_limit"] = pd.NA
    if "extended_hours" not in work.columns:
        work["extended_hours"] = False
    work["requested_qty"] = pd.to_numeric(work["requested_qty"], errors="coerce")
    work["reference_price"] = pd.to_numeric(work["reference_price"], errors="coerce")
    work["estimated_price"] = pd.to_numeric(work["estimated_price"], errors="coerce")
    work["price_limit"] = pd.to_numeric(work["price_limit"], errors="coerce")
    work["extended_hours"] = work["extended_hours"].apply(
        lambda value: False
        if pd.isna(value)
        else str(value).strip().lower() in {"true", "1", "yes", "y", "on"}
    )
    if "source_path" not in work.columns:
        work["source_path"] = ""
    normalized = work[
        [
            "sample_id",
            "ticker",
            "direction",
            "requested_qty",
            "reference_price",
            "estimated_price",
            "price_limit",
            "extended_hours",
            "source_path",
        ]
    ].copy()
    normalized["requested_qty"] = pd.to_numeric(normalized["requested_qty"], errors="coerce").fillna(0.0)
    normalized["reference_price"] = pd.to_numeric(normalized["reference_price"], errors="coerce")
    normalized["estimated_price"] = pd.to_numeric(normalized["estimated_price"], errors="coerce")
    normalized["price_limit"] = pd.to_numeric(normalized["price_limit"], errors="coerce")
    normalized["extended_hours"] = normalized["extended_hours"].astype(bool)
    normalized["ticker"] = normalized["ticker"].astype(str).str.strip()
    normalized["direction"] = normalized["direction"].astype(str).str.strip().str.lower()
    normalized = normalized.loc[normalized["requested_qty"] > 0].reset_index(drop=True)
    return normalized


def load_orders_from_run_root(run_root: Path) -> list[FillSourceBasket]:
    sources: list[FillSourceBasket] = []
    approval_dirs = sorted(run_root.glob("samples/*/approval"))
    for approval_dir in approval_dirs:
        orders_path = approval_dir / "final_orders_oms.csv"
        if not orders_path.exists():
            continue
        sample_id = approval_dir.parent.name
        frame = _read_orders_frame(orders_path)
        normalized = _normalize_order_source_frame(frame, sample_id=sample_id)
        normalized["source_path"] = str(orders_path)
        sources.append(FillSourceBasket(sample_id=sample_id, source_path=orders_path, orders=normalized))
    return sources


def load_orders_from_orders_oms(path: Path) -> list[FillSourceBasket]:
    frame = _read_orders_frame(path)
    if not frame.empty and "sample_id" in frame.columns:
        sample_id = str(frame["sample_id"].astype(str).str.strip().replace({"": path.stem}).iloc[0])
    else:
        sample_id = path.stem
    normalized = _normalize_order_source_frame(frame, sample_id=sample_id)
    normalized["source_path"] = str(path)
    return [FillSourceBasket(sample_id=sample_id, source_path=path, orders=normalized)]


def is_us_market_open(*, clock_payload: dict[str, Any] | None = None, now_utc: datetime | None = None) -> bool:
    if clock_payload is not None:
        raw_is_open = clock_payload.get("is_open")
        if isinstance(raw_is_open, bool):
            return raw_is_open
        if str(raw_is_open).strip().lower() in {"true", "1", "yes"}:
            return True
        if str(raw_is_open).strip().lower() in {"false", "0", "no"}:
            return False

    current_utc = now_utc or datetime.now(timezone.utc)
    current_et = current_utc.astimezone(ET)
    if current_et.weekday() >= 5:
        return False
    start = dt_time(9, 30)
    end = dt_time(16, 0)
    return start <= current_et.time() <= end


def generate_run_id(*, source_type: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{source_type}_{timestamp}_{uuid4().hex[:8]}"


def _fill_price_for_row(row: dict[str, Any]) -> tuple[float | None, bool]:
    avg_fill = _safe_float(row.get("avg_fill_price"), None)
    fallback_used = False
    if avg_fill is None:
        avg_fill = _safe_float(row.get("estimated_price"), None)
        fallback_used = avg_fill is not None
    return avg_fill, fallback_used


def build_fill_orders_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "sample_id",
        "ticker",
        "direction",
        "requested_qty",
        "filled_qty",
        "avg_fill_price",
        "reference_price",
        "estimated_price",
        "requested_notional",
        "filled_notional",
        "fill_ratio",
        "status",
        "reject_reason",
        "broker_order_id",
        "submitted_at_utc",
        "terminal_at_utc",
        "latency_seconds",
        "poll_count",
        "timeout_cancelled",
        "cancel_requested",
        "cancel_acknowledged",
        "avg_fill_price_fallback_used",
        "status_history",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)
    frame = pd.DataFrame(rows).copy()
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    frame = frame[columns]
    return frame


def build_fill_events_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "sample_id",
        "ticker",
        "broker_order_id",
        "event_at_utc",
        "status",
        "filled_qty",
        "filled_avg_price",
        "reject_reason",
        "event_type",
    ]
    events: list[dict[str, Any]] = []
    for row in rows:
        history = row.get("status_history") or []
        if isinstance(history, list) and history:
            for event in history:
                events.append(
                    {
                        "sample_id": row.get("sample_id"),
                        "ticker": row.get("ticker"),
                        "broker_order_id": row.get("broker_order_id"),
                        "event_at_utc": event.get("event_at_utc"),
                        "status": _normalized_status(event.get("status")),
                        "filled_qty": _safe_float(event.get("filled_qty"), None),
                        "filled_avg_price": _safe_float(event.get("filled_avg_price"), None),
                        "reject_reason": event.get("reject_reason"),
                        "event_type": event.get("event_type", "poll"),
                    }
                )
        else:
            events.append(
                {
                    "sample_id": row.get("sample_id"),
                    "ticker": row.get("ticker"),
                    "broker_order_id": row.get("broker_order_id"),
                    "event_at_utc": row.get("terminal_at_utc") or row.get("submitted_at_utc") or _utc_now_iso(),
                    "status": _normalized_status(row.get("status")),
                    "filled_qty": _safe_float(row.get("filled_qty"), None),
                    "filled_avg_price": _safe_float(row.get("avg_fill_price"), None),
                    "reject_reason": row.get("reject_reason"),
                    "event_type": "terminal",
                }
            )
    if not events:
        return pd.DataFrame(columns=columns)
    frame = pd.DataFrame(events)
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame[columns]


def build_fill_manifest(
    *,
    run_id: str,
    created_at: str,
    market: str,
    broker: str,
    notes: str,
    source_type: str,
    source_path: str,
    requested_order_rows: pd.DataFrame,
    filled_order_rows: pd.DataFrame,
    execution_result: ExecutionResult,
    latency_values: list[float],
    avg_fill_price_fallback_used: bool,
    event_granularity: str = DEFAULT_EVENT_GRANULARITY,
) -> dict[str, Any]:
    order_count = int(len(requested_order_rows))
    submitted_count = int(execution_result.submitted_count)
    filled_count = int(execution_result.filled_count)
    partial_count = int(execution_result.partial_count)
    unfilled_count = int(execution_result.unfilled_count)
    rejected_count = int(execution_result.rejected_count)
    timeout_cancelled_count = int(getattr(execution_result, "timeout_cancelled_count", 0))
    if not filled_order_rows.empty and "status" in filled_order_rows.columns:
        status_counts = filled_order_rows["status"].astype(str).str.strip().str.lower().value_counts(dropna=False)
        status_mix = {str(key): int(value) for key, value in status_counts.items()}
    else:
        status_mix = {}
    if "filled_qty" in filled_order_rows.columns and not filled_order_rows.empty:
        filled_mask = pd.to_numeric(filled_order_rows["filled_qty"], errors="coerce").fillna(0.0) > 0
        total_filled_qty = float(
            pd.to_numeric(filled_order_rows.loc[filled_mask, "filled_qty"], errors="coerce").fillna(0.0).sum()
        )
        total_filled_notional = float(
            pd.to_numeric(filled_order_rows.loc[filled_mask, "filled_notional"], errors="coerce").fillna(0.0).sum()
        )
        total_requested_notional = float(
            pd.to_numeric(requested_order_rows["requested_notional"], errors="coerce").fillna(0.0).sum()
        )
        avg_fill_price_mean = (
            float(total_filled_notional / total_filled_qty) if total_filled_qty > 0 else None
        )
        has_any_filled_orders = bool(filled_mask.any())
    else:
        total_requested_notional = float(
            pd.to_numeric(requested_order_rows.get("requested_notional"), errors="coerce").fillna(0.0).sum()
            if not requested_order_rows.empty and "requested_notional" in requested_order_rows.columns
            else 0.0
        )
        total_filled_notional = 0.0
        avg_fill_price_mean = None
        has_any_filled_orders = False
    fill_rate = float(filled_count / submitted_count) if submitted_count > 0 else None
    latency_series = pd.Series(latency_values, dtype="float64") if latency_values else pd.Series(dtype="float64")
    latency_seconds_mean = float(latency_series.mean()) if not latency_series.empty else None
    latency_seconds_p50 = float(latency_series.quantile(0.5)) if not latency_series.empty else None
    latency_seconds_p95 = float(latency_series.quantile(0.95)) if not latency_series.empty else None
    return {
        "run_id": run_id,
        "created_at": created_at,
        "market": market,
        "broker": broker,
        "notes": notes,
        "source_type": source_type,
        "source_path": source_path,
        "order_count": order_count,
        "submitted_count": submitted_count,
        "filled_count": filled_count,
        "partial_count": partial_count,
        "unfilled_count": unfilled_count,
        "rejected_count": rejected_count,
        "timeout_cancelled_count": timeout_cancelled_count,
        "fill_rate": fill_rate,
        "total_requested_notional": total_requested_notional,
        "total_filled_notional": total_filled_notional,
        "avg_fill_price_mean": avg_fill_price_mean,
        "avg_fill_price_mean_definition": "sum(filled_notional) / sum(filled_qty) for rows with filled_qty > 0",
        "avg_fill_price_fallback_used": bool(avg_fill_price_fallback_used),
        "latency_seconds_mean": latency_seconds_mean,
        "latency_seconds_p50": latency_seconds_p50,
        "latency_seconds_p95": latency_seconds_p95,
        "status_mix": status_mix,
        "has_any_filled_orders": has_any_filled_orders,
        "event_granularity": event_granularity,
    }


def write_fill_collection_artifacts(
    *,
    output_dir: Path,
    manifest: dict[str, Any],
    order_rows: pd.DataFrame,
    event_rows: pd.DataFrame,
    account_before: dict[str, Any],
    account_after: dict[str, Any],
    positions_before: pd.DataFrame,
    positions_after: pd.DataFrame,
    execution_result: ExecutionResult,
    reconciliation_report: ReconciliationReport,
    execution_result_frame: pd.DataFrame | None = None,
    summary_markdown: str | None = None,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "alpaca_fill_manifest.json"
    orders_path = output_dir / "alpaca_fill_orders.csv"
    events_path = output_dir / "alpaca_fill_events.csv"
    account_before_path = output_dir / "broker_account_before.json"
    account_after_path = output_dir / "broker_account_after.json"
    positions_before_path = output_dir / "broker_positions_before.csv"
    positions_after_path = output_dir / "broker_positions_after.csv"
    reconciliation_path = output_dir / "reconciliation_report.json"
    execution_json_path = output_dir / "execution_result.json"
    execution_csv_path = output_dir / "execution_result.csv"

    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    order_rows.to_csv(orders_path, index=False, encoding="utf-8")
    event_rows.to_csv(events_path, index=False, encoding="utf-8")
    account_before_path.write_text(json.dumps(account_before, ensure_ascii=False, indent=2), encoding="utf-8")
    account_after_path.write_text(json.dumps(account_after, ensure_ascii=False, indent=2), encoding="utf-8")
    positions_before.to_csv(positions_before_path, index=False, encoding="utf-8")
    positions_after.to_csv(positions_after_path, index=False, encoding="utf-8")
    reconciliation_path.write_text(
        json.dumps(reconciliation_report.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    execution_json_path.write_text(json.dumps(execution_result.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    if execution_result_frame is None:
        order_rows.to_csv(execution_csv_path, index=False, encoding="utf-8")
    else:
        execution_result_frame.to_csv(execution_csv_path, index=False, encoding="utf-8")
    if summary_markdown is not None:
        (output_dir / "alpaca_fill_summary.md").write_text(summary_markdown, encoding="utf-8")
    return {
        "alpaca_fill_manifest": manifest_path,
        "alpaca_fill_orders": orders_path,
        "alpaca_fill_events": events_path,
        "broker_account_before": account_before_path,
        "broker_account_after": account_after_path,
        "broker_positions_before": positions_before_path,
        "broker_positions_after": positions_after_path,
        "reconciliation_report": reconciliation_path,
        "execution_result_json": execution_json_path,
        "execution_result_csv": execution_csv_path,
    }
