"""Dry-run helpers for the paper calibration sprint."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from portfolio_os.alpha.neutral_targets import (
    build_neutral_order_frame,
    build_neutral_target_frame,
    build_neutral_target_manifest,
)
from portfolio_os.execution import fill_collection
from portfolio_os.execution.models import ExecutionResult
from portfolio_os.execution.paper_calibration import (
    build_paper_calibration_payload,
    render_paper_calibration_report_markdown,
)
from portfolio_os.storage.runs import prepare_paper_calibration_artifacts
from portfolio_os.storage.snapshots import write_json, write_text


@dataclass
class PaperCalibrationDryRunResult:
    target_path: str
    manifest_path: str
    payload_path: str
    report_path: str


@dataclass
class PaperCalibrationPaperResult:
    target_path: str
    manifest_path: str
    payload_path: str
    report_path: str
    fill_manifest_path: str
    fill_orders_path: str
    reconciliation_report_path: str


def _requested_orders_frame(order_frame: pd.DataFrame, *, sample_id: str) -> pd.DataFrame:
    requested = order_frame.copy()
    requested["sample_id"] = sample_id
    requested["requested_qty"] = pd.to_numeric(requested["quantity"], errors="coerce").fillna(0.0)
    requested["reference_price"] = pd.NA
    requested["estimated_price"] = pd.NA
    requested["requested_notional"] = pd.NA
    return requested[
        [
            "sample_id",
            "ticker",
            "direction",
            "requested_qty",
            "reference_price",
            "estimated_price",
            "requested_notional",
        ]
    ].copy()


def _filled_order_rows(execution_result: ExecutionResult, *, sample_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for order in execution_result.orders:
        filled_notional = (
            float(order.filled_qty) * float(order.avg_fill_price)
            if order.avg_fill_price is not None
            else None
        )
        fill_ratio = float(order.filled_qty) / float(order.requested_qty) if float(order.requested_qty) > 0 else 0.0
        rows.append(
            {
                "sample_id": sample_id,
                "ticker": order.ticker,
                "direction": order.direction,
                "requested_qty": float(order.requested_qty),
                "filled_qty": float(order.filled_qty),
                "avg_fill_price": order.avg_fill_price,
                "reference_price": None,
                "estimated_price": None,
                "requested_notional": None,
                "filled_notional": filled_notional,
                "fill_ratio": fill_ratio,
                "status": order.status,
                "reject_reason": order.reject_reason,
                "broker_order_id": order.broker_order_id or order.order_id,
                "submitted_at_utc": order.submitted_at_utc,
                "terminal_at_utc": order.terminal_at_utc,
                "latency_seconds": None,
                "poll_count": int(order.poll_count),
                "timeout_cancelled": bool(order.timeout_cancelled),
                "cancel_requested": bool(order.cancel_requested),
                "cancel_acknowledged": bool(order.cancel_acknowledged),
                "avg_fill_price_fallback_used": False,
                "status_history": order.status_history,
            }
        )
    return rows


def run_paper_calibration_dry_run(
    *,
    output_dir: Path,
    tickers: list[str],
    gross_target_weight: float,
    perturbation_bps: float,
    perturbation_seed: int | None,
    expected_assumptions: dict[str, Any],
) -> PaperCalibrationDryRunResult:
    """Build deterministic dry-run artifacts for paper calibration."""

    artifacts = prepare_paper_calibration_artifacts(output_dir)
    target_frame = build_neutral_target_frame(
        tickers=tickers,
        gross_target_weight=gross_target_weight,
        perturbation_bps=perturbation_bps,
        perturbation_seed=perturbation_seed,
    )
    manifest = build_neutral_target_manifest(
        target_frame=target_frame,
        strategy_name="neutral_buy_and_hold",
        perturbation_bps=perturbation_bps,
        perturbation_seed=perturbation_seed,
    )
    payload = build_paper_calibration_payload(
        strategy_name="neutral_buy_and_hold",
        target_manifest=manifest,
        execution_result=ExecutionResult(),
        expected_assumptions=expected_assumptions,
    )

    Path(artifacts.target_path).parent.mkdir(parents=True, exist_ok=True)
    target_frame.to_csv(artifacts.target_path, index=False)
    write_json(artifacts.manifest_path, manifest)
    write_json(artifacts.payload_path, payload)
    write_text(artifacts.report_path, render_paper_calibration_report_markdown(payload))

    return PaperCalibrationDryRunResult(
        target_path=artifacts.target_path,
        manifest_path=artifacts.manifest_path,
        payload_path=artifacts.payload_path,
        report_path=artifacts.report_path,
    )


def run_paper_calibration_paper(
    *,
    output_dir: Path,
    tickers: list[str],
    quantity: float,
    expected_assumptions: dict[str, Any],
    adapter: Any,
) -> PaperCalibrationPaperResult:
    """Run a thin paper-calibration flow against an injected broker adapter."""

    artifacts = prepare_paper_calibration_artifacts(output_dir)
    target_frame = build_neutral_target_frame(
        tickers=tickers,
        gross_target_weight=1.0,
        perturbation_bps=0.0,
        perturbation_seed=None,
    )
    order_frame = build_neutral_order_frame(
        tickers=tickers,
        quantity=quantity,
        direction="buy",
    )
    manifest = build_neutral_target_manifest(
        target_frame=target_frame,
        strategy_name="neutral_buy_and_hold",
        perturbation_bps=0.0,
        perturbation_seed=None,
    )
    adapter.connect()
    account_before = adapter.query_account()
    positions_before = adapter.query_positions()
    execution_result: ExecutionResult = adapter.submit_orders_with_telemetry(order_frame)
    account_after = adapter.query_account()
    positions_after = adapter.query_positions()
    expected_positions = order_frame.rename(columns={"quantity": "expected_quantity"})[
        ["ticker", "expected_quantity"]
    ].copy()
    reconciliation_report = adapter.reconcile(expected_positions)

    requested_frame = _requested_orders_frame(order_frame, sample_id=artifacts.run_id)
    filled_rows = _filled_order_rows(execution_result, sample_id=artifacts.run_id)
    filled_frame = fill_collection.build_fill_orders_frame(filled_rows)
    event_frame = fill_collection.build_fill_events_frame(filled_rows)
    latency_values = [
        float(value)
        for value in pd.to_numeric(filled_frame.get("latency_seconds"), errors="coerce").dropna().tolist()
    ] if not filled_frame.empty else []
    fill_manifest = fill_collection.build_fill_manifest(
        run_id=artifacts.run_id,
        created_at=artifacts.created_at,
        market="us",
        broker="alpaca",
        notes="neutral paper calibration run",
        source_type="paper_calibration",
        source_path=str(output_dir),
        requested_order_rows=requested_frame,
        filled_order_rows=filled_frame,
        execution_result=execution_result,
        latency_values=latency_values,
        avg_fill_price_fallback_used=False,
    )
    payload = build_paper_calibration_payload(
        strategy_name="neutral_buy_and_hold",
        target_manifest={**manifest, "mode": "paper", "order_count": int(len(order_frame))},
        execution_result=execution_result,
        expected_assumptions=expected_assumptions,
    )

    Path(artifacts.target_path).parent.mkdir(parents=True, exist_ok=True)
    target_frame.to_csv(artifacts.target_path, index=False)
    write_json(artifacts.manifest_path, {**manifest, "mode": "paper", "order_count": int(len(order_frame))})
    write_json(artifacts.payload_path, payload)
    report_markdown = render_paper_calibration_report_markdown(payload)
    write_text(artifacts.report_path, report_markdown)
    fill_artifacts = fill_collection.write_fill_collection_artifacts(
        output_dir=Path(artifacts.output_dir),
        manifest=fill_manifest,
        order_rows=filled_frame,
        event_rows=event_frame,
        account_before=account_before,
        account_after=account_after,
        positions_before=positions_before,
        positions_after=positions_after,
        execution_result=execution_result,
        reconciliation_report=reconciliation_report,
        execution_result_frame=filled_frame,
        summary_markdown=report_markdown,
    )

    return PaperCalibrationPaperResult(
        target_path=artifacts.target_path,
        manifest_path=artifacts.manifest_path,
        payload_path=artifacts.payload_path,
        report_path=artifacts.report_path,
        fill_manifest_path=str(fill_artifacts["alpaca_fill_manifest"]),
        fill_orders_path=str(fill_artifacts["alpaca_fill_orders"]),
        reconciliation_report_path=str(fill_artifacts["reconciliation_report"]),
    )
