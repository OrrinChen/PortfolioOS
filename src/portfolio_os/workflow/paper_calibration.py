"""Dry-run helpers for the paper calibration sprint."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from portfolio_os.alpha.neutral_targets import (
    build_neutral_order_frame,
    build_neutral_target_frame,
    build_neutral_target_manifest,
)
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
    execution_result: ExecutionResult = adapter.submit_orders_with_telemetry(order_frame)
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
    write_text(artifacts.report_path, render_paper_calibration_report_markdown(payload))

    return PaperCalibrationPaperResult(
        target_path=artifacts.target_path,
        manifest_path=artifacts.manifest_path,
        payload_path=artifacts.payload_path,
        report_path=artifacts.report_path,
    )
