from __future__ import annotations

from pathlib import Path

import pandas as pd

from portfolio_os.execution.models import ExecutionResult, OrderExecutionRecord
from portfolio_os.workflow.paper_calibration import run_paper_calibration_dry_run, run_paper_calibration_paper


class _FakePaperAdapter:
    def submit_orders_with_telemetry(self, orders_df: pd.DataFrame) -> ExecutionResult:
        _ = orders_df
        return ExecutionResult(
            orders=[
                OrderExecutionRecord(
                    ticker="SPY",
                    direction="buy",
                    requested_qty=3,
                    filled_qty=3,
                    avg_fill_price=500.0,
                    status="filled",
                    poll_count=2,
                )
            ],
            submitted_count=1,
            filled_count=1,
            partial_count=0,
            unfilled_count=0,
            rejected_count=0,
            timeout_cancelled_count=0,
        )


def test_paper_calibration_dry_run_writes_target_manifest_payload_and_report(tmp_path: Path) -> None:
    output_dir = tmp_path / "paper_calibration"

    result = run_paper_calibration_dry_run(
        output_dir=output_dir,
        tickers=["SPY"],
        gross_target_weight=1.0,
        perturbation_bps=0.0,
        perturbation_seed=None,
        expected_assumptions={"participation_limit": 0.05, "slippage_model": "baseline"},
    )

    assert Path(result.target_path).exists()
    assert Path(result.manifest_path).exists()
    assert Path(result.payload_path).exists()
    assert Path(result.report_path).exists()

    frame = pd.read_csv(result.target_path)
    assert frame["ticker"].tolist() == ["SPY"]


def test_paper_calibration_paper_writes_realized_payload_and_report(tmp_path: Path) -> None:
    output_dir = tmp_path / "paper_calibration_live"

    result = run_paper_calibration_paper(
        output_dir=output_dir,
        tickers=["SPY"],
        quantity=3,
        expected_assumptions={"participation_limit": 0.05, "slippage_model": "baseline"},
        adapter=_FakePaperAdapter(),
    )

    assert Path(result.manifest_path).exists()
    assert Path(result.payload_path).exists()
    assert Path(result.report_path).exists()

    payload = pd.read_json(result.payload_path, typ="series")
    assert payload["strategy_name"] == "neutral_buy_and_hold"
