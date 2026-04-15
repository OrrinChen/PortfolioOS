from __future__ import annotations

from pathlib import Path

import pandas as pd

from portfolio_os.execution.models import ExecutionResult, OrderExecutionRecord
from portfolio_os.workflow.paper_calibration import run_paper_calibration_dry_run, run_paper_calibration_paper


class _FakePaperAdapter:
    def connect(self) -> bool:
        return True

    def submit_orders_with_telemetry(self, orders_df: pd.DataFrame) -> ExecutionResult:
        _ = orders_df
        self._submitted = True
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

    def query_account(self) -> dict:
        return {"account_number": "paper-demo", "cash": "100000.00"}

    def query_positions(self) -> pd.DataFrame:
        if not hasattr(self, "_submitted"):
            return pd.DataFrame(
                [
                    {
                        "ticker": "AAPL",
                        "quantity": 2.0,
                        "market_value": 400.0,
                        "avg_entry_price": 190.0,
                        "current_price": 200.0,
                        "unrealized_pnl": 20.0,
                    }
                ]
            )
        return pd.DataFrame(
            [
                {
                    "ticker": "AAPL",
                    "quantity": 2.0,
                    "market_value": 400.0,
                    "avg_entry_price": 190.0,
                    "current_price": 200.0,
                    "unrealized_pnl": 20.0,
                },
                {
                    "ticker": "SPY",
                    "quantity": 3.0,
                    "market_value": 1500.0,
                    "avg_entry_price": 500.0,
                    "current_price": 500.0,
                    "unrealized_pnl": 0.0,
                },
            ]
        )

    def reconcile(self, expected_positions: pd.DataFrame):
        from portfolio_os.execution.models import ReconciliationDetail, ReconciliationReport

        expected = expected_positions.sort_values("ticker").reset_index(drop=True)
        return ReconciliationReport(
            matched_count=len(expected),
            mismatched_count=0,
            missing_in_broker=[],
            missing_in_system=[],
            details=[
                ReconciliationDetail(
                    ticker=str(row["ticker"]),
                    expected_quantity=float(row["expected_quantity"]),
                    actual_quantity=float(row["expected_quantity"]),
                    quantity_diff=0.0,
                    expected_value=float(row["expected_value"]),
                    actual_value=float(row["expected_value"]),
                    value_diff=0.0,
                )
                for row in expected.to_dict(orient="records")
            ],
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
    assert Path(result.fill_manifest_path).exists()
    assert Path(result.fill_orders_path).exists()
    assert Path(result.reconciliation_report_path).exists()

    payload = pd.read_json(result.payload_path, typ="series")
    assert payload["strategy_name"] == "neutral_buy_and_hold"

    fill_manifest = pd.read_json(result.fill_manifest_path, typ="series")
    assert float(fill_manifest["total_requested_notional"]) > 0.0

    reconciliation = pd.read_json(result.reconciliation_report_path, typ="series")
    assert reconciliation["mismatched_count"] == 0
    assert reconciliation["missing_in_system"] == []
