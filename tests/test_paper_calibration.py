from __future__ import annotations

import unittest

from portfolio_os.execution.models import ExecutionResult, OrderExecutionRecord
from portfolio_os.execution.paper_calibration import (
    build_paper_calibration_payload,
    render_paper_calibration_report_markdown,
)


class PaperCalibrationReportTests(unittest.TestCase):
    def _result(self) -> ExecutionResult:
        return ExecutionResult(
            orders=[
                OrderExecutionRecord(
                    ticker="SPY",
                    direction="buy",
                    requested_qty=10,
                    filled_qty=10,
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

    def test_build_payload_includes_required_sections(self) -> None:
        payload = build_paper_calibration_payload(
            strategy_name="neutral_buy_and_hold",
            target_manifest={"selected_tickers": ["SPY"], "selected_count": 1},
            execution_result=self._result(),
            expected_assumptions={"participation_limit": 0.05, "slippage_model": "baseline"},
            reference_snapshot_summary={
                "captured_ticker_count": 1,
                "with_reference_price_count": 1,
                "with_mid_price_count": 1,
                "fallback_reference_count": 0,
            },
        )

        self.assertEqual(payload["strategy_name"], "neutral_buy_and_hold")
        self.assertIn("realized_summary", payload)
        self.assertIn("expected_assumptions", payload)
        self.assertIn("deviation_summary", payload)
        self.assertIn("reference_snapshot_summary", payload)

    def test_render_report_mentions_fill_rate_and_slippage(self) -> None:
        payload = build_paper_calibration_payload(
            strategy_name="neutral_buy_and_hold",
            target_manifest={"selected_tickers": ["SPY"], "selected_count": 1},
            execution_result=self._result(),
            expected_assumptions={"participation_limit": 0.05, "slippage_model": "baseline"},
            reference_snapshot_summary={
                "captured_ticker_count": 1,
                "with_reference_price_count": 1,
                "with_mid_price_count": 1,
                "fallback_reference_count": 0,
            },
        )

        report = render_paper_calibration_report_markdown(payload)
        self.assertIn("fill rate", report.lower())
        self.assertIn("slippage", report.lower())
        self.assertIn("neutral_buy_and_hold", report)
        self.assertIn("reference snapshot", report.lower())


if __name__ == "__main__":
    unittest.main()
