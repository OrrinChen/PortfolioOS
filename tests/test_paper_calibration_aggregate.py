from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.workflow.paper_calibration_aggregate import run_paper_calibration_aggregate


def _write_sample_run(
    run_dir: Path,
    *,
    ticker: str,
    captured_at_utc: str,
    latest_trade_at_utc: str,
    submitted_at_utc: str,
    mid_price: float,
    spread_bps: float,
    filled_price: float,
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "ticker": ticker,
                "captured_at_utc": captured_at_utc,
                "latest_trade_price": mid_price,
                "latest_trade_at_utc": latest_trade_at_utc,
                "bid_price": mid_price * (1.0 - (spread_bps / 20000.0)),
                "ask_price": mid_price * (1.0 + (spread_bps / 20000.0)),
                "mid_price": mid_price,
                "spread_bps": spread_bps,
                "reference_price": mid_price,
                "reference_price_source": "mid_price",
            }
        ]
    ).to_csv(run_dir / "pretrade_reference_snapshot.csv", index=False)
    pd.DataFrame(
        [
            {
                "sample_id": run_dir.name,
                "ticker": ticker,
                "direction": "buy",
                "requested_qty": 1.0,
                "filled_qty": 1.0,
                "avg_fill_price": filled_price,
                "reference_price": mid_price,
                "estimated_price": mid_price,
                "requested_notional": mid_price,
                "filled_notional": filled_price,
                "fill_ratio": 1.0,
                "status": "filled",
                "reject_reason": "",
                "broker_order_id": f"order-{run_dir.name}",
                "submitted_at_utc": submitted_at_utc,
                "terminal_at_utc": submitted_at_utc,
                "latency_seconds": 0.0,
                "poll_count": 1,
                "timeout_cancelled": False,
                "cancel_requested": False,
                "cancel_acknowledged": False,
                "avg_fill_price_fallback_used": False,
                "status_history": "[]",
            }
        ]
    ).to_csv(run_dir / "alpaca_fill_orders.csv", index=False)


def test_run_paper_calibration_aggregate_builds_observations_and_summary(tmp_path: Path) -> None:
    input_root = tmp_path / "inputs"
    output_dir = tmp_path / "aggregate"
    _write_sample_run(
        input_root / "paper_calibration_live_2026-04-16" / "run_001",
        ticker="SPY",
        captured_at_utc="2026-04-16T14:30:00+00:00",
        latest_trade_at_utc="2026-04-16T14:29:59+00:00",
        submitted_at_utc="2026-04-16T14:30:04+00:00",
        mid_price=500.00,
        spread_bps=2.0,
        filled_price=500.05,
    )
    _write_sample_run(
        input_root / "paper_calibration_live_2026-04-16" / "run_002",
        ticker="SPY",
        captured_at_utc="2026-04-16T18:30:00+00:00",
        latest_trade_at_utc="2026-04-16T18:29:59+00:00",
        submitted_at_utc="2026-04-16T18:30:08+00:00",
        mid_price=501.00,
        spread_bps=4.0,
        filled_price=500.98,
    )

    result = run_paper_calibration_aggregate(
        input_root=input_root,
        output_dir=output_dir,
    )

    assert Path(result.observations_path).exists()
    assert Path(result.summary_path).exists()
    assert result.observation_count == 2

    observations = pd.read_csv(result.observations_path)
    assert observations["ticker"].tolist() == ["SPY", "SPY"]
    assert observations["reference_price_source"].tolist() == ["mid_price", "mid_price"]
    assert observations["capture_to_submit_latency_seconds"].tolist() == pytest.approx([4.0, 8.0])
    assert observations["half_spread_bps"].tolist() == pytest.approx([1.0, 2.0])
    assert observations["drift_bps"].tolist() == pytest.approx([1.0, -0.3992015968], abs=1e-6)
    assert observations["drift_vs_half_spread"].tolist() == pytest.approx([1.0, -0.1996007984], abs=1e-6)

    summary = Path(result.summary_path).read_text(encoding="utf-8")
    assert "Paper Calibration Drift Summary" in summary
    assert "Observation Count" in summary
    assert "Latency Regression" in summary
    assert "Time-Of-Day Buckets" in summary
