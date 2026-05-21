from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.alpha.sue_placebo_failure_attribution import (
    SuePlaceboFailureAttributionConfig,
    build_sue_placebo_failure_attribution,
    validate_sue_placebo_failure_attribution_report_language,
    write_sue_placebo_failure_attribution_artifacts,
)


def _write_placebo_fixture(tmp_path: Path) -> tuple[Path, Path]:
    events_path = tmp_path / "events.csv"
    prices_path = tmp_path / "crsp_daily.csv"
    event_rows = []
    price_rows = []
    event_specs = [
        ("A", 1.0, 1.0),
        ("B", 0.5, 1.0),
        ("C", -0.5, 1.0),
        ("D", -1.0, 2.0),
        ("E", 2.0, 2.0),
    ]
    for month_idx, (announcement_date, tradable_date) in enumerate(
        [("2020-02-03", "2020-02-04"), ("2020-03-02", "2020-03-03")],
        start=1,
    ):
        for idx, (symbol, raw, expected_eps) in enumerate(event_specs, start=1):
            permno = month_idx * 10000 + idx
            event_rows.append(
                {
                    "event_id": f"SUE-{month_idx}-{symbol}",
                    "symbol": symbol,
                    "permno": permno,
                    "ibes_ticker": symbol,
                    "cusip": f"{permno:08d}",
                    "fiscal_period": "2019Q4",
                    "announcement_date": announcement_date,
                    "event_available_timestamp": f"{announcement_date}T21:15:00Z",
                    "tradable_timestamp": f"{tradable_date}T14:30:00Z",
                    "rebalance_date": tradable_date,
                    "actual_eps": expected_eps + raw,
                    "expected_eps": expected_eps,
                    "sue_value": raw,
                    "sue_definition": "actual_eps_minus_latest_pit_consensus",
                    "estimate_snapshot_date": "2020-01-15",
                    "price_anchor_date": tradable_date,
                    "return_window_start": str(pd.bdate_range(tradable_date, periods=3)[2].date()),
                    "return_window_end": str(pd.bdate_range(tradable_date, periods=25)[-1].date()),
                    "data_source": "WRDS_IBES_CRSP_LOCAL_EXTRACT",
                    "link_method": "test_exact_link",
                    "pit_safety_status": "pit_safe",
                    "diagnostic_only": False,
                    "fetched_at": "2026-05-08T00:00:00Z",
                }
            )
            dates = pd.bdate_range(pd.Timestamp(tradable_date) - pd.offsets.BDay(15), periods=60)
            for date in dates:
                rel = len([d for d in dates if d <= date]) - 16
                pre_event_rebound = 0.04 * raw if -8 <= rel <= -6 else 0.0
                post_event_move = 0.01 * raw if 2 <= rel <= 4 else 0.0
                price_rows.append(
                    {
                        "permno": permno,
                        "date": date.date().isoformat(),
                        "prc": 50.0 + idx,
                        "ret": pre_event_rebound + post_event_move,
                    }
                )
    pd.DataFrame(event_rows).to_csv(events_path, index=False)
    pd.DataFrame(price_rows).to_csv(prices_path, index=False)
    return events_path, prices_path


def test_placebo_failure_attribution_reports_shift_and_overlap(tmp_path: Path) -> None:
    events_path, prices_path = _write_placebo_fixture(tmp_path)
    result = build_sue_placebo_failure_attribution(
        SuePlaceboFailureAttributionConfig(
            events_path=str(events_path),
            crsp_daily_path=str(prices_path),
            output_dir=str(tmp_path / "out"),
            report_path=str(tmp_path / "report.md"),
            score_name="surprise_pct_actual_eps",
            shift_trading_days=[-10, -5, -2, 0, 2, 5, 10],
            min_rank_ic_names=4,
            min_spread_names=5,
        )
    )

    assert set(result.placebo_shift_curve["shift_trading_days"]) == {-10, -5, -2, 0, 2, 5, 10}
    assert {"plus_2_plus_2", "plus_2_plus_3", "plus_2_plus_22"}.issubset(
        set(result.placebo_shift_curve["window_name"])
    )
    assert result.attribution_summary["score_name"] == "surprise_pct_actual_eps"
    assert result.attribution_summary["q2_evaluation_ran"] is False
    assert result.attribution_summary["optimizer_path_evaluation_ran"] is False
    assert result.attribution_summary["production_approval_claimed"] is False
    assert result.return_window_overlap_audit["shifted_anchors_used"] is True
    assert result.return_window_overlap_audit["original_anchor_reused_for_shifted_windows"] is False
    assert result.return_window_overlap_audit["event_available_after_tradable_violations"] == 0
    assert result.return_window_overlap_audit["live_return_window_start_before_tradable_count"] == 0
    assert result.denominator_tail_audit["missing_coverage_encoded_as_zero_alpha"] is False
    assert result.regime_concentration_report["schema_version"] == "sue_placebo_regime_concentration.v1"
    assert "H1E did not select a production SUE score" in result.report_text

    artifacts = write_sue_placebo_failure_attribution_artifacts(result)
    for key in [
        "placebo_shift_curve",
        "live_vs_placebo_by_month",
        "live_vs_placebo_by_sector",
        "live_vs_placebo_by_size_liquidity",
        "return_window_overlap_audit",
        "market_adjustment_report",
        "denominator_tail_audit",
        "regime_concentration_report",
        "attribution_summary",
        "report",
    ]:
        assert artifacts[key].exists()
    summary = json.loads(artifacts["attribution_summary"].read_text(encoding="utf-8"))
    assert summary["score_selection_ran"] is False


def test_placebo_failure_report_language_rejects_misleading_claims() -> None:
    for phrase in [
        "production approved",
        "paper ready",
        "live trading",
        "broker execution",
        "order generation",
        "SUE alpha is proven",
        "selected production score",
    ]:
        with pytest.raises(ValueError, match="misleading"):
            validate_sue_placebo_failure_attribution_report_language(f"Report says {phrase}.")
