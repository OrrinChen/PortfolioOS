from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.alpha.sue_regime_filter_placebo_check import (
    SueRegimeFilterPlaceboConfig,
    build_sue_regime_filter_placebo_check,
    validate_sue_regime_filter_placebo_report_language,
    write_sue_regime_filter_placebo_artifacts,
)


def _write_regime_fixture(tmp_path: Path) -> tuple[Path, Path]:
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
    date_specs = [
        ("2020-02-03", "2020-02-04"),
        ("2020-03-02", "2020-03-03"),
        ("2020-04-06", "2020-04-07"),
    ]
    for month_idx, (announcement_date, tradable_date) in enumerate(date_specs, start=1):
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
            dates = pd.bdate_range(pd.Timestamp(tradable_date) - pd.offsets.BDay(15), periods=70)
            for date in dates:
                rel = len([d for d in dates if d <= date]) - 16
                pre_event_rebound = 0.04 * raw if -8 <= rel <= -6 else 0.0
                post_event_move = 0.01 * raw if 2 <= rel <= 4 else 0.0
                march_volatility = 0.10 if date.strftime("%Y-%m") == "2020-03" and idx % 2 == 0 else 0.0
                low_liquidity = date.strftime("%Y-%m") == "2020-04"
                price_rows.append(
                    {
                        "permno": permno,
                        "date": date.date().isoformat(),
                        "prc": 50.0 + idx,
                        "ret": pre_event_rebound + post_event_move + march_volatility,
                        "vol": 10 if low_liquidity else 100000,
                    }
                )
    pd.DataFrame(event_rows).to_csv(events_path, index=False)
    pd.DataFrame(price_rows).to_csv(prices_path, index=False)
    return events_path, prices_path


def test_regime_filter_placebo_check_reruns_filtered_curves(tmp_path: Path) -> None:
    events_path, prices_path = _write_regime_fixture(tmp_path)
    result = build_sue_regime_filter_placebo_check(
        SueRegimeFilterPlaceboConfig(
            events_path=str(events_path),
            crsp_daily_path=str(prices_path),
            output_dir=str(tmp_path / "out"),
            report_path=str(tmp_path / "report.md"),
            min_rank_ic_names=4,
            min_spread_names=5,
            high_volatility_week_quantile=0.8,
            low_liquidity_week_quantile=0.4,
        )
    )

    scenarios = set(result.score_gate_summary["filter_name"])
    assert {
        "baseline",
        "exclude_march_2020",
        "exclude_high_volatility_weeks",
        "exclude_low_liquidity_weeks",
        "exclude_market_regime_weeks",
    }.issubset(scenarios)
    baseline = result.score_gate_summary.loc[result.score_gate_summary["filter_name"].eq("baseline")].iloc[0]
    no_march = result.score_gate_summary.loc[result.score_gate_summary["filter_name"].eq("exclude_march_2020")].iloc[0]
    high_vol = result.score_gate_summary.loc[
        result.score_gate_summary["filter_name"].eq("exclude_high_volatility_weeks")
    ].iloc[0]
    low_liq = result.score_gate_summary.loc[
        result.score_gate_summary["filter_name"].eq("exclude_low_liquidity_weeks")
    ].iloc[0]
    assert int(no_march["surviving_event_count"]) < int(baseline["surviving_event_count"])
    assert int(high_vol["excluded_event_count"]) > 0
    assert int(low_liq["excluded_event_count"]) > 0
    assert result.regime_filter_summary["score_selection_ran"] is False
    assert result.regime_filter_summary["q2_evaluation_ran"] is False
    assert result.regime_filter_summary["optimizer_path_evaluation_ran"] is False
    assert result.regime_filter_summary["production_approval_claimed"] is False
    assert result.regime_filter_summary["missing_coverage_encoded_as_zero_alpha"] is False
    assert result.regime_filter_summary["low_liquidity_filter_source"] == "dollar_volume"
    assert "H1E.2 validates the market-regime attribution only" in result.report_text

    artifacts = write_sue_regime_filter_placebo_artifacts(result)
    for key in [
        "score_gate_summary",
        "filtered_placebo_shift_curve",
        "regime_week_classification",
        "regime_filter_summary",
        "report",
    ]:
        assert artifacts[key].exists()
    summary = json.loads(artifacts["regime_filter_summary"].read_text(encoding="utf-8"))
    assert summary["selected_score"] is None


def test_regime_filter_report_language_rejects_misleading_claims() -> None:
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
            validate_sue_regime_filter_placebo_report_language(f"Report says {phrase}.")
