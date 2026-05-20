from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from portfolio_os.alpha.sue_score_definition_gate import (
    SueScoreDefinitionGateConfig,
    build_sue_score_definition_gate,
    validate_sue_score_definition_gate_report_language,
    write_sue_score_definition_gate_artifacts,
)


def _write_gate_fixture(tmp_path: Path) -> tuple[Path, Path]:
    events_path = tmp_path / "events.csv"
    prices_path = tmp_path / "crsp_daily.csv"
    event_rows = []
    price_rows = []
    patterns = [
        ("RAWLOW", -100.0, 1000.0, 0.10),
        ("PCTLOW", -1.0, 2.0, -0.10),
        ("MID", 0.0, 1.0, 0.00),
        ("PCTHIGH", 1.0, 1.0, 0.10),
        ("RAWHIGH", 10.0, 1000.0, -0.10),
    ]
    date_pairs = [("2020-01-01", "2020-01-02"), ("2020-01-31", "2020-02-03")]
    for date_idx, (announcement_date, rebalance_date) in enumerate(date_pairs, start=1):
        for idx, (symbol, raw, expected, realized) in enumerate(patterns, start=1):
            permno = date_idx * 10000 + idx
            event_rows.append(
                {
                    "event_id": f"SUE-{rebalance_date}-{symbol}",
                    "symbol": symbol,
                    "permno": permno,
                    "ibes_ticker": symbol,
                    "cusip": f"{permno:08d}",
                    "fiscal_period": "2019Q4",
                    "announcement_date": announcement_date,
                    "event_available_timestamp": f"{announcement_date}T21:15:00Z",
                    "tradable_timestamp": f"{rebalance_date}T14:30:00Z",
                    "rebalance_date": rebalance_date,
                    "actual_eps": expected + raw,
                    "expected_eps": expected,
                    "sue_value": raw,
                    "sue_definition": "actual_eps_minus_latest_pit_consensus",
                    "estimate_snapshot_date": "2019-12-31",
                    "price_anchor_date": rebalance_date,
                    "return_window_start": str(pd.bdate_range(rebalance_date, periods=3)[2].date()),
                    "return_window_end": str(pd.bdate_range(rebalance_date, periods=25)[-1].date()),
                    "data_source": "WRDS_IBES_CRSP_LOCAL_EXTRACT",
                    "link_method": "test_exact_link",
                    "pit_safety_status": "pit_safe",
                    "diagnostic_only": False,
                    "fetched_at": "2026-05-08T00:00:00Z",
                }
            )
            for row_idx, date in enumerate(pd.bdate_range(rebalance_date, periods=30)):
                price_rows.append(
                    {
                        "permno": permno,
                        "date": date.date().isoformat(),
                        "prc": 100.0 + idx,
                        "ret": realized if row_idx == 2 else 0.0,
                    }
                )
    pd.DataFrame(event_rows).to_csv(events_path, index=False)
    pd.DataFrame(price_rows).to_csv(prices_path, index=False)
    return events_path, prices_path


def test_scale_aware_sue_gate_selects_candidate_and_downgrades_raw(tmp_path: Path) -> None:
    events_path, prices_path = _write_gate_fixture(tmp_path)
    output_dir = tmp_path / "gate"
    report_path = tmp_path / "gate_report.md"

    result = build_sue_score_definition_gate(
        SueScoreDefinitionGateConfig(
            events_path=str(events_path),
            crsp_daily_path=str(prices_path),
            output_dir=str(output_dir),
            report_path=str(report_path),
            denominator_abs_min=0.01,
            min_rank_ic_names=4,
            min_spread_names=5,
        )
    )

    registry = {row["score_name"]: row for row in result.score_registry}
    assert registry["raw_eps_diff"]["diagnostic_only"] is True
    assert registry["raw_eps_diff"]["selected"] is False
    assert result.score_selection_summary["selected_score"] == "surprise_pct_actual_eps"
    assert result.score_selection_summary["interpretation"] in {
        "scale_aware_sue_candidate_selected",
        "scale_aware_sue_positive_but_needs_q2",
    }
    assert result.score_selection_summary["q2_evaluation_ran"] is False
    assert result.score_selection_summary["optimizer_path_evaluation_ran"] is False
    assert result.score_selection_summary["alpha_registry_promoted"] is False
    assert result.score_selection_summary["production_approval_claimed"] is False
    assert result.score_selection_summary["missing_coverage_encoded_as_zero_alpha"] is False

    raw = result.window_metrics.loc[
        result.window_metrics["score_name"].eq("raw_eps_diff")
        & result.window_metrics["window_name"].eq("plus_2_plus_22")
    ].iloc[0]
    selected = result.window_metrics.loc[
        result.window_metrics["score_name"].eq("surprise_pct_actual_eps")
        & result.window_metrics["window_name"].eq("plus_2_plus_22")
    ].iloc[0]
    assert raw["mean_top_bottom_spread"] < 0
    assert selected["mean_rank_ic"] > 0
    assert selected["mean_top_bottom_spread"] > 0
    assert result.denominator_guard_report["missing_coverage_encoded_as_zero_alpha"] is False
    assert result.placebo_report["placebo_diagnostics_generated"] is True
    assert "raw EPS diff is diagnostic-only after this phase" in result.report_text

    artifacts = write_sue_score_definition_gate_artifacts(result)
    for key in [
        "score_registry",
        "score_grid",
        "window_metrics",
        "placebo_report",
        "denominator_guard_report",
        "tail_concentration_report",
        "size_liquidity_bucket_report",
        "sector_exposure_report",
        "score_selection_summary",
        "report",
    ]:
        assert artifacts[key].exists()
    saved_registry = yaml.safe_load(artifacts["score_registry"].read_text(encoding="utf-8"))
    assert saved_registry["scores"]["raw_eps_diff"]["diagnostic_only"] is True
    summary = json.loads(artifacts["score_selection_summary"].read_text(encoding="utf-8"))
    assert summary["selected_score"] == "surprise_pct_actual_eps"


def test_score_definition_gate_report_language_rejects_misleading_claims() -> None:
    for phrase in [
        "production approved",
        "paper ready",
        "live trading",
        "broker execution",
        "order generation",
        "real historical SUE alpha proven",
        "guaranteed tradable alpha",
        "auto trading",
        "investment recommendation",
    ]:
        with pytest.raises(ValueError, match="misleading"):
            validate_sue_score_definition_gate_report_language(f"Report says {phrase}.")
