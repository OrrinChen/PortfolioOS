from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.alpha.sue_score_definition_diagnostics import (
    SueScoreDefinitionDiagnosticsConfig,
    build_sue_score_definition_diagnostics,
    validate_sue_score_definition_report_language,
    write_sue_score_definition_diagnostics_artifacts,
)


def _write_score_definition_fixture(tmp_path: Path) -> tuple[Path, Path]:
    events_path = tmp_path / "events.csv"
    prices_path = tmp_path / "crsp_daily.csv"
    event_rows = []
    price_rows = []
    raw_scores = [-100.0, -1.0, 0.0, 1.0, 10.0]
    expected = [1000.0, 1.0, 1.0, 1.0, 1000.0]
    returns = [0.10, -0.10, 0.00, 0.10, -0.10]
    symbols = ["RAWLOW", "PCTLOW", "MID", "PCTHIGH", "RAWHIGH"]
    for idx, (symbol, raw, exp, realized) in enumerate(zip(symbols, raw_scores, expected, returns), start=1):
        permno = 10000 + idx
        event_rows.append(
            {
                "event_id": f"SUE-{symbol}",
                "symbol": symbol,
                "permno": permno,
                "ibes_ticker": symbol,
                "cusip": f"{idx:08d}",
                "fiscal_period": "2019Q4",
                "announcement_date": "2020-01-01",
                "event_available_timestamp": "2020-01-01T21:15:00Z",
                "tradable_timestamp": "2020-01-02T14:30:00Z",
                "rebalance_date": "2020-01-02",
                "actual_eps": exp + raw,
                "expected_eps": exp,
                "sue_value": raw,
                "sue_definition": "actual_eps_minus_latest_pit_consensus",
                "estimate_snapshot_date": "2019-12-31",
                "price_anchor_date": "2020-01-02",
                "return_window_start": "2020-01-06",
                "return_window_end": "2020-02-03",
                "data_source": "WRDS_IBES_CRSP_LOCAL_EXTRACT",
                "link_method": "test_exact_link",
                "pit_safety_status": "pit_safe",
                "diagnostic_only": False,
                "fetched_at": "2026-05-07T00:00:00Z",
            }
        )
        for date_idx, date in enumerate(pd.bdate_range("2020-01-02", periods=25)):
            price_rows.append(
                {
                    "permno": permno,
                    "date": date.date().isoformat(),
                    "prc": 100.0,
                    "ret": realized if date_idx == 2 else 0.0,
                }
            )
    pd.DataFrame(event_rows).to_csv(events_path, index=False)
    pd.DataFrame(price_rows).to_csv(prices_path, index=False)
    return events_path, prices_path


def test_score_definition_diagnostics_identifies_raw_scale_problem(tmp_path: Path) -> None:
    events_path, prices_path = _write_score_definition_fixture(tmp_path)
    output_dir = tmp_path / "diagnostics"
    report_path = tmp_path / "report.md"

    result = build_sue_score_definition_diagnostics(
        SueScoreDefinitionDiagnosticsConfig(
            events_path=str(events_path),
            crsp_daily_path=str(prices_path),
            output_dir=str(output_dir),
            report_path=str(report_path),
        )
    )

    grid = result.score_definition_grid
    raw = grid.loc[
        grid["score_name"].eq("raw_eps_diff") & grid["window_name"].eq("plus_2_plus_22")
    ].iloc[0]
    pct = grid.loc[
        grid["score_name"].eq("surprise_pct_expected_eps") & grid["window_name"].eq("plus_2_plus_22")
    ].iloc[0]
    assert raw["mean_top_bottom_spread"] < 0
    assert pct["mean_top_bottom_spread"] > 0
    assert result.summary["preferred_diagnostic_score"] == "surprise_pct_actual_eps"
    assert result.summary["raw_eps_diff_scale_warning"] is True
    assert result.summary["q2_evaluation_ran"] is False
    assert result.summary["optimizer_path_evaluation_ran"] is False
    assert result.summary["production_approval_claimed"] is False
    assert result.summary["missing_coverage_encoded_as_zero_alpha"] is False
    assert "raw EPS difference is not the preferred SUE score" in result.report_text

    artifacts = write_sue_score_definition_diagnostics_artifacts(result)
    assert artifacts["score_definition_grid"].exists()
    assert artifacts["diagnostic_summary"].exists()
    summary = json.loads(artifacts["diagnostic_summary"].read_text(encoding="utf-8"))
    assert summary["raw_eps_diff_scale_warning"] is True


def test_score_definition_report_language_rejects_misleading_claims() -> None:
    for phrase in [
        "production approved",
        "paper ready",
        "live trading",
        "broker execution",
        "order generation",
        "SUE alpha is proven",
    ]:
        with pytest.raises(ValueError, match="misleading"):
            validate_sue_score_definition_report_language(f"Report says {phrase}.")
