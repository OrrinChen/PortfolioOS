from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.full_market_locked_validation import run_full_market_locked_validation


def test_full_market_locked_validation_writes_locked_diagnostic_artifacts(tmp_path: Path) -> None:
    returns_path = tmp_path / "returns_long.csv"
    output_dir = tmp_path / "locked_validation"
    _write_returns_fixture(returns_path)

    result = run_full_market_locked_validation(
        returns_panel_path=returns_path,
        candidate={
            "candidate_id": "reversal_1d",
            "search_kind": "leaf",
            "window": "post_1_1",
            "side": "top",
            "quantile": 0.8,
        },
        output_dir=output_dir,
        random_seed=11,
    )

    locked_candidate = json.loads(Path(result.locked_candidate_path).read_text(encoding="utf-8"))
    by_split = pd.read_csv(result.by_split_path)
    placebo = pd.read_csv(result.placebo_report_path)
    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    report = Path(result.report_path).read_text(encoding="utf-8").lower()

    assert result.validation_status == "evaluated"
    assert result.decision_label in {"locked_validation_passed", "locked_validation_failed", "blocked_placebo_dominance"}
    assert locked_candidate["formula_modified"] is False
    assert locked_candidate["threshold_modified"] is False
    assert set(by_split["split"]) == {"train", "validation", "test"}
    assert {
        "sample_count",
        "mean_return",
        "t_stat",
        "hit_rate",
        "month_breadth",
        "issuer_breadth",
        "top10_abs_return_concentration",
    }.issubset(by_split.columns)
    assert {"same_coverage_random", "shifted_date"}.issubset(set(placebo["placebo_type"]))
    assert summary["measurement_spec_written"] is False
    assert summary["d3_charter_allowed"] is False
    assert summary["q1_entry_allowed"] is False
    assert summary["q2_entry_allowed"] is False
    assert summary["or_optimizer_used"] is False
    assert summary["alpha_registry_update_allowed"] is False
    assert summary["production_approval"] is False
    assert summary["expected_return_panel_written"] is False
    assert summary["not_alpha_evidence"] is True
    assert "locked validation" in report
    assert "diagnostic only" in report
    assert "q2 remains closed" in report


def test_full_market_locked_validation_blocks_missing_returns_with_empty_artifacts(tmp_path: Path) -> None:
    result = run_full_market_locked_validation(
        returns_panel_path=tmp_path / "missing_returns.csv",
        candidate={
            "candidate_id": "momentum_reversal_blend",
            "search_kind": "template",
            "window": "post_1_22",
            "side": "top",
            "quantile": 0.9,
        },
        output_dir=tmp_path / "locked_validation",
    )

    by_split = pd.read_csv(result.by_split_path)
    placebo = pd.read_csv(result.placebo_report_path)
    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))

    assert result.validation_status == "blocked"
    assert result.decision_label == "blocked_data_coverage"
    assert by_split.empty
    assert placebo.empty
    assert summary["decision_label"] == "blocked_data_coverage"
    assert summary["unavailable_reason"] == "missing_or_invalid_returns_panel"
    assert summary["measurement_spec_written"] is False
    assert summary["q1_entry_allowed"] is False
    assert summary["q2_entry_allowed"] is False
    assert summary["not_alpha_evidence"] is True


def _write_returns_fixture(path: Path) -> None:
    rows: list[dict[str, object]] = []
    tickers = [f"T{index:02d}" for index in range(12)]
    dates = pd.bdate_range("2024-01-02", periods=180)
    for day_index, date in enumerate(dates):
        cycle = day_index % 5
        for ticker_index, ticker in enumerate(tickers):
            base = ((ticker_index % 4) - 1.5) * 0.0003
            if ticker_index < 3 and cycle == 0:
                ret = -0.030 - ticker_index * 0.001
            elif ticker_index < 3 and cycle == 2:
                ret = 0.017 + ticker_index * 0.002 + (day_index % 7) * 0.0002
            elif ticker_index >= 9 and cycle == 0:
                ret = 0.010 + base
            elif ticker_index >= 9 and cycle == 2:
                ret = -0.004 + base
            else:
                ret = base
            rows.append({"date": date.date().isoformat(), "ticker": ticker, "return": ret})
    pd.DataFrame(rows).to_csv(path, index=False)
