from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.full_market_supervisor import run_full_market_multifactor_supervisor


def test_full_market_supervisor_retries_candidates_without_opening_downstream(tmp_path: Path) -> None:
    returns_path = tmp_path / "returns_long.csv"
    output_dir = tmp_path / "supervisor"
    _write_returns_fixture(returns_path)

    result = run_full_market_multifactor_supervisor(
        returns_panel_path=returns_path,
        output_dir=output_dir,
        max_attempts=3,
    )

    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    attempts = pd.read_csv(result.attempts_path)
    report = Path(result.report_path).read_text(encoding="utf-8").lower()

    assert result.validation_status in {"evaluated", "blocked"}
    assert summary["sweep_ran"] is True
    assert summary["attempt_count"] >= 1
    assert len(attempts) == summary["attempt_count"]
    assert summary["measurement_spec_written"] is False
    assert summary["d3_charter_allowed"] is False
    assert summary["q1_entry_allowed"] is False
    assert summary["q2_entry_allowed"] is False
    assert summary["or_optimizer_used"] is False
    assert summary["alpha_registry_update_allowed"] is False
    assert summary["expected_return_panel_written"] is False
    assert summary["non_claims"]["production_approval"] is False
    assert "supervisor retry loop" in report
    assert "q2 remains closed" in report


def test_full_market_supervisor_blocks_missing_returns_before_attempts(tmp_path: Path) -> None:
    result = run_full_market_multifactor_supervisor(
        returns_panel_path=tmp_path / "missing_returns.csv",
        output_dir=tmp_path / "supervisor",
        max_attempts=2,
    )

    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    attempts = pd.read_csv(result.attempts_path)

    assert result.validation_status == "blocked"
    assert summary["decision_state"] == "blocked_data_coverage"
    assert summary["attempt_count"] == 0
    assert attempts.empty
    assert summary["q2_entry_allowed"] is False


def _write_returns_fixture(path: Path) -> None:
    rows: list[dict[str, object]] = []
    tickers = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH"]
    dates = pd.bdate_range("2023-01-03", periods=180)
    for day_index, date in enumerate(dates):
        regime = 1 if day_index < 120 else -1
        for ticker_index, ticker in enumerate(tickers):
            base = ((ticker_index % 4) - 1.5) * 0.0005
            if ticker in {"AAA", "BBB", "CCC"} and day_index % 12 == 0:
                ret = 0.035 * regime
            elif ticker in {"AAA", "BBB", "CCC"} and day_index % 12 in {1, 2, 3, 4, 5}:
                ret = 0.012 * regime
            elif ticker in {"GGG", "HHH"} and day_index % 10 == 0:
                ret = -0.025
            elif ticker in {"GGG", "HHH"} and day_index % 10 in {1, 2, 3}:
                ret = 0.009
            else:
                ret = base
            rows.append({"date": date.date().isoformat(), "ticker": ticker, "return": ret})
    pd.DataFrame(rows).to_csv(path, index=False)
