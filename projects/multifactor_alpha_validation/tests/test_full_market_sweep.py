from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.full_market_sweep import run_full_market_multifactor_sweep


def test_full_market_sweep_writes_search_burden_without_unlocking_downstream(tmp_path: Path) -> None:
    returns_path = tmp_path / "returns_long.csv"
    output_dir = tmp_path / "full_market_sweep"
    _write_returns_fixture(returns_path)

    result = run_full_market_multifactor_sweep(
        returns_panel_path=returns_path,
        output_dir=output_dir,
        top_n=3,
    )

    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    feature_cache = pd.read_csv(result.feature_cache_path)
    pocket_grid = pd.read_csv(result.pocket_grid_path)
    template_grid = pd.read_csv(result.template_grid_path)
    placebo = pd.read_csv(result.placebo_top_pockets_path)
    report = Path(result.report_path).read_text(encoding="utf-8").lower()

    assert result.validation_status == "evaluated"
    assert result.decision_state == "full_market_sweep_diagnostic_only"
    assert summary["full_market_scope"] is True
    assert summary["search_burden"]["searched_pocket_count"] > 0
    assert summary["search_burden"]["searched_template_count"] > 0
    assert summary["d3_charter_allowed"] is False
    assert summary["measurement_spec_written"] is False
    assert summary["q1_entry_allowed"] is False
    assert summary["q2_entry_allowed"] is False
    assert summary["or_optimizer_used"] is False
    assert summary["alpha_registry_update_allowed"] is False
    assert summary["non_claims"]["production_approval"] is False

    assert not feature_cache.empty
    assert "forward_return" not in " ".join(feature_cache.columns).lower()
    assert {"mean_return", "t_stat", "hit_rate", "month_breadth", "issuer_breadth"}.issubset(pocket_grid.columns)
    assert {"template_id", "component_count", "search_profile_score"}.issubset(template_grid.columns)
    assert {"same_coverage_random_top", "shifted_date_top"}.issubset(set(placebo["placebo_type"]))
    assert "diagnostic only" in report
    assert "measurement spec is not written" in report
    assert "q2 remains closed" in report


def test_full_market_sweep_blocks_missing_returns_without_fabricating_features(tmp_path: Path) -> None:
    result = run_full_market_multifactor_sweep(
        returns_panel_path=tmp_path / "missing_returns.csv",
        output_dir=tmp_path / "full_market_sweep",
    )

    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    pocket_grid = pd.read_csv(result.pocket_grid_path)

    assert result.validation_status == "blocked"
    assert summary["decision_state"] == "blocked_data_coverage"
    assert summary["unavailable_reason"] == "missing_returns_panel"
    assert summary["fabricated_features"] is False
    assert summary["measurement_spec_written"] is False
    assert summary["q2_entry_allowed"] is False
    assert pocket_grid.empty


def _write_returns_fixture(path: Path) -> None:
    rows: list[dict[str, object]] = []
    tickers = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]
    dates = pd.bdate_range("2024-01-02", periods=90)
    for day_index, date in enumerate(dates):
        for ticker_index, ticker in enumerate(tickers):
            base = ((day_index % 7) - 3) * 0.001 + (ticker_index - 2) * 0.0004
            if ticker in {"AAA", "BBB"} and day_index % 11 == 0:
                ret = 0.045
            elif ticker in {"AAA", "BBB"} and day_index % 11 in {1, 2, 3, 4, 5}:
                ret = -0.012
            elif ticker in {"EEE", "FFF"} and day_index % 13 == 0:
                ret = -0.038
            elif ticker in {"EEE", "FFF"} and day_index % 13 in {1, 2, 3}:
                ret = 0.014
            else:
                ret = base
            rows.append({"date": date.date().isoformat(), "ticker": ticker, "return": ret})
    pd.DataFrame(rows).to_csv(path, index=False)
