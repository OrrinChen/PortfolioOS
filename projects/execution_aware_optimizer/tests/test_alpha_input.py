from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.domain.errors import InputValidationError

from execution_aware_optimizer.alpha_input import clean_alpha_scores, load_alpha_scores


def test_alpha_input_validates_required_columns() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2026-01-31"],
            "symbol": ["AAA"],
        }
    )

    with pytest.raises(InputValidationError, match="alpha_score"):
        clean_alpha_scores(frame, source_name="missing_score.csv")


def test_alpha_input_parses_dates_and_drops_missing_scores(tmp_path: Path) -> None:
    alpha_path = tmp_path / "alpha.csv"
    pd.DataFrame(
        [
            {"date": "2026-01-31", "symbol": "AAA", "alpha_score": 0.4},
            {"date": "2026-01-31", "symbol": "BBB", "alpha_score": None},
            {"date": "2026-02-28", "symbol": "AAA", "alpha_score": -0.2},
        ]
    ).to_csv(alpha_path, index=False)

    result = load_alpha_scores(alpha_path)

    assert list(result.panel["date"]) == [pd.Timestamp("2026-01-31"), pd.Timestamp("2026-02-28")]
    assert list(result.panel["symbol"]) == ["AAA", "AAA"]
    assert list(result.panel["ticker"]) == ["AAA", "AAA"]
    assert result.report.missing_alpha_score_count == 1
    assert result.report.row_count_input == 3
    assert result.report.row_count_output == 2


def test_alpha_input_rank_normalizes_by_date() -> None:
    frame = pd.DataFrame(
        [
            {"date": "2026-01-31", "symbol": "LOW", "alpha_score": -10.0},
            {"date": "2026-01-31", "symbol": "MID", "alpha_score": 0.0},
            {"date": "2026-01-31", "symbol": "HIGH", "alpha_score": 10.0},
            {"date": "2026-02-28", "symbol": "ONLY", "alpha_score": 99.0},
        ]
    )

    result = clean_alpha_scores(frame, rank_normalize_by_date=True)
    keyed = result.panel.set_index(["date", "symbol"])

    assert keyed.loc[(pd.Timestamp("2026-01-31"), "LOW"), "raw_alpha_score"] == pytest.approx(-10.0)
    assert keyed.loc[(pd.Timestamp("2026-01-31"), "LOW"), "alpha_score"] == pytest.approx(-1.0)
    assert keyed.loc[(pd.Timestamp("2026-01-31"), "MID"), "alpha_score"] == pytest.approx(0.0)
    assert keyed.loc[(pd.Timestamp("2026-01-31"), "HIGH"), "alpha_score"] == pytest.approx(1.0)
    assert keyed.loc[(pd.Timestamp("2026-02-28"), "ONLY"), "alpha_score"] == pytest.approx(0.0)
    assert result.report.rank_normalized is True


def test_alpha_input_winsorizes_scores_by_date_before_normalization() -> None:
    frame = pd.DataFrame(
        [
            {"date": "2026-01-31", "symbol": "A", "alpha_score": -100.0},
            {"date": "2026-01-31", "symbol": "B", "alpha_score": 0.0},
            {"date": "2026-01-31", "symbol": "C", "alpha_score": 100.0},
        ]
    )

    result = clean_alpha_scores(frame, winsorize_quantile=0.25)

    assert list(result.panel["alpha_score"]) == pytest.approx([-50.0, 0.0, 50.0])
    assert result.report.winsorize_quantile == pytest.approx(0.25)


def test_example_alpha_fixture_is_independent_and_loadable() -> None:
    fixture_path = Path(__file__).resolve().parents[1] / "fixtures" / "example_alpha_scores.csv"

    result = load_alpha_scores(fixture_path, rank_normalize_by_date=True)

    assert result.report.row_count_input == 6
    assert result.report.row_count_output == 6
    assert result.panel["alpha_source"].eq("example_fixture").all()
    assert result.panel["date"].nunique() == 2
    assert result.panel["alpha_score"].between(-1.0, 1.0).all()
