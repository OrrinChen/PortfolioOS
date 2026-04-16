from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from portfolio_os.alpha.backtest_bridge import (
    build_alpha_only_target_weights,
    build_alpha_snapshot_for_rebalance,
)


def _write_alpha_bridge_returns_fixture(tmp_path: Path) -> Path:
    returns_path = tmp_path / "returns_long.csv"
    dates = pd.date_range("2025-01-02", periods=180, freq="B")
    rows: list[dict[str, object]] = []
    for ticker_index in range(25):
        ticker = f"T{ticker_index:02d}"
        strength = 0.0025 - 0.0001 * ticker_index
        for day_index, date_value in enumerate(dates):
            seasonal = ((day_index % 10) - 5) * 0.0001
            rows.append(
                {
                    "date": date_value.strftime("%Y-%m-%d"),
                    "ticker": ticker,
                    "return": float(strength + seasonal),
                }
            )
    pd.DataFrame(rows).to_csv(returns_path, index=False)
    return returns_path


def test_build_alpha_only_target_weights_selects_top_quintile_equal_weight() -> None:
    frame = pd.DataFrame(
        {
            "ticker": ["A", "B", "C", "D", "E"],
            "alpha_score": [5.0, 4.0, 3.0, 2.0, 1.0],
        }
    )

    weights = build_alpha_only_target_weights(frame, quantiles=5)

    assert weights == {"A": 1.0, "B": 0.0, "C": 0.0, "D": 0.0, "E": 0.0}


def test_build_alpha_snapshot_uses_only_history_up_to_rebalance_date(tmp_path: Path) -> None:
    returns_path = _write_alpha_bridge_returns_fixture(tmp_path)

    snapshot = build_alpha_snapshot_for_rebalance(
        returns_file=returns_path,
        rebalance_date="2025-08-29",
        quantiles=5,
        min_evaluation_dates=20,
    )

    assert pd.to_datetime(snapshot.signal_frame["date"]).max() <= pd.Timestamp("2025-08-29")
    assert snapshot.current_cross_section["date"].nunique() == 1
    assert snapshot.current_cross_section["date"].iloc[0] == "2025-08-29"
    assert pd.to_datetime(snapshot.ic_frame["date"]).max() < pd.Timestamp("2025-08-29")


def test_build_alpha_snapshot_outputs_expected_return_and_quantile_columns(tmp_path: Path) -> None:
    returns_path = _write_alpha_bridge_returns_fixture(tmp_path)

    snapshot = build_alpha_snapshot_for_rebalance(
        returns_file=returns_path,
        rebalance_date="2025-08-29",
        quantiles=5,
        min_evaluation_dates=20,
    )

    assert {
        "ticker",
        "alpha_score",
        "alpha_rank_pct",
        "alpha_zscore",
        "expected_return",
        "quantile",
    } <= set(snapshot.current_cross_section.columns)
    assert snapshot.current_cross_section["expected_return"].notna().all()
    assert snapshot.current_cross_section["quantile"].between(1, 5).all()
    assert snapshot.current_cross_section["ticker"].nunique() == 25
    assert len([weight for weight in snapshot.alpha_only_target_weights.values() if weight > 0.0]) == 5
    assert abs(sum(snapshot.alpha_only_target_weights.values()) - 1.0) < 1e-12


def test_build_alpha_snapshot_deannualizes_expected_return_to_decision_horizon(tmp_path: Path) -> None:
    returns_path = _write_alpha_bridge_returns_fixture(tmp_path)

    snapshot = build_alpha_snapshot_for_rebalance(
        returns_file=returns_path,
        rebalance_date="2025-08-29",
        quantiles=5,
        min_evaluation_dates=20,
        decision_horizon_days=21,
    )

    frame = snapshot.current_cross_section
    annualized_spread = float(frame["annualized_top_bottom_spread"].iloc[0])
    period_spread = float(frame["period_top_bottom_spread"].iloc[0])
    confidence = float(frame["signal_strength_confidence"].iloc[0])
    z_gap = float(
        frame.loc[frame["quantile"] == 5, "alpha_zscore"].mean()
        - frame.loc[frame["quantile"] == 1, "alpha_zscore"].mean()
    )
    expected_returns = (
        confidence
        * period_spread
        * frame["alpha_zscore"]
        / z_gap
    ).clip(lower=-0.30, upper=0.30)

    assert frame["decision_horizon_days"].nunique() == 1
    assert int(frame["decision_horizon_days"].iloc[0]) == 21
    assert period_spread == pytest.approx((1.0 + annualized_spread) ** (21.0 / 252.0) - 1.0)
    assert frame["expected_return"].to_numpy(dtype=float) == pytest.approx(expected_returns.to_numpy(dtype=float))
