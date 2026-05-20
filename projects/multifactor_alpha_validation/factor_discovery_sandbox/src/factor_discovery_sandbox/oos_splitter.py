"""Rolling train/validation/test splits for FD real-data OOS validation."""

from __future__ import annotations

import pandas as pd


def build_rolling_oos_splits(
    signal_dates: list[pd.Timestamp] | pd.Series,
    train_window_months: int = 36,
    validation_window_months: int = 12,
    max_horizon_months: int = 3,
) -> pd.DataFrame:
    """Build rolling OOS rebalance splits without future dates."""

    dates = [pd.Timestamp(date) for date in sorted(pd.to_datetime(signal_dates).dropna().unique())]
    rows: list[dict[str, object]] = []
    last_start_index = max(0, len(dates) - max_horizon_months)
    for position in range(train_window_months, last_start_index):
        rebalance_date = dates[position]
        window_dates = dates[position - train_window_months : position]
        period = "validation" if position < train_window_months + validation_window_months else "test"
        rows.append(
            {
                "schema_version": "fd_real_oos_split.v1",
                "rebalance_date": rebalance_date.date().isoformat(),
                "rebalance_index": position,
                "period": period,
                "train_window_start": window_dates[0].date().isoformat(),
                "train_window_end": window_dates[-1].date().isoformat(),
                "train_window_months": train_window_months,
                "validation_window_months": validation_window_months,
                "max_horizon_months": max_horizon_months,
                "future_dates_used": False,
            }
        )
    return pd.DataFrame(rows)
