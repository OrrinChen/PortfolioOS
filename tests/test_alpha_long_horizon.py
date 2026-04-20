from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from portfolio_os.alpha.long_horizon import (
    build_bad_month_cohort_membership,
    build_bootstrap_null_summary,
    build_cohort_vector_comparison_summary,
    build_conditional_spread_summary,
    build_leg_concentration_metrics,
    build_factor_focus_pressure_summary,
    build_frame_overlap_summary,
    build_focus_month_absorption_summary,
    build_horizon_factor_ladder,
    build_month_end_signal_frame,
    build_shared_date_frame_map,
    build_spread_distribution_summary,
    build_temporal_distribution_summary,
    build_single_factor_residual_frame,
    classify_absorption_regime,
    fit_factor_attribution,
)


def _toy_returns_panel(periods: int = 90) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=periods)
    base = np.linspace(-1.0, 1.0, periods)
    return pd.DataFrame(
        {
            "AAA": 0.0030 + 0.0004 * base,
            "BBB": -0.0025 - 0.0003 * base,
            "CCC": 0.0010 * np.sin(np.linspace(0.0, 8.0, periods)),
            "DDD": 0.0005 * np.cos(np.linspace(0.0, 10.0, periods)),
        },
        index=dates,
    )


def test_build_month_end_signal_frame_uses_month_end_sampling() -> None:
    returns_panel = _toy_returns_panel()

    result = build_month_end_signal_frame(
        returns_panel,
        reversal_lookback_days=5,
        momentum_lookback_days=10,
        momentum_skip_days=2,
        forward_horizon_days=5,
        reversal_weight=0.0,
        momentum_weight=1.0,
        quantiles=4,
        min_assets_per_date=4,
        trailing_market_window_days=20,
        trailing_signal_window_months=2,
    )

    assert not result.empty
    month_end_dates = set(returns_panel.groupby(returns_panel.index.to_period("M")).apply(lambda x: x.index.max()))
    assert set(pd.to_datetime(result["date"])) <= month_end_dates
    assert {"market_trailing_return", "trailing_signal_spread"} <= set(result.columns)
    assert result["observation_count"].min() >= 4


def test_build_month_end_signal_frame_handles_named_price_index() -> None:
    returns_panel = _toy_returns_panel()
    returns_panel.index.name = "Date"

    result = build_month_end_signal_frame(
        returns_panel,
        reversal_lookback_days=5,
        momentum_lookback_days=10,
        momentum_skip_days=2,
        forward_horizon_days=5,
        reversal_weight=0.0,
        momentum_weight=1.0,
        quantiles=4,
        min_assets_per_date=4,
        trailing_market_window_days=20,
        trailing_signal_window_months=2,
    )

    assert not result.empty


def test_fit_factor_attribution_recovers_known_betas() -> None:
    dates = pd.date_range("2022-01-31", periods=24, freq="ME")
    angle = np.linspace(0.0, 4.0, len(dates))
    factor_frame = pd.DataFrame(
        {
            "date": dates,
            "Mom": np.linspace(-0.03, 0.04, len(dates)),
            "Mkt-RF": 0.02 * np.sin(angle),
            "QMJ": 0.015 * np.cos(angle),
        }
    )
    monthly_frame = pd.DataFrame(
        {
            "date": dates,
            "top_bottom_spread": 0.50 * factor_frame["Mom"] - 0.25 * factor_frame["QMJ"] + 0.02,
        }
    )

    attribution = fit_factor_attribution(
        monthly_frame,
        factor_frame,
        response_column="top_bottom_spread",
        factor_columns=["Mom", "Mkt-RF", "QMJ"],
    )

    beta_lookup = attribution.set_index("term")["beta"]
    assert beta_lookup["Mom"] == pytest.approx(0.50, abs=1e-6)
    assert beta_lookup["QMJ"] == pytest.approx(-0.25, abs=1e-6)
    assert beta_lookup["const"] == pytest.approx(0.02, abs=1e-6)


def test_build_conditional_spread_summary_splits_market_and_signal_states() -> None:
    monthly_frame = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-31", periods=4, freq="ME"),
            "top_bottom_spread": [0.10, -0.05, 0.20, -0.10],
            "market_trailing_return": [0.30, 0.25, -0.05, -0.10],
            "trailing_signal_spread": [0.02, -0.01, 0.03, -0.04],
        }
    )

    summary = build_conditional_spread_summary(monthly_frame, spread_column="top_bottom_spread")

    market_counts = {
        row["bucket"]: int(row["count"])
        for _, row in summary.loc[summary["split_name"] == "market_state"].iterrows()
    }
    signal_means = {
        row["bucket"]: float(row["mean_spread"])
        for _, row in summary.loc[summary["split_name"] == "signal_state"].iterrows()
    }

    assert market_counts == {"high_12m_market": 2, "low_12m_market": 2}
    assert signal_means["positive_trailing_spread"] == pytest.approx(0.15)
    assert signal_means["nonpositive_trailing_spread"] == pytest.approx(-0.075)


def test_fit_factor_attribution_aligns_trading_month_end_to_calendar_month_end() -> None:
    monthly_frame = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-30", "2024-02-29", "2024-03-28", "2024-04-30"]),
            "top_bottom_spread": [0.02, -0.04, 0.05, -0.08],
        }
    )
    factor_frame = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30"]),
            "Mom": [0.01, -0.02, 0.025, -0.04],
        }
    )

    attribution = fit_factor_attribution(
        monthly_frame,
        factor_frame,
        response_column="top_bottom_spread",
        factor_columns=["Mom"],
    )

    beta_lookup = attribution.set_index("term")["beta"]
    assert beta_lookup["Mom"] == pytest.approx(2.0, abs=1e-6)


def test_build_single_factor_residual_frame_recovers_expected_component() -> None:
    monthly_frame = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-30", "2024-02-29", "2024-03-28", "2024-04-30"]),
            "top_bottom_spread": [0.02, -0.04, 0.05, -0.08],
        }
    )
    factor_frame = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30"]),
            "Mom": [0.01, -0.02, 0.025, -0.04],
        }
    )

    residual_frame = build_single_factor_residual_frame(
        monthly_frame,
        factor_frame,
        response_column="top_bottom_spread",
        factor_column="Mom",
    )
    february = residual_frame.loc[residual_frame["month_key"] == pd.Period("2024-02", freq="M")].iloc[0]

    assert february["factor_component"] == pytest.approx(-0.04, abs=1e-6)
    assert february["matching_absorption_share"] == pytest.approx(1.0, abs=1e-6)


def test_focus_month_absorption_summary_uses_matching_sign_share() -> None:
    residual_frame = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-02-29", "2024-04-30"]),
            "month_key": pd.PeriodIndex(["2024-02", "2024-04"], freq="M"),
            "response_value": [-0.04, -0.08],
            "factor_value": [-0.02, 0.01],
            "factor_component": [-0.04, 0.02],
            "factor_residual": [0.0, -0.10],
            "model_residual": [0.0, -0.11],
            "matching_absorption_share": [1.0, 0.0],
            "offsetting_share": [0.0, 0.25],
        }
    )
    focus_summary = build_focus_month_absorption_summary(residual_frame, focus_months=["2024-02", "2024-04"])
    february = focus_summary.loc[focus_summary["focus_month"] == "2024-02"].iloc[0]
    april = focus_summary.loc[focus_summary["focus_month"] == "2024-04"].iloc[0]

    assert february["matching_absorption_share"] == pytest.approx(1.0, abs=1e-6)
    assert april["matching_absorption_share"] == pytest.approx(0.0, abs=1e-6)
    assert april["offsetting_share"] == pytest.approx(0.25, abs=1e-6)


def test_classify_absorption_regime_uses_matching_share_thresholds() -> None:
    summary = pd.DataFrame(
        {
            "focus_month": ["a", "b", "c"],
            "matching_absorption_share": [0.10, 0.15, 0.20],
        }
    )
    assert classify_absorption_regime(summary) == "independent_residual"

    summary["matching_absorption_share"] = [0.75, 0.80, 0.90]
    assert classify_absorption_regime(summary) == "momentum_absorbed"

    summary["matching_absorption_share"] = [0.45, 0.55, 0.60]
    assert classify_absorption_regime(summary) == "mixed"


def test_build_shared_date_frame_map_filters_to_common_months() -> None:
    frame_a = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-29"]),
            "top_bottom_spread": [0.01, 0.02, 0.03],
            "observation_count": [10, 11, 12],
        }
    )
    frame_b = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-02-29", "2024-03-29", "2024-04-30"]),
            "top_bottom_spread": [0.04, 0.05, 0.06],
            "observation_count": [21, 22, 23],
        }
    )

    shared = build_shared_date_frame_map({"5d": frame_a, "21d": frame_b})

    assert list(shared["5d"]["date"]) == list(pd.to_datetime(["2024-02-29", "2024-03-29"]))
    assert list(shared["21d"]["date"]) == list(pd.to_datetime(["2024-02-29", "2024-03-29"]))


def test_build_frame_overlap_summary_reports_shared_and_exclusive_months() -> None:
    frame_a = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-29"]),
            "observation_count": [10, 11, 12],
        }
    )
    frame_b = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-02-29", "2024-03-29", "2024-04-30"]),
            "observation_count": [21, 22, 23],
        }
    )

    summary = build_frame_overlap_summary({"5d": frame_a, "21d": frame_b}).set_index("frame_label")

    assert summary.loc["5d", "month_count"] == 3
    assert summary.loc["5d", "shared_month_count"] == 2
    assert summary.loc["5d", "exclusive_month_count"] == 1
    assert summary.loc["5d", "mean_observation_count"] == pytest.approx(11.0)
    assert summary.loc["21d", "month_count"] == 3
    assert summary.loc["21d", "shared_month_count"] == 2
    assert summary.loc["21d", "exclusive_month_count"] == 1
    assert summary.loc["21d", "mean_observation_count"] == pytest.approx(22.0)


def test_build_horizon_factor_ladder_uses_shared_dates() -> None:
    factor_frame = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30"]),
            "Mom": [0.01, 0.02, 0.03, 0.04],
        }
    )
    frame_5d = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-29"]),
            "top_bottom_spread": [0.11, 0.04, 0.06],
            "observation_count": [10, 10, 10],
        }
    )
    frame_21d = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-02-29", "2024-03-29", "2024-04-30"]),
            "top_bottom_spread": [0.04, 0.06, 0.08],
            "observation_count": [10, 10, 10],
        }
    )

    ladder = build_horizon_factor_ladder(
        {"5d": frame_5d, "21d": frame_21d},
        factor_frame,
        response_column="top_bottom_spread",
        factor_columns=["Mom"],
        use_shared_dates=True,
    )
    beta_lookup = ladder.loc[ladder["term"] == "Mom"].set_index("frame_label")["beta"]
    count_lookup = ladder.loc[ladder["term"] == "Mom"].set_index("frame_label")["observation_count"]

    assert beta_lookup["5d"] == pytest.approx(2.0, abs=1e-6)
    assert beta_lookup["21d"] == pytest.approx(2.0, abs=1e-6)
    assert count_lookup["5d"] == 2
    assert count_lookup["21d"] == 2


def test_build_spread_distribution_summary_captures_tail_and_drawdown() -> None:
    monthly_frame = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30", "2024-05-31", "2024-06-30"]
            ),
            "top_bottom_spread": [0.10, -0.20, 0.05, -0.10, 0.02, -0.03],
        }
    )

    summary = build_spread_distribution_summary(monthly_frame, spread_column="top_bottom_spread", worst_n=3).iloc[0]

    assert summary["observation_count"] == 6
    assert summary["negative_month_ratio"] == pytest.approx(0.5)
    assert summary["worst_month_spread"] == pytest.approx(-0.20)
    assert summary["worst_1_negative_share"] == pytest.approx(0.6060606, rel=1e-5)
    assert summary["worst_3_negative_share"] == pytest.approx(1.0, abs=1e-6)
    assert summary["max_drawdown"] == pytest.approx(-0.2520136, rel=1e-5)


def test_build_factor_focus_pressure_summary_summarizes_factor_classifications() -> None:
    summary = build_factor_focus_pressure_summary(
        {
            "Mom": pd.DataFrame(
                {
                    "matching_absorption_share": [0.05, 0.02, 0.01],
                    "offsetting_share": [0.00, 0.00, 0.00],
                }
            ),
            "QMJ": pd.DataFrame(
                {
                    "matching_absorption_share": [0.80, 0.75, 0.70],
                    "offsetting_share": [0.00, 0.00, 0.00],
                }
            ),
        }
    ).set_index("factor_name")

    assert summary.loc["Mom", "classification"] == "independent_residual"
    assert summary.loc["Mom", "mean_matching_absorption_share"] == pytest.approx(0.0266667, rel=1e-5)
    assert summary.loc["QMJ", "classification"] == "momentum_absorbed"
    assert summary.loc["QMJ", "mean_matching_absorption_share"] == pytest.approx(0.75, abs=1e-6)


def test_build_bad_month_cohort_membership_splits_outer_inner_and_non_bad() -> None:
    monthly_frame = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-31", periods=10, freq="ME"),
            "top_bottom_spread": [0.10, 0.08, 0.05, 0.03, 0.01, -0.01, -0.02, -0.03, -0.10, -0.20],
        }
    )

    membership = build_bad_month_cohort_membership(monthly_frame, spread_column="top_bottom_spread", bad_quantile=0.2)

    label_lookup = membership.set_index(membership["date"].dt.strftime("%Y-%m-%d"))["cohort_label"]
    assert label_lookup["2024-10-31"] == "outer_half"
    assert label_lookup["2024-09-30"] == "inner_half"
    assert (membership["cohort_label"] == "non_bad").sum() == 8
    assert membership["is_bad_month"].sum() == 2


def test_build_bootstrap_null_summary_reports_percentile_and_ratio() -> None:
    summary = build_bootstrap_null_summary(
        observed_value=0.15,
        bootstrap_values=np.array([0.10, 0.20, 0.30]),
        comparison_name="outer_vs_non_bad",
    ).iloc[0]

    assert summary["comparison_name"] == "outer_vs_non_bad"
    assert summary["bootstrap_median"] == pytest.approx(0.20, abs=1e-6)
    assert summary["observed_to_bootstrap_median_ratio"] == pytest.approx(0.75, abs=1e-6)
    assert summary["bootstrap_percentile"] == pytest.approx(1.0 / 3.0, abs=1e-6)


def test_build_temporal_distribution_summary_reports_year_distribution() -> None:
    membership = pd.DataFrame(
        {
            "date": pd.to_datetime(["2010-01-31", "2011-01-31", "2012-01-31", "2018-01-31", "2019-01-31"]),
            "cohort_label": ["outer_half", "outer_half", "inner_half", "inner_half", "non_bad"],
        }
    )

    summary = build_temporal_distribution_summary(membership).set_index("cohort_label")

    assert summary.loc["outer_half", "month_count"] == 2
    assert summary.loc["outer_half", "median_year"] == pytest.approx(2010.5, abs=1e-6)
    assert summary.loc["inner_half", "month_count"] == 2
    assert summary.loc["inner_half", "median_year"] == pytest.approx(2015.0, abs=1e-6)


def test_build_leg_concentration_metrics_applies_small_loss_guard_per_leg() -> None:
    leg_frame = pd.DataFrame(
        {
            "leg_label": ["top", "top", "bottom", "bottom"],
            "adverse_contribution": [0.002, 0.002, 0.01, 0.02],
        }
    )

    metrics = build_leg_concentration_metrics(
        leg_frame,
        leg_column="leg_label",
        adverse_contribution_column="adverse_contribution",
        loss_floor=0.005,
    ).set_index("leg_label")

    assert np.isnan(metrics.loc["top", "hhi"])
    assert np.isnan(metrics.loc["top", "effective_n"])
    assert metrics.loc["bottom", "hhi"] == pytest.approx((1.0 / 3.0) ** 2 + (2.0 / 3.0) ** 2, abs=1e-6)
    assert metrics.loc["bottom", "effective_n"] == pytest.approx(1.8, rel=1e-6)


def test_build_cohort_vector_comparison_summary_reports_distance_against_bootstrap_null() -> None:
    vector_frame = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-01-31",
                    "2024-01-31",
                    "2024-02-29",
                    "2024-02-29",
                    "2024-03-31",
                    "2024-03-31",
                    "2024-04-30",
                    "2024-04-30",
                ]
            ),
            "cohort_label": [
                "outer_half",
                "outer_half",
                "inner_half",
                "inner_half",
                "non_bad",
                "non_bad",
                "non_bad",
                "non_bad",
            ],
            "vector_key": ["A", "B", "A", "B", "A", "B", "A", "B"],
            "vector_value": [0.80, 0.20, 0.75, 0.25, 0.20, 0.80, 0.25, 0.75],
        }
    )

    summary = build_cohort_vector_comparison_summary(
        vector_frame,
        dimension_name="size_bucket",
        metric_name="l1_distance",
        metric_direction="distance",
        bootstrap_iterations=200,
        random_seed=7,
    ).iloc[0]

    assert summary["dimension_name"] == "size_bucket"
    assert summary["outer_inner_metric"] == pytest.approx(0.10, abs=1e-6)
    assert summary["outer_non_bad_bootstrap_median"] > summary["outer_inner_metric"]
    assert 0.0 <= summary["outer_inner_vs_outer_non_bad_percentile"] <= 1.0


def test_build_cohort_vector_comparison_summary_supports_rank_correlation() -> None:
    vector_frame = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-01-31",
                    "2024-01-31",
                    "2024-01-31",
                    "2024-02-29",
                    "2024-02-29",
                    "2024-02-29",
                    "2024-03-31",
                    "2024-03-31",
                    "2024-03-31",
                    "2024-04-30",
                    "2024-04-30",
                    "2024-04-30",
                ]
            ),
            "cohort_label": [
                "outer_half",
                "outer_half",
                "outer_half",
                "inner_half",
                "inner_half",
                "inner_half",
                "non_bad",
                "non_bad",
                "non_bad",
                "non_bad",
                "non_bad",
                "non_bad",
            ],
            "vector_key": ["A", "B", "C"] * 4,
            "vector_value": [
                0.60,
                0.30,
                0.10,
                0.55,
                0.35,
                0.10,
                0.10,
                0.30,
                0.60,
                0.30,
                0.10,
                0.60,
            ],
        }
    )

    summary = build_cohort_vector_comparison_summary(
        vector_frame,
        dimension_name="sector_label",
        metric_name="rank_correlation",
        metric_direction="similarity",
        bootstrap_iterations=200,
        random_seed=7,
    ).iloc[0]

    assert summary["dimension_name"] == "sector_label"
    assert summary["outer_inner_metric"] == pytest.approx(1.0, abs=1e-6)
    assert 0.0 <= summary["outer_inner_vs_outer_non_bad_percentile"] <= 1.0
