from __future__ import annotations

import pandas as pd
import pytest

from portfolio_os.alpha.state_transition_panel import (
    build_state_transition_daily_panel,
    build_upper_limit_event_conditioned_null_draw,
    build_upper_limit_event_conditioned_null_pool,
    build_upper_limit_event_conditioned_null_summary,
    build_upper_limit_matched_control_comparison_frame,
    build_state_transition_matching_covariates,
    build_upper_limit_pre_event_placebo_comparison_frame,
    build_upper_limit_pilot_expression_frame,
    build_upper_limit_matched_non_event_control_frame,
    extract_upper_limit_daily_state_slice,
)
from portfolio_os.domain.errors import InputValidationError


def _daily_bar_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2026-04-01",
                "ticker": "000001",
                "open": 10.00,
                "high": 11.00,
                "low": 9.95,
                "close": 11.00,
                "volume": 1_000_000,
                "amount": 10_500_000,
                "upper_limit_price": 11.00,
                "lower_limit_price": 9.00,
                "tradable": True,
            },
            {
                "date": "2026-04-02",
                "ticker": "000001",
                "open": 11.10,
                "high": 11.20,
                "low": 10.80,
                "close": 10.90,
                "volume": 900_000,
                "amount": 9_900_000,
                "upper_limit_price": 12.10,
                "lower_limit_price": 9.90,
                "tradable": True,
            },
            {
                "date": "2026-04-01",
                "ticker": "000002",
                "open": 20.00,
                "high": 22.00,
                "low": 19.80,
                "close": 21.20,
                "volume": 2_000_000,
                "amount": 42_000_000,
                "upper_limit_price": 22.00,
                "lower_limit_price": 18.00,
                "tradable": True,
            },
            {
                "date": "2026-04-02",
                "ticker": "000002",
                "open": 21.00,
                "high": 21.10,
                "low": 20.00,
                "close": 20.40,
                "volume": 1_500_000,
                "amount": 31_000_000,
                "upper_limit_price": 23.32,
                "lower_limit_price": 19.08,
                "tradable": True,
            },
        ]
    )


def _matched_control_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2026-04-01",
                "ticker": "000001",
                "tradable": True,
                "upper_limit_touched": True,
                "sealed_upper_limit": True,
                "failed_upper_limit": False,
                "industry": "Industrials",
                "size_tercile": 2,
                "liquidity_tercile": 2,
                "recent_realized_volatility": 0.18,
                "recent_return_state": 0.12,
            },
            {
                "date": "2026-04-01",
                "ticker": "000002",
                "tradable": True,
                "upper_limit_touched": True,
                "sealed_upper_limit": False,
                "failed_upper_limit": True,
                "industry": "Consumer",
                "size_tercile": 1,
                "liquidity_tercile": 1,
                "recent_realized_volatility": 0.25,
                "recent_return_state": -0.02,
            },
            {
                "date": "2026-04-01",
                "ticker": "000003",
                "tradable": True,
                "upper_limit_touched": False,
                "sealed_upper_limit": False,
                "failed_upper_limit": False,
                "industry": "Industrials",
                "size_tercile": 2,
                "liquidity_tercile": 2,
                "recent_realized_volatility": 0.17,
                "recent_return_state": 0.11,
            },
            {
                "date": "2026-04-01",
                "ticker": "000004",
                "tradable": True,
                "upper_limit_touched": False,
                "sealed_upper_limit": False,
                "failed_upper_limit": False,
                "industry": "Industrials",
                "size_tercile": 2,
                "liquidity_tercile": 2,
                "recent_realized_volatility": 0.32,
                "recent_return_state": 0.06,
            },
            {
                "date": "2026-04-01",
                "ticker": "000005",
                "tradable": True,
                "upper_limit_touched": False,
                "sealed_upper_limit": False,
                "failed_upper_limit": False,
                "industry": "Consumer",
                "size_tercile": 1,
                "liquidity_tercile": 1,
                "recent_realized_volatility": 0.24,
                "recent_return_state": -0.03,
            },
            {
                "date": "2026-04-01",
                "ticker": "000006",
                "tradable": False,
                "upper_limit_touched": False,
                "sealed_upper_limit": False,
                "failed_upper_limit": False,
                "industry": "Consumer",
                "size_tercile": 1,
                "liquidity_tercile": 1,
                "recent_realized_volatility": 0.249,
                "recent_return_state": -0.021,
            },
        ]
    )


def _matching_covariate_daily_bar_fixture() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for date_value, closes, amounts in [
        ("2026-03-30", {"000001": 10.0, "000002": 20.0, "000003": 5.0}, {"000001": 10_000_000, "000002": 30_000_000, "000003": 5_000_000}),
        ("2026-03-31", {"000001": 10.5, "000002": 20.2, "000003": 6.0}, {"000001": 11_000_000, "000002": 31_000_000, "000003": 4_000_000}),
        ("2026-04-01", {"000001": 11.0, "000002": 20.1, "000003": 5.5}, {"000001": 12_000_000, "000002": 29_000_000, "000003": 6_000_000}),
        ("2026-04-02", {"000001": 11.5, "000002": 20.3, "000003": 6.5}, {"000001": 13_000_000, "000002": 32_000_000, "000003": 7_000_000}),
    ]:
        for ticker, close in closes.items():
            rows.append(
                {
                    "date": date_value,
                    "ticker": ticker,
                    "open": close * 0.99,
                    "high": close * 1.01,
                    "low": close * 0.98,
                    "close": close,
                    "volume": 1_000_000,
                    "amount": amounts[ticker],
                    "upper_limit_price": close * 1.10,
                    "lower_limit_price": close * 0.90,
                    "tradable": True,
                }
            )
    return pd.DataFrame(rows)


def _matching_reference_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"ticker": "000001", "industry": "Industrials", "issuer_total_shares": 10_000_000.0},
            {"ticker": "000002", "industry": "Financials", "issuer_total_shares": 20_000_000.0},
            {"ticker": "000003", "industry": "Technology", "issuer_total_shares": 30_000_000.0},
        ]
    )


def _matched_control_forward_panel_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2026-04-01",
                "ticker": "000003",
                "next_open_return": 0.01,
                "next_intraday_return": 0.02,
                "next_close_return": 0.0302,
            },
            {
                "date": "2026-04-01",
                "ticker": "000005",
                "next_open_return": -0.01,
                "next_intraday_return": -0.02,
                "next_close_return": -0.0298,
            },
        ]
    )


def _pre_event_placebo_daily_bar_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2026-03-31",
                "ticker": "000001",
                "open": 9.50,
                "high": 10.05,
                "low": 9.45,
                "close": 10.00,
                "volume": 950_000,
                "amount": 9_600_000,
                "upper_limit_price": 10.45,
                "lower_limit_price": 8.55,
                "tradable": True,
            },
            {
                "date": "2026-04-01",
                "ticker": "000001",
                "open": 10.00,
                "high": 11.00,
                "low": 9.95,
                "close": 11.00,
                "volume": 1_000_000,
                "amount": 10_500_000,
                "upper_limit_price": 11.00,
                "lower_limit_price": 9.00,
                "tradable": True,
            },
            {
                "date": "2026-04-02",
                "ticker": "000001",
                "open": 11.10,
                "high": 11.20,
                "low": 10.80,
                "close": 10.90,
                "volume": 900_000,
                "amount": 9_900_000,
                "upper_limit_price": 12.10,
                "lower_limit_price": 9.90,
                "tradable": True,
            },
            {
                "date": "2026-03-31",
                "ticker": "000002",
                "open": 20.50,
                "high": 20.70,
                "low": 19.90,
                "close": 20.00,
                "volume": 1_900_000,
                "amount": 39_000_000,
                "upper_limit_price": 22.55,
                "lower_limit_price": 18.45,
                "tradable": True,
            },
            {
                "date": "2026-04-01",
                "ticker": "000002",
                "open": 20.00,
                "high": 22.00,
                "low": 19.80,
                "close": 21.20,
                "volume": 2_000_000,
                "amount": 42_000_000,
                "upper_limit_price": 22.00,
                "lower_limit_price": 18.00,
                "tradable": True,
            },
            {
                "date": "2026-04-02",
                "ticker": "000002",
                "open": 21.00,
                "high": 21.10,
                "low": 20.00,
                "close": 20.40,
                "volume": 1_500_000,
                "amount": 31_000_000,
                "upper_limit_price": 23.32,
                "lower_limit_price": 19.08,
                "tradable": True,
            },
        ]
    )


def _event_conditioned_null_pool_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2026-04-01",
                "expression_id": "P1_SEALED_UPPER_LIMIT",
                "mechanism_id": "M1",
                "state_anchor": "SEALED_UPPER_LIMIT",
                "event_ticker": "000001",
                "signal_value": 1.0,
                "expected_sign": 1.0,
                "forward_return": 0.10,
                "event_type_bucket": "SEALED_UPPER_LIMIT",
                "horizon_bucket": "NEXT_CLOSE",
                "size_tercile": 2,
                "liquidity_tercile": 2,
                "conditioning_bucket_key": "SEALED_UPPER_LIMIT|NEXT_CLOSE|2|2",
            },
            {
                "date": "2026-04-01",
                "expression_id": "P1_SEALED_UPPER_LIMIT",
                "mechanism_id": "M1",
                "state_anchor": "SEALED_UPPER_LIMIT",
                "event_ticker": "000007",
                "signal_value": 1.0,
                "expected_sign": 1.0,
                "forward_return": 0.30,
                "event_type_bucket": "SEALED_UPPER_LIMIT",
                "horizon_bucket": "NEXT_CLOSE",
                "size_tercile": 2,
                "liquidity_tercile": 2,
                "conditioning_bucket_key": "SEALED_UPPER_LIMIT|NEXT_CLOSE|2|2",
            },
            {
                "date": "2026-04-01",
                "expression_id": "P4_NEXT_DAY_AFTER_FAILED",
                "mechanism_id": "M5",
                "state_anchor": "FAILED_UPPER_LIMIT",
                "event_ticker": "000002",
                "signal_value": -1.0,
                "expected_sign": -1.0,
                "forward_return": -0.20,
                "event_type_bucket": "FAILED_UPPER_LIMIT",
                "horizon_bucket": "NEXT_INTRADAY",
                "size_tercile": 1,
                "liquidity_tercile": 1,
                "conditioning_bucket_key": "FAILED_UPPER_LIMIT|NEXT_INTRADAY|1|1",
            },
        ]
    )


def test_build_state_transition_daily_panel_derives_upper_limit_states() -> None:
    panel = build_state_transition_daily_panel(_daily_bar_fixture())
    same_day = panel.loc[panel["date"] == "2026-04-01"].set_index("ticker")

    assert bool(same_day.loc["000001", "upper_limit_touched"]) is True
    assert bool(same_day.loc["000001", "sealed_upper_limit"]) is True
    assert bool(same_day.loc["000001", "failed_upper_limit"]) is False

    assert bool(same_day.loc["000002", "upper_limit_touched"]) is True
    assert bool(same_day.loc["000002", "sealed_upper_limit"]) is False
    assert bool(same_day.loc["000002", "failed_upper_limit"]) is True


def test_build_state_transition_daily_panel_derives_next_day_returns() -> None:
    panel = build_state_transition_daily_panel(_daily_bar_fixture())
    same_day = panel.loc[
        (panel["date"] == "2026-04-01") & (panel["ticker"] == "000001")
    ].iloc[0]

    assert same_day["next_open_return"] == pytest.approx(11.10 / 11.00 - 1.0)
    assert same_day["next_intraday_return"] == pytest.approx(10.90 / 11.10 - 1.0)
    assert same_day["next_close_return"] == pytest.approx(10.90 / 11.00 - 1.0)


def test_build_state_transition_daily_panel_rejects_missing_required_columns() -> None:
    bad = _daily_bar_fixture().drop(columns=["upper_limit_price"])

    with pytest.raises(InputValidationError, match="missing required columns"):
        build_state_transition_daily_panel(bad)


def test_extract_upper_limit_daily_state_slice_keeps_only_pilot_rows() -> None:
    panel = build_state_transition_daily_panel(_daily_bar_fixture())
    pilot = extract_upper_limit_daily_state_slice(panel)

    assert set(pilot["ticker"]) == {"000001", "000002"}
    assert {
        "sealed_upper_limit",
        "failed_upper_limit",
        "next_open_return",
        "next_intraday_return",
        "next_close_return",
    } <= set(pilot.columns)
    assert bool(pilot.loc[pilot["ticker"] == "000001", "sealed_upper_limit"].iloc[0]) is True
    assert bool(pilot.loc[pilot["ticker"] == "000002", "failed_upper_limit"].iloc[0]) is True


def test_build_upper_limit_pilot_expression_frame_emits_p1_to_p4_rows() -> None:
    panel = build_state_transition_daily_panel(_daily_bar_fixture())

    expression_frame = build_upper_limit_pilot_expression_frame(panel)
    keyed = expression_frame.set_index(["expression_id", "ticker"])

    assert set(expression_frame["expression_id"]) == {
        "P1_SEALED_UPPER_LIMIT",
        "P2_FAILED_UPPER_LIMIT",
        "P3_NEXT_DAY_AFTER_SEALED",
        "P4_NEXT_DAY_AFTER_FAILED",
    }
    assert list(expression_frame.columns) == [
        "date",
        "ticker",
        "expression_id",
        "mechanism_id",
        "state_anchor",
        "signal_value",
        "expected_sign",
        "forward_return",
        "next_open_return",
        "next_intraday_return",
        "next_close_return",
    ]

    assert keyed.loc[("P1_SEALED_UPPER_LIMIT", "000001"), "mechanism_id"] == "M1"
    assert keyed.loc[("P1_SEALED_UPPER_LIMIT", "000001"), "state_anchor"] == "SEALED_UPPER_LIMIT"
    assert keyed.loc[("P1_SEALED_UPPER_LIMIT", "000001"), "signal_value"] == pytest.approx(1.0)
    assert keyed.loc[("P1_SEALED_UPPER_LIMIT", "000001"), "expected_sign"] == pytest.approx(1.0)
    assert keyed.loc[("P1_SEALED_UPPER_LIMIT", "000001"), "forward_return"] == pytest.approx(10.90 / 11.00 - 1.0)

    assert keyed.loc[("P2_FAILED_UPPER_LIMIT", "000002"), "mechanism_id"] == "M2"
    assert keyed.loc[("P2_FAILED_UPPER_LIMIT", "000002"), "state_anchor"] == "FAILED_UPPER_LIMIT"
    assert keyed.loc[("P2_FAILED_UPPER_LIMIT", "000002"), "signal_value"] == pytest.approx(-1.0)
    assert keyed.loc[("P2_FAILED_UPPER_LIMIT", "000002"), "expected_sign"] == pytest.approx(-1.0)
    assert keyed.loc[("P2_FAILED_UPPER_LIMIT", "000002"), "forward_return"] == pytest.approx(20.40 / 21.20 - 1.0)

    assert keyed.loc[("P3_NEXT_DAY_AFTER_SEALED", "000001"), "mechanism_id"] == "M5"
    assert keyed.loc[("P3_NEXT_DAY_AFTER_SEALED", "000001"), "state_anchor"] == "SEALED_UPPER_LIMIT"
    assert keyed.loc[("P3_NEXT_DAY_AFTER_SEALED", "000001"), "signal_value"] == pytest.approx(1.0)
    assert keyed.loc[("P3_NEXT_DAY_AFTER_SEALED", "000001"), "expected_sign"] == pytest.approx(1.0)
    assert keyed.loc[("P3_NEXT_DAY_AFTER_SEALED", "000001"), "forward_return"] == pytest.approx(10.90 / 11.10 - 1.0)

    assert keyed.loc[("P4_NEXT_DAY_AFTER_FAILED", "000002"), "mechanism_id"] == "M5"
    assert keyed.loc[("P4_NEXT_DAY_AFTER_FAILED", "000002"), "state_anchor"] == "FAILED_UPPER_LIMIT"
    assert keyed.loc[("P4_NEXT_DAY_AFTER_FAILED", "000002"), "signal_value"] == pytest.approx(-1.0)
    assert keyed.loc[("P4_NEXT_DAY_AFTER_FAILED", "000002"), "expected_sign"] == pytest.approx(-1.0)
    assert keyed.loc[("P4_NEXT_DAY_AFTER_FAILED", "000002"), "forward_return"] == pytest.approx(20.40 / 21.00 - 1.0)


def test_build_upper_limit_matched_non_event_control_frame_selects_same_bucket_nearest_neighbor() -> None:
    matched = build_upper_limit_matched_non_event_control_frame(_matched_control_fixture())
    keyed = matched.set_index("event_ticker")

    assert list(matched.columns) == [
        "date",
        "event_ticker",
        "control_ticker",
        "event_state_anchor",
        "industry",
        "size_tercile",
        "liquidity_tercile",
        "match_distance",
        "event_recent_realized_volatility",
        "control_recent_realized_volatility",
        "event_recent_return_state",
        "control_recent_return_state",
    ]
    assert keyed.loc["000001", "control_ticker"] == "000003"
    assert keyed.loc["000001", "event_state_anchor"] == "SEALED_UPPER_LIMIT"
    assert keyed.loc["000001", "match_distance"] == pytest.approx(
        abs(0.18 - 0.17) + abs(0.12 - 0.11)
    )

    assert keyed.loc["000002", "control_ticker"] == "000005"
    assert keyed.loc["000002", "event_state_anchor"] == "FAILED_UPPER_LIMIT"
    assert keyed.loc["000002", "match_distance"] == pytest.approx(
        abs(0.25 - 0.24) + abs(-0.02 - (-0.03))
    )


def test_build_upper_limit_matched_non_event_control_frame_rejects_missing_matching_columns() -> None:
    bad = _matched_control_fixture().drop(columns=["recent_return_state"])

    with pytest.raises(InputValidationError, match="requires matching columns"):
        build_upper_limit_matched_non_event_control_frame(bad)


def test_build_state_transition_matching_covariates_derives_reference_and_rolling_fields() -> None:
    panel = build_state_transition_daily_panel(_matching_covariate_daily_bar_fixture())

    enriched = build_state_transition_matching_covariates(
        panel,
        _matching_reference_fixture(),
        lookback_days=3,
    )
    latest = enriched.loc[enriched["date"] == "2026-04-02"].set_index("ticker")

    assert latest.loc["000001", "industry"] == "Industrials"
    assert latest.loc["000002", "industry"] == "Financials"
    assert latest.loc["000001", "float_market_cap"] == pytest.approx(11.5 * 10_000_000.0)
    assert latest.loc["000002", "float_market_cap"] == pytest.approx(20.3 * 20_000_000.0)

    assert int(latest.loc["000001", "size_tercile"]) == 1
    assert int(latest.loc["000003", "size_tercile"]) == 2
    assert int(latest.loc["000002", "size_tercile"]) == 3

    assert int(latest.loc["000003", "liquidity_tercile"]) == 1
    assert int(latest.loc["000001", "liquidity_tercile"]) == 2
    assert int(latest.loc["000002", "liquidity_tercile"]) == 3

    assert latest.loc["000001", "recent_return_state"] == pytest.approx(11.5 / 10.0 - 1.0)
    assert latest.loc["000003", "recent_return_state"] == pytest.approx(6.5 / 5.0 - 1.0)
    assert float(latest.loc["000003", "recent_realized_volatility"]) > float(
        latest.loc["000002", "recent_realized_volatility"]
    )


def test_build_state_transition_matching_covariates_rejects_missing_reference_columns() -> None:
    panel = build_state_transition_daily_panel(_matching_covariate_daily_bar_fixture())
    bad_reference = _matching_reference_fixture().drop(columns=["industry"])

    with pytest.raises(InputValidationError, match="requires reference columns"):
        build_state_transition_matching_covariates(panel, bad_reference, lookback_days=3)


def test_build_upper_limit_matched_control_comparison_frame_joins_event_and_control_horizons() -> None:
    event_panel = build_state_transition_daily_panel(_daily_bar_fixture())
    expression_frame = build_upper_limit_pilot_expression_frame(event_panel)
    matched_controls = build_upper_limit_matched_non_event_control_frame(_matched_control_fixture())

    comparison = build_upper_limit_matched_control_comparison_frame(
        expression_frame,
        matched_controls,
        _matched_control_forward_panel_fixture(),
    )
    keyed = comparison.set_index(["expression_id", "event_ticker"])

    assert list(comparison.columns) == [
        "date",
        "expression_id",
        "mechanism_id",
        "state_anchor",
        "event_ticker",
        "control_ticker",
        "signal_value",
        "expected_sign",
        "event_forward_return",
        "control_forward_return",
        "excess_forward_return",
    ]
    assert keyed.loc[("P1_SEALED_UPPER_LIMIT", "000001"), "control_ticker"] == "000003"
    assert keyed.loc[("P1_SEALED_UPPER_LIMIT", "000001"), "control_forward_return"] == pytest.approx(0.0302)
    assert keyed.loc[("P1_SEALED_UPPER_LIMIT", "000001"), "excess_forward_return"] == pytest.approx(
        (10.90 / 11.00 - 1.0) - 0.0302
    )

    assert keyed.loc[("P4_NEXT_DAY_AFTER_FAILED", "000002"), "control_ticker"] == "000005"
    assert keyed.loc[("P4_NEXT_DAY_AFTER_FAILED", "000002"), "control_forward_return"] == pytest.approx(-0.02)
    assert keyed.loc[("P4_NEXT_DAY_AFTER_FAILED", "000002"), "excess_forward_return"] == pytest.approx(
        (20.40 / 21.00 - 1.0) - (-0.02)
    )


def test_build_upper_limit_pre_event_placebo_comparison_frame_aligns_prior_horizons() -> None:
    event_panel = build_state_transition_daily_panel(_pre_event_placebo_daily_bar_fixture())
    expression_frame = build_upper_limit_pilot_expression_frame(event_panel.loc[event_panel["date"] == "2026-04-01"])

    comparison = build_upper_limit_pre_event_placebo_comparison_frame(
        expression_frame,
        event_panel,
    )
    keyed = comparison.set_index(["expression_id", "event_ticker"])

    assert list(comparison.columns) == [
        "date",
        "expression_id",
        "mechanism_id",
        "state_anchor",
        "event_ticker",
        "signal_value",
        "expected_sign",
        "event_forward_return",
        "placebo_forward_return",
        "placebo_excess_return",
    ]
    assert keyed.loc[("P1_SEALED_UPPER_LIMIT", "000001"), "placebo_forward_return"] == pytest.approx(0.10)
    assert keyed.loc[("P1_SEALED_UPPER_LIMIT", "000001"), "placebo_excess_return"] == pytest.approx(
        (10.90 / 11.00 - 1.0) - 0.10
    )

    assert keyed.loc[("P3_NEXT_DAY_AFTER_SEALED", "000001"), "placebo_forward_return"] == pytest.approx(
        10.00 / 9.50 - 1.0
    )
    assert keyed.loc[("P4_NEXT_DAY_AFTER_FAILED", "000002"), "placebo_forward_return"] == pytest.approx(
        20.00 / 20.50 - 1.0
    )


def test_build_upper_limit_event_conditioned_null_pool_attaches_p001_strata() -> None:
    event_panel = build_state_transition_daily_panel(_daily_bar_fixture())
    expression_frame = build_upper_limit_pilot_expression_frame(event_panel)

    null_pool = build_upper_limit_event_conditioned_null_pool(
        expression_frame,
        _matched_control_fixture(),
    )
    keyed = null_pool.set_index(["expression_id", "event_ticker"])

    assert list(null_pool.columns) == [
        "date",
        "expression_id",
        "mechanism_id",
        "state_anchor",
        "event_ticker",
        "signal_value",
        "expected_sign",
        "forward_return",
        "event_type_bucket",
        "horizon_bucket",
        "size_tercile",
        "liquidity_tercile",
        "conditioning_bucket_key",
    ]
    assert keyed.loc[("P1_SEALED_UPPER_LIMIT", "000001"), "event_type_bucket"] == "SEALED_UPPER_LIMIT"
    assert keyed.loc[("P1_SEALED_UPPER_LIMIT", "000001"), "horizon_bucket"] == "NEXT_CLOSE"
    assert keyed.loc[("P1_SEALED_UPPER_LIMIT", "000001"), "conditioning_bucket_key"] == (
        "SEALED_UPPER_LIMIT|NEXT_CLOSE|2|2"
    )
    assert keyed.loc[("P3_NEXT_DAY_AFTER_SEALED", "000001"), "conditioning_bucket_key"] == (
        "SEALED_UPPER_LIMIT|NEXT_INTRADAY|2|2"
    )
    assert keyed.loc[("P4_NEXT_DAY_AFTER_FAILED", "000002"), "conditioning_bucket_key"] == (
        "FAILED_UPPER_LIMIT|NEXT_INTRADAY|1|1"
    )


def test_build_upper_limit_event_conditioned_null_pool_rejects_missing_conditioning_columns() -> None:
    event_panel = build_state_transition_daily_panel(_daily_bar_fixture())
    expression_frame = build_upper_limit_pilot_expression_frame(event_panel)
    bad_conditioning = _matched_control_fixture().drop(columns=["size_tercile"])

    with pytest.raises(InputValidationError, match="requires conditioning columns"):
        build_upper_limit_event_conditioned_null_pool(expression_frame, bad_conditioning)


def test_build_upper_limit_event_conditioned_null_draw_shuffles_within_conditioning_buckets() -> None:
    null_pool = _event_conditioned_null_pool_fixture()

    draw = build_upper_limit_event_conditioned_null_draw(null_pool, random_seed=7)
    keyed = draw.set_index("event_ticker")

    assert list(draw.columns) == [
        "date",
        "expression_id",
        "mechanism_id",
        "state_anchor",
        "event_ticker",
        "signal_value",
        "expected_sign",
        "forward_return",
        "event_type_bucket",
        "horizon_bucket",
        "size_tercile",
        "liquidity_tercile",
        "conditioning_bucket_key",
        "null_forward_return",
        "null_seed",
    ]
    assert set(
        keyed.loc[["000001", "000007"], "null_forward_return"].tolist()
    ) == {0.10, 0.30}
    assert keyed.loc["000002", "null_forward_return"] == pytest.approx(-0.20)
    assert int(keyed.loc["000001", "null_seed"]) == 7


def test_build_upper_limit_event_conditioned_null_draw_rejects_missing_pool_columns() -> None:
    bad_pool = _event_conditioned_null_pool_fixture().drop(columns=["conditioning_bucket_key"])

    with pytest.raises(InputValidationError, match="requires null-pool columns"):
        build_upper_limit_event_conditioned_null_draw(bad_pool, random_seed=7)


def test_build_upper_limit_event_conditioned_null_summary_reports_degenerate_nulls() -> None:
    null_pool = _event_conditioned_null_pool_fixture()

    summary = build_upper_limit_event_conditioned_null_summary(
        null_pool,
        random_seeds=[7, 8, 9],
    )
    keyed = summary.set_index("expression_id")

    assert list(summary.columns) == [
        "expression_id",
        "mechanism_id",
        "state_anchor",
        "observation_count",
        "null_seed_count",
        "observed_mean_forward_return",
        "null_mean_forward_return_median",
        "null_mean_forward_return_std",
        "null_mean_forward_return_unique_count",
        "null_is_degenerate",
        "observed_mean_forward_return_null_percentile",
    ]
    assert int(keyed.loc["P1_SEALED_UPPER_LIMIT", "observation_count"]) == 2
    assert keyed.loc["P1_SEALED_UPPER_LIMIT", "observed_mean_forward_return"] == pytest.approx(0.20)
    assert keyed.loc["P1_SEALED_UPPER_LIMIT", "null_mean_forward_return_median"] == pytest.approx(0.20)
    assert int(keyed.loc["P1_SEALED_UPPER_LIMIT", "null_mean_forward_return_unique_count"]) == 1
    assert bool(keyed.loc["P1_SEALED_UPPER_LIMIT", "null_is_degenerate"]) is True
    assert pd.isna(keyed.loc["P1_SEALED_UPPER_LIMIT", "observed_mean_forward_return_null_percentile"])

    assert int(keyed.loc["P4_NEXT_DAY_AFTER_FAILED", "observation_count"]) == 1
    assert keyed.loc["P4_NEXT_DAY_AFTER_FAILED", "null_mean_forward_return_median"] == pytest.approx(-0.20)
    assert bool(keyed.loc["P4_NEXT_DAY_AFTER_FAILED", "null_is_degenerate"]) is True


def test_build_upper_limit_event_conditioned_null_summary_rejects_empty_seed_list() -> None:
    null_pool = _event_conditioned_null_pool_fixture()

    with pytest.raises(InputValidationError, match="random_seeds must not be empty"):
        build_upper_limit_event_conditioned_null_summary(null_pool, random_seeds=[])
