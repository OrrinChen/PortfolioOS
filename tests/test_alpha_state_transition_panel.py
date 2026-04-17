from __future__ import annotations

import json

import pandas as pd
import pytest

from portfolio_os.alpha.state_transition_pilot import (
    load_upper_limit_pilot_daily_panel_csv,
    run_upper_limit_pilot_artifact_bundle,
    run_upper_limit_pilot_artifact_bundle_from_daily_csv,
)
from portfolio_os.alpha.state_transition_panel import (
    build_state_transition_daily_panel,
    build_upper_limit_event_conditioned_null_draw,
    build_upper_limit_event_conditioned_null_pool,
    build_upper_limit_event_conditioned_null_summary,
    build_upper_limit_pilot_read_frame,
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


def _upper_limit_pilot_daily_csv_fixture() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    shares_by_ticker = {
        "000001": 10_000_000.0,
        "000002": 10_000_000.0,
        "000003": 10_000_000.0,
        "000004": 10_000_000.0,
        "000005": 10_000_000.0,
    }
    industry_by_ticker = {
        "000001": " Industrials ",
        "000002": "Industrials",
        "000003": "Consumer",
        "000004": "Consumer",
        "000005": "Technology",
    }
    daily_spec = {
        "2026-03-30": {
            "000001": {"close": 6.50, "amount": 7_000_000},
            "000002": {"close": 5.70, "amount": 6_000_000},
            "000003": {"close": 7.50, "amount": 8_000_000},
            "000004": {"close": 8.30, "amount": 9_000_000},
            "000005": {"close": 4.80, "amount": 4_000_000},
        },
        "2026-03-31": {
            "000001": {"close": 6.80, "amount": 7_200_000},
            "000002": {"close": 5.90, "amount": 6_200_000},
            "000003": {"close": 7.70, "amount": 8_200_000},
            "000004": {"close": 8.60, "amount": 9_200_000},
            "000005": {"close": 4.90, "amount": 4_200_000},
        },
        "2026-04-01": {
            "000001": {
                "open": 6.90,
                "high": 7.70,
                "low": 6.85,
                "close": 7.70,
                "upper_limit_price": 7.70,
                "amount": 7_400_000,
            },
            "000002": {
                "open": 5.95,
                "high": 6.20,
                "low": 5.90,
                "close": 6.00,
                "upper_limit_price": 6.49,
                "amount": 6_400_000,
            },
            "000003": {
                "open": 7.75,
                "high": 8.10,
                "low": 7.70,
                "close": 8.00,
                "upper_limit_price": 8.47,
                "amount": 8_400_000,
            },
            "000004": {
                "open": 8.70,
                "high": 9.90,
                "low": 8.60,
                "close": 9.45,
                "upper_limit_price": 9.90,
                "amount": 9_400_000,
            },
            "000005": {
                "open": 4.95,
                "high": 5.10,
                "low": 4.90,
                "close": 5.00,
                "upper_limit_price": 5.39,
                "amount": 4_400_000,
            },
        },
        "2026-04-02": {
            "000001": {"close": 7.90, "amount": 7_500_000},
            "000002": {"close": 6.05, "amount": 6_450_000},
            "000003": {"close": 8.10, "amount": 8_450_000},
            "000004": {"close": 9.10, "amount": 9_450_000},
            "000005": {"close": 5.05, "amount": 4_450_000},
        },
    }
    for date_value, ticker_map in daily_spec.items():
        for ticker, values in ticker_map.items():
            close = float(values["close"])
            rows.append(
                {
                    "date": date_value,
                    "ticker": f" {ticker} ",
                    "open": float(values.get("open", close * 0.99)),
                    "high": float(values.get("high", close * 1.01)),
                    "low": float(values.get("low", close * 0.98)),
                    "close": close,
                    "volume": 1_000_000,
                    "amount": float(values["amount"]),
                    "upper_limit_price": float(values.get("upper_limit_price", close * 1.10)),
                    "lower_limit_price": float(values.get("lower_limit_price", close * 0.90)),
                    "tradable": "true",
                    "industry": industry_by_ticker[ticker],
                    "issuer_total_shares": shares_by_ticker[ticker],
                }
            )
    return pd.DataFrame(rows)


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


def test_build_upper_limit_pilot_read_frame_joins_live_control_placebo_and_null_reads() -> None:
    event_panel = build_state_transition_daily_panel(_pre_event_placebo_daily_bar_fixture())
    expression_frame = build_upper_limit_pilot_expression_frame(event_panel.loc[event_panel["date"] == "2026-04-01"])
    matched_controls = build_upper_limit_matched_non_event_control_frame(_matched_control_fixture())
    control_comparison = build_upper_limit_matched_control_comparison_frame(
        expression_frame,
        matched_controls,
        _matched_control_forward_panel_fixture(),
    )
    placebo_comparison = build_upper_limit_pre_event_placebo_comparison_frame(
        expression_frame,
        event_panel,
    )
    null_pool = build_upper_limit_event_conditioned_null_pool(expression_frame, _matched_control_fixture())
    null_summary = build_upper_limit_event_conditioned_null_summary(
        null_pool,
        random_seeds=[7, 8, 9],
    )

    read_frame = build_upper_limit_pilot_read_frame(
        expression_frame,
        control_comparison,
        placebo_comparison,
        null_summary,
    )
    keyed = read_frame.set_index("expression_id")

    assert list(read_frame.columns) == [
        "expression_id",
        "mechanism_id",
        "state_anchor",
        "observation_count",
        "observed_mean_forward_return",
        "mean_excess_vs_control",
        "mean_excess_vs_placebo",
        "null_seed_count",
        "null_mean_forward_return_median",
        "null_mean_forward_return_std",
        "null_mean_forward_return_unique_count",
        "null_is_degenerate",
        "observed_mean_forward_return_null_percentile",
    ]
    assert keyed.loc["P1_SEALED_UPPER_LIMIT", "observed_mean_forward_return"] == pytest.approx(10.90 / 11.00 - 1.0)
    assert keyed.loc["P1_SEALED_UPPER_LIMIT", "mean_excess_vs_control"] == pytest.approx(
        (10.90 / 11.00 - 1.0) - 0.0302
    )
    assert keyed.loc["P1_SEALED_UPPER_LIMIT", "mean_excess_vs_placebo"] == pytest.approx(
        (10.90 / 11.00 - 1.0) - 0.10
    )
    assert bool(keyed.loc["P1_SEALED_UPPER_LIMIT", "null_is_degenerate"]) is True


def test_build_upper_limit_pilot_read_frame_rejects_missing_null_summary_columns() -> None:
    event_panel = build_state_transition_daily_panel(_pre_event_placebo_daily_bar_fixture())
    expression_frame = build_upper_limit_pilot_expression_frame(event_panel.loc[event_panel["date"] == "2026-04-01"])
    matched_controls = build_upper_limit_matched_non_event_control_frame(_matched_control_fixture())
    control_comparison = build_upper_limit_matched_control_comparison_frame(
        expression_frame,
        matched_controls,
        _matched_control_forward_panel_fixture(),
    )
    placebo_comparison = build_upper_limit_pre_event_placebo_comparison_frame(
        expression_frame,
        event_panel,
    )
    bad_null_summary = pd.DataFrame(
        [
            {
                "expression_id": "P1_SEALED_UPPER_LIMIT",
                "mechanism_id": "M1",
            }
        ]
    )

    with pytest.raises(InputValidationError, match="requires null-summary columns"):
        build_upper_limit_pilot_read_frame(
            expression_frame,
            control_comparison,
            placebo_comparison,
            bad_null_summary,
        )


def test_run_upper_limit_pilot_artifact_bundle_writes_expected_artifacts(tmp_path) -> None:
    event_panel = build_state_transition_daily_panel(_pre_event_placebo_daily_bar_fixture())
    expression_frame = build_upper_limit_pilot_expression_frame(event_panel.loc[event_panel["date"] == "2026-04-01"])
    matched_controls = build_upper_limit_matched_non_event_control_frame(_matched_control_fixture())
    control_comparison = build_upper_limit_matched_control_comparison_frame(
        expression_frame,
        matched_controls,
        _matched_control_forward_panel_fixture(),
    )
    placebo_comparison = build_upper_limit_pre_event_placebo_comparison_frame(
        expression_frame,
        event_panel,
    )
    null_pool = build_upper_limit_event_conditioned_null_pool(expression_frame, _matched_control_fixture())
    output_dir = tmp_path / "upper_limit_pilot"

    result = run_upper_limit_pilot_artifact_bundle(
        expression_frame=expression_frame,
        control_comparison_frame=control_comparison,
        placebo_comparison_frame=placebo_comparison,
        null_pool=null_pool,
        random_seeds=[7, 8, 9],
        output_dir=output_dir,
    )

    expected_files = {
        "expression_frame.csv",
        "control_comparison.csv",
        "placebo_comparison.csv",
        "null_pool.csv",
        "null_summary.csv",
        "pilot_read_frame.csv",
        "summary.json",
        "note.md",
    }
    assert expected_files == {path.name for path in output_dir.iterdir() if path.is_file()}
    assert result.output_dir == output_dir
    assert len(result.read_frame) == 4

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["pilot_name"] == "upper_limit_daily_state"
    assert summary["expression_count"] == 4
    assert summary["null_seed_count"] == 3
    assert summary["degenerate_expression_count"] == 4
    assert "P1_SEALED_UPPER_LIMIT" in summary["degenerate_expression_ids"]

    note_text = (output_dir / "note.md").read_text(encoding="utf-8")
    assert "Upper-Limit Pilot Read" in note_text
    assert "degenerate" in note_text.lower()


def test_load_upper_limit_pilot_daily_panel_csv_normalizes_and_derives_state_columns(tmp_path) -> None:
    daily_path = tmp_path / "state_transition_daily.csv"
    _upper_limit_pilot_daily_csv_fixture().to_csv(daily_path, index=False)

    panel = load_upper_limit_pilot_daily_panel_csv(daily_path)
    keyed = panel.set_index(["date", "ticker"])

    assert "upper_limit_touched" in panel.columns
    assert "sealed_upper_limit" in panel.columns
    assert bool(keyed.loc[("2026-04-01", "000001"), "sealed_upper_limit"]) is True
    assert bool(keyed.loc[("2026-04-01", "000004"), "failed_upper_limit"]) is True
    assert keyed.loc[("2026-04-01", "000001"), "industry"] == "Industrials"
    assert keyed.loc[("2026-04-01", "000001"), "issuer_total_shares"] == pytest.approx(10_000_000.0)


def test_load_upper_limit_pilot_daily_panel_csv_rejects_missing_reference_columns(tmp_path) -> None:
    daily_path = tmp_path / "state_transition_daily_missing_reference.csv"
    _upper_limit_pilot_daily_csv_fixture().drop(columns=["industry"]).to_csv(daily_path, index=False)

    with pytest.raises(InputValidationError, match="requires pilot reference columns"):
        load_upper_limit_pilot_daily_panel_csv(daily_path)


def test_run_upper_limit_pilot_artifact_bundle_from_daily_csv_writes_expected_artifacts(tmp_path) -> None:
    daily_path = tmp_path / "state_transition_daily.csv"
    _upper_limit_pilot_daily_csv_fixture().to_csv(daily_path, index=False)
    output_dir = tmp_path / "upper_limit_daily_csv_pilot"

    result = run_upper_limit_pilot_artifact_bundle_from_daily_csv(
        daily_panel_path=daily_path,
        lookback_days=2,
        random_seeds=[7, 8, 9],
        output_dir=output_dir,
    )

    expected_files = {
        "expression_frame.csv",
        "control_comparison.csv",
        "placebo_comparison.csv",
        "null_pool.csv",
        "null_summary.csv",
        "pilot_read_frame.csv",
        "summary.json",
        "note.md",
    }
    assert expected_files == {path.name for path in output_dir.iterdir() if path.is_file()}
    assert result.summary_payload["pilot_name"] == "upper_limit_daily_state"
    assert result.summary_payload["expression_count"] == 4
    assert len(result.read_frame) == 4
