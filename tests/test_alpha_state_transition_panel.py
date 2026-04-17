from __future__ import annotations

import pandas as pd
import pytest

from portfolio_os.alpha.state_transition_panel import (
    build_state_transition_daily_panel,
    build_upper_limit_pilot_expression_frame,
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
