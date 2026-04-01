from __future__ import annotations

import pandas as pd
import pytest

from portfolio_os.data.loaders import parse_bool
from portfolio_os.data.market import load_market_snapshot
from portfolio_os.data.portfolio import build_portfolio_frame, load_holdings, load_portfolio_state, load_target_weights
from portfolio_os.data.reference import load_reference_snapshot
from portfolio_os.domain.errors import InputValidationError


def test_portfolio_csv_parsing(sample_context: dict) -> None:
    holdings = load_holdings(sample_context["sample_dir"] / "holdings_example.csv")
    targets = load_target_weights(sample_context["sample_dir"] / "target_example.csv")
    portfolio_state = load_portfolio_state(sample_context["sample_dir"] / "portfolio_state_example.yaml")
    frame = build_portfolio_frame(holdings, targets)

    assert portfolio_state.available_cash == 1300000.0
    assert frame.loc[frame["ticker"] == "601012", "quantity"].item() == 0
    assert frame.loc[frame["ticker"] == "600519", "quantity"].item() == 180
    assert len(frame) == 7


def test_parse_bool_accepts_supported_values() -> None:
    truthy = ["true", "TRUE", "1", " yes "]
    falsy = ["false", "FALSE", "0", " no "]
    assert all(parse_bool(value, "field") for value in truthy)
    assert not any(parse_bool(value, "field") for value in falsy)


def test_market_missing_required_ticker_raises(sample_context: dict, tmp_path) -> None:
    market_path = tmp_path / "market_missing.csv"
    frame = pd.read_csv(sample_context["sample_dir"] / "market_example.csv", dtype={"ticker": str})
    frame = frame[frame["ticker"] != "601012"]
    frame.to_csv(market_path, index=False)

    with pytest.raises(InputValidationError, match="missing required ticker"):
        load_market_snapshot(market_path, sample_context["portfolio_frame"]["ticker"].tolist())


def test_reference_missing_industry_raises(sample_context: dict, tmp_path) -> None:
    reference_path = tmp_path / "reference_missing_industry.csv"
    frame = pd.read_csv(sample_context["sample_dir"] / "reference_example.csv", dtype={"ticker": str})
    frame.loc[frame["ticker"] == "600519", "industry"] = ""
    frame.to_csv(reference_path, index=False)

    with pytest.raises(InputValidationError, match="missing industry"):
        load_reference_snapshot(reference_path, sample_context["portfolio_frame"]["ticker"].tolist())
