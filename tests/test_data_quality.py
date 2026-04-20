from __future__ import annotations

import pandas as pd
import pytest

from portfolio_os.compliance.pretrade import collect_data_quality_findings
from portfolio_os.data.market import load_market_snapshot
from portfolio_os.data.portfolio import load_holdings, load_portfolio_state, load_target_weights
from portfolio_os.domain.errors import InputValidationError


def test_duplicate_ticker_in_holdings_fails_fast(tmp_path) -> None:
    holdings_path = tmp_path / "holdings_duplicate.csv"
    holdings_path.write_text("ticker,quantity\n600519,100\n600519,200\n", encoding="utf-8")

    with pytest.raises(InputValidationError, match="Duplicate ticker"):
        load_holdings(holdings_path)


def test_market_non_positive_values_fail_fast(sample_context: dict, tmp_path) -> None:
    market_path = tmp_path / "market_bad.csv"
    frame = pd.read_csv(sample_context["sample_dir"] / "market_example.csv", dtype={"ticker": str})
    frame.loc[frame["ticker"] == "300750", "adv_shares"] = 0
    frame.to_csv(market_path, index=False)

    with pytest.raises(InputValidationError, match="non-positive values in adv_shares"):
        load_market_snapshot(market_path, sample_context["portfolio_frame"]["ticker"].tolist())


def test_market_close_and_vwap_non_positive_fail_fast(sample_context: dict, tmp_path) -> None:
    close_path = tmp_path / "market_bad_close.csv"
    vwap_path = tmp_path / "market_bad_vwap.csv"
    frame = pd.read_csv(sample_context["sample_dir"] / "market_example.csv", dtype={"ticker": str})

    frame_close = frame.copy()
    frame_close.loc[frame_close["ticker"] == "300750", "close"] = 0
    frame_close.to_csv(close_path, index=False)
    with pytest.raises(InputValidationError, match="non-positive values in close"):
        load_market_snapshot(close_path, sample_context["portfolio_frame"]["ticker"].tolist())

    frame_vwap = frame.copy()
    frame_vwap.loc[frame_vwap["ticker"] == "300750", "vwap"] = 0
    frame_vwap.to_csv(vwap_path, index=False)
    with pytest.raises(InputValidationError, match="non-positive values in vwap"):
        load_market_snapshot(vwap_path, sample_context["portfolio_frame"]["ticker"].tolist())


def test_target_weight_sum_above_one_fails_fast(tmp_path) -> None:
    target_path = tmp_path / "target_bad.csv"
    target_path.write_text(
        "ticker,target_weight\n600519,0.7\n300750,0.4\n",
        encoding="utf-8",
    )

    with pytest.raises(InputValidationError, match="above 1.0"):
        load_target_weights(target_path)


def test_negative_portfolio_state_values_fail_fast(tmp_path) -> None:
    state_path = tmp_path / "portfolio_state_bad.yaml"
    state_path.write_text(
        "account_id: test\nas_of_date: '2026-03-23'\navailable_cash: -1\nmin_cash_buffer: 0\naccount_type: public_fund\n",
        encoding="utf-8",
    )

    with pytest.raises(InputValidationError, match="available_cash cannot be negative"):
        load_portfolio_state(state_path)


def test_data_quality_warning_for_small_target_weight_sum(sample_context: dict) -> None:
    universe = sample_context["universe"].copy()
    universe["target_weight"] = [0.0, 0.01, 0.0, 0.01, 0.0, 0.0, 0.0]
    findings = collect_data_quality_findings(universe, sample_context["config"])

    codes = {finding.code for finding in findings}
    assert "target_weight_sum_near_zero" in codes
    warning = next(finding for finding in findings if finding.code == "target_weight_sum_near_zero")
    assert warning.category.value == "data_quality"
    assert warning.blocking is False


def test_data_quality_warning_for_reference_benchmark_anomaly(sample_context: dict) -> None:
    universe = sample_context["universe"].copy()
    universe["benchmark_weight"] = 0.25
    findings = collect_data_quality_findings(universe, sample_context["config"])

    codes = {finding.code for finding in findings}
    assert "benchmark_weight_total_anomaly" in codes
