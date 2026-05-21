from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.insider_disclosure_d2_sell_contrast import (
    run_planned_vs_discretionary_sell_contrast_d2,
)


def test_sell_contrast_d2_observes_discretionary_sell_contrast_without_formula(tmp_path: Path) -> None:
    registry_path = _write_sell_registry_fixture(tmp_path / "registry.csv", include_planned=True)
    price_path = _write_sell_price_fixture(tmp_path / "prices.csv")
    benchmark_path = _write_benchmark_fixture(tmp_path / "benchmark.csv")

    result = run_planned_vs_discretionary_sell_contrast_d2(
        event_registry_path=registry_path,
        price_panel_path=price_path,
        benchmark_panel_path=benchmark_path,
        output_dir=tmp_path / "d2_sell",
        minimum_discretionary_sell_events=4,
        minimum_planned_sell_events=4,
        minimum_event_month_count=2,
        minimum_label_coverage_share=0.75,
    )

    assert result.summary["schema_version"] == "insider_disclosure_d2_sell_contrast_summary.v1"
    assert result.summary["stage"] == "D2-INSIDER-02"
    assert result.summary["candidate_id"] == "planned_vs_discretionary_sell_contrast_post_2023"
    assert result.summary["overall_decision"] == "observable"
    assert result.summary["allow_d3_charter_for"] == ["planned_vs_discretionary_sell_contrast_post_2023"]
    assert result.summary["formula_score_written"] is False
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["production_approval_claimed"] is False
    assert result.summary["no_view_not_zero_alpha"] is True

    subset_counts = pd.read_csv(result.artifacts["sell_event_subset_counts"])
    assert set(subset_counts["event_subset"]) >= {
        "discretionary_sell",
        "planned_sell",
        "unknown_plan_flag",
        "compensation_controls",
    }
    plan_report = pd.read_csv(result.artifacts["plan_flag_coverage_report"])
    assert plan_report["planned_sell_event_count"].iloc[0] == 6
    assert plan_report["discretionary_sell_event_count"].iloc[0] == 6
    assert plan_report["unknown_plan_flag_count"].iloc[0] == 2

    car = pd.read_csv(result.artifacts["sell_car_window_panel"])
    post = car[(car["window"] == "post_1_22") & (car["label_status"] == "observed")]
    disc = post[post["event_subset"] == "discretionary_sell"]["mean_abnormal_return"].iloc[0]
    planned = post[post["event_subset"] == "planned_sell"]["mean_abnormal_return"].iloc[0]
    assert disc < planned
    assert "formula_score" not in car.columns
    assert "expected_return" not in car.columns

    placebo = pd.read_csv(result.artifacts["sell_placebo_report"])
    assert {
        "shift_minus_5",
        "shift_plus_5",
        "same_coverage_random",
        "role_label_randomized",
        "issuer_non_event",
        "compensation_control",
    }.issubset(set(placebo["placebo_name"]))
    assert placebo["status"].eq("pass").all()

    report = result.artifacts["d2_sell_contrast_report"].read_text(encoding="utf-8").lower()
    assert "no-formula observability only" in report
    assert "not alpha evidence" in report
    for forbidden in [
        "production approved",
        "paper ready",
        "live trading",
        "broker execution",
        "order generation",
        "alpha passed",
        "q2-ready",
        "tradable alpha",
    ]:
        assert forbidden not in report


def test_sell_contrast_d2_blocks_when_planned_sell_flag_coverage_is_missing(tmp_path: Path) -> None:
    registry_path = _write_sell_registry_fixture(tmp_path / "registry.csv", include_planned=False)
    price_path = _write_sell_price_fixture(tmp_path / "prices.csv")

    result = run_planned_vs_discretionary_sell_contrast_d2(
        event_registry_path=registry_path,
        price_panel_path=price_path,
        output_dir=tmp_path / "d2_sell_blocked",
        minimum_discretionary_sell_events=4,
        minimum_planned_sell_events=4,
        minimum_event_month_count=2,
        minimum_label_coverage_share=0.75,
    )

    assert result.summary["overall_decision"] == "blocked_plan_flag_coverage"
    assert result.summary["allow_d3_charter_for"] == []
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False


def test_sell_contrast_d2_rejects_expected_return_inputs(tmp_path: Path) -> None:
    registry_path = _write_sell_registry_fixture(tmp_path / "registry.csv", include_planned=True)
    registry = pd.read_csv(registry_path)
    registry["expected_return"] = 0.01
    registry.to_csv(registry_path, index=False)
    price_path = _write_sell_price_fixture(tmp_path / "prices.csv")

    try:
        run_planned_vs_discretionary_sell_contrast_d2(
            event_registry_path=registry_path,
            price_panel_path=price_path,
            output_dir=tmp_path / "d2_sell_rejected",
        )
    except ValueError as exc:
        message = str(exc)
    else:  # pragma: no cover - proves the guard fired
        raise AssertionError("expected D2 sell contrast to reject downstream fields")

    assert "forbidden input columns" in message
    assert "expected_return" in message


def _write_sell_registry_fixture(path: Path, include_planned: bool) -> Path:
    rows = []
    for idx in range(6):
        rows.append(_event_row(idx, "DISC", "discretionary_sell", "S", "false", "2024-01-03", "cfo"))
    if include_planned:
        for idx in range(6):
            rows.append(_event_row(100 + idx, "PLAN", "planned_sell", "S", "true", "2024-02-05", "director"))
    else:
        for idx in range(6):
            rows.append(_event_row(100 + idx, "PLAN", "unknown_plan_flag", "S", "", "2024-02-05", "director"))
    for idx in range(2):
        rows.append(_event_row(200 + idx, "UNKN", "unknown_plan_flag", "S", "", "2024-03-04", "other_officer"))
    for idx in range(4):
        rows.append(_event_row(300 + idx, "COMP", "compensation_controls", "A", "", "2024-01-03", "director"))
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _event_row(
    idx: int,
    ticker: str,
    subset: str,
    code: str,
    plan_flag: str,
    signal_date: str,
    role: str,
) -> dict[str, object]:
    return {
        "event_id": f"event_{idx}",
        "issuer_cik": f"10{idx:04d}",
        "ticker": ticker,
        "accession_number": f"0000000000-24-{idx:06d}",
        "form_type": "4",
        "filing_accepted_ts": f"{signal_date}T18:00:00+00:00",
        "visibility_timestamp": f"{signal_date}T18:00:00+00:00",
        "tradable_timestamp": f"{signal_date}T13:30:00+00:00",
        "reporting_owner_cik": f"20{idx:04d}",
        "reporting_owner_name_hash": f"owner_{idx}",
        "role_bucket": role,
        "is_director": role == "director",
        "is_officer": role in {"cfo", "other_officer"},
        "officer_title_bucket": role.upper(),
        "is_10pct_owner": False,
        "transaction_code": code,
        "acquired_disposed": "D",
        "transaction_date": signal_date,
        "transaction_shares": 1000,
        "transaction_price": 50.0,
        "transaction_dollar_value": 50_000.0,
        "security_title": "Common Stock",
        "is_derivative": False,
        "ownership_direct_or_indirect": "D",
        "post_transaction_holding": 10_000,
        "rule_10b5_1_flag": plan_flag,
        "plan_adoption_date": "2023-10-01" if plan_flag == "true" else "",
        "event_subset": subset,
        "event_cluster_id": f"{ticker}_{idx}",
        "market_cap_at_event": 1_000_000_000,
        "adv_20d": 5_000_000,
        "spread_proxy": 0.001,
        "sector": "technology",
        "size_bucket": "mid",
        "liquidity_bucket": "high",
        "coverage_state": "covered" if subset != "unknown_plan_flag" else "no_view",
        "no_view_reason": "unknown_post_2023_plan_flag" if subset == "unknown_plan_flag" else "",
        "diagnostic_only": subset in {"unknown_plan_flag", "compensation_controls"},
        "event_month": signal_date[:7],
    }


def _write_sell_price_fixture(path: Path) -> Path:
    dates = pd.bdate_range("2023-11-01", periods=180)
    rows = []
    for ticker, event_date, post_daily_return in [
        ("DISC", pd.Timestamp("2024-01-03"), -0.003),
        ("PLAN", pd.Timestamp("2024-02-05"), -0.0003),
        ("UNKN", pd.Timestamp("2024-03-04"), -0.001),
        ("COMP", pd.Timestamp("2024-01-03"), -0.0002),
    ]:
        price = 100.0
        event_end = dates[dates.searchsorted(event_date) + 44]
        for date in dates:
            if event_date < date <= event_end:
                daily_return = post_daily_return
            else:
                daily_return = 0.0
            price *= 1 + daily_return
            rows.append(
                {
                    "ticker": ticker,
                    "date": date.date().isoformat(),
                    "adjusted_close": price,
                    "volume": 1_000_000,
                    "market_cap": 1_000_000_000,
                    "dollar_volume": price * 1_000_000,
                    "bid_ask_spread": 0.001,
                    "sector": "technology",
                },
            )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _write_benchmark_fixture(path: Path) -> Path:
    dates = pd.bdate_range("2023-11-01", periods=180)
    price = 100.0
    rows = []
    for date in dates:
        price *= 1.0
        rows.append({"date": date.date().isoformat(), "adjusted_close": price})
    pd.DataFrame(rows).to_csv(path, index=False)
    return path
