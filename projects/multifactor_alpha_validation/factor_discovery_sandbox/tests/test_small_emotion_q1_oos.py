from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from factor_discovery_sandbox.small_emotion_q1_oos import run_small_emotion_q1_oos_review


def test_q1_oos_review_generates_falsifiers_and_preserves_boundaries(tmp_path: Path) -> None:
    spec_path = _write_measurement_spec(tmp_path / "measurement_spec.yaml")
    price_path = _write_q1_price_fixture(tmp_path / "prices.csv")
    benchmark_path = _write_q1_benchmark_fixture(tmp_path / "benchmark.csv")
    delisting_path = _write_delisting_fixture(tmp_path / "delistings.csv")

    result = run_small_emotion_q1_oos_review(
        measurement_spec_path=spec_path,
        price_panel_path=price_path,
        benchmark_panel_path=benchmark_path,
        delisting_path=delisting_path,
        output_dir=tmp_path / "q1",
        min_history_observations=20,
        minimum_event_count=2,
        minimum_event_month_count=2,
        minimum_oos_event_count=1,
    )

    assert result.summary["schema_version"] == "small_emotion_q1_oos_summary.v1"
    assert result.summary["stage"] == "Q1-SMALL-EMOTION-01"
    assert result.summary["measurement_spec_id"] == "small_cap_sharpened_up_shock_reversal_post_1_22_v0"
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["optimizer_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["production_approval_claimed"] is False

    event_panel = pd.read_csv(result.artifacts["q1_event_panel"])
    assert not event_panel.empty
    assert event_panel["signal_state"].eq("active").all()
    assert event_panel["no_view_not_zero_alpha"].eq(True).all()
    assert (event_panel["prior_5d_return"] >= 0.2).all()

    labels = pd.read_csv(result.artifacts["q1_window_return_panel"])
    assert {"post_1_5", "post_1_10", "post_1_22", "post_1_44", "pre_5_1", "pre_10_1", "pre_20_1"}.issubset(
        set(labels["window"]),
    )
    primary = labels[(labels["window"] == "post_1_22") & (labels["label_status"] == "observed")]
    assert primary["directional_return"].mean() > 0

    oos = pd.read_csv(result.artifacts["q1_oos_split_report"])
    assert {"train", "test"}.issubset(set(oos["split"]))
    assert oos["mean_directional_return"].notna().any()

    falsifier = pd.read_csv(result.artifacts["q1_falsifier_report"])
    assert {
        "shift_minus_5",
        "shift_plus_5",
        "shift_minus_10",
        "shift_plus_10",
        "same_coverage_random",
        "large_cap_matched_shock",
        "stale_price_matched",
        "adv_capacity_matched",
    }.issubset(set(falsifier["falsifier_name"]))

    decision = json.loads(result.artifacts["q1_decision_summary"].read_text(encoding="utf-8"))
    assert decision["q1_decision"] in {
        "passed_q1_research_review",
        "mixed_oos_or_falsifier_risk",
        "blocked_placebo_dominance",
        "blocked_oos_failure",
        "hold_insufficient_sample",
        "failed_q1",
    }
    assert decision["promotion_gate_allowed"] in {True, False}
    assert decision["q2_entry_allowed"] is False
    assert decision["expected_return_panel_written"] is False

    report = result.artifacts["q1_oos_report"].read_text(encoding="utf-8").lower()
    assert "q1 falsifier/oos review only" in report
    for forbidden in ["production approved", "paper ready", "live trading", "broker execution", "order generation", "q2-ready"]:
        assert forbidden not in report


def test_q1_oos_supports_full_market_path_predicates_and_stale_clean_filter(tmp_path: Path) -> None:
    spec_path = _write_full_market_path_spec(tmp_path / "measurement_spec.yaml")
    price_path = _write_full_market_path_price_fixture(tmp_path / "prices.csv")
    benchmark_path = _write_q1_benchmark_fixture(tmp_path / "benchmark.csv")
    delisting_path = _write_delisting_fixture(tmp_path / "delistings.csv")

    result = run_small_emotion_q1_oos_review(
        measurement_spec_path=spec_path,
        price_panel_path=price_path,
        benchmark_panel_path=benchmark_path,
        delisting_path=delisting_path,
        output_dir=tmp_path / "q1_full_market",
        min_history_observations=20,
        minimum_event_count=1,
        minimum_event_month_count=1,
        minimum_oos_event_count=1,
        max_falsifier_events=20,
        exclude_stale_price_events=True,
    )

    events = pd.read_csv(result.artifacts["q1_event_panel"])
    assert not events.empty
    assert set(events["asset_id"].unique()) == {"BIGPASS"}
    assert result.summary["measurement_spec_id"] == "small_emotion_full_market_path_test_v0"
    assert result.summary["exclude_stale_price_events"] is True
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["optimizer_entry_allowed"] is False


def _write_measurement_spec(path: Path) -> Path:
    path.write_text(
        yaml.safe_dump(
            {
                "schema_version": "small_emotion_measurement_spec.v1",
                "measurement_spec_id": "small_cap_sharpened_up_shock_reversal_post_1_22_v0",
                "signal_definition": {
                    "mechanism": "up_shock_reversal",
                    "filters": {
                        "shock_threshold": 0.05,
                        "volume_spike_threshold": 1.5,
                        "prior_5d_min_return": 0.2,
                        "prior_20d_min_return": None,
                        "close_location_filter": "all",
                        "low_price_filter": "all",
                        "market_cap_bucket": "micro",
                        "liquidity_filter": "all",
                        "spread_filter": "all",
                        "regime_filter": "market_up_20d",
                        "adv_min_dollars": 75_000.0,
                    },
                },
                "label_contract": {
                    "primary_window": "post_1_22",
                    "diagnostic_windows": ["post_1_5", "post_1_10", "post_1_22", "post_1_44"],
                    "pre_event_audit_windows": ["pre_5_1", "pre_10_1", "pre_20_1"],
                },
                "coverage_policy": {"missing_signal_policy": "no_view_not_zero_alpha"},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _write_full_market_path_spec(path: Path) -> Path:
    path.write_text(
        yaml.safe_dump(
            {
                "schema_version": "small_emotion_measurement_spec.v1",
                "measurement_spec_id": "small_emotion_full_market_path_test_v0",
                "signal_definition": {
                    "mechanism": "up_shock_reversal",
                    "filters": {
                        "shock_threshold": 0.15,
                        "volume_spike_threshold": 1.0,
                        "adv_min_dollars": 50_000.0,
                        "spread_filter": "all",
                        "market_cap_bucket": "all_full_market",
                        "path_predicates": "open_to_close_le_minus_5pct & prior5_ge_20pct",
                    },
                },
                "label_contract": {
                    "primary_window": "post_1_22",
                    "diagnostic_windows": ["post_1_5", "post_1_10", "post_1_22", "post_1_44"],
                    "pre_event_audit_windows": ["pre_5_1", "pre_10_1", "pre_20_1"],
                },
                "coverage_policy": {"missing_signal_policy": "no_view_not_zero_alpha"},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _write_q1_price_fixture(path: Path) -> Path:
    dates = pd.bdate_range("2024-01-02", periods=125)
    specs = [
        ("LEAF1", 100_000_000, 180_000, "technology", {35, 72}),
        ("LEAF2", 120_000_000, 170_000, "consumer", {42, 84}),
        ("BIG", 8_000_000_000, 2_000_000, "technology", {35, 72}),
        ("STALE", 100_000_000, 0, "consumer", {35}),
    ]
    rows: list[dict[str, object]] = []
    for asset_idx, (ticker, market_cap, base_volume, sector, event_days) in enumerate(specs):
        price = 10.0 + asset_idx
        for idx, date in enumerate(dates):
            ret = 0.0004
            volume = base_volume
            if idx in event_days:
                ret = 0.06
                volume = max(base_volume * 4, 1)
            elif any(event_day - 5 <= idx < event_day for event_day in event_days):
                ret = 0.045
            elif any(event_day < idx <= event_day + 22 for event_day in event_days):
                ret = -0.006 if ticker.startswith("LEAF") else -0.001
            elif ticker == "STALE":
                ret = 0.0
                volume = 0
            price *= 1 + ret
            rows.append(
                {
                    "asset_id": ticker,
                    "ticker": ticker,
                    "date": date.date().isoformat(),
                    "adjusted_close": price,
                    "raw_close": price,
                    "return": ret,
                    "volume": volume,
                    "dollar_volume": price * volume,
                    "market_cap": market_cap,
                    "bid_ask_spread": 0.012 if market_cap < 300_000_000 else 0.004,
                    "share_code": 11,
                    "exchange_code": 3,
                    "common_share": True,
                    "sector": sector,
                    "industry": sector,
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _write_full_market_path_price_fixture(path: Path) -> Path:
    dates = pd.bdate_range("2024-01-02", periods=90)
    specs = [
        ("BIGPASS", 8_000_000_000, "pass"),
        ("BIGFAIL_OPEN", 8_000_000_000, "fail_open"),
        ("STALEPASS", 8_000_000_000, "stale"),
    ]
    rows: list[dict[str, object]] = []
    event_day = 35
    for asset_idx, (ticker, market_cap, mode) in enumerate(specs):
        price = 20.0 + asset_idx
        for idx, date in enumerate(dates):
            ret = 0.0005
            volume = 200_000
            if event_day - 5 <= idx < event_day:
                ret = 0.0 if mode == "stale" and idx == event_day - 1 else 0.05
            elif idx == event_day:
                ret = 0.16
                volume = 800_000
            elif event_day < idx <= event_day + 22:
                ret = -0.01 if mode in {"pass", "stale"} else 0.002
            prev = price
            price *= 1 + ret
            if idx == event_day and mode in {"pass", "stale"}:
                open_price = prev * 1.25
            elif idx == event_day:
                open_price = prev * 1.10
            else:
                open_price = prev
            rows.append(
                {
                    "asset_id": ticker,
                    "ticker": ticker,
                    "date": date.date().isoformat(),
                    "adjusted_open": open_price,
                    "raw_open": open_price,
                    "adjusted_close": price,
                    "raw_close": price,
                    "high": max(open_price, price) * 1.01,
                    "low": min(open_price, price) * 0.99,
                    "return": ret,
                    "volume": volume,
                    "dollar_volume": price * volume,
                    "market_cap": market_cap,
                    "bid_ask_spread": 0.004,
                    "share_code": 11,
                    "exchange_code": 3,
                    "common_share": True,
                    "sector": "technology",
                    "industry": "software",
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _write_q1_benchmark_fixture(path: Path) -> Path:
    dates = pd.bdate_range("2024-01-02", periods=125)
    rows = [{"date": date.date().isoformat(), "benchmark": "IWM", "adjusted_close": 100.0, "return": 0.0003} for date in dates]
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _write_delisting_fixture(path: Path) -> Path:
    pd.DataFrame(
        [{"asset_id": "NONE", "delisting_date": "2024-06-01", "delisting_return": -0.5, "inactive_reason": "TEST"}]
    ).to_csv(path, index=False)
    return path
