from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.small_emotion_d2 import _decision, run_small_emotion_d2_observability


def test_small_emotion_d2_blocks_when_required_daily_fields_are_missing(tmp_path: Path) -> None:
    price_path = tmp_path / "prices_missing_market_cap.csv"
    pd.DataFrame(
        [
            {
                "asset_id": "A",
                "ticker": "A",
                "date": "2024-01-02",
                "adjusted_close": 10.0,
                "return": 0.01,
                "volume": 100_000,
            }
        ]
    ).to_csv(price_path, index=False)
    benchmark_path = _write_benchmark_fixture(tmp_path / "benchmark.csv")
    delisting_path = _write_delisting_fixture(tmp_path / "delistings.csv")

    result = run_small_emotion_d2_observability(
        price_panel_path=price_path,
        benchmark_panel_path=benchmark_path,
        delisting_path=delisting_path,
        output_dir=tmp_path / "small_emotion",
    )

    assert result.summary["stage"] == "D2-SMALL-EMOTION-01"
    assert result.summary["overall_decision"] == "blocked_data_coverage"
    assert result.summary["allow_d3_charter_for"] == []
    assert result.summary["formula_score_written"] is False
    assert result.summary["measurement_spec_written"] is False
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["optimizer_entry_allowed"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["production_approval_claimed"] is False

    coverage = json.loads(result.artifacts["data_coverage_report"].read_text(encoding="utf-8"))
    assert "market_cap" in coverage["missing_required_price_columns"]
    assert not (tmp_path / "small_emotion" / "measurement_spec.yaml").exists()
    assert not (tmp_path / "small_emotion" / "expected_return_panel.csv").exists()


def test_small_emotion_d2_writes_guards_placebos_and_allows_at_most_one_d3_mechanism(tmp_path: Path) -> None:
    price_path = _write_price_fixture(tmp_path / "prices.csv")
    benchmark_path = _write_benchmark_fixture(tmp_path / "benchmark.csv")
    delisting_path = _write_delisting_fixture(tmp_path / "delistings.csv")

    result = run_small_emotion_d2_observability(
        price_panel_path=price_path,
        benchmark_panel_path=benchmark_path,
        delisting_path=delisting_path,
        output_dir=tmp_path / "small_emotion",
        minimum_subset_events=3,
        minimum_event_month_count=2,
        minimum_label_coverage_share=0.70,
        min_history_observations=20,
        min_adv_dollars=75_000.0,
    )

    assert result.summary["schema_version"] == "small_emotion_d2_summary.v1"
    assert result.summary["active_mainline"] == "D2-SMALL-EMOTION-01"
    assert result.summary["d2_insider_02_state"] == "stopped_before_d3"
    assert result.summary["d2_8k_01_state"] == "hold_pending_data_coverage"
    assert result.summary["no_view_not_zero_alpha"] is True
    assert len(result.summary["allow_d3_charter_for"]) <= 1
    assert result.summary["overall_decision"] in {
        "observable_panic_reversal",
        "observable_fomo_continuation",
        "observable_liquidity_vacuum_reversal",
        "mixed_narrow_scope",
        "blocked_stale_price",
        "blocked_cost_liquidity",
        "blocked_placebo_dominance",
        "blocked_data_coverage",
        "hold_insufficient_sample",
        "not_observable",
    }
    for key in [
        "formula_score_written",
        "measurement_spec_written",
        "q1_entry_allowed",
        "q2_entry_allowed",
        "expected_return_panel_written",
        "optimizer_entry_allowed",
        "portfolio_construction_allowed",
        "alpha_registry_update_allowed",
        "paper_ready",
        "live_ready",
        "broker_order_path_opened",
        "production_approval_claimed",
    ]:
        assert result.summary[key] is False

    registry = pd.read_csv(result.artifacts["small_emotion_event_registry"])
    assert {
        "panic_overreaction_candidate",
        "fomo_continuation_candidate",
        "liquidity_vacuum_reversal_candidate",
    }.intersection(set(registry["event_subset"]))
    assert "expected_return" not in registry.columns
    assert "formula_score" not in registry.columns
    no_view = registry[registry["coverage_state"].eq("no_view")]
    assert not no_view.empty
    assert no_view["no_view_reason"].str.len().gt(0).all()
    assert "zero_alpha" not in set(no_view["no_view_reason"].astype(str))

    stale = pd.read_csv(result.artifacts["stale_price_guard_report"])
    capacity = pd.read_csv(result.artifacts["adv_capacity_guard_report"])
    assert stale["stale_price_guard_generated"].eq(True).all()
    assert capacity["capacity_guard_generated"].eq(True).all()

    placebo = pd.read_csv(result.artifacts["placebo_report"])
    assert {
        "same_coverage_random",
        "shift_minus_5",
        "shift_plus_5",
        "shift_minus_10",
        "shift_plus_10",
        "large_cap_matched_shock",
        "sector_size_liquidity_matched_non_shock",
        "stale_price_matched",
        "adv_capacity_matched",
    }.issubset(set(placebo["placebo_name"]))

    car = pd.read_csv(result.artifacts["car_window_panel"])
    assert {
        "pre_20_1",
        "pre_10_1",
        "pre_5_1",
        "event_0_1",
        "post_1_5",
        "post_1_10",
        "post_1_22",
        "post_1_44",
    }.issubset(set(car["window"]))

    report = result.artifacts["d2_small_emotion_observability_report"].read_text(encoding="utf-8").lower()
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


def test_small_emotion_d2_uses_subset_level_stale_guard_for_d3_selection() -> None:
    counts = pd.DataFrame(
        [
            {
                "event_subset": "fomo_continuation_candidate",
                "active_event_count": 100,
                "event_month_count": 12,
                "label_coverage_share": 0.90,
            },
            {
                "event_subset": "liquidity_vacuum_reversal_candidate",
                "active_event_count": 100,
                "event_month_count": 12,
                "label_coverage_share": 0.90,
            },
        ]
    )
    diagnostics = pd.DataFrame(
        [
            {
                "event_subset": "fomo_continuation_candidate",
                "post_1_22_directional_return": 0.01,
                "live_post_direction_matches_preregistered_mechanism": True,
                "pre_event_dominates_post": False,
            },
            {
                "event_subset": "liquidity_vacuum_reversal_candidate",
                "post_1_22_directional_return": 0.02,
                "live_post_direction_matches_preregistered_mechanism": True,
                "pre_event_dominates_post": False,
            },
        ]
    )
    placebo = pd.DataFrame(
        [
            {
                "event_subset": "fomo_continuation_candidate",
                "placebo_name": "stale_price_matched",
                "placebo_dominates_live": True,
            },
            {
                "event_subset": "liquidity_vacuum_reversal_candidate",
                "placebo_name": "stale_price_matched",
                "placebo_dominates_live": False,
            },
        ]
    )
    stale_guard = pd.DataFrame([{"stale_placebo_dominates_live": True}])
    capacity_guard = pd.DataFrame([{"capacity_guard_fatal": False}])

    decision, allow_d3 = _decision(
        counts=counts,
        diagnostics=diagnostics,
        placebo=placebo,
        stale_guard=stale_guard,
        capacity_guard=capacity_guard,
        minimum_subset_events=50,
        minimum_event_month_count=12,
        minimum_label_coverage_share=0.70,
    )

    assert decision == "observable_liquidity_vacuum_reversal"
    assert allow_d3 == ["liquidity_vacuum_reversal"]


def _write_price_fixture(path: Path) -> Path:
    dates = pd.bdate_range("2024-01-02", periods=95)
    specs = [
        ("PANIC", "panic", 350_000_000, 160_000, 0.012, "industrial"),
        ("FOMO", "fomo", 450_000_000, 180_000, 0.011, "technology"),
        ("VAC", "vacuum", 220_000_000, 90_000, 0.030, "consumer"),
        ("STALE", "stale", 300_000_000, 0, 0.040, "consumer"),
        ("BIG", "large", 10_000_000_000, 1_500_000, 0.006, "technology"),
        ("CTRL", "control", 500_000_000, 200_000, 0.010, "industrial"),
    ]
    rows: list[dict[str, object]] = []
    for asset_idx, (ticker, pattern, market_cap, base_volume, spread, sector) in enumerate(specs):
        price = 20.0 + asset_idx * 3.0
        for idx, date in enumerate(dates):
            ret = 0.0005
            volume = base_volume
            if pattern == "panic" and idx in {30, 52, 74}:
                ret = -0.13
                volume = base_volume * 4
            elif pattern == "panic" and any(start < idx <= start + 22 for start in {30, 52, 74}):
                ret = 0.006
            elif pattern == "fomo" and idx in {34, 56, 78}:
                ret = 0.12
                volume = base_volume * 4
            elif pattern == "fomo" and any(start < idx <= start + 22 for start in {34, 56, 78}):
                ret = 0.005
            elif pattern == "vacuum" and idx in {38, 60, 82}:
                ret = -0.16
                volume = max(1, base_volume // 4)
            elif pattern == "vacuum" and any(start < idx <= start + 22 for start in {38, 60, 82}):
                ret = 0.007
            elif pattern == "stale":
                ret = 0.0
                volume = 0 if idx % 2 == 0 else 10
            elif pattern == "large" and idx in {30, 52, 74}:
                ret = -0.13
                volume = base_volume * 4
            elif pattern == "large" and any(start < idx <= start + 22 for start in {30, 52, 74}):
                ret = 0.001

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
                    "bid_ask_spread": spread,
                    "share_code": 11,
                    "exchange_code": 3,
                    "common_share": True,
                    "sector": sector,
                    "industry": sector,
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _write_benchmark_fixture(path: Path) -> Path:
    dates = pd.bdate_range("2024-01-02", periods=95)
    price = 100.0
    rows = []
    for date in dates:
        ret = 0.0004
        price *= 1 + ret
        rows.append(
            {
                "date": date.date().isoformat(),
                "benchmark": "IWM",
                "adjusted_close": price,
                "return": ret,
            }
        )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _write_delisting_fixture(path: Path) -> Path:
    pd.DataFrame(
        [
            {
                "asset_id": "DEL",
                "delisting_date": "2024-03-29",
                "delisting_return": -0.55,
                "inactive_reason": "TEST",
            }
        ]
    ).to_csv(path, index=False)
    return path
