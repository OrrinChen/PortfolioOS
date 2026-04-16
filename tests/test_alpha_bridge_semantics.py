from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from portfolio_os.alpha.bridge_semantics import (
    compute_hold_through_metrics,
    recommend_guard_protocol,
    render_guard_protocol_comparison_note,
    resolve_negative_spread_protocol,
    summarize_guard_protocol_results,
)


def test_resolve_negative_spread_protocol_floor_to_zero() -> None:
    decision = resolve_negative_spread_protocol(
        -0.0032,
        forward_horizon_days=5,
        decision_horizon_days=21,
        protocol="floor_to_zero",
    )

    assert decision.status == "spread_floor_to_zero"
    assert decision.should_abstain is False
    assert decision.annualized_top_bottom_spread == 0.0
    assert decision.period_top_bottom_spread == 0.0


def test_resolve_negative_spread_protocol_signed_spread_keeps_negative_direction() -> None:
    decision = resolve_negative_spread_protocol(
        -0.0032,
        forward_horizon_days=5,
        decision_horizon_days=21,
        protocol="signed_spread",
    )

    assert decision.status == "signed_negative_spread"
    assert decision.should_abstain is False
    assert decision.annualized_top_bottom_spread < 0.0
    assert decision.period_top_bottom_spread < 0.0


def test_resolve_negative_spread_protocol_explicit_abstain_returns_null_spread() -> None:
    decision = resolve_negative_spread_protocol(
        -0.0032,
        forward_horizon_days=5,
        decision_horizon_days=21,
        protocol="explicit_abstain",
    )

    assert decision.status == "explicit_abstain"
    assert decision.should_abstain is True
    assert decision.annualized_top_bottom_spread is None
    assert decision.period_top_bottom_spread is None


def test_compute_hold_through_metrics_tracks_retained_positions_by_count_and_value() -> None:
    metrics = compute_hold_through_metrics(
        tickers=pd.Index(["AAA", "BBB", "CCC"]),
        pre_trade_quantities=pd.Series([10, 5, 0], index=["AAA", "BBB", "CCC"], dtype=float),
        post_trade_quantities=np.array([8.0, 0.0, 4.0], dtype=float),
        price_row=pd.Series([10.0, 20.0, 30.0], index=["AAA", "BBB", "CCC"], dtype=float),
    )

    assert metrics.pre_held_position_count == 2
    assert metrics.retained_position_count == 1
    assert metrics.liquidated_preheld_count == 1
    assert metrics.hold_through_rate_count == pytest.approx(0.5)
    assert metrics.hold_through_rate_value == pytest.approx(0.5)
    assert metrics.liquidated_preheld_value == pytest.approx(100.0)


def test_summarize_guard_protocol_results_computes_deltas_vs_abstain() -> None:
    detail_frame = pd.DataFrame(
        [
            {
                "rebalance_date": "2025-11-28",
                "protocol": "explicit_abstain",
                "turnover": 0.02,
                "gross_traded_notional": 20_000.0,
                "hold_through_rate_count": 1.0,
                "hold_through_rate_value": 1.0,
            },
            {
                "rebalance_date": "2025-12-31",
                "protocol": "explicit_abstain",
                "turnover": 0.03,
                "gross_traded_notional": 30_000.0,
                "hold_through_rate_count": 1.0,
                "hold_through_rate_value": 1.0,
            },
            {
                "rebalance_date": "2025-11-28",
                "protocol": "floor_to_zero",
                "turnover": 0.10,
                "gross_traded_notional": 100_000.0,
                "hold_through_rate_count": 0.5,
                "hold_through_rate_value": 0.4,
            },
            {
                "rebalance_date": "2025-12-31",
                "protocol": "floor_to_zero",
                "turnover": 0.12,
                "gross_traded_notional": 120_000.0,
                "hold_through_rate_count": 0.6,
                "hold_through_rate_value": 0.5,
            },
        ]
    )

    summary = summarize_guard_protocol_results(detail_frame, baseline_protocol="explicit_abstain")

    abstain_row = summary.loc[summary["protocol"] == "explicit_abstain"].iloc[0]
    floor_row = summary.loc[summary["protocol"] == "floor_to_zero"].iloc[0]

    assert abstain_row["guard_event_count"] == 2
    assert abstain_row["mean_turnover_delta_vs_baseline"] == pytest.approx(0.0)
    assert floor_row["guard_event_count"] == 2
    assert floor_row["mean_hold_through_rate_count"] == pytest.approx(0.55)
    assert floor_row["mean_hold_through_rate_value"] == pytest.approx(0.45)
    assert floor_row["mean_turnover_delta_vs_baseline"] == pytest.approx(0.085)
    assert floor_row["mean_gross_traded_notional_delta_vs_baseline"] == pytest.approx(85_000.0)


def test_recommend_guard_protocol_prefers_high_hold_through_then_low_turnover() -> None:
    summary = pd.DataFrame(
        [
            {
                "protocol": "explicit_abstain",
                "mean_hold_through_rate_value": 0.95,
                "mean_turnover_delta_vs_baseline": 0.0,
                "mean_gross_traded_notional_delta_vs_baseline": 0.0,
            },
            {
                "protocol": "floor_to_zero",
                "mean_hold_through_rate_value": 0.55,
                "mean_turnover_delta_vs_baseline": 0.08,
                "mean_gross_traded_notional_delta_vs_baseline": 80_000.0,
            },
            {
                "protocol": "signed_spread",
                "mean_hold_through_rate_value": 0.10,
                "mean_turnover_delta_vs_baseline": 0.20,
                "mean_gross_traded_notional_delta_vs_baseline": 200_000.0,
            },
        ]
    )

    assert recommend_guard_protocol(summary) == "explicit_abstain"


def test_render_guard_protocol_comparison_note_calls_out_runtime_contract_gap() -> None:
    detail = pd.DataFrame(
        [
            {
                "rebalance_date": "2025-11-28",
                "protocol": "explicit_abstain",
                "alpha_snapshot_present": False,
                "zero_expected_return_count": 0,
                "turnover": 0.02,
                "gross_traded_notional": 20_000.0,
                "hold_through_rate_count": 1.0,
                "hold_through_rate_value": 1.0,
            },
            {
                "rebalance_date": "2025-11-28",
                "protocol": "floor_to_zero",
                "alpha_snapshot_present": True,
                "zero_expected_return_count": 50,
                "turnover": 0.02,
                "gross_traded_notional": 20_000.0,
                "hold_through_rate_count": 1.0,
                "hold_through_rate_value": 1.0,
            },
        ]
    )
    summary = pd.DataFrame(
        [
            {
                "protocol": "explicit_abstain",
                "guard_event_count": 1,
                "mean_turnover": 0.02,
                "mean_gross_traded_notional": 20_000.0,
                "mean_hold_through_rate_count": 1.0,
                "mean_hold_through_rate_value": 1.0,
                "mean_turnover_delta_vs_baseline": 0.0,
                "mean_gross_traded_notional_delta_vs_baseline": 0.0,
            },
            {
                "protocol": "floor_to_zero",
                "guard_event_count": 1,
                "mean_turnover": 0.02,
                "mean_gross_traded_notional": 20_000.0,
                "mean_hold_through_rate_count": 1.0,
                "mean_hold_through_rate_value": 1.0,
                "mean_turnover_delta_vs_baseline": 0.0,
                "mean_gross_traded_notional_delta_vs_baseline": 0.0,
            },
        ]
    )

    note = render_guard_protocol_comparison_note(
        detail_frame=detail,
        summary_frame=summary,
        baseline_protocol="explicit_abstain",
        recommended_protocol="explicit_abstain",
    )

    assert "runtime still uses `floor_to_zero`" in note
    assert "zero-valued expected returns under `floor_to_zero` arise from the guard path" in note
