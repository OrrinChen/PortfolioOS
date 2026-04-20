from __future__ import annotations

import pandas as pd
import pytest

from portfolio_os.alpha.package_audit import build_counterfactual_alpha_panel, build_real_alpha_package_audit


def test_build_real_alpha_package_audit_separates_cold_start_guard_and_active() -> None:
    rebalance_schedule = ["2025-01-31", "2025-02-28", "2025-03-31"]
    alpha_panel = pd.DataFrame(
        {
            "date": ["2025-02-28"] * 3 + ["2025-03-31"] * 3,
            "ticker": ["AAA", "BBB", "CCC"] * 2,
            "expected_return": [0.02, 0.00, -0.02, 0.0, 0.0, 0.0],
            "quantile": [5, 3, 1, 5, 3, 1],
            "signal_strength_confidence": [0.8, 0.8, 0.8, 0.5, 0.5, 0.5],
            "annualized_top_bottom_spread": [0.15, 0.15, 0.15, 0.0, 0.0, 0.0],
            "period_top_bottom_spread": [0.012, 0.012, 0.012, 0.0, 0.0, 0.0],
            "decision_horizon_days": [2, 2, 2, 2, 2, 2],
            "raw_mean_top_bottom_spread": [0.004, 0.004, 0.004, -0.003, -0.003, -0.003],
            "negative_spread_protocol": ["floor_to_zero"] * 6,
            "alpha_protocol_status": ["active_positive_spread"] * 3 + ["spread_floor_to_zero"] * 3,
        }
    )
    period_attribution = pd.DataFrame(
        {
            "strategy": ["optimizer", "optimizer", "naive_pro_rata", "naive_pro_rata"],
            "period_index": [1, 2, 1, 2],
            "start_date": ["2025-02-28", "2025-03-31", "2025-02-28", "2025-03-31"],
            "end_date": ["2025-03-04", "2025-04-02", "2025-03-04", "2025-04-02"],
            "gross_traded_notional": [100_000.0, 50_000.0, 90_000.0, 45_000.0],
            "turnover": [0.10, 0.05, 0.09, 0.045],
            "active_trading_pnl": [120.0, 40.0, 30.0, 10.0],
            "trading_cost_pnl": [-20.0, -8.0, -12.0, -5.0],
            "period_pnl": [100.0, 32.0, 18.0, 5.0],
            "optimizer_vs_naive_period_pnl_delta": [82.0, 27.0, 0.0, 0.0],
        }
    )
    returns_panel = pd.DataFrame(
        {
            "AAA": [0.01, 0.00, 0.02, 0.01, 0.00],
            "BBB": [0.00, 0.00, 0.00, 0.00, 0.00],
            "CCC": [-0.01, 0.00, -0.02, -0.01, 0.00],
        },
        index=pd.to_datetime(
            ["2025-01-31", "2025-02-28", "2025-03-03", "2025-03-31", "2025-04-01"]
        ),
    )

    audit = build_real_alpha_package_audit(
        rebalance_schedule=rebalance_schedule,
        alpha_panel=alpha_panel,
        period_attribution=period_attribution,
        returns_panel=returns_panel,
    )

    coverage = audit.coverage_frame.set_index("date")
    assert coverage.loc["2025-01-31", "alpha_state"] == "cold_start"
    assert coverage.loc["2025-02-28", "alpha_state"] == "active_nonzero"
    assert coverage.loc["2025-03-31", "alpha_state"] == "spread_floor_to_zero"
    assert audit.summary_payload["coverage"]["rebalance_count"] == 3
    assert audit.summary_payload["coverage"]["alpha_ready_count"] == 2
    assert audit.summary_payload["coverage"]["alpha_active_count"] == 1
    assert audit.summary_payload["coverage"]["guard_zero_count"] == 1
    assert audit.summary_payload["coverage"]["cold_start_count"] == 1

    mapping = audit.mapping_frame
    assert list(mapping["date"]) == ["2025-02-28"]
    assert float(mapping.iloc[0]["rank_ic"]) > 0.99
    assert bool(mapping.iloc[0]["spread_sign_match"]) is True

    thickness = audit.thickness_frame
    assert list(thickness["start_date"]) == ["2025-02-28"]
    assert float(thickness.iloc[0]["net_active_pnl"]) == pytest.approx(100.0)
    assert float(audit.summary_payload["thickness"]["gross_active_trading_pnl"]) == pytest.approx(120.0)
    assert float(audit.summary_payload["thickness"]["net_active_pnl"]) == pytest.approx(100.0)
    assert float(audit.summary_payload["thickness"]["gross_to_net_retention"]) == pytest.approx(100.0 / 120.0)


def test_build_real_alpha_package_audit_marks_negative_mapping_when_realized_spread_flips() -> None:
    rebalance_schedule = ["2025-02-28"]
    alpha_panel = pd.DataFrame(
        {
            "date": ["2025-02-28"] * 4,
            "ticker": ["AAA", "BBB", "CCC", "DDD"],
            "expected_return": [0.03, 0.01, -0.01, -0.03],
            "quantile": [5, 4, 2, 1],
            "signal_strength_confidence": [1.0] * 4,
            "annualized_top_bottom_spread": [0.20] * 4,
            "period_top_bottom_spread": [0.015] * 4,
            "decision_horizon_days": [1] * 4,
            "raw_mean_top_bottom_spread": [0.005] * 4,
            "negative_spread_protocol": ["floor_to_zero"] * 4,
            "alpha_protocol_status": ["active_positive_spread"] * 4,
        }
    )
    period_attribution = pd.DataFrame(
        {
            "strategy": ["optimizer"],
            "period_index": [1],
            "start_date": ["2025-02-28"],
            "end_date": ["2025-03-03"],
            "gross_traded_notional": [75_000.0],
            "turnover": [0.08],
            "active_trading_pnl": [-30.0],
            "trading_cost_pnl": [-10.0],
            "period_pnl": [-40.0],
            "optimizer_vs_naive_period_pnl_delta": [-15.0],
        }
    )
    returns_panel = pd.DataFrame(
        {
            "AAA": [0.00, -0.03],
            "BBB": [0.00, -0.01],
            "CCC": [0.00, 0.01],
            "DDD": [0.00, 0.03],
        },
        index=pd.to_datetime(["2025-02-28", "2025-03-03"]),
    )

    audit = build_real_alpha_package_audit(
        rebalance_schedule=rebalance_schedule,
        alpha_panel=alpha_panel,
        period_attribution=period_attribution,
        returns_panel=returns_panel,
    )

    assert len(audit.mapping_frame) == 1
    assert bool(audit.mapping_frame.iloc[0]["spread_sign_match"]) is False
    assert float(audit.mapping_frame.iloc[0]["rank_ic"]) < 0.0
    assert float(audit.summary_payload["mapping"]["positive_rank_ic_ratio"]) == pytest.approx(0.0)
    assert "## Coverage" in audit.report_markdown
    assert "## Mapping" in audit.report_markdown
    assert "## Thickness" in audit.report_markdown


def test_build_real_alpha_package_audit_distinguishes_insufficient_history_from_guard_zero() -> None:
    rebalance_schedule = ["2025-09-30", "2025-10-31", "2025-11-28"]
    alpha_panel = pd.DataFrame(
        {
            "date": ["2025-09-30"] * 3 + ["2025-10-31"] * 3 + ["2025-11-28"] * 3,
            "ticker": ["AAA", "BBB", "CCC"] * 3,
            "expected_return": [0.0, 0.0, 0.0, 0.03, 0.0, -0.03, 0.0, 0.0, 0.0],
            "quantile": [5, 3, 1] * 3,
            "signal_strength_confidence": [0.0, 0.0, 0.0, 0.8, 0.8, 0.8, 0.2, 0.2, 0.2],
            "annualized_top_bottom_spread": [0.0, 0.0, 0.0, 0.20, 0.20, 0.20, 0.0, 0.0, 0.0],
            "period_top_bottom_spread": [0.0, 0.0, 0.0, 0.015, 0.015, 0.015, 0.0, 0.0, 0.0],
            "decision_horizon_days": [1] * 9,
            "raw_mean_top_bottom_spread": [0.01, 0.01, 0.01, 0.006, 0.006, 0.006, -0.003, -0.003, -0.003],
            "negative_spread_protocol": ["floor_to_zero"] * 9,
            "alpha_protocol_status": [
                "insufficient_history",
                "insufficient_history",
                "insufficient_history",
                "active_nonzero",
                "active_nonzero",
                "active_nonzero",
                "spread_floor_to_zero",
                "spread_floor_to_zero",
                "spread_floor_to_zero",
            ],
        }
    )
    period_attribution = pd.DataFrame(
        {
            "strategy": ["optimizer", "optimizer"],
            "period_index": [1, 2],
            "start_date": ["2025-10-31", "2025-11-28"],
            "end_date": ["2025-11-03", "2025-12-01"],
            "gross_traded_notional": [100_000.0, 25_000.0],
            "turnover": [0.10, 0.03],
            "active_trading_pnl": [120.0, -5.0],
            "trading_cost_pnl": [-20.0, -2.0],
            "period_pnl": [100.0, -7.0],
            "optimizer_vs_naive_period_pnl_delta": [82.0, -3.0],
        }
    )
    returns_panel = pd.DataFrame(
        {
            "AAA": [0.0, 0.03, 0.00, -0.01],
            "BBB": [0.0, 0.00, 0.00, 0.00],
            "CCC": [0.0, -0.03, 0.00, 0.01],
        },
        index=pd.to_datetime(["2025-09-30", "2025-10-31", "2025-11-03", "2025-12-01"]),
    )

    audit = build_real_alpha_package_audit(
        rebalance_schedule=rebalance_schedule,
        alpha_panel=alpha_panel,
        period_attribution=period_attribution,
        returns_panel=returns_panel,
    )

    coverage = audit.coverage_frame.set_index("date")
    assert coverage.loc["2025-09-30", "alpha_state"] == "insufficient_history"
    assert coverage.loc["2025-11-28", "alpha_state"] == "spread_floor_to_zero"
    assert audit.summary_payload["coverage"]["insufficient_history_count"] == 1
    assert audit.summary_payload["coverage"]["spread_floor_to_zero_count"] == 1
    assert audit.summary_payload["coverage"]["guard_zero_count"] == 1


def test_build_real_alpha_package_audit_counterfactual_bypass_promotes_floor_zero_months_into_mapping() -> None:
    rebalance_schedule = ["2025-10-31", "2025-11-28"]
    alpha_panel = pd.DataFrame(
        {
            "date": ["2025-10-31"] * 4 + ["2025-11-28"] * 4,
            "ticker": ["AAA", "BBB", "CCC", "DDD"] * 2,
            "expected_return": [0.03, 0.01, -0.01, -0.03, 0.0, 0.0, 0.0, 0.0],
            "quantile": [5, 4, 2, 1] * 2,
            "alpha_score": [0.40, 0.20, -0.20, -0.40, 0.50, 0.10, -0.10, -0.50],
            "alpha_zscore": [1.3, 0.4, -0.4, -1.3, 1.5, 0.5, -0.5, -1.5],
            "signal_strength_confidence": [0.9] * 4 + [0.25] * 4,
            "annualized_top_bottom_spread": [0.20] * 4 + [0.0] * 4,
            "period_top_bottom_spread": [0.015] * 4 + [0.0] * 4,
            "decision_horizon_days": [1] * 8,
            "raw_mean_top_bottom_spread": [0.006] * 4 + [-0.004] * 4,
            "negative_spread_protocol": ["floor_to_zero"] * 8,
            "alpha_protocol_status": ["active_nonzero"] * 4 + ["spread_floor_to_zero"] * 4,
        }
    )
    period_attribution = pd.DataFrame(
        {
            "strategy": ["optimizer", "optimizer"],
            "period_index": [1, 2],
            "start_date": ["2025-10-31", "2025-11-28"],
            "end_date": ["2025-11-03", "2025-12-01"],
            "gross_traded_notional": [80_000.0, 40_000.0],
            "turnover": [0.09, 0.04],
            "active_trading_pnl": [30.0, -10.0],
            "trading_cost_pnl": [-8.0, -3.0],
            "period_pnl": [22.0, -13.0],
            "optimizer_vs_naive_period_pnl_delta": [12.0, -6.0],
        }
    )
    returns_panel = pd.DataFrame(
        {
            "AAA": [0.0, -0.03, 0.0, 0.02],
            "BBB": [0.0, -0.01, 0.0, 0.01],
            "CCC": [0.0, 0.01, 0.0, -0.01],
            "DDD": [0.0, 0.03, 0.0, -0.02],
        },
        index=pd.to_datetime(["2025-10-31", "2025-11-03", "2025-11-28", "2025-12-01"]),
    )

    baseline_audit = build_real_alpha_package_audit(
        rebalance_schedule=rebalance_schedule,
        alpha_panel=alpha_panel,
        period_attribution=period_attribution,
        returns_panel=returns_panel,
    )
    counterfactual_audit = build_real_alpha_package_audit(
        rebalance_schedule=rebalance_schedule,
        alpha_panel=alpha_panel,
        period_attribution=period_attribution,
        returns_panel=returns_panel,
        counterfactual_negative_spread_mode="signed_spread",
    )

    assert baseline_audit.summary_payload["mapping"]["active_period_count"] == 1
    assert counterfactual_audit.summary_payload["mapping"]["active_period_count"] == 2
    assert counterfactual_audit.summary_payload["coverage"]["counterfactual_promoted_count"] == 1


def test_build_counterfactual_alpha_panel_promotes_signed_spread_months_without_mutating_input() -> None:
    alpha_panel = pd.DataFrame(
        {
            "date": ["2025-11-28"] * 4,
            "ticker": ["AAA", "BBB", "CCC", "DDD"],
            "expected_return": [0.0, 0.0, 0.0, 0.0],
            "quantile": [5, 4, 2, 1],
            "alpha_zscore": [1.5, 0.5, -0.5, -1.5],
            "signal_strength_confidence": [0.25] * 4,
            "annualized_top_bottom_spread": [0.0] * 4,
            "period_top_bottom_spread": [0.0] * 4,
            "decision_horizon_days": [1] * 4,
            "raw_mean_top_bottom_spread": [-0.004] * 4,
            "negative_spread_protocol": ["floor_to_zero"] * 4,
            "alpha_protocol_status": ["spread_floor_to_zero"] * 4,
        }
    )

    rebuilt, promoted_dates = build_counterfactual_alpha_panel(
        alpha_panel=alpha_panel,
        negative_spread_mode="signed_spread",
        forward_horizon_days=21,
        max_abs_expected_return=0.2,
    )

    assert set(promoted_dates) == {"2025-11-28"}
    assert bool((rebuilt["expected_return"].abs() > 0).any())
    assert bool((alpha_panel["expected_return"].abs() == 0).all())
