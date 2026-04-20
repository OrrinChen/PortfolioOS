from __future__ import annotations

import pytest

from portfolio_os.optimizer.acceptance_proof import (
    build_deterministic_synthetic_alpha_frame,
    evaluate_synthetic_alpha_case,
    summarize_acceptance_proof,
    SyntheticAlphaAcceptanceCase,
)


def _alpha_isolated_config(sample_context: dict):
    config = sample_context["config"].model_copy(deep=True)
    config.risk_model.enabled = False
    config.objective_weights.alpha_weight = 1.0
    config.objective_weights.target_deviation = 0.0
    config.objective_weights.transaction_fee = 0.0
    config.objective_weights.turnover_penalty = 0.0
    config.objective_weights.slippage_penalty = 0.0
    return config


def test_build_deterministic_synthetic_alpha_frame_is_centered(sample_context: dict) -> None:
    frame = build_deterministic_synthetic_alpha_frame(sample_context["universe"])

    assert list(frame.columns) == ["ticker", "synthetic_alpha_rank_pct", "synthetic_alpha_score"]
    assert len(frame) == len(sample_context["universe"])
    assert frame["synthetic_alpha_score"].mean() == pytest.approx(0.0, abs=1e-12)
    assert float(frame["synthetic_alpha_score"].min()) < 0.0
    assert float(frame["synthetic_alpha_score"].max()) > 0.0
    assert frame["synthetic_alpha_score"].is_monotonic_increasing


def test_evaluate_synthetic_alpha_case_zero_scale_stays_at_zero_alpha(sample_context: dict) -> None:
    case = evaluate_synthetic_alpha_case(
        sample_context["universe"],
        _alpha_isolated_config(sample_context),
        alpha_scale=0.0,
    )

    assert case.alpha_share_abs_weighted == pytest.approx(0.0)
    assert all(abs(value) <= 1e-12 for value in case.expected_return_by_ticker.values())
    assert case.alignment_spearman == pytest.approx(0.0, abs=1e-12)
    assert case.top_minus_bottom_weight_delta == pytest.approx(0.0, abs=1e-9)


def test_evaluate_synthetic_alpha_case_positive_scale_pushes_weights_with_alpha(sample_context: dict) -> None:
    case = evaluate_synthetic_alpha_case(
        sample_context["universe"],
        _alpha_isolated_config(sample_context),
        alpha_scale=1.0,
    )

    assert case.alpha_share_abs_weighted > 0.99
    assert case.alignment_spearman > 0.40
    assert case.base_alignment_spearman > 0.40
    assert case.top_minus_bottom_weight_delta > 0.0
    assert case.base_top_minus_bottom_weight_delta > 0.0
    assert case.continuous_gross_traded_notional > 0.0
    assert case.repaired_gross_traded_notional >= 0.0


def test_evaluate_synthetic_alpha_case_sign_flip_reverses_direction(sample_context: dict) -> None:
    config = _alpha_isolated_config(sample_context)
    positive = evaluate_synthetic_alpha_case(
        sample_context["universe"],
        config,
        alpha_scale=1.0,
        alpha_sign=1,
    )
    negative = evaluate_synthetic_alpha_case(
        sample_context["universe"],
        config,
        alpha_scale=1.0,
        alpha_sign=-1,
    )

    assert positive.base_alignment_spearman > 0.40
    assert negative.base_alignment_spearman < -0.40
    assert positive.base_top_minus_bottom_weight_delta > 0.0
    assert negative.base_top_minus_bottom_weight_delta < 0.0


def test_summarize_acceptance_proof_detects_monotone_positive_scales() -> None:
    cases = [
        SyntheticAlphaAcceptanceCase(
            alpha_scale=0.0,
            alpha_sign=1,
            solver_status="optimal",
            alpha_share_abs_weighted=0.0,
            alignment_spearman=0.0,
            base_alignment_spearman=0.0,
            top_minus_bottom_weight_delta=0.0,
            base_top_minus_bottom_weight_delta=0.0,
            continuous_gross_traded_notional=0.0,
            repaired_gross_traded_notional=0.0,
            repair_retention_ratio=0.0,
            active_share=0.0,
            objective_cash=0.30,
            effective_n_invested=20.0,
            current_weights={},
            post_trade_weights={},
            weight_change_by_ticker={},
            expected_return_by_ticker={},
        ),
        SyntheticAlphaAcceptanceCase(
            alpha_scale=0.5,
            alpha_sign=1,
            solver_status="optimal",
            alpha_share_abs_weighted=0.10,
            alignment_spearman=0.30,
            base_alignment_spearman=0.30,
            top_minus_bottom_weight_delta=0.01,
            base_top_minus_bottom_weight_delta=0.01,
            continuous_gross_traded_notional=100.0,
            repaired_gross_traded_notional=80.0,
            repair_retention_ratio=0.8,
            active_share=0.10,
            objective_cash=0.25,
            effective_n_invested=18.0,
            current_weights={},
            post_trade_weights={},
            weight_change_by_ticker={},
            expected_return_by_ticker={},
        ),
        SyntheticAlphaAcceptanceCase(
            alpha_scale=1.0,
            alpha_sign=1,
            solver_status="optimal",
            alpha_share_abs_weighted=0.20,
            alignment_spearman=0.50,
            base_alignment_spearman=0.50,
            top_minus_bottom_weight_delta=0.02,
            base_top_minus_bottom_weight_delta=0.02,
            continuous_gross_traded_notional=150.0,
            repaired_gross_traded_notional=120.0,
            repair_retention_ratio=0.8,
            active_share=0.12,
            objective_cash=0.20,
            effective_n_invested=17.0,
            current_weights={},
            post_trade_weights={},
            weight_change_by_ticker={},
            expected_return_by_ticker={},
        ),
        SyntheticAlphaAcceptanceCase(
            alpha_scale=1.0,
            alpha_sign=-1,
            solver_status="optimal",
            alpha_share_abs_weighted=0.22,
            alignment_spearman=0.52,
            base_alignment_spearman=-0.52,
            top_minus_bottom_weight_delta=0.02,
            base_top_minus_bottom_weight_delta=-0.02,
            continuous_gross_traded_notional=160.0,
            repaired_gross_traded_notional=130.0,
            repair_retention_ratio=0.8125,
            active_share=0.13,
            objective_cash=0.19,
            effective_n_invested=16.5,
            current_weights={},
            post_trade_weights={},
            weight_change_by_ticker={},
            expected_return_by_ticker={},
        ),
    ]

    summary = summarize_acceptance_proof(cases)

    assert summary["all_positive_scales_optimal"] is True
    assert summary["positive_scale_alpha_share_monotone"] is True
    assert summary["positive_scale_base_alignment_positive"] is True
    assert summary["sign_flip_reverses_base_alignment"] is True
