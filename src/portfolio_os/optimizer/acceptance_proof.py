"""Optimizer-isolated synthetic alpha acceptance diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import NormalDist

import numpy as np
import pandas as pd

from portfolio_os.domain.errors import InputValidationError
from portfolio_os.optimizer.rebalancer import run_rebalance
from portfolio_os.utils.config import AppConfig


_NORMAL_DIST = NormalDist()


@dataclass(frozen=True)
class SyntheticAlphaAcceptanceCase:
    """Summary metrics for one synthetic-alpha rebalance evaluation."""

    alpha_scale: float
    alpha_sign: int
    solver_status: str
    alpha_share_abs_weighted: float
    alignment_spearman: float
    base_alignment_spearman: float
    top_minus_bottom_weight_delta: float
    base_top_minus_bottom_weight_delta: float
    continuous_gross_traded_notional: float
    repaired_gross_traded_notional: float
    repair_retention_ratio: float
    active_share: float
    objective_cash: float
    effective_n_invested: float
    current_weights: dict[str, float]
    post_trade_weights: dict[str, float]
    weight_change_by_ticker: dict[str, float]
    expected_return_by_ticker: dict[str, float]


def _safe_spearman(left: pd.Series, right: pd.Series) -> float:
    """Return Spearman correlation or 0 when undefined."""

    pair = pd.concat([left, right], axis=1).dropna()
    if len(pair) < 2:
        return 0.0
    if pair.iloc[:, 0].nunique() <= 1 or pair.iloc[:, 1].nunique() <= 1:
        return 0.0
    value = float(pair.iloc[:, 0].corr(pair.iloc[:, 1], method="spearman"))
    if not np.isfinite(value):
        return 0.0
    return value


def _effective_n_invested(post_trade_weights: pd.Series) -> float:
    """Return the inverse Herfindahl count over invested long weights."""

    invested = post_trade_weights[post_trade_weights > 1e-12].astype(float)
    if invested.empty:
        return 0.0
    return float(1.0 / np.square(invested).sum())


def build_deterministic_synthetic_alpha_frame(universe: pd.DataFrame) -> pd.DataFrame:
    """Build a deterministic, centered cross-sectional synthetic alpha vector."""

    if "ticker" not in universe.columns:
        raise InputValidationError("Universe must contain a ticker column.")
    ticker_frame = pd.DataFrame({"ticker": universe["ticker"].astype(str)}).drop_duplicates().sort_values("ticker")
    ticker_frame = ticker_frame.reset_index(drop=True)
    count = len(ticker_frame)
    if count < 2:
        raise InputValidationError("Synthetic alpha diagnostics require at least two tickers.")
    rank_pct = pd.Series(
        np.arange(1, count + 1, dtype=float) / float(count + 1),
        index=ticker_frame.index,
    )
    score = rank_pct.map(lambda value: float(_NORMAL_DIST.inv_cdf(float(value))))
    score = score - float(score.mean())
    ticker_frame["synthetic_alpha_rank_pct"] = rank_pct.astype(float)
    ticker_frame["synthetic_alpha_score"] = score.astype(float)
    return ticker_frame


def evaluate_synthetic_alpha_case(
    universe: pd.DataFrame,
    config: AppConfig,
    *,
    alpha_scale: float,
    alpha_sign: int = 1,
    alpha_frame: pd.DataFrame | None = None,
) -> SyntheticAlphaAcceptanceCase:
    """Run one rebalance with synthetic expected returns and summarize acceptance metrics."""

    if alpha_sign not in {-1, 1}:
        raise InputValidationError("alpha_sign must be either +1 or -1.")
    synthetic = (
        alpha_frame.loc[:, ["ticker", "synthetic_alpha_score"]].copy()
        if alpha_frame is not None
        else build_deterministic_synthetic_alpha_frame(universe).loc[:, ["ticker", "synthetic_alpha_score"]].copy()
    )
    work = universe.copy()
    work["ticker"] = work["ticker"].astype(str)
    work = work.merge(synthetic, on="ticker", how="left")
    if work["synthetic_alpha_score"].isna().any():
        raise InputValidationError("Synthetic alpha frame must cover every universe ticker.")
    work["expected_return"] = (
        float(alpha_scale) * float(alpha_sign) * work["synthetic_alpha_score"].astype(float)
    )

    rebalance_run = run_rebalance(work, config)
    optimization_result = rebalance_run.optimization_result

    current_weights = pd.Series(optimization_result.current_weights, dtype=float).sort_index()
    post_trade_weights = pd.Series(optimization_result.post_trade_weights, dtype=float).reindex(current_weights.index).fillna(0.0)
    target_weights = pd.Series(optimization_result.target_weights, dtype=float).reindex(current_weights.index).fillna(0.0)
    expected_return = (
        work.set_index("ticker")["expected_return"]
        .astype(float)
        .reindex(current_weights.index)
        .fillna(0.0)
    )
    base_alpha_score = (
        work.set_index("ticker")["synthetic_alpha_score"]
        .astype(float)
        .reindex(current_weights.index)
        .fillna(0.0)
    )
    weight_change = post_trade_weights - current_weights
    alpha_share_abs_weighted = float(
        optimization_result.objective_decomposition.get("components", {})
        .get("alpha_reward", {})
        .get("share_abs_weighted", 0.0)
    )
    if expected_return.nunique() <= 1:
        top_minus_bottom_weight_delta = 0.0
    else:
        rank_pct = expected_return.rank(method="first", pct=True)
        top_mask = rank_pct >= 0.8
        bottom_mask = rank_pct <= 0.2
        top_minus_bottom_weight_delta = float(weight_change.loc[top_mask].mean() - weight_change.loc[bottom_mask].mean())
    base_rank_pct = base_alpha_score.rank(method="first", pct=True)
    base_top_mask = base_rank_pct >= 0.8
    base_bottom_mask = base_rank_pct <= 0.2
    base_top_minus_bottom_weight_delta = float(
        weight_change.loc[base_top_mask].mean() - weight_change.loc[base_bottom_mask].mean()
    )

    benchmark_cash = float(1.0 - target_weights.sum())
    post_trade_cash_fraction = float(optimization_result.post_trade_cash_estimate / optimization_result.pre_trade_nav)
    objective_cash = float(post_trade_cash_fraction - benchmark_cash)
    continuous_gross = float(optimization_result.gross_traded_notional)
    repaired_gross = float(rebalance_run.basket.gross_traded_notional)
    repair_retention_ratio = float(repaired_gross / continuous_gross) if continuous_gross > 0.0 else 0.0

    return SyntheticAlphaAcceptanceCase(
        alpha_scale=float(alpha_scale),
        alpha_sign=int(alpha_sign),
        solver_status=str(optimization_result.status),
        alpha_share_abs_weighted=alpha_share_abs_weighted,
        alignment_spearman=_safe_spearman(expected_return, weight_change),
        base_alignment_spearman=_safe_spearman(base_alpha_score, weight_change),
        top_minus_bottom_weight_delta=top_minus_bottom_weight_delta,
        base_top_minus_bottom_weight_delta=base_top_minus_bottom_weight_delta,
        continuous_gross_traded_notional=continuous_gross,
        repaired_gross_traded_notional=repaired_gross,
        repair_retention_ratio=repair_retention_ratio,
        active_share=float(0.5 * np.abs(post_trade_weights - target_weights).sum()),
        objective_cash=objective_cash,
        effective_n_invested=_effective_n_invested(post_trade_weights),
        current_weights={str(ticker): float(value) for ticker, value in current_weights.items()},
        post_trade_weights={str(ticker): float(value) for ticker, value in post_trade_weights.items()},
        weight_change_by_ticker={str(ticker): float(value) for ticker, value in weight_change.items()},
        expected_return_by_ticker={str(ticker): float(value) for ticker, value in expected_return.items()},
    )


def summarize_acceptance_proof(cases: list[SyntheticAlphaAcceptanceCase]) -> dict[str, bool | float | int]:
    """Summarize the core proof invariants across a small acceptance grid."""

    if not cases:
        raise InputValidationError("At least one synthetic alpha case is required.")
    positive_cases = sorted(
        [case for case in cases if case.alpha_sign == 1 and case.alpha_scale > 0.0],
        key=lambda case: case.alpha_scale,
    )
    negative_cases = [case for case in cases if case.alpha_sign == -1]
    positive_alpha_shares = [float(case.alpha_share_abs_weighted) for case in positive_cases]
    positive_alignments = [float(case.base_alignment_spearman) for case in positive_cases]
    sign_flip_case = negative_cases[0] if negative_cases else None
    return {
        "case_count": int(len(cases)),
        "positive_case_count": int(len(positive_cases)),
        "negative_case_count": int(len(negative_cases)),
        "all_positive_scales_optimal": bool(
            positive_cases and all(str(case.solver_status).lower().startswith("optimal") for case in positive_cases)
        ),
        "positive_scale_alpha_share_monotone": bool(
            positive_cases
            and all(
                later >= earlier - 1e-12
                for earlier, later in zip(positive_alpha_shares, positive_alpha_shares[1:], strict=False)
            )
        ),
        "positive_scale_base_alignment_positive": bool(
            positive_cases and all(alignment > 0.0 for alignment in positive_alignments)
        ),
        "sign_flip_reverses_base_alignment": bool(
            sign_flip_case is not None and float(sign_flip_case.base_alignment_spearman) < 0.0
        ),
        "sign_flip_reverses_base_top_bottom": bool(
            sign_flip_case is not None and float(sign_flip_case.base_top_minus_bottom_weight_delta) < 0.0
        ),
    }
