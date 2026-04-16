"""Semantic helpers for alpha-bridge protocol comparison."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

from portfolio_os.domain.errors import InputValidationError


_ANNUALIZATION_FACTOR = 252.0

AlphaNegativeSpreadProtocol = Literal["floor_to_zero", "signed_spread", "explicit_abstain"]


@dataclass(frozen=True)
class AlphaSpreadProtocolDecision:
    """Resolved trailing-spread protocol for one rebalance date."""

    protocol: AlphaNegativeSpreadProtocol
    status: str
    raw_mean_top_bottom_spread: float
    annualized_top_bottom_spread: float | None
    period_top_bottom_spread: float | None
    should_abstain: bool


@dataclass(frozen=True)
class HoldThroughMetrics:
    """How much of the pre-trade book survives one guard-event rebalance."""

    pre_held_position_count: int
    retained_position_count: int
    liquidated_preheld_count: int
    hold_through_rate_count: float
    hold_through_rate_value: float
    liquidated_preheld_value: float


def _deannualize_return(annualized_return: float, horizon_days: int) -> float:
    if horizon_days <= 0:
        raise InputValidationError("decision_horizon_days must be positive.")
    if annualized_return <= -1.0:
        raise InputValidationError("annualized_return must be greater than -1.0 to deannualize.")
    return float((1.0 + float(annualized_return)) ** (float(horizon_days) / _ANNUALIZATION_FACTOR) - 1.0)


def resolve_negative_spread_protocol(
    raw_mean_top_bottom_spread: float,
    *,
    forward_horizon_days: int,
    decision_horizon_days: int,
    protocol: AlphaNegativeSpreadProtocol,
) -> AlphaSpreadProtocolDecision:
    """Resolve how a negative trailing spread should be expressed downstream."""

    raw_spread = float(raw_mean_top_bottom_spread)
    if forward_horizon_days <= 0:
        raise InputValidationError("forward_horizon_days must be positive.")
    if decision_horizon_days <= 0:
        raise InputValidationError("decision_horizon_days must be positive.")

    annualized_spread = float(raw_spread * (_ANNUALIZATION_FACTOR / float(forward_horizon_days)))
    if raw_spread >= 0.0:
        return AlphaSpreadProtocolDecision(
            protocol=protocol,
            status="nonzero_alpha" if annualized_spread > 0.0 else "zero_alpha",
            raw_mean_top_bottom_spread=raw_spread,
            annualized_top_bottom_spread=annualized_spread,
            period_top_bottom_spread=_deannualize_return(annualized_spread, decision_horizon_days),
            should_abstain=False,
        )

    if protocol == "floor_to_zero":
        return AlphaSpreadProtocolDecision(
            protocol=protocol,
            status="spread_floor_to_zero",
            raw_mean_top_bottom_spread=raw_spread,
            annualized_top_bottom_spread=0.0,
            period_top_bottom_spread=0.0,
            should_abstain=False,
        )
    if protocol == "signed_spread":
        return AlphaSpreadProtocolDecision(
            protocol=protocol,
            status="signed_negative_spread",
            raw_mean_top_bottom_spread=raw_spread,
            annualized_top_bottom_spread=annualized_spread,
            period_top_bottom_spread=_deannualize_return(annualized_spread, decision_horizon_days),
            should_abstain=False,
        )
    if protocol == "explicit_abstain":
        return AlphaSpreadProtocolDecision(
            protocol=protocol,
            status="explicit_abstain",
            raw_mean_top_bottom_spread=raw_spread,
            annualized_top_bottom_spread=None,
            period_top_bottom_spread=None,
            should_abstain=True,
        )
    raise InputValidationError(f"Unsupported negative spread protocol: {protocol}")


def compute_hold_through_metrics(
    *,
    tickers: pd.Index,
    pre_trade_quantities: pd.Series,
    post_trade_quantities: np.ndarray,
    price_row: pd.Series,
) -> HoldThroughMetrics:
    """Measure how many pre-trade positions remain after one rebalance."""

    tickers_index = pd.Index([str(item) for item in tickers], dtype="object")
    pre_quantities = (
        pd.Series(pre_trade_quantities, copy=True)
        .astype(float)
        .reindex(tickers_index, fill_value=0.0)
    )
    post_quantities = pd.Series(np.asarray(post_trade_quantities, dtype=float), index=tickers_index, dtype=float)
    prices = pd.Series(price_row, copy=True).astype(float).reindex(tickers_index, fill_value=0.0)

    pre_held_mask = pre_quantities > 0.0
    retained_mask = pre_held_mask & (post_quantities > 0.0)
    liquidated_mask = pre_held_mask & ~retained_mask

    pre_held_count = int(pre_held_mask.sum())
    retained_count = int(retained_mask.sum())
    liquidated_count = int(liquidated_mask.sum())

    pre_held_value = float((pre_quantities.loc[pre_held_mask] * prices.loc[pre_held_mask]).sum())
    retained_value = float((pre_quantities.loc[retained_mask] * prices.loc[retained_mask]).sum())
    liquidated_value = float((pre_quantities.loc[liquidated_mask] * prices.loc[liquidated_mask]).sum())

    hold_through_rate_count = float(retained_count / pre_held_count) if pre_held_count else 1.0
    hold_through_rate_value = float(retained_value / pre_held_value) if pre_held_value else 1.0

    return HoldThroughMetrics(
        pre_held_position_count=pre_held_count,
        retained_position_count=retained_count,
        liquidated_preheld_count=liquidated_count,
        hold_through_rate_count=hold_through_rate_count,
        hold_through_rate_value=hold_through_rate_value,
        liquidated_preheld_value=liquidated_value,
    )


def summarize_guard_protocol_results(
    detail_frame: pd.DataFrame,
    *,
    baseline_protocol: str = "explicit_abstain",
) -> pd.DataFrame:
    """Aggregate per-event protocol behavior and compute deltas versus a baseline protocol."""

    if detail_frame.empty:
        return pd.DataFrame(
            columns=[
                "protocol",
                "guard_event_count",
                "mean_turnover",
                "mean_gross_traded_notional",
                "mean_hold_through_rate_count",
                "mean_hold_through_rate_value",
                "mean_turnover_delta_vs_baseline",
                "mean_gross_traded_notional_delta_vs_baseline",
            ]
        )

    required = {
        "rebalance_date",
        "protocol",
        "turnover",
        "gross_traded_notional",
        "hold_through_rate_count",
        "hold_through_rate_value",
    }
    missing = sorted(required - set(detail_frame.columns))
    if missing:
        raise InputValidationError(
            "detail_frame missing required columns: " + ", ".join(missing)
        )

    work = detail_frame.copy()
    baseline = (
        work.loc[work["protocol"] == baseline_protocol, ["rebalance_date", "turnover", "gross_traded_notional"]]
        .rename(
            columns={
                "turnover": "baseline_turnover",
                "gross_traded_notional": "baseline_gross_traded_notional",
            }
        )
        .copy()
    )
    if baseline.empty:
        raise InputValidationError(f"Baseline protocol {baseline_protocol} is missing from detail_frame.")

    work = work.merge(baseline, on="rebalance_date", how="left")
    work["turnover_delta_vs_baseline"] = (
        pd.to_numeric(work["turnover"], errors="coerce")
        - pd.to_numeric(work["baseline_turnover"], errors="coerce")
    )
    work["gross_traded_notional_delta_vs_baseline"] = (
        pd.to_numeric(work["gross_traded_notional"], errors="coerce")
        - pd.to_numeric(work["baseline_gross_traded_notional"], errors="coerce")
    )

    summary = (
        work.groupby("protocol", as_index=False)
        .agg(
            guard_event_count=("rebalance_date", "nunique"),
            mean_turnover=("turnover", "mean"),
            mean_gross_traded_notional=("gross_traded_notional", "mean"),
            mean_hold_through_rate_count=("hold_through_rate_count", "mean"),
            mean_hold_through_rate_value=("hold_through_rate_value", "mean"),
            mean_turnover_delta_vs_baseline=("turnover_delta_vs_baseline", "mean"),
            mean_gross_traded_notional_delta_vs_baseline=("gross_traded_notional_delta_vs_baseline", "mean"),
        )
        .sort_values("protocol")
        .reset_index(drop=True)
    )
    return summary


def render_guard_protocol_comparison_note(
    *,
    detail_frame: pd.DataFrame,
    summary_frame: pd.DataFrame,
    baseline_protocol: str,
    recommended_protocol: str | None = None,
) -> str:
    """Render a concise Markdown summary for the guard-protocol comparison."""

    lines = [
        "# Alpha Bridge Guard Protocol Comparison",
        "",
        f"- guard event count: `{int(detail_frame['rebalance_date'].nunique()) if not detail_frame.empty else 0}`",
        f"- baseline protocol for turnover deltas: `{baseline_protocol}`",
    ]
    if recommended_protocol:
        lines.append(f"- recommended protocol: `{recommended_protocol}`")
    lines.extend(["", "## Protocol Summary", ""])
    if summary_frame.empty:
        lines.append("- No guard-event rows were available.")
        return "\n".join(lines) + "\n"

    for row in summary_frame.to_dict(orient="records"):
        lines.extend(
            [
                f"### `{row['protocol']}`",
                f"- guard events: `{int(row['guard_event_count'])}`",
                f"- mean hold-through (count): `{float(row['mean_hold_through_rate_count']):.4f}`",
                f"- mean hold-through (value): `{float(row['mean_hold_through_rate_value']):.4f}`",
                f"- mean turnover: `{float(row['mean_turnover']):.4f}`",
                f"- mean turnover delta vs `{baseline_protocol}`: `{float(row['mean_turnover_delta_vs_baseline']):.4f}`",
                f"- mean gross traded notional delta vs `{baseline_protocol}`: `{float(row['mean_gross_traded_notional_delta_vs_baseline']):.2f}`",
                "",
            ]
        )
    return "\n".join(lines)


def recommend_guard_protocol(summary_frame: pd.DataFrame) -> str:
    """Pick the protocol with the best retention / turnover trade-off."""

    required = {
        "protocol",
        "mean_hold_through_rate_value",
        "mean_turnover_delta_vs_baseline",
        "mean_gross_traded_notional_delta_vs_baseline",
    }
    missing = sorted(required - set(summary_frame.columns))
    if missing:
        raise InputValidationError(
            "summary_frame missing required columns: " + ", ".join(missing)
        )
    if summary_frame.empty:
        raise InputValidationError("summary_frame must not be empty.")

    ranked = summary_frame.sort_values(
        by=[
            "mean_hold_through_rate_value",
            "mean_turnover_delta_vs_baseline",
            "mean_gross_traded_notional_delta_vs_baseline",
            "protocol",
        ],
        ascending=[False, True, True, True],
    ).reset_index(drop=True)
    return str(ranked.iloc[0]["protocol"])
