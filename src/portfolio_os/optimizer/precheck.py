"""Pre-solver feasibility and consistency checks for rebalance runs."""

from __future__ import annotations

import numpy as np
import pandas as pd

from portfolio_os.compliance.findings import build_finding
from portfolio_os.domain.enums import FindingCategory, FindingSeverity, RepairStatus
from portfolio_os.domain.models import ComplianceFinding
from portfolio_os.utils.config import AppConfig


def _locked_trade_mask(universe: pd.DataFrame) -> np.ndarray:
    """Return tickers that cannot be actively traded on either side."""

    blocked_mask = (
        (~universe["tradable"].astype(bool))
        | universe["upper_limit_hit"].astype(bool)
        | universe["lower_limit_hit"].astype(bool)
    ).to_numpy(dtype=bool)
    both_side_blacklist_mask = (
        universe["blacklist_buy"].astype(bool)
        & universe["blacklist_sell"].astype(bool)
    ).to_numpy(dtype=bool)
    return blocked_mask | both_side_blacklist_mask


def collect_rebalance_precheck_findings(
    universe: pd.DataFrame,
    config: AppConfig,
) -> list[ComplianceFinding]:
    """Collect non-blocking diagnostics before calling the optimizer."""

    prices = universe["estimated_price"].to_numpy(dtype=float)
    quantities = universe["quantity"].to_numpy(dtype=float)
    pre_trade_nav = float(np.sum(prices * quantities) + config.portfolio_state.available_cash)
    if pre_trade_nav <= 0:
        return []

    locked_mask = _locked_trade_mask(universe)
    current_weights = np.divide(
        prices * quantities,
        pre_trade_nav,
        out=np.zeros_like(prices, dtype=float),
        where=pre_trade_nav > 0,
    )
    single_name_limit = config.effective_single_name_limit
    findings: list[ComplianceFinding] = []

    for ticker, locked, weight in zip(
        universe["ticker"].astype(str).tolist(),
        locked_mask.tolist(),
        current_weights.tolist(),
        strict=True,
    ):
        if not locked or weight <= single_name_limit + 1e-9:
            continue
        findings.append(
            build_finding(
                "locked_single_name_above_limit",
                FindingCategory.RISK,
                FindingSeverity.INFO,
                "Ticker is currently locked from trading and already above the active single-name limit; solver will preserve the current bound for this name.",
                ticker=ticker,
                rule_source="optimizer.precheck.locked_single_name",
                blocking=False,
                repair_status=RepairStatus.NOT_NEEDED,
                details={
                    "current_weight": float(weight),
                    "effective_single_name_limit": float(single_name_limit),
                    "reason_label": "locked_position_already_over_limit",
                },
            )
        )

    for industry, bounds in config.constraints.industry_bounds.items():
        industry_mask = (universe["industry"] == industry).to_numpy(dtype=bool)
        if not industry_mask.any():
            continue
        industry_exposure = float(np.sum(prices[industry_mask] * quantities[industry_mask]) / pre_trade_nav)
        industry_locked_mask = locked_mask[industry_mask]
        if not bool(industry_locked_mask.all()):
            continue
        if bounds.max is not None and industry_exposure > bounds.max + 1e-9:
            findings.append(
                build_finding(
                    "locked_industry_above_max",
                    FindingCategory.RISK,
                    FindingSeverity.INFO,
                    "Industry is fully locked from trading and already above max bound; solver will preserve the current exposure ceiling for this industry.",
                    rule_source="optimizer.precheck.locked_industry",
                    blocking=False,
                    repair_status=RepairStatus.NOT_NEEDED,
                    details={
                        "industry": industry,
                        "industry_exposure": industry_exposure,
                        "industry_max_bound": float(bounds.max),
                        "reason_label": "locked_industry_above_max",
                    },
                )
            )
        if bounds.min is not None and industry_exposure + 1e-9 < bounds.min:
            findings.append(
                build_finding(
                    "locked_industry_below_min",
                    FindingCategory.RISK,
                    FindingSeverity.INFO,
                    "Industry is fully locked from trading and already below min bound; solver will preserve the current exposure floor for this industry.",
                    rule_source="optimizer.precheck.locked_industry",
                    blocking=False,
                    repair_status=RepairStatus.NOT_NEEDED,
                    details={
                        "industry": industry,
                        "industry_exposure": industry_exposure,
                        "industry_min_bound": float(bounds.min),
                        "reason_label": "locked_industry_below_min",
                    },
                )
            )

    return findings
