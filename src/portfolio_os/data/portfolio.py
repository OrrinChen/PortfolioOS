"""Holdings, target, and account-state loaders."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from portfolio_os.data.import_profiles import ImportProfile
from portfolio_os.data.loaders import (
    ensure_columns,
    ensure_non_negative,
    ensure_unique_tickers,
    normalize_ticker,
    read_input_frame,
    read_yaml,
)
from portfolio_os.compliance.findings import build_finding
from portfolio_os.domain.enums import FindingCategory, FindingSeverity, RepairStatus
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.domain.models import ComplianceFinding, Holding, PortfolioState, TargetWeight
from portfolio_os.utils.config import AppConfig


def load_holdings(path: str | Path, *, import_profile: ImportProfile | None = None) -> list[Holding]:
    """Load current holdings from CSV."""

    frame = read_input_frame(path, input_type="holdings", import_profile=import_profile)
    ensure_columns(frame, ["ticker", "quantity"], str(path))
    frame["ticker"] = frame["ticker"].map(normalize_ticker)
    ensure_unique_tickers(frame, str(path))
    ensure_non_negative(frame, "quantity", str(path))
    holdings: list[Holding] = []
    for row in frame.to_dict(orient="records"):
        avg_cost = row.get("avg_cost")
        holdings.append(
            Holding(
                ticker=row["ticker"],
                quantity=int(row["quantity"]),
                avg_cost=float(avg_cost) if pd.notna(avg_cost) else None,
            )
        )
    return holdings


def load_target_weights(path: str | Path, *, import_profile: ImportProfile | None = None) -> list[TargetWeight]:
    """Load target weights from CSV."""

    frame = read_input_frame(path, input_type="target", import_profile=import_profile)
    ensure_columns(frame, ["ticker", "target_weight"], str(path))
    frame["ticker"] = frame["ticker"].map(normalize_ticker)
    ensure_unique_tickers(frame, str(path))
    if frame["target_weight"].isna().any():
        raise InputValidationError("target.csv contains missing target_weight values.")
    if ((frame["target_weight"] < 0) | (frame["target_weight"] > 1)).any():
        raise InputValidationError("Target weights must be between 0 and 1.")
    total_target_weight = float(frame["target_weight"].sum())
    if total_target_weight > 1.0 + 1e-9:
        raise InputValidationError(
            f"Target weights sum to {total_target_weight:.4f}, which is above 1.0."
        )
    return [
        TargetWeight(ticker=row["ticker"], target_weight=float(row["target_weight"]))
        for row in frame.to_dict(orient="records")
    ]


def load_portfolio_state(path: str | Path) -> PortfolioState:
    """Load account-level state from YAML."""

    payload = read_yaml(path)
    state = PortfolioState.model_validate(payload)
    if state.available_cash < 0:
        raise InputValidationError("available_cash cannot be negative.")
    if state.min_cash_buffer < 0:
        raise InputValidationError("min_cash_buffer cannot be negative.")
    return state


def build_portfolio_frame(holdings: list[Holding], targets: list[TargetWeight]) -> pd.DataFrame:
    """Outer-join holdings and targets on ticker."""

    holdings_frame = pd.DataFrame([item.model_dump(mode="json") for item in holdings])
    targets_frame = pd.DataFrame([item.model_dump(mode="json") for item in targets])
    if holdings_frame.empty:
        holdings_frame = pd.DataFrame(columns=["ticker", "quantity", "avg_cost"])
    if targets_frame.empty:
        targets_frame = pd.DataFrame(columns=["ticker", "target_weight"])
    merged = holdings_frame.merge(targets_frame, on="ticker", how="outer")
    merged["quantity"] = merged["quantity"].fillna(0).astype(int)
    merged["target_weight"] = merged["target_weight"].fillna(0.0).astype(float)
    if "avg_cost" not in merged.columns:
        merged["avg_cost"] = None
    return merged.sort_values("ticker").reset_index(drop=True)


def build_portfolio_data_quality_findings(
    portfolio_frame: pd.DataFrame,
    config: AppConfig,
) -> list[ComplianceFinding]:
    """Return non-blocking portfolio-input quality findings."""

    findings: list[ComplianceFinding] = []
    target_weight_sum = float(portfolio_frame["target_weight"].sum())
    if 0.0 <= target_weight_sum < 0.05:
        findings.append(
            build_finding(
                "target_weight_sum_near_zero",
                FindingCategory.DATA_QUALITY,
                FindingSeverity.WARNING,
                "Target weight sum is very small, so the rebalance may not express a meaningful target portfolio.",
                rule_source="target.csv",
                blocking=False,
                repair_status=RepairStatus.NOT_NEEDED,
                details={"target_weight_sum": target_weight_sum},
            )
        )
    max_target_weight = float(portfolio_frame["target_weight"].max()) if not portfolio_frame.empty else 0.0
    concentration_threshold = max(0.50, config.effective_single_name_limit * 2.0)
    if max_target_weight > concentration_threshold:
        ticker = str(portfolio_frame.loc[portfolio_frame["target_weight"].idxmax(), "ticker"])
        findings.append(
            build_finding(
                "target_weight_extreme_concentration",
                FindingCategory.DATA_QUALITY,
                FindingSeverity.WARNING,
                "Target file contains an extremely concentrated weight that is unlikely to be executable under the current mandate.",
                ticker=ticker,
                rule_source="target.csv",
                blocking=False,
                repair_status=RepairStatus.NOT_NEEDED,
                details={
                    "max_target_weight": max_target_weight,
                    "concentration_threshold": concentration_threshold,
                },
            )
        )
    return findings
