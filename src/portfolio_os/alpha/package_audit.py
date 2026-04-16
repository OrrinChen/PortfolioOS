"""Audit helpers for real alpha package coverage, mapping, and thickness."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from portfolio_os.alpha.bridge_semantics import AlphaNegativeSpreadProtocol, resolve_negative_spread_protocol


@dataclass(frozen=True)
class RealAlphaPackageAudit:
    """Structured real-alpha package audit outputs."""

    coverage_frame: pd.DataFrame
    mapping_frame: pd.DataFrame
    thickness_frame: pd.DataFrame
    summary_payload: dict[str, Any]
    report_markdown: str


def _safe_correlation(left: pd.Series, right: pd.Series, method: str) -> float:
    """Return one correlation value while handling small or constant samples."""

    clean = pd.DataFrame({"left": left, "right": right}).dropna()
    if len(clean) < 2:
        return 0.0
    if clean["left"].nunique() < 2 or clean["right"].nunique() < 2:
        return 0.0
    value = clean["left"].corr(clean["right"], method=method)
    if pd.isna(value):
        return 0.0
    return float(value)


def _forward_returns_panel(returns_panel: pd.DataFrame, horizon_days: int) -> pd.DataFrame:
    """Compound future returns over one forward horizon using next-trading-day entry."""

    if horizon_days <= 0:
        raise ValueError("horizon_days must be positive.")
    shifted_components = [(1.0 + returns_panel.shift(-step)) for step in range(1, int(horizon_days) + 1)]
    compounded = shifted_components[0].copy()
    for component in shifted_components[1:]:
        compounded = compounded * component
    return compounded - 1.0


def _fmt_pct(value: float) -> str:
    return f"{float(value) * 100.0:.2f}%"


def _fmt_float(value: float) -> str:
    return f"{float(value):.4f}"


def _first_optional_float(frame: pd.DataFrame, column: str) -> float:
    """Return one optional float column value, or NaN when absent."""

    if column not in frame.columns or frame.empty:
        return float("nan")
    return float(frame[column].iloc[0])


def _first_optional_str(frame: pd.DataFrame, column: str, default: str = "") -> str:
    """Return one optional string column value, or a default when absent."""

    if column not in frame.columns or frame.empty:
        return str(default)
    return str(frame[column].iloc[0])


def _derive_alpha_state(
    *,
    date_frame: pd.DataFrame,
    nonzero_count: int,
) -> str:
    """Classify one rebalance date into one mutually exclusive terminal state."""

    if nonzero_count > 0:
        return "active_nonzero"

    status = _first_optional_str(date_frame, "alpha_protocol_status", default="")
    if status == "insufficient_history":
        return "insufficient_history"
    if status == "spread_floor_to_zero":
        return "spread_floor_to_zero"
    if status == "explicit_abstain":
        return "explicit_abstain"

    confidence = _first_optional_float(date_frame, "signal_strength_confidence")
    raw_spread = _first_optional_float(date_frame, "raw_mean_top_bottom_spread")
    annualized_spread = _first_optional_float(date_frame, "annualized_top_bottom_spread")
    period_spread = _first_optional_float(date_frame, "period_top_bottom_spread")

    if pd.notna(confidence) and abs(float(confidence)) <= 1e-15:
        return "insufficient_history"
    if (
        pd.notna(raw_spread)
        and float(raw_spread) < 0.0
        and pd.notna(annualized_spread)
        and abs(float(annualized_spread)) <= 1e-15
        and pd.notna(period_spread)
        and abs(float(period_spread)) <= 1e-15
    ):
        return "spread_floor_to_zero"
    if (
        pd.notna(annualized_spread)
        and abs(float(annualized_spread)) <= 1e-15
        and pd.notna(period_spread)
        and abs(float(period_spread)) <= 1e-15
    ):
        return "ready_zero_expected_return"
    return "ready_zero_expected_return"


def _build_counterfactual_alpha_panel(
    *,
    alpha_panel: pd.DataFrame,
    negative_spread_mode: AlphaNegativeSpreadProtocol | None,
    forward_horizon_days: int,
    max_abs_expected_return: float,
) -> tuple[pd.DataFrame, set[str]]:
    """Optionally rebuild spread-floor months under one diagnostic-only protocol."""

    work = alpha_panel.copy()
    if negative_spread_mode is None or work.empty:
        return work, set()

    promoted_dates: set[str] = set()
    work["date"] = pd.to_datetime(work["date"]).dt.normalize()
    required = {"alpha_zscore", "quantile", "signal_strength_confidence", "raw_mean_top_bottom_spread", "decision_horizon_days"}
    if not required.issubset(work.columns):
        return work, promoted_dates

    for date_value, date_frame in work.groupby("date", sort=True):
        nonzero_count = int((date_frame["expected_return"].astype(float).abs() > 1e-15).sum())
        if _derive_alpha_state(date_frame=date_frame, nonzero_count=nonzero_count) != "spread_floor_to_zero":
            continue
        raw_spread = _first_optional_float(date_frame, "raw_mean_top_bottom_spread")
        confidence = _first_optional_float(date_frame, "signal_strength_confidence")
        decision_horizon_days = int(_first_optional_float(date_frame, "decision_horizon_days"))
        if not np.isfinite(raw_spread) or not np.isfinite(confidence) or decision_horizon_days <= 0:
            continue
        protocol_decision = resolve_negative_spread_protocol(
            raw_spread,
            forward_horizon_days=int(forward_horizon_days),
            decision_horizon_days=decision_horizon_days,
            protocol=negative_spread_mode,
        )
        if protocol_decision.should_abstain:
            work.loc[date_frame.index, "expected_return"] = 0.0
            work.loc[date_frame.index, "annualized_top_bottom_spread"] = np.nan
            work.loc[date_frame.index, "period_top_bottom_spread"] = np.nan
            work.loc[date_frame.index, "negative_spread_protocol"] = str(negative_spread_mode)
            work.loc[date_frame.index, "alpha_protocol_status"] = str(protocol_decision.status)
            continue

        top_quantile = int(date_frame["quantile"].max())
        bottom_quantile = int(date_frame["quantile"].min())
        top_mean = float(date_frame.loc[date_frame["quantile"] == top_quantile, "alpha_zscore"].astype(float).mean())
        bottom_mean = float(date_frame.loc[date_frame["quantile"] == bottom_quantile, "alpha_zscore"].astype(float).mean())
        z_gap = max(top_mean - bottom_mean, 1e-6)
        expected = (
            float(confidence)
            * float(protocol_decision.period_top_bottom_spread or 0.0)
            * date_frame["alpha_zscore"].astype(float)
            / z_gap
        ).clip(lower=-float(max_abs_expected_return), upper=float(max_abs_expected_return))
        work.loc[date_frame.index, "expected_return"] = expected.astype(float).to_numpy()
        work.loc[date_frame.index, "annualized_top_bottom_spread"] = float(protocol_decision.annualized_top_bottom_spread or 0.0)
        work.loc[date_frame.index, "period_top_bottom_spread"] = float(protocol_decision.period_top_bottom_spread or 0.0)
        work.loc[date_frame.index, "negative_spread_protocol"] = str(negative_spread_mode)
        work.loc[date_frame.index, "alpha_protocol_status"] = str(protocol_decision.status)
        if bool((expected.abs() > 1e-15).any()):
            promoted_dates.add(pd.Timestamp(date_value).strftime("%Y-%m-%d"))
    return work, promoted_dates


def _build_coverage_frame(
    *,
    rebalance_schedule: list[str] | list[pd.Timestamp],
    alpha_panel: pd.DataFrame,
    counterfactual_promoted_dates: set[str] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    promoted_dates = counterfactual_promoted_dates or set()
    if alpha_panel.empty:
        alpha_panel = pd.DataFrame(columns=["date", "expected_return"])
    work = alpha_panel.copy()
    if not work.empty:
        work["date"] = pd.to_datetime(work["date"]).dt.strftime("%Y-%m-%d")

    for date_value in [pd.Timestamp(item).strftime("%Y-%m-%d") for item in rebalance_schedule]:
        date_frame = work.loc[work["date"] == date_value].copy()
        if date_frame.empty:
            rows.append(
                {
                    "date": date_value,
                    "alpha_ready": False,
                    "alpha_active": False,
                    "alpha_state": "cold_start",
                    "ticker_count": 0,
                    "nonzero_expected_return_count": 0,
                    "nonzero_expected_return_ratio": 0.0,
                    "mean_abs_expected_return": 0.0,
                    "max_abs_expected_return": 0.0,
                    "signal_strength_confidence": np.nan,
                    "raw_mean_top_bottom_spread": np.nan,
                    "annualized_top_bottom_spread": np.nan,
                    "period_top_bottom_spread": np.nan,
                    "negative_spread_protocol": "",
                    "alpha_protocol_status": "cold_start",
                }
            )
            continue

        expected = date_frame["expected_return"].astype(float)
        nonzero_mask = expected.abs() > 1e-15
        nonzero_count = int(nonzero_mask.sum())
        alpha_state = _derive_alpha_state(date_frame=date_frame, nonzero_count=nonzero_count)
        rows.append(
            {
                "date": date_value,
                "alpha_ready": True,
                "alpha_active": bool(nonzero_count > 0),
                "alpha_state": alpha_state,
                "ticker_count": int(len(date_frame)),
                "nonzero_expected_return_count": nonzero_count,
                "nonzero_expected_return_ratio": float(nonzero_count / float(len(date_frame))) if len(date_frame) else 0.0,
                "mean_abs_expected_return": float(expected.abs().mean()) if len(date_frame) else 0.0,
                "max_abs_expected_return": float(expected.abs().max()) if len(date_frame) else 0.0,
                "signal_strength_confidence": _first_optional_float(date_frame, "signal_strength_confidence"),
                "raw_mean_top_bottom_spread": _first_optional_float(date_frame, "raw_mean_top_bottom_spread"),
                "annualized_top_bottom_spread": _first_optional_float(date_frame, "annualized_top_bottom_spread"),
                "period_top_bottom_spread": _first_optional_float(date_frame, "period_top_bottom_spread"),
                "negative_spread_protocol": _first_optional_str(date_frame, "negative_spread_protocol"),
                "alpha_protocol_status": _first_optional_str(
                    date_frame,
                    "alpha_protocol_status",
                    default=("active_nonzero" if nonzero_count > 0 else alpha_state),
                ),
                "counterfactual_promoted": bool(date_value in promoted_dates),
            }
        )
    return pd.DataFrame(rows)


def _build_mapping_frame(
    *,
    alpha_panel: pd.DataFrame,
    returns_panel: pd.DataFrame,
) -> pd.DataFrame:
    if alpha_panel.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "ticker_count",
                "decision_horizon_days",
                "nonzero_expected_return_count",
                "mean_abs_expected_return",
                "max_abs_expected_return",
                "rank_ic",
                "ic",
                "realized_top_bottom_spread",
                "bridge_period_top_bottom_spread",
                "spread_sign_match",
                "nonzero_sign_accuracy",
            ]
        )

    panel = alpha_panel.copy()
    panel["date"] = pd.to_datetime(panel["date"]).dt.normalize()
    panel["ticker"] = panel["ticker"].astype(str)
    returns_panel = returns_panel.copy().sort_index()
    returns_panel.index = pd.to_datetime(returns_panel.index).normalize()
    rows: list[dict[str, Any]] = []

    for date_value, date_frame in panel.groupby("date", sort=True):
        expected = date_frame["expected_return"].astype(float)
        nonzero_mask = expected.abs() > 1e-15
        if not bool(nonzero_mask.any()):
            continue
        horizon_days = int(date_frame["decision_horizon_days"].iloc[0])
        forward_returns = _forward_returns_panel(returns_panel, horizon_days)
        if date_value not in forward_returns.index:
            continue
        realized_row = forward_returns.loc[date_value].rename("realized_forward_return").reset_index()
        realized_row.columns = ["ticker", "realized_forward_return"]
        merged = date_frame.merge(realized_row, on="ticker", how="left").dropna(
            subset=["expected_return", "realized_forward_return"]
        )
        if merged.empty:
            continue
        top_quantile = int(merged["quantile"].max())
        bottom_quantile = int(merged["quantile"].min())
        realized_top_bottom_spread = float(
            merged.loc[merged["quantile"] == top_quantile, "realized_forward_return"].mean()
            - merged.loc[merged["quantile"] == bottom_quantile, "realized_forward_return"].mean()
        )
        sign_mask = (
            (merged["expected_return"].astype(float).abs() > 1e-15)
            & (merged["realized_forward_return"].astype(float).abs() > 1e-15)
        )
        sign_accuracy = np.nan
        if bool(sign_mask.any()):
            sign_accuracy = float(
                (
                    np.sign(merged.loc[sign_mask, "expected_return"].astype(float))
                    == np.sign(merged.loc[sign_mask, "realized_forward_return"].astype(float))
                ).mean()
            )
        bridge_spread = float(merged["period_top_bottom_spread"].iloc[0])
        rows.append(
            {
                "date": pd.Timestamp(date_value).strftime("%Y-%m-%d"),
                "ticker_count": int(len(merged)),
                "decision_horizon_days": horizon_days,
                "nonzero_expected_return_count": int(nonzero_mask.sum()),
                "mean_abs_expected_return": float(merged["expected_return"].astype(float).abs().mean()),
                "max_abs_expected_return": float(merged["expected_return"].astype(float).abs().max()),
                "rank_ic": _safe_correlation(
                    merged["expected_return"].astype(float),
                    merged["realized_forward_return"].astype(float),
                    method="spearman",
                ),
                "ic": _safe_correlation(
                    merged["expected_return"].astype(float),
                    merged["realized_forward_return"].astype(float),
                    method="pearson",
                ),
                "realized_top_bottom_spread": realized_top_bottom_spread,
                "bridge_period_top_bottom_spread": bridge_spread,
                "spread_sign_match": bool(np.sign(bridge_spread) == np.sign(realized_top_bottom_spread)),
                "nonzero_sign_accuracy": sign_accuracy,
            }
        )
    return pd.DataFrame(rows)


def _build_thickness_frame(
    *,
    period_attribution: pd.DataFrame,
    active_dates: set[str],
) -> pd.DataFrame:
    optimizer_rows = period_attribution.loc[period_attribution["strategy"] == "optimizer"].copy()
    if optimizer_rows.empty:
        return pd.DataFrame(
            columns=[
                "period_index",
                "start_date",
                "end_date",
                "gross_traded_notional",
                "turnover",
                "active_trading_pnl",
                "trading_cost_pnl",
                "net_active_pnl",
                "gross_to_net_retention",
                "optimizer_vs_naive_period_pnl_delta",
            ]
        )
    optimizer_rows["start_date"] = pd.to_datetime(optimizer_rows["start_date"]).dt.strftime("%Y-%m-%d")
    optimizer_rows = optimizer_rows.loc[optimizer_rows["start_date"].isin(active_dates)].copy()
    if optimizer_rows.empty:
        return pd.DataFrame(
            columns=[
                "period_index",
                "start_date",
                "end_date",
                "gross_traded_notional",
                "turnover",
                "active_trading_pnl",
                "trading_cost_pnl",
                "net_active_pnl",
                "gross_to_net_retention",
                "optimizer_vs_naive_period_pnl_delta",
            ]
        )
    optimizer_rows["net_active_pnl"] = (
        optimizer_rows["active_trading_pnl"].astype(float) + optimizer_rows["trading_cost_pnl"].astype(float)
    )
    optimizer_rows["gross_to_net_retention"] = np.where(
        optimizer_rows["active_trading_pnl"].astype(float) > 0.0,
        optimizer_rows["net_active_pnl"].astype(float) / optimizer_rows["active_trading_pnl"].astype(float),
        np.nan,
    )
    columns = [
        "period_index",
        "start_date",
        "end_date",
        "gross_traded_notional",
        "turnover",
        "active_trading_pnl",
        "trading_cost_pnl",
        "net_active_pnl",
        "gross_to_net_retention",
        "optimizer_vs_naive_period_pnl_delta",
    ]
    return optimizer_rows.reindex(columns=columns).reset_index(drop=True)


def _build_summary_payload(
    *,
    coverage_frame: pd.DataFrame,
    mapping_frame: pd.DataFrame,
    thickness_frame: pd.DataFrame,
) -> dict[str, Any]:
    ready_mask = coverage_frame["alpha_ready"].astype(bool)
    active_mask = coverage_frame["alpha_active"].astype(bool)
    state_counts = coverage_frame["alpha_state"].astype(str).value_counts()
    active_period_count = int(len(mapping_frame))
    gross_active = float(thickness_frame["active_trading_pnl"].sum()) if not thickness_frame.empty else 0.0
    net_active = float(thickness_frame["net_active_pnl"].sum()) if not thickness_frame.empty else 0.0
    return {
        "coverage": {
            "rebalance_count": int(len(coverage_frame)),
            "alpha_ready_count": int(ready_mask.sum()),
            "alpha_active_count": int(active_mask.sum()),
            "guard_zero_count": int(
                state_counts.get("spread_floor_to_zero", 0)
                + state_counts.get("explicit_abstain", 0)
                + state_counts.get("ready_zero_expected_return", 0)
            ),
            "cold_start_count": int(state_counts.get("cold_start", 0)),
            "insufficient_history_count": int(state_counts.get("insufficient_history", 0)),
            "spread_floor_to_zero_count": int(state_counts.get("spread_floor_to_zero", 0)),
            "explicit_abstain_count": int(state_counts.get("explicit_abstain", 0)),
            "ready_zero_expected_return_count": int(state_counts.get("ready_zero_expected_return", 0)),
            "counterfactual_promoted_count": int(coverage_frame.get("counterfactual_promoted", pd.Series(dtype=bool)).sum()),
            "alpha_ready_ratio": float(ready_mask.mean()) if len(coverage_frame) else 0.0,
            "alpha_active_ratio": float(active_mask.mean()) if len(coverage_frame) else 0.0,
            "mean_confidence_when_ready": float(
                coverage_frame.loc[ready_mask, "signal_strength_confidence"].astype(float).mean()
            )
            if bool(ready_mask.any())
            else 0.0,
        },
        "mapping": {
            "active_period_count": active_period_count,
            "mean_rank_ic": float(mapping_frame["rank_ic"].astype(float).mean()) if not mapping_frame.empty else 0.0,
            "positive_rank_ic_ratio": float((mapping_frame["rank_ic"].astype(float) > 0.0).mean())
            if not mapping_frame.empty
            else 0.0,
            "mean_realized_top_bottom_spread": float(mapping_frame["realized_top_bottom_spread"].astype(float).mean())
            if not mapping_frame.empty
            else 0.0,
            "spread_sign_match_ratio": float(mapping_frame["spread_sign_match"].astype(bool).mean())
            if not mapping_frame.empty
            else 0.0,
            "mean_nonzero_sign_accuracy": float(mapping_frame["nonzero_sign_accuracy"].dropna().astype(float).mean())
            if not mapping_frame.empty and mapping_frame["nonzero_sign_accuracy"].dropna().any()
            else 0.0,
        },
        "thickness": {
            "active_period_count": int(len(thickness_frame)),
            "gross_active_trading_pnl": gross_active,
            "trading_cost_pnl": float(thickness_frame["trading_cost_pnl"].sum()) if not thickness_frame.empty else 0.0,
            "net_active_pnl": net_active,
            "gross_to_net_retention": float(net_active / gross_active) if gross_active > 0.0 else 0.0,
            "positive_net_active_period_ratio": float((thickness_frame["net_active_pnl"].astype(float) > 0.0).mean())
            if not thickness_frame.empty
            else 0.0,
            "mean_turnover": float(thickness_frame["turnover"].astype(float).mean()) if not thickness_frame.empty else 0.0,
            "total_optimizer_vs_naive_period_pnl_delta": float(
                thickness_frame["optimizer_vs_naive_period_pnl_delta"].astype(float).sum()
            )
            if not thickness_frame.empty and "optimizer_vs_naive_period_pnl_delta" in thickness_frame.columns
            else 0.0,
        },
    }


def _render_report(
    *,
    coverage_frame: pd.DataFrame,
    mapping_frame: pd.DataFrame,
    thickness_frame: pd.DataFrame,
    summary_payload: dict[str, Any],
) -> str:
    coverage = summary_payload["coverage"]
    mapping = summary_payload["mapping"]
    thickness = summary_payload["thickness"]
    lines = [
        "# Real Alpha Package Audit",
        "",
        "## Coverage",
        f"- Rebalances: {coverage['rebalance_count']}",
        f"- Alpha-ready: {coverage['alpha_ready_count']} ({_fmt_pct(coverage['alpha_ready_ratio'])})",
        f"- Alpha-active: {coverage['alpha_active_count']} ({_fmt_pct(coverage['alpha_active_ratio'])})",
        f"- Guard-zero: {coverage['guard_zero_count']}",
        f"- Cold-start: {coverage['cold_start_count']}",
        f"- Insufficient-history: {coverage['insufficient_history_count']}",
        f"- Spread-floor-to-zero: {coverage['spread_floor_to_zero_count']}",
        "",
        "## Mapping",
        f"- Active periods: {mapping['active_period_count']}",
        f"- Mean rank IC: {_fmt_float(mapping['mean_rank_ic'])}",
        f"- Positive rank IC ratio: {_fmt_pct(mapping['positive_rank_ic_ratio'])}",
        f"- Mean realized top-bottom spread: {_fmt_pct(mapping['mean_realized_top_bottom_spread'])}",
        "",
        "## Thickness",
        f"- Gross active trading PnL: {_fmt_float(thickness['gross_active_trading_pnl'])}",
        f"- Trading cost PnL: {_fmt_float(thickness['trading_cost_pnl'])}",
        f"- Net active PnL: {_fmt_float(thickness['net_active_pnl'])}",
        f"- Gross-to-net retention: {_fmt_pct(thickness['gross_to_net_retention'])}",
        "",
        "### Coverage By Rebalance",
        "",
        "| Date | State | Ready | Active | Nonzero Count | Max | Confidence | Status |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in coverage_frame.to_dict(orient="records"):
        lines.append(
            f"| {row['date']} | {row['alpha_state']} | {str(bool(row['alpha_ready'])).lower()} | "
            f"{str(bool(row['alpha_active'])).lower()} | {int(row['nonzero_expected_return_count'])} | "
            f"{_fmt_float(float(row['max_abs_expected_return']))} | "
            f"{_fmt_float(float(row['signal_strength_confidence'])) if pd.notna(row['signal_strength_confidence']) else 'n/a'} | "
            f"{row['alpha_protocol_status']} |"
        )
    if not mapping_frame.empty:
        lines.extend(
            [
                "",
                "### Mapping By Active Rebalance",
                "",
                "| Date | Rank IC | Realized Top-Bottom | Sign Match | Sign Accuracy |",
                "| --- | ---: | ---: | --- | ---: |",
            ]
        )
        for row in mapping_frame.to_dict(orient="records"):
            sign_accuracy = row["nonzero_sign_accuracy"]
            lines.append(
                f"| {row['date']} | {_fmt_float(float(row['rank_ic']))} | "
                f"{_fmt_pct(float(row['realized_top_bottom_spread']))} | {str(bool(row['spread_sign_match'])).lower()} | "
                f"{_fmt_pct(float(sign_accuracy)) if pd.notna(sign_accuracy) else 'n/a'} |"
            )
    if not thickness_frame.empty:
        lines.extend(
            [
                "",
                "### Thickness By Active Period",
                "",
                "| Start | Net Active PnL | Turnover | Retention | Vs Naive Delta |",
                "| --- | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in thickness_frame.to_dict(orient="records"):
            retention = row["gross_to_net_retention"]
            lines.append(
                f"| {row['start_date']} | {_fmt_float(float(row['net_active_pnl']))} | "
                f"{_fmt_float(float(row['turnover']))} | "
                f"{_fmt_pct(float(retention)) if pd.notna(retention) else 'n/a'} | "
                f"{_fmt_float(float(row['optimizer_vs_naive_period_pnl_delta']))} |"
            )
    lines.append("")
    return "\n".join(lines)


def build_real_alpha_package_audit(
    *,
    rebalance_schedule: list[str] | list[pd.Timestamp],
    alpha_panel: pd.DataFrame,
    period_attribution: pd.DataFrame,
    returns_panel: pd.DataFrame,
    counterfactual_negative_spread_mode: AlphaNegativeSpreadProtocol | None = None,
    counterfactual_forward_horizon_days: int = 5,
    counterfactual_max_abs_expected_return: float = 0.30,
) -> RealAlphaPackageAudit:
    """Build a compact audit of real alpha package coverage, mapping, and thickness."""

    audit_panel, promoted_dates = _build_counterfactual_alpha_panel(
        alpha_panel=alpha_panel,
        negative_spread_mode=counterfactual_negative_spread_mode,
        forward_horizon_days=counterfactual_forward_horizon_days,
        max_abs_expected_return=counterfactual_max_abs_expected_return,
    )
    coverage_frame = _build_coverage_frame(
        rebalance_schedule=rebalance_schedule,
        alpha_panel=audit_panel,
        counterfactual_promoted_dates=promoted_dates,
    )
    mapping_frame = _build_mapping_frame(
        alpha_panel=audit_panel,
        returns_panel=returns_panel,
    )
    active_dates = set(
        coverage_frame.loc[coverage_frame["alpha_state"] == "active_nonzero", "date"].astype(str).tolist()
    )
    thickness_frame = _build_thickness_frame(
        period_attribution=period_attribution,
        active_dates=active_dates,
    )
    summary_payload = _build_summary_payload(
        coverage_frame=coverage_frame,
        mapping_frame=mapping_frame,
        thickness_frame=thickness_frame,
    )
    report_markdown = _render_report(
        coverage_frame=coverage_frame,
        mapping_frame=mapping_frame,
        thickness_frame=thickness_frame,
        summary_payload=summary_payload,
    )
    return RealAlphaPackageAudit(
        coverage_frame=coverage_frame,
        mapping_frame=mapping_frame,
        thickness_frame=thickness_frame,
        summary_payload=summary_payload,
        report_markdown=report_markdown,
    )
