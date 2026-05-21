"""FD-S1 small-cap universe tiering."""

from __future__ import annotations

import numpy as np
import pandas as pd


def build_small_cap_universe_tiers(
    prices: pd.DataFrame,
    universe: pd.DataFrame,
    signal_dates: list[pd.Timestamp] | None = None,
    min_price: float = 5.0,
    min_adv: float = 0.0,
) -> pd.DataFrame:
    """Split an investable PIT universe into control, small-cap, and microcap tiers."""

    price_frame = _normalize_prices(prices)
    universe_frame = _normalize_universe(universe)
    if price_frame.empty or universe_frame.empty:
        return _empty_tier_frame()

    dates = signal_dates or _month_end_signal_dates(price_frame["date"])
    adv = _adv_panel(price_frame)
    rows: list[dict[str, object]] = []
    for signal_date in dates:
        date = pd.Timestamp(signal_date)
        point = price_frame[price_frame["date"] == date].copy()
        if point.empty:
            continue
        active = _active_members(universe_frame, date)
        if active.empty:
            continue
        point = point.merge(active, on="asset_id", how="inner", suffixes=("", "_universe"))
        if point.empty:
            continue
        point["market_cap"] = _market_cap(point)
        point["adv_3m"] = point["asset_id"].map(adv.get(date, {})).astype(float)
        point["price"] = pd.to_numeric(point["adjusted_close"], errors="coerce")
        point = point.dropna(subset=["market_cap", "adv_3m", "price"])
        if point.empty:
            continue
        point["mcap_percentile"] = point["market_cap"].rank(pct=True, method="first")
        for row in point.itertuples(index=False):
            tier = _tier_from_percentile(float(row.mcap_percentile))
            price_pass = float(row.price) >= float(min_price)
            adv_pass = float(row.adv_3m) > float(min_adv)
            share_pass = _share_class_pass(row)
            if tier == "microcap_quarantine":
                diagnostic_only = True
                candidate_allowed = False
                reason = "microcap_quarantine"
            elif tier == "small_cap_investable":
                diagnostic_only = False
                candidate_allowed = bool(price_pass and adv_pass and share_pass)
                reason = "investable" if candidate_allowed else "failed_investability_filter"
            else:
                diagnostic_only = True
                candidate_allowed = False
                reason = "large_cap_control"
            rows.append(
                {
                    "schema_version": "fd_small_cap_universe_tiering.v1",
                    "rebalance_date": date.date().isoformat(),
                    "asset_id": str(row.asset_id),
                    "ticker": str(getattr(row, "ticker", "") or ""),
                    "sector": str(getattr(row, "sector", getattr(row, "sector_universe", "")) or ""),
                    "industry": str(getattr(row, "industry", getattr(row, "industry_universe", "")) or ""),
                    "market_cap": float(row.market_cap),
                    "log_market_cap": float(np.log(max(float(row.market_cap), 1e-12))),
                    "adv_3m": float(row.adv_3m),
                    "log_adv_3m": float(np.log(max(float(row.adv_3m), 1e-12))),
                    "price": float(row.price),
                    "mcap_percentile": float(row.mcap_percentile),
                    "universe_tier": tier,
                    "diagnostic_only": diagnostic_only,
                    "candidate_decision_allowed": candidate_allowed,
                    "price_filter_pass": bool(price_pass),
                    "adv_filter_pass": bool(adv_pass),
                    "share_class_filter_pass": bool(share_pass),
                    "reason": reason,
                    "microcap_quarantine_allowed_use": "diagnostic_only" if tier == "microcap_quarantine" else "",
                    "not_alpha_evidence": True,
                    "direct_q2_entry_allowed": False,
                }
            )
    return pd.DataFrame(rows, columns=_tier_columns())


def _normalize_prices(prices: pd.DataFrame) -> pd.DataFrame:
    frame = prices.copy()
    if frame.empty:
        return frame
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    if "asset_id" not in frame.columns and "permno" in frame.columns:
        frame["asset_id"] = frame["permno"].astype(str)
    frame["asset_id"] = frame["asset_id"].astype(str)
    for column in ["adjusted_close", "volume", "market_cap", "shares_outstanding", "shares_float"]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _normalize_universe(universe: pd.DataFrame) -> pd.DataFrame:
    frame = universe.copy()
    if frame.empty:
        return frame
    if "asset_id" not in frame.columns and "permno" in frame.columns:
        frame["asset_id"] = frame["permno"].astype(str)
    frame["asset_id"] = frame["asset_id"].astype(str)
    for column in ["membership_start", "membership_end", "date"]:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    if "membership_start" not in frame.columns:
        frame["membership_start"] = pd.Timestamp.min
    if "membership_end" not in frame.columns:
        frame["membership_end"] = pd.Timestamp.max
    return frame


def _month_end_signal_dates(dates: pd.Series) -> list[pd.Timestamp]:
    series = pd.to_datetime(dates, errors="coerce").dropna().sort_values()
    if series.empty:
        return []
    return [pd.Timestamp(value) for value in series.groupby(series.dt.to_period("M")).max().tolist()]


def _active_members(universe: pd.DataFrame, date: pd.Timestamp) -> pd.DataFrame:
    start = pd.to_datetime(universe["membership_start"], errors="coerce").fillna(pd.Timestamp.min)
    end = pd.to_datetime(universe["membership_end"], errors="coerce").fillna(pd.Timestamp.max)
    active = universe[(start <= date) & (end >= date)].copy()
    return active.drop_duplicates("asset_id")


def _adv_panel(prices: pd.DataFrame) -> dict[pd.Timestamp, dict[str, float]]:
    frame = prices.sort_values(["asset_id", "date"]).copy()
    frame["dollar_volume"] = frame["adjusted_close"] * frame["volume"]
    frame["adv_3m"] = frame.groupby("asset_id")["dollar_volume"].transform(lambda s: s.rolling(63, min_periods=20).mean())
    point = frame[["date", "asset_id", "adv_3m"]].dropna()
    return {
        pd.Timestamp(date): dict(zip(group["asset_id"].astype(str), group["adv_3m"].astype(float), strict=False))
        for date, group in point.groupby("date")
    }


def _market_cap(frame: pd.DataFrame) -> pd.Series:
    if "market_cap" in frame.columns:
        market_cap = pd.to_numeric(frame["market_cap"], errors="coerce")
        if market_cap.notna().any():
            return market_cap
    share_column = "shares_float" if "shares_float" in frame.columns else "shares_outstanding"
    if share_column in frame.columns:
        return pd.to_numeric(frame["adjusted_close"], errors="coerce") * pd.to_numeric(frame[share_column], errors="coerce")
    return pd.Series(np.nan, index=frame.index)


def _tier_from_percentile(percentile: float) -> str:
    if percentile <= 0.20:
        return "microcap_quarantine"
    if percentile <= 0.60:
        return "small_cap_investable"
    return "large_cap_control"


def _share_class_pass(row: object) -> bool:
    common_share = getattr(row, "common_share", None)
    if common_share is not None and str(common_share).lower() in {"true", "1", "yes"}:
        return True
    share_code = getattr(row, "share_code", None)
    if share_code is not None and str(share_code) in {"10", "11", "10.0", "11.0"}:
        return True
    return common_share is None and share_code is None


def _empty_tier_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=_tier_columns())


def _tier_columns() -> list[str]:
    return [
        "schema_version",
        "rebalance_date",
        "asset_id",
        "ticker",
        "sector",
        "industry",
        "market_cap",
        "log_market_cap",
        "adv_3m",
        "log_adv_3m",
        "price",
        "mcap_percentile",
        "universe_tier",
        "diagnostic_only",
        "candidate_decision_allowed",
        "price_filter_pass",
        "adv_filter_pass",
        "share_class_filter_pass",
        "reason",
        "microcap_quarantine_allowed_use",
        "not_alpha_evidence",
        "direct_q2_entry_allowed",
    ]
