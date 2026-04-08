"""Event-basket target helpers for dry-run target translation."""

from __future__ import annotations

import math
from typing import Iterable

import pandas as pd


def build_event_basket_target_frame(
    candidates: pd.DataFrame,
    *,
    whitelist: Iterable[str],
    min_cohort_size: int = 6,
    top_fraction: float = 1.0 / 3.0,
    max_new_entries: int = 5,
) -> pd.DataFrame:
    """Build one deterministic target frame from a same-day event cohort."""

    if top_fraction <= 0.0 or top_fraction > 1.0:
        raise ValueError("top_fraction must be in (0, 1].")
    if min_cohort_size <= 0:
        raise ValueError("min_cohort_size must be positive.")
    if max_new_entries <= 0:
        raise ValueError("max_new_entries must be positive.")

    whitelist_set = {str(ticker).strip().upper() for ticker in whitelist if str(ticker).strip()}
    frame = candidates.copy()
    frame["ticker"] = frame["ticker"].astype(str).str.strip().str.upper()
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
    frame = frame.dropna(subset=["ticker", "score"]).copy()
    frame = frame.loc[frame["ticker"].isin(whitelist_set)].copy()
    if len(frame) < min_cohort_size:
        return pd.DataFrame(columns=["ticker", "target_weight"])

    frame = frame.sort_values(["score", "ticker"], ascending=[False, True]).reset_index(drop=True)
    selected_count = min(max_new_entries, max(1, int(math.floor(len(frame) * top_fraction))))
    winners = frame.head(selected_count).copy()
    winners["target_weight"] = 1.0 / float(len(winners))
    return winners.loc[:, ["ticker", "target_weight"]].reset_index(drop=True)


def build_event_target_manifest(
    *,
    event_date: str,
    target_frame: pd.DataFrame,
    cohort_size: int,
    whitelist_size: int,
    min_cohort_size: int,
    top_fraction: float,
    max_new_entries: int,
) -> dict[str, object]:
    """Build a compact dry-run manifest for one event-target translation."""

    return {
        "event_date": str(event_date),
        "cohort_size": int(cohort_size),
        "whitelist_size": int(whitelist_size),
        "selected_count": int(len(target_frame)),
        "selected_tickers": target_frame.get("ticker", pd.Series(dtype=object)).astype(str).tolist(),
        "target_weight_sum": float(pd.to_numeric(target_frame.get("target_weight"), errors="coerce").fillna(0.0).sum())
        if not target_frame.empty
        else 0.0,
        "selection_policy": {
            "min_cohort_size": int(min_cohort_size),
            "top_fraction": float(top_fraction),
            "max_new_entries": int(max_new_entries),
        },
    }
