"""Target-cache utilities for small-cap FD diagnostics."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

import pandas as pd

from .small_cap_data_admission import GUARDS
from .small_cap_quality_family import (
    _forward_targets,
    _load_manifest,
    _load_section_csv,
    _next_trading_dates,
    _normalize_benchmark,
    _normalize_delistings,
    _normalize_prices,
    _normalize_universe,
)
from .small_cap_universe import build_small_cap_universe_tiers


REQUIRED_TARGET_HORIZONS = (1, 3, 6)


def build_forward_target_cache_from_manifest(
    manifest_path: str | Path,
    output_dir: str | Path,
    horizons: tuple[int, ...] = REQUIRED_TARGET_HORIZONS,
) -> dict[str, Path]:
    """Build 1m/3m/6m forward-return target cache from the local PIT bundle."""

    manifest_file = Path(manifest_path)
    manifest = _load_manifest(manifest_file)
    prices = _normalize_prices(_load_section_csv(manifest, manifest_file, "prices"))
    universe = _normalize_universe(_load_section_csv(manifest, manifest_file, "universe"))
    benchmark = _normalize_benchmark(_load_section_csv(manifest, manifest_file, "benchmark"))
    delistings = _normalize_delistings(_load_section_csv(manifest, manifest_file, "delisting"))
    if prices.empty or universe.empty or benchmark.empty:
        return write_forward_target_cache_from_panel(pd.DataFrame(), output_dir, horizons=horizons)
    tiers = build_small_cap_universe_tiers(prices=prices, universe=universe)
    close = prices.pivot_table(index="date", columns="asset_id", values="adjusted_close", aggfunc="last").sort_index()
    benchmark_close = benchmark.drop_duplicates("date").set_index("date")["adjusted_close"].sort_index()
    signal_dates = [pd.Timestamp(value) for value in sorted(tiers["rebalance_date"].dropna().unique())]
    next_dates = _next_trading_dates(close.index, signal_dates)
    target = _forward_targets(close, benchmark_close, signal_dates, next_dates, tiers, delistings, horizons=horizons)
    return write_forward_target_cache_from_panel(
        target,
        output_dir,
        horizons=horizons,
        source_manifest=str(manifest_file),
        corporate_action_adjusted=bool(manifest.get("prices", {}).get("adjusted", True))
        if isinstance(manifest.get("prices"), Mapping)
        else True,
        delisting_adjusted=not delistings.empty,
    )


def write_forward_target_cache_from_panel(
    target_panel: pd.DataFrame,
    output_dir: str | Path,
    horizons: tuple[int, ...] = REQUIRED_TARGET_HORIZONS,
    source_manifest: str | None = None,
    corporate_action_adjusted: bool = True,
    delisting_adjusted: bool = True,
) -> dict[str, Path]:
    """Write a normalized forward-return cache and horizon audit."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    forward_path = output_path / "forward_returns_1m_3m_6m.csv"
    audit_path = output_path / "target_cache_audit.json"

    target = _normalize_target_panel(target_panel)
    if "horizon_months" in target.columns:
        target = target[target["horizon_months"].isin(horizons)].copy()
    for column, default in {
        "target_visibility_rule": "forward_return_visible_only_after_exit_timestamp",
        "target_timestamp": "",
    }.items():
        if column not in target.columns:
            target[column] = default
    for key, value in GUARDS.items():
        target[key] = value
    target.to_csv(forward_path, index=False)
    audit = _target_cache_audit(
        target,
        horizons=horizons,
        source_manifest=source_manifest,
        corporate_action_adjusted=corporate_action_adjusted,
        delisting_adjusted=delisting_adjusted,
    )
    audit_path.write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"forward_returns": forward_path, "audit": audit_path}


def _normalize_target_panel(target_panel: pd.DataFrame) -> pd.DataFrame:
    target = target_panel.copy()
    if target.empty:
        return target
    if "asset_id" in target.columns:
        target["asset_id"] = target["asset_id"].astype(str)
    if "rebalance_date" in target.columns:
        target["rebalance_date"] = pd.to_datetime(target["rebalance_date"], errors="coerce").dt.date.astype("string")
    if "target_timestamp" in target.columns:
        target["target_timestamp"] = pd.to_datetime(target["target_timestamp"], errors="coerce").dt.date.astype("string")
    for column in target.columns:
        if column not in {"rebalance_date", "asset_id", "period", "target_timestamp", "target_visibility_rule"}:
            converted = pd.to_numeric(target[column], errors="coerce")
            if converted.notna().any():
                target[column] = converted
    return target


def _target_cache_audit(
    target: pd.DataFrame,
    *,
    horizons: tuple[int, ...],
    source_manifest: str | None,
    corporate_action_adjusted: bool,
    delisting_adjusted: bool,
) -> dict[str, object]:
    horizon_audit = []
    for horizon in horizons:
        horizon_frame = (
            target[pd.to_numeric(target.get("horizon_months", pd.Series(dtype=float)), errors="coerce") == horizon]
            if not target.empty
            else pd.DataFrame()
        )
        available = int(horizon_frame["forward_market_relative_return"].notna().sum()) if "forward_market_relative_return" in horizon_frame.columns else 0
        missing = int(len(horizon_frame) - available)
        target_timestamp = ""
        if "target_timestamp" in horizon_frame.columns and horizon_frame["target_timestamp"].notna().any():
            target_timestamp = str(horizon_frame["target_timestamp"].dropna().max())
        horizon_audit.append(
            {
                "target_horizon": horizon,
                "available_row_count": available,
                "missing_row_count": missing,
                "corporate_action_adjusted": corporate_action_adjusted,
                "delisting_adjusted": delisting_adjusted,
                "target_timestamp": target_timestamp,
                "target_visibility_rule": "forward_return_visible_only_after_exit_timestamp",
            }
        )
    return {
        "schema_version": "fd_small_cap_target_cache_audit.v1",
        "source_manifest": source_manifest,
        "required_horizons": list(horizons),
        "all_required_horizons_available": all(row["available_row_count"] > 0 for row in horizon_audit),
        "horizon_audit": horizon_audit,
        **GUARDS,
    }
