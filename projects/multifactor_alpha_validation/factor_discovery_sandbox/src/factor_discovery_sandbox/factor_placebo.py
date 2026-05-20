"""FD-R5 placebo, robustness, and family diagnostics."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .family_diagnostics import build_factor_family_summary, render_factor_family_diagnostics
from .real_factor_replay import _load_manifest, _load_section_csv, _normalize_universe
from .robustness_suite import (
    aggregate_placebo_report,
    build_robustness_by_period,
    build_robustness_by_regime,
    compute_rebalance_diagnostics,
)


@dataclass(frozen=True)
class FDRealPlaceboRobustnessResult:
    """Artifacts and summary for FD-R5."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_real_placebo_robustness(
    manifest_path: str | Path,
    factor_panel_path: str | Path,
    oos_score_panel_path: str | Path,
    oos_decile_spread_path: str | Path,
    output_dir: str | Path,
) -> FDRealPlaceboRobustnessResult:
    """Run FD-R5 placebo and robustness checks without allocator entry."""

    manifest_file = Path(manifest_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    manifest = _load_manifest(manifest_file)
    universe = _normalize_universe(_load_section_csv(manifest, manifest_file, "universe"))
    factor_panel = pd.read_csv(factor_panel_path, usecols=["factor_id", "coverage_status", "known_correlation_family"])
    score_panel = _normalize_score_panel(pd.read_csv(oos_score_panel_path, low_memory=False))
    _ = pd.read_csv(oos_decile_spread_path)

    score_panel = _attach_sector(score_panel, universe)
    variants = _build_score_variants(score_panel)
    diagnostics = pd.concat(
        [compute_rebalance_diagnostics(frame, "diagnostic_score", test_name) for test_name, frame in variants.items()],
        ignore_index=True,
    )
    placebo_report = aggregate_placebo_report(diagnostics)
    live_diagnostics = diagnostics[diagnostics["test_name"] == "live_oos_score"]
    robustness_by_period = build_robustness_by_period(live_diagnostics)
    robustness_by_regime = build_robustness_by_regime(score_panel)
    family_summary = build_factor_family_summary(factor_panel)
    placebo_status, recommended_next_action = _placebo_decision(placebo_report)

    summary = {
        "schema_version": "fd_real_placebo_robustness_summary.v1",
        "stage": "FD-R5",
        "manifest_path": str(manifest_file),
        "factor_panel_path": str(factor_panel_path),
        "oos_score_panel_path": str(oos_score_panel_path),
        "oos_decile_spread_path": str(oos_decile_spread_path),
        "placebo_test_count": int(placebo_report["test_name"].nunique()) if not placebo_report.empty else 0,
        "placebo_status": placebo_status,
        "recommended_next_action": recommended_next_action,
        "allocator_entry_allowed": False,
        "direct_q2_entry_allowed": False,
        "alpha_success_claimed": False,
        "production_approval_claimed": False,
        "not_alpha_evidence": True,
    }

    artifacts = {
        "placebo_report": output_path / "placebo_report.csv",
        "robustness_by_period": output_path / "robustness_by_period.csv",
        "robustness_by_regime": output_path / "robustness_by_regime.csv",
        "factor_family_diagnostics": output_path / "factor_family_diagnostics.md",
        "placebo_robustness_summary": output_path / "placebo_robustness_summary.json",
    }
    placebo_report.to_csv(artifacts["placebo_report"], index=False)
    robustness_by_period.to_csv(artifacts["robustness_by_period"], index=False)
    robustness_by_regime.to_csv(artifacts["robustness_by_regime"], index=False)
    artifacts["placebo_robustness_summary"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["factor_family_diagnostics"].write_text(
        render_factor_family_diagnostics(family_summary, summary, placebo_status),
        encoding="utf-8",
    )

    return FDRealPlaceboRobustnessResult(summary=summary, artifacts=artifacts)


def _normalize_score_panel(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized["rebalance_date"] = pd.to_datetime(normalized["rebalance_date"], errors="coerce")
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    normalized["score"] = pd.to_numeric(normalized["score"], errors="coerce")
    for column in ("forward_asset_return", "forward_benchmark_return", "forward_excess_return"):
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized["forward_return_available"] = normalized["forward_return_available"].astype(str).str.lower().isin(
        {"true", "1", "yes"}
    )
    return normalized


def _attach_sector(score_panel: pd.DataFrame, universe: pd.DataFrame) -> pd.DataFrame:
    if "sector" not in universe.columns:
        score_panel = score_panel.copy()
        score_panel["sector"] = "unknown"
        return score_panel
    rows: list[pd.DataFrame] = []
    start = universe["membership_start"].fillna(pd.Timestamp.min)
    end = universe["membership_end"].fillna(pd.Timestamp.max)
    as_of = universe["as_of_timestamp"].fillna(start)
    universe = universe.copy()
    universe["asset_id"] = universe["asset_id"].astype(str)
    for rebalance_date, group in score_panel.groupby("rebalance_date"):
        active = universe[(start <= rebalance_date) & (end >= rebalance_date) & (as_of <= rebalance_date)]
        sector_map = active.drop_duplicates("asset_id", keep="last").set_index("asset_id")["sector"].astype(str)
        enriched = group.copy()
        enriched["sector"] = enriched["asset_id"].map(sector_map).fillna("unknown")
        rows.append(enriched)
    return pd.concat(rows, ignore_index=True) if rows else score_panel.assign(sector="unknown")


def _build_score_variants(score_panel: pd.DataFrame) -> dict[str, pd.DataFrame]:
    base = score_panel.copy()
    variants: dict[str, pd.DataFrame] = {}
    variants["live_oos_score"] = _with_score(base, base["score"])
    variants["shuffled_cross_section_placebo"] = _with_score(
        base,
        base.groupby(["rebalance_date", "horizon_months"])["score"].transform(lambda series: series.sort_index().shift(1)),
    )
    variants["lagged_signal_placebo"] = _with_score(
        base,
        base.sort_values("rebalance_date").groupby(["asset_id", "horizon_months"])["score"].shift(1),
    )
    variants["rebalance_date_shifted_placebo"] = _with_score(
        base,
        base.sort_values("rebalance_date").groupby(["asset_id", "horizon_months"])["score"].shift(2),
    )
    variants["random_same_coverage_placebo"] = _with_score(
        base,
        base.apply(_deterministic_random_score, axis=1),
    )
    variants["sector_neutral_placebo"] = _with_score(
        base,
        base["score"]
        - base.groupby(["rebalance_date", "horizon_months", "sector"])["score"].transform("mean"),
    )
    variants["future_return_leakage_negative_control"] = _with_score(base, base["forward_excess_return"])
    return variants


def _with_score(frame: pd.DataFrame, diagnostic_score: pd.Series) -> pd.DataFrame:
    result = frame.copy()
    result["diagnostic_score"] = diagnostic_score.where(result["coverage_state"] == "active_score")
    return result


def _deterministic_random_score(row: pd.Series) -> float:
    if row.get("coverage_state") != "active_score":
        return np.nan
    key = f"{row.get('rebalance_date')}|{row.get('asset_id')}|{row.get('horizon_months')}"
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(digest[:12], 16) / float(16**12 - 1) - 0.5


def _placebo_decision(placebo_report: pd.DataFrame) -> tuple[str, str]:
    if placebo_report.empty:
        return "insufficient_placebo_evidence", "needs_more_evidence_before_allocator"
    test_rows = placebo_report[placebo_report["period"] == "test"]
    live = test_rows[test_rows["test_name"] == "live_oos_score"]
    comparators = test_rows[
        test_rows["test_name"].isin(
            {
                "shuffled_cross_section_placebo",
                "lagged_signal_placebo",
                "random_same_coverage_placebo",
                "rebalance_date_shifted_placebo",
            }
        )
    ]
    if live.empty or comparators.empty:
        return "insufficient_placebo_evidence", "needs_more_evidence_before_allocator"
    live_spread = float(live["mean_top_bottom_spread"].mean())
    live_rank_ic = float(live["mean_rank_ic"].mean())
    comparator_spread = float(comparators["mean_top_bottom_spread"].median())
    if live_spread > max(0.0, comparator_spread) and live_rank_ic > 0.0:
        return "passed_initial_placebo_gate", "needs_more_evidence_before_allocator"
    return "failed_placebo_gate", "stop_before_allocator"
