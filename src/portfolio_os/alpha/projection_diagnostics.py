"""Diagnostics helpers for AlphaView projection outputs."""

from __future__ import annotations

from typing import Any, Sequence


def rank_projected_expected_returns(
    expected_return_panel: Sequence[dict[str, Any]],
    date: str,
) -> list[str]:
    """Rank projected symbols by optimizer-input expected return for one date."""

    rows = [
        row
        for row in expected_return_panel
        if str(row.get("date")) == str(date) and row.get("expected_return") is not None
    ]
    rows.sort(key=lambda row: (-float(row["expected_return"]), str(row["symbol"])))
    return [str(row["symbol"]) for row in rows]


def build_projection_diagnostic_row(
    *,
    date: str,
    active_views: list[str],
    abstained_views: list[str],
    coverage_count: int,
    horizon_conversion: list[dict[str, Any]],
    decay_applied: list[dict[str, Any]],
    final_expected_return_scale: dict[str, float],
) -> dict[str, Any]:
    """Build one deterministic projection diagnostic row."""

    return {
        "date": str(date),
        "active_views": sorted(active_views),
        "abstained_views": sorted(set(abstained_views)),
        "coverage_count": int(coverage_count),
        "horizon_conversion": horizon_conversion,
        "decay_applied": decay_applied,
        "final_expected_return_scale": final_expected_return_scale,
    }
