"""Factor-family diagnostics for FD real-data validation."""

from __future__ import annotations

from typing import Mapping

import pandas as pd


def build_factor_family_summary(factor_panel: pd.DataFrame) -> pd.DataFrame:
    """Summarize FD-R3 coverage by declared correlation family."""

    panel = factor_panel.copy()
    if "known_correlation_family" not in panel.columns:
        panel["known_correlation_family"] = "unknown"
    family = (
        panel.groupby(["known_correlation_family", "factor_id"], as_index=False)
        .agg(
            row_count=("coverage_status", "size"),
            active_view_rows=("coverage_status", lambda series: int((series == "active_view").sum())),
            explicit_abstain_rows=("coverage_status", lambda series: int((series == "explicit_abstain").sum())),
        )
        .sort_values(["known_correlation_family", "factor_id"])
    )
    family["coverage_ratio"] = family["active_view_rows"] / family["row_count"]
    return (
        family.groupby("known_correlation_family", as_index=False)
        .agg(
            factor_count=("factor_id", "nunique"),
            row_count=("row_count", "sum"),
            active_view_rows=("active_view_rows", "sum"),
            explicit_abstain_rows=("explicit_abstain_rows", "sum"),
            mean_factor_coverage=("coverage_ratio", "mean"),
        )
        .sort_values("known_correlation_family")
    )


def render_factor_family_diagnostics(
    family_summary: pd.DataFrame,
    summary: Mapping[str, object],
    placebo_status: str,
) -> str:
    """Render the FD-R5 factor-family diagnostic report."""

    lines = [
        "# Factor Family Diagnostics",
        "",
        "not alpha evidence",
        f"allocator entry: {str(summary['allocator_entry_allowed']).lower()}",
        f"placebo status: {placebo_status}",
        "direct Q2 entry: not allowed",
        "",
        "## Families",
    ]
    if family_summary.empty:
        lines.append("- no factor-family coverage rows were available")
    else:
        for row in family_summary.itertuples(index=False):
            lines.append(
                f"- {row.known_correlation_family}: factors={int(row.factor_count)}, "
                f"mean_coverage={row.mean_factor_coverage:.6f}, "
                f"explicit_abstain_rows={int(row.explicit_abstain_rows)}"
            )
    lines.extend(
        [
            "",
            "## Boundary",
            "- FD-R5 is a robustness and placebo gate.",
            "- It does not allocate capital, approve production, enter Q2, or update Alpha Registry.",
            "",
        ]
    )
    return "\n".join(lines)
