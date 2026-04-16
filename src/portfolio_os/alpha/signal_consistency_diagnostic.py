"""Pointwise consistency diagnostics between production and canonical signal views."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


DEFAULT_MIN_OVERLAP_THRESHOLD = 10
_OVERLAP_BUCKET_SIZES = (5, 10)


def _safe_rank_correlation(left: pd.Series, right: pd.Series, *, method: str) -> float:
    clean = pd.DataFrame({"left": left, "right": right}).dropna()
    if len(clean) < 2:
        return float("nan")
    if clean["left"].nunique() < 2 or clean["right"].nunique() < 2:
        return float("nan")
    value = clean["left"].corr(clean["right"], method=method)
    return float(value) if pd.notna(value) else float("nan")


def _normalize_canonical_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"date", "ticker", "alpha_score"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"canonical_cross_section missing required columns: {sorted(missing)}")
    work = frame.loc[:, ["date", "ticker", "alpha_score"]].copy()
    work["date"] = pd.to_datetime(work["date"]).dt.normalize()
    work["ticker"] = work["ticker"].astype(str)
    work["alpha_score"] = pd.to_numeric(work["alpha_score"], errors="coerce")
    return work.dropna(subset=["date", "ticker", "alpha_score"]).sort_values(["date", "ticker"]).reset_index(drop=True)


def _normalize_production_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"date", "ticker", "alpha_score", "expected_return"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"production view missing required columns: {sorted(missing)}")
    work = frame.loc[:, ["date", "ticker", "alpha_score", "expected_return"]].copy()
    work["date"] = pd.to_datetime(work["date"]).dt.normalize()
    work["ticker"] = work["ticker"].astype(str)
    work["alpha_score"] = pd.to_numeric(work["alpha_score"], errors="coerce")
    work["expected_return"] = pd.to_numeric(work["expected_return"], errors="coerce")
    return (
        work.dropna(subset=["date", "ticker", "alpha_score", "expected_return"])
        .sort_values(["date", "ticker"])
        .reset_index(drop=True)
    )


def _rank_overlap(
    merged: pd.DataFrame,
    *,
    production_column: str,
    canonical_column: str,
    bucket_size: int,
    top: bool,
) -> float:
    if len(merged) < int(bucket_size):
        return float("nan")
    ascending = not bool(top)
    production_tickers = (
        merged.sort_values([production_column, "ticker"], ascending=[ascending, True]).head(bucket_size)["ticker"]
    )
    canonical_tickers = (
        merged.sort_values([canonical_column, "ticker"], ascending=[ascending, True]).head(bucket_size)["ticker"]
    )
    return float(len(set(production_tickers).intersection(set(canonical_tickers))) / float(bucket_size))


def _build_empty_month_row(
    *,
    date_value: pd.Timestamp,
    production_view: str,
    canonical_ticker_count: int,
    production_ticker_count: int,
    ticker_overlap_count: int,
    view_has_month: bool,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "date": pd.Timestamp(date_value).strftime("%Y-%m-%d"),
        "production_view": str(production_view),
        "view_has_month": bool(view_has_month),
        "canonical_ticker_count": int(canonical_ticker_count),
        "production_ticker_count": int(production_ticker_count),
        "ticker_overlap_count": int(ticker_overlap_count),
        "alpha_vs_canonical_spearman": float("nan"),
        "expected_return_vs_canonical_spearman": float("nan"),
    }
    for bucket_size in _OVERLAP_BUCKET_SIZES:
        row[f"top_{bucket_size}_overlap"] = float("nan")
        row[f"bottom_{bucket_size}_overlap"] = float("nan")
    return row


def _month_level_detail(
    *,
    date_value: pd.Timestamp,
    production_view: str,
    canonical_month: pd.DataFrame,
    production_month: pd.DataFrame,
    min_overlap_threshold: int,
) -> tuple[dict[str, Any], pd.DataFrame]:
    merged = production_month.merge(
        canonical_month,
        on=["date", "ticker"],
        how="inner",
        suffixes=("_production", "_canonical"),
    )
    row = _build_empty_month_row(
        date_value=date_value,
        production_view=production_view,
        canonical_ticker_count=len(canonical_month),
        production_ticker_count=len(production_month),
        ticker_overlap_count=len(merged),
        view_has_month=not production_month.empty,
    )
    if merged.empty:
        return row, merged

    for bucket_size in _OVERLAP_BUCKET_SIZES:
        row[f"top_{bucket_size}_overlap"] = _rank_overlap(
            merged,
            production_column="expected_return",
            canonical_column="alpha_score_canonical",
            bucket_size=bucket_size,
            top=True,
        )
        row[f"bottom_{bucket_size}_overlap"] = _rank_overlap(
            merged,
            production_column="expected_return",
            canonical_column="alpha_score_canonical",
            bucket_size=bucket_size,
            top=False,
        )

    if len(merged) < int(min_overlap_threshold):
        return row, merged

    row["alpha_vs_canonical_spearman"] = _safe_rank_correlation(
        merged["alpha_score_production"],
        merged["alpha_score_canonical"],
        method="spearman",
    )
    row["expected_return_vs_canonical_spearman"] = _safe_rank_correlation(
        merged["expected_return"],
        merged["alpha_score_canonical"],
        method="spearman",
    )
    return row, merged


@dataclass
class SignalConsistencyReport:
    per_month_frame: pd.DataFrame
    pooled_summary_frame: pd.DataFrame
    metadata: dict[str, Any]

    def to_markdown(self) -> str:
        def _summary_line(production_view: str) -> str:
            if self.pooled_summary_frame.empty:
                return f"- `{production_view}`: no pooled summary rows."
            match = self.pooled_summary_frame.loc[self.pooled_summary_frame["production_view"] == production_view]
            if match.empty:
                return f"- `{production_view}`: no pooled summary rows."
            row = match.iloc[0]
            return (
                f"- `{production_view}`: active_months={int(row['active_month_count'])}, "
                f"pooled_alpha_vs_canonical_spearman={float(row['pooled_alpha_vs_canonical_spearman']):.4f}, "
                f"pooled_expected_return_vs_canonical_spearman={float(row['pooled_expected_return_vs_canonical_spearman']):.4f}, "
                f"mean_top_5_overlap={float(row['mean_top_5_overlap']):.2f}, "
                f"mean_bottom_5_overlap={float(row['mean_bottom_5_overlap']):.2f}"
            )

        per_month_text = (
            self.per_month_frame.to_string(index=False) if not self.per_month_frame.empty else "No active-month rows."
        )
        pooled_text = (
            self.pooled_summary_frame.to_string(index=False)
            if not self.pooled_summary_frame.empty
            else "No pooled summary rows."
        )
        metadata_lines = "\n".join(f"- `{key}`: `{value}`" for key, value in sorted(self.metadata.items()))
        return "\n".join(
            [
                "# Signal Consistency Diagnostic",
                "",
                "## Provenance",
                metadata_lines or "- none",
                "",
                "## Claim Levels",
                "- `B1 claim`: long-horizon full-sample evidence about whether spread floor behaves like crash protection.",
                "- `production claim`: small-sample production evidence about whether spread floor filtered better or worse months.",
                "- `consistency claim`: pipeline-mechanical evidence about whether production ordering matches canonical ordering.",
                "",
                "These claims live at different levels (full-sample statistical / small-sample conditional / pipeline-mechanical) and answer different questions; agreement or disagreement between them is not evidence for or against each other.",
                "",
                "## Key Read",
                _summary_line("baseline"),
                _summary_line("signed_spread"),
                "- Reading rule: if pooled `alpha_score` consistency stays near 1.0 while pooled `expected_return` consistency collapses, the production pipeline is preserving score ordering but the expected-return mapping for the promoted months is no longer aligned with canonical ordering.",
                "",
                "## Pooled Summary",
                "```text",
                pooled_text,
                "```",
                "",
                "## Per-Month Detail",
                "```text",
                per_month_text,
                "```",
            ]
        )


def build_signal_consistency_report(
    *,
    canonical_cross_section: pd.DataFrame,
    production_views: dict[str, pd.DataFrame],
    metadata: dict[str, Any] | None = None,
    min_overlap_threshold: int = DEFAULT_MIN_OVERLAP_THRESHOLD,
) -> SignalConsistencyReport:
    if not production_views:
        raise ValueError("production_views cannot be empty.")

    canonical = _normalize_canonical_frame(canonical_cross_section)
    normalized_views = {
        str(view_name): _normalize_production_frame(view_frame)
        for view_name, view_frame in production_views.items()
    }

    all_months = sorted(
        {
            pd.Timestamp(value)
            for view_frame in normalized_views.values()
            for value in pd.to_datetime(view_frame["date"]).unique().tolist()
        }
    )

    detail_rows: list[dict[str, Any]] = []
    pooled_summary_rows: list[dict[str, Any]] = []

    for production_view, production_frame in normalized_views.items():
        pooled_frames: list[pd.DataFrame] = []
        for date_value in all_months:
            canonical_month = canonical.loc[canonical["date"] == date_value].copy()
            production_month = production_frame.loc[production_frame["date"] == date_value].copy()
            row, merged = _month_level_detail(
                date_value=date_value,
                production_view=production_view,
                canonical_month=canonical_month,
                production_month=production_month,
                min_overlap_threshold=min_overlap_threshold,
            )
            detail_rows.append(row)
            if len(merged) >= int(min_overlap_threshold):
                pooled_frames.append(merged)

        if pooled_frames:
            pooled = pd.concat(pooled_frames, ignore_index=True)
            pooled_alpha_spearman = _safe_rank_correlation(
                pooled["alpha_score_production"],
                pooled["alpha_score_canonical"],
                method="spearman",
            )
            pooled_expected_spearman = _safe_rank_correlation(
                pooled["expected_return"],
                pooled["alpha_score_canonical"],
                method="spearman",
            )
            pooled_alpha_kendall = _safe_rank_correlation(
                pooled["alpha_score_production"],
                pooled["alpha_score_canonical"],
                method="kendall",
            )
            pooled_expected_kendall = _safe_rank_correlation(
                pooled["expected_return"],
                pooled["alpha_score_canonical"],
                method="kendall",
            )
            pooled_observation_count = int(len(pooled))
        else:
            pooled_alpha_spearman = float("nan")
            pooled_expected_spearman = float("nan")
            pooled_alpha_kendall = float("nan")
            pooled_expected_kendall = float("nan")
            pooled_observation_count = 0

        view_detail = [row for row in detail_rows if row["production_view"] == production_view]
        pooled_summary_rows.append(
            {
                "production_view": production_view,
                "active_month_count": int(sum(1 for row in view_detail if bool(row["view_has_month"]))),
                "months_meeting_overlap_threshold": int(sum(1 for frame in pooled_frames if not frame.empty)),
                "pooled_observation_count": pooled_observation_count,
                "pooled_alpha_vs_canonical_spearman": pooled_alpha_spearman,
                "pooled_expected_return_vs_canonical_spearman": pooled_expected_spearman,
                "pooled_alpha_vs_canonical_kendall": pooled_alpha_kendall,
                "pooled_expected_return_vs_canonical_kendall": pooled_expected_kendall,
                "mean_top_5_overlap": float(
                    pd.Series([row["top_5_overlap"] for row in view_detail], dtype=float).mean(skipna=True)
                ),
                "mean_top_10_overlap": float(
                    pd.Series([row["top_10_overlap"] for row in view_detail], dtype=float).mean(skipna=True)
                ),
                "mean_bottom_5_overlap": float(
                    pd.Series([row["bottom_5_overlap"] for row in view_detail], dtype=float).mean(skipna=True)
                ),
                "mean_bottom_10_overlap": float(
                    pd.Series([row["bottom_10_overlap"] for row in view_detail], dtype=float).mean(skipna=True)
                ),
            }
        )

    per_month_frame = pd.DataFrame(detail_rows).sort_values(["date", "production_view"]).reset_index(drop=True)
    pooled_summary_frame = pd.DataFrame(pooled_summary_rows).sort_values("production_view").reset_index(drop=True)

    merged_metadata = {
        "min_overlap_threshold": int(min_overlap_threshold),
        "pooled_method": "concat_then_correlate",
        "production_views": list(normalized_views.keys()),
    }
    if metadata:
        merged_metadata.update(metadata)

    return SignalConsistencyReport(
        per_month_frame=per_month_frame,
        pooled_summary_frame=pooled_summary_frame,
        metadata=merged_metadata,
    )
