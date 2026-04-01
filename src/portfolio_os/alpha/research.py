"""Baseline alpha research workflow built from normalized returns history."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from portfolio_os.alpha.report import render_alpha_research_report
from portfolio_os.data.loaders import ensure_columns, normalize_ticker, read_csv
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.storage.snapshots import write_json, write_text

_PRIMARY_SIGNAL_NAME = "blended_alpha"
_SIGNAL_COLUMNS = {
    "reversal_only": "reversal_rank",
    "momentum_only": "momentum_rank",
    _PRIMARY_SIGNAL_NAME: "alpha_score",
}


@dataclass
class AlphaResearchResult:
    """Serializable alpha research outputs plus written artifact locations."""

    returns_file: Path
    output_dir: Path
    signal_frame: pd.DataFrame
    ic_frame: pd.DataFrame
    signal_summary_frame: pd.DataFrame
    summary_payload: dict[str, object]
    report_markdown: str


def _load_returns_panel(path: str | Path) -> pd.DataFrame:
    """Load long-form returns and pivot to a date x ticker matrix."""

    frame = read_csv(path)
    ensure_columns(frame, ["date", "ticker", "return"], str(path))
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    if frame["date"].isna().any():
        raise InputValidationError("returns_long.csv contains invalid date values.")
    frame["ticker"] = frame["ticker"].map(normalize_ticker)
    frame["return"] = pd.to_numeric(frame["return"], errors="coerce")
    if frame["return"].isna().any():
        raise InputValidationError("returns_long.csv contains non-numeric return values.")
    duplicated = frame.duplicated(subset=["date", "ticker"], keep=False)
    if duplicated.any():
        raise InputValidationError("returns_long.csv contains duplicate (date, ticker) rows.")
    panel = frame.pivot(index="date", columns="ticker", values="return").sort_index()
    if panel.empty:
        raise InputValidationError("returns_long.csv produced an empty returns panel.")
    return panel


def load_alpha_returns_panel(path: str | Path) -> pd.DataFrame:
    """Public wrapper that loads a date x ticker returns panel for alpha workflows."""

    return _load_returns_panel(path)


def _rolling_compound(returns_panel: pd.DataFrame, lookback_days: int) -> pd.DataFrame:
    """Compound simple returns across one trailing window."""

    if lookback_days <= 0:
        raise InputValidationError("lookback_days must be positive.")
    return (1.0 + returns_panel).rolling(window=int(lookback_days)).apply(np.prod, raw=True) - 1.0


def _build_reversal_signal(returns_panel: pd.DataFrame, lookback_days: int) -> pd.DataFrame:
    """Negative trailing return as a simple short-horizon reversal signal."""

    return -_rolling_compound(returns_panel, lookback_days)


def _build_momentum_signal(
    returns_panel: pd.DataFrame,
    *,
    lookback_days: int,
    skip_days: int,
) -> pd.DataFrame:
    """Trailing momentum with a skip window before the forecast date."""

    if skip_days < 0:
        raise InputValidationError("skip_days cannot be negative.")
    momentum = _rolling_compound(returns_panel, lookback_days)
    if skip_days:
        momentum = momentum.shift(int(skip_days))
    return momentum


def _centered_cross_sectional_rank(signal_frame: pd.DataFrame) -> pd.DataFrame:
    """Convert raw signal values into centered cross-sectional ranks."""

    return signal_frame.rank(axis=1, pct=True, method="average") - 0.5


def _build_forward_returns(returns_panel: pd.DataFrame, horizon_days: int) -> pd.DataFrame:
    """Compound future returns over the chosen forecast horizon."""

    if horizon_days <= 0:
        raise InputValidationError("forward_horizon_days must be positive.")
    shifted_components = [(1.0 + returns_panel.shift(-step)) for step in range(1, int(horizon_days) + 1)]
    compounded = shifted_components[0].copy()
    for component in shifted_components[1:]:
        compounded = compounded * component
    return compounded - 1.0


def _stack_named_frame(frame: pd.DataFrame, value_name: str) -> pd.DataFrame:
    """Convert one wide date x ticker frame into long-form rows."""

    stacked = frame.copy()
    stacked.index.name = "date"
    stacked = stacked.reset_index().melt(id_vars="date", var_name="ticker", value_name=value_name)
    stacked["date"] = pd.to_datetime(stacked["date"]).dt.strftime("%Y-%m-%d")
    return stacked


def build_alpha_research_frame(
    returns_panel: pd.DataFrame,
    *,
    reversal_lookback_days: int,
    momentum_lookback_days: int,
    momentum_skip_days: int,
    forward_horizon_days: int,
    reversal_weight: float,
    momentum_weight: float,
) -> pd.DataFrame:
    """Build one long-form signal-and-label research frame."""

    reversal_raw = _build_reversal_signal(returns_panel, reversal_lookback_days)
    momentum_raw = _build_momentum_signal(
        returns_panel,
        lookback_days=momentum_lookback_days,
        skip_days=momentum_skip_days,
    )
    reversal_rank = _centered_cross_sectional_rank(reversal_raw)
    momentum_rank = _centered_cross_sectional_rank(momentum_raw)
    alpha_score = float(reversal_weight) * reversal_rank + float(momentum_weight) * momentum_rank
    forward_return = _build_forward_returns(returns_panel, forward_horizon_days)

    merged = _stack_named_frame(reversal_raw, "reversal_raw")
    for frame, value_name in (
        (momentum_raw, "momentum_raw"),
        (reversal_rank, "reversal_rank"),
        (momentum_rank, "momentum_rank"),
        (alpha_score, "alpha_score"),
        (forward_return, "forward_return"),
    ):
        merged = merged.merge(_stack_named_frame(frame, value_name), on=["date", "ticker"], how="inner")
    merged = merged.dropna(subset=["alpha_score", "forward_return"]).sort_values(["date", "ticker"]).reset_index(drop=True)
    if merged.empty:
        raise InputValidationError("Alpha research frame is empty after applying signal and label windows.")
    return merged


def _safe_correlation(left: pd.Series, right: pd.Series, method: str) -> float:
    """Return one correlation value while handling small or constant samples."""

    if len(left) < 2 or len(right) < 2:
        return 0.0
    if left.nunique() < 2 or right.nunique() < 2:
        return 0.0
    correlation = left.corr(right, method=method)
    if pd.isna(correlation):
        return 0.0
    return float(correlation)


def _signal_quantile_buckets(frame: pd.DataFrame, *, score_column: str, quantiles: int) -> pd.Series:
    """Bucket one score column into quantiles using cross-sectional ranks."""

    return np.ceil(frame[score_column].rank(method="first", pct=True) * quantiles).clip(1, quantiles)


def _top_bottom_spread(frame: pd.DataFrame, *, score_column: str, quantiles: int) -> float:
    """Compute top-minus-bottom forward return spread from score buckets."""

    if len(frame) < quantiles:
        return 0.0
    rank_bucket = _signal_quantile_buckets(frame, score_column=score_column, quantiles=quantiles)
    top_forward = frame.loc[rank_bucket == quantiles, "forward_return"].mean()
    bottom_forward = frame.loc[rank_bucket == 1, "forward_return"].mean()
    if pd.isna(top_forward) or pd.isna(bottom_forward):
        return 0.0
    return float(top_forward - bottom_forward)


def build_alpha_ic_frame(
    signal_frame: pd.DataFrame,
    *,
    min_assets_per_date: int,
    quantiles: int,
) -> pd.DataFrame:
    """Summarize one row of alpha diagnostics per date."""

    rows: list[dict[str, object]] = []
    for date_value, date_frame in signal_frame.groupby("date", sort=True):
        for signal_name, score_column in _SIGNAL_COLUMNS.items():
            clean = date_frame.dropna(subset=[score_column, "forward_return"]).copy()
            if len(clean) < int(min_assets_per_date):
                continue
            rank_bucket = _signal_quantile_buckets(clean, score_column=score_column, quantiles=quantiles)
            rows.append(
                {
                    "date": str(date_value),
                    "signal_name": signal_name,
                    "observation_count": int(len(clean)),
                    "ic": _safe_correlation(clean[score_column], clean["forward_return"], method="pearson"),
                    "rank_ic": _safe_correlation(clean[score_column], clean["forward_return"], method="spearman"),
                    "top_bottom_spread": _top_bottom_spread(clean, score_column=score_column, quantiles=quantiles),
                    "top_forward_return": float(
                        clean.loc[rank_bucket == quantiles, "forward_return"].mean() or 0.0
                    ),
                    "bottom_forward_return": float(
                        clean.loc[rank_bucket == 1, "forward_return"].mean() or 0.0
                    ),
                }
            )
    if not rows:
        raise InputValidationError("No alpha evaluation dates survived the minimum asset threshold.")
    ic_frame = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    return ic_frame


def build_alpha_signal_summary_frame(ic_frame: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-date diagnostics into one row per evaluated signal."""

    rows: list[dict[str, object]] = []
    for signal_name, signal_ic_frame in ic_frame.groupby("signal_name", sort=False):
        best_rank_ic_row = signal_ic_frame.loc[signal_ic_frame["rank_ic"].idxmax()]
        worst_rank_ic_row = signal_ic_frame.loc[signal_ic_frame["rank_ic"].idxmin()]
        rows.append(
            {
                "signal_name": str(signal_name),
                "evaluation_date_count": int(len(signal_ic_frame)),
                "mean_ic": float(signal_ic_frame["ic"].mean()),
                "mean_rank_ic": float(signal_ic_frame["rank_ic"].mean()),
                "positive_rank_ic_ratio": float((signal_ic_frame["rank_ic"] > 0.0).mean()),
                "mean_top_bottom_spread": float(signal_ic_frame["top_bottom_spread"].mean()),
                "best_rank_ic_date": str(best_rank_ic_row["date"]),
                "best_rank_ic": float(best_rank_ic_row["rank_ic"]),
                "worst_rank_ic_date": str(worst_rank_ic_row["date"]),
                "worst_rank_ic": float(worst_rank_ic_row["rank_ic"]),
            }
        )
    return pd.DataFrame(rows).sort_values(["mean_rank_ic", "signal_name"], ascending=[False, True]).reset_index(drop=True)


def build_alpha_summary_payload(
    signal_frame: pd.DataFrame,
    ic_frame: pd.DataFrame,
    signal_summary_frame: pd.DataFrame,
    *,
    returns_file: Path,
    reversal_lookback_days: int,
    momentum_lookback_days: int,
    momentum_skip_days: int,
    forward_horizon_days: int,
    reversal_weight: float,
    momentum_weight: float,
    min_assets_per_date: int,
    quantiles: int,
) -> dict[str, object]:
    """Build one compact JSON-serializable alpha research summary."""

    signal_summary_lookup = signal_summary_frame.set_index("signal_name")
    if _PRIMARY_SIGNAL_NAME not in signal_summary_lookup.index:
        raise InputValidationError("Primary blended alpha diagnostics are missing from the evaluation summary.")
    primary_signal_row = signal_summary_lookup.loc[_PRIMARY_SIGNAL_NAME]
    best_signal_row = signal_summary_frame.loc[signal_summary_frame["mean_rank_ic"].idxmax()]
    return {
        "returns_file": str(returns_file),
        "date_range": {
            "start": str(signal_frame["date"].min()),
            "end": str(signal_frame["date"].max()),
        },
        "ticker_count": int(signal_frame["ticker"].nunique()),
        "signal_row_count": int(len(signal_frame)),
        "evaluation_date_count": int(primary_signal_row["evaluation_date_count"]),
        "mean_ic": float(primary_signal_row["mean_ic"]),
        "mean_rank_ic": float(primary_signal_row["mean_rank_ic"]),
        "positive_rank_ic_ratio": float(primary_signal_row["positive_rank_ic_ratio"]),
        "mean_top_bottom_spread": float(primary_signal_row["mean_top_bottom_spread"]),
        "best_rank_ic_date": str(primary_signal_row["best_rank_ic_date"]),
        "best_rank_ic": float(primary_signal_row["best_rank_ic"]),
        "worst_rank_ic_date": str(primary_signal_row["worst_rank_ic_date"]),
        "worst_rank_ic": float(primary_signal_row["worst_rank_ic"]),
        "primary_signal_name": _PRIMARY_SIGNAL_NAME,
        "best_signal_name": str(best_signal_row["signal_name"]),
        "best_signal_mean_rank_ic": float(best_signal_row["mean_rank_ic"]),
        "signal_summaries": signal_summary_frame.to_dict(orient="records"),
        "parameters": {
            "reversal_lookback_days": int(reversal_lookback_days),
            "momentum_lookback_days": int(momentum_lookback_days),
            "momentum_skip_days": int(momentum_skip_days),
            "forward_horizon_days": int(forward_horizon_days),
            "reversal_weight": float(reversal_weight),
            "momentum_weight": float(momentum_weight),
            "min_assets_per_date": int(min_assets_per_date),
            "quantiles": int(quantiles),
        },
    }


def run_alpha_research(
    *,
    returns_file: str | Path,
    output_dir: str | Path,
    reversal_lookback_days: int = 21,
    momentum_lookback_days: int = 126,
    momentum_skip_days: int = 21,
    forward_horizon_days: int = 5,
    reversal_weight: float = 0.5,
    momentum_weight: float = 0.5,
    min_assets_per_date: int = 10,
    quantiles: int = 5,
) -> AlphaResearchResult:
    """Run the baseline alpha research workflow and write artifacts."""

    returns_path = Path(returns_file)
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    returns_panel = _load_returns_panel(returns_path)
    signal_frame = build_alpha_research_frame(
        returns_panel,
        reversal_lookback_days=reversal_lookback_days,
        momentum_lookback_days=momentum_lookback_days,
        momentum_skip_days=momentum_skip_days,
        forward_horizon_days=forward_horizon_days,
        reversal_weight=reversal_weight,
        momentum_weight=momentum_weight,
    )
    ic_frame = build_alpha_ic_frame(
        signal_frame,
        min_assets_per_date=min_assets_per_date,
        quantiles=quantiles,
    )
    signal_summary_frame = build_alpha_signal_summary_frame(ic_frame)
    summary_payload = build_alpha_summary_payload(
        signal_frame,
        ic_frame,
        signal_summary_frame,
        returns_file=returns_path,
        reversal_lookback_days=reversal_lookback_days,
        momentum_lookback_days=momentum_lookback_days,
        momentum_skip_days=momentum_skip_days,
        forward_horizon_days=forward_horizon_days,
        reversal_weight=reversal_weight,
        momentum_weight=momentum_weight,
        min_assets_per_date=min_assets_per_date,
        quantiles=quantiles,
    )
    report_markdown = render_alpha_research_report(
        summary_payload,
        ic_frame=ic_frame,
        signal_summary_frame=signal_summary_frame,
    )

    signal_frame.to_csv(output_root / "alpha_signal_panel.csv", index=False)
    ic_frame.to_csv(output_root / "alpha_ic_by_date.csv", index=False)
    signal_summary_frame.to_csv(output_root / "alpha_signal_summary.csv", index=False)
    write_json(output_root / "alpha_research_summary.json", summary_payload)
    write_text(output_root / "alpha_research_report.md", report_markdown)

    return AlphaResearchResult(
        returns_file=returns_path,
        output_dir=output_root,
        signal_frame=signal_frame,
        ic_frame=ic_frame,
        signal_summary_frame=signal_summary_frame,
        summary_payload=summary_payload,
        report_markdown=report_markdown,
    )
