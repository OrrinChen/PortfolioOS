"""Long-horizon alpha diagnostics for monthly signal stress testing."""

from __future__ import annotations

import numpy as np
import pandas as pd

from portfolio_os.alpha.research import build_alpha_ic_frame, build_alpha_research_frame
from portfolio_os.domain.errors import InputValidationError


def _month_end_dates(index: pd.Index) -> pd.DatetimeIndex:
    """Return the actual trading month-end dates present in one index."""

    dates = pd.DatetimeIndex(pd.to_datetime(index)).sort_values()
    month_end = dates.to_series().groupby(dates.to_period("M")).max()
    return pd.DatetimeIndex(month_end.tolist())


def _trailing_compound(return_series: pd.Series, window_days: int) -> pd.Series:
    """Compound one trailing window of simple returns."""

    if window_days <= 0:
        raise InputValidationError("trailing_market_window_days must be positive.")
    return (1.0 + return_series).rolling(window=int(window_days)).apply(np.prod, raw=True) - 1.0


def build_month_end_signal_frame(
    returns_panel: pd.DataFrame,
    *,
    reversal_lookback_days: int,
    momentum_lookback_days: int,
    momentum_skip_days: int,
    forward_horizon_days: int,
    reversal_weight: float,
    momentum_weight: float,
    quantiles: int = 5,
    min_assets_per_date: int = 20,
    trailing_market_window_days: int = 252,
    trailing_signal_window_months: int = 12,
) -> pd.DataFrame:
    """Build one monthly signal diagnostic frame sampled on trading month-end dates."""

    if returns_panel.empty:
        raise InputValidationError("returns_panel cannot be empty.")
    if trailing_signal_window_months <= 0:
        raise InputValidationError("trailing_signal_window_months must be positive.")

    research_frame = build_alpha_research_frame(
        returns_panel,
        reversal_lookback_days=reversal_lookback_days,
        momentum_lookback_days=momentum_lookback_days,
        momentum_skip_days=momentum_skip_days,
        forward_horizon_days=forward_horizon_days,
        reversal_weight=reversal_weight,
        momentum_weight=momentum_weight,
    ).copy()
    research_frame["date"] = pd.to_datetime(research_frame["date"]).dt.normalize()

    monthly_dates = _month_end_dates(returns_panel.index)
    research_frame = research_frame.loc[research_frame["date"].isin(monthly_dates)].copy()
    if research_frame.empty:
        raise InputValidationError("No month-end alpha research rows survived signal construction.")

    ic_frame = build_alpha_ic_frame(
        research_frame.assign(date=research_frame["date"].dt.strftime("%Y-%m-%d")),
        min_assets_per_date=min_assets_per_date,
        quantiles=quantiles,
    )
    monthly_frame = (
        ic_frame.loc[ic_frame["signal_name"] == "blended_alpha"]
        .copy()
        .sort_values("date")
        .reset_index(drop=True)
    )
    if monthly_frame.empty:
        raise InputValidationError("No blended_alpha month-end rows survived the evaluation threshold.")

    monthly_frame["date"] = pd.to_datetime(monthly_frame["date"]).dt.normalize()

    market_return = returns_panel.mean(axis=1, skipna=True)
    market_trailing = _trailing_compound(market_return, window_days=trailing_market_window_days).rename(
        "market_trailing_return"
    )
    market_frame = market_trailing.reset_index()
    first_column = str(market_frame.columns[0])
    if first_column != "date":
        market_frame = market_frame.rename(columns={first_column: "date"})
    market_frame["date"] = pd.to_datetime(market_frame["date"]).dt.normalize()

    monthly_frame = monthly_frame.merge(market_frame, on="date", how="left")
    monthly_frame["trailing_signal_spread"] = (
        pd.to_numeric(monthly_frame["top_bottom_spread"], errors="coerce")
        .shift(1)
        .rolling(window=int(trailing_signal_window_months))
        .mean()
    )
    return monthly_frame.reset_index(drop=True)


def fit_factor_attribution(
    monthly_frame: pd.DataFrame,
    factor_frame: pd.DataFrame,
    *,
    response_column: str,
    factor_columns: list[str],
) -> pd.DataFrame:
    """Fit one plain OLS factor attribution and return tidy coefficients."""

    if not factor_columns:
        raise InputValidationError("factor_columns cannot be empty.")

    left = monthly_frame.loc[:, ["date", response_column]].copy()
    right = factor_frame.loc[:, ["date", *factor_columns]].copy()
    left["date"] = pd.to_datetime(left["date"]).dt.normalize()
    right["date"] = pd.to_datetime(right["date"]).dt.normalize()
    left["month_key"] = left["date"].dt.to_period("M")
    right["month_key"] = right["date"].dt.to_period("M")

    merged = (
        left.merge(right.drop(columns=["date"]), on="month_key", how="inner")
        .dropna()
        .reset_index(drop=True)
    )
    if merged.empty:
        raise InputValidationError("No overlapping observations survived factor attribution merging.")

    y = pd.to_numeric(merged[response_column], errors="coerce").to_numpy(dtype=float)
    X_factors = merged.loc[:, factor_columns].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float)
    X = np.column_stack([np.ones(len(X_factors)), X_factors])

    beta = np.linalg.pinv(X) @ y
    fitted = X @ beta
    residual = y - fitted
    observation_count, parameter_count = X.shape
    dof = max(int(observation_count - parameter_count), 1)
    sigma2 = float((residual @ residual) / float(dof))
    covariance = sigma2 * np.linalg.pinv(X.T @ X)
    std_error = np.sqrt(np.diag(covariance))
    with np.errstate(divide="ignore", invalid="ignore"):
        t_value = np.where(std_error > 0.0, beta / std_error, 0.0)

    total_ss = float(np.sum((y - y.mean()) ** 2))
    residual_ss = float(residual @ residual)
    r_squared = 0.0 if total_ss <= 0.0 else float(1.0 - (residual_ss / total_ss))

    rows: list[dict[str, object]] = []
    for term, coefficient, error, t_stat in zip(["const", *factor_columns], beta, std_error, t_value):
        rows.append(
            {
                "response_column": response_column,
                "term": str(term),
                "beta": float(coefficient),
                "std_error": float(error),
                "t_value": float(t_stat),
                "r_squared": float(r_squared),
                "observation_count": int(observation_count),
            }
        )
    return pd.DataFrame(rows)


def build_single_factor_residual_frame(
    monthly_frame: pd.DataFrame,
    factor_frame: pd.DataFrame,
    *,
    response_column: str,
    factor_column: str,
) -> pd.DataFrame:
    """Build one month-aligned residual frame for a single factor."""

    left = monthly_frame.loc[:, ["date", response_column]].copy()
    right = factor_frame.loc[:, ["date", factor_column]].copy()
    left["date"] = pd.to_datetime(left["date"]).dt.normalize()
    right["date"] = pd.to_datetime(right["date"]).dt.normalize()
    left["month_key"] = left["date"].dt.to_period("M")
    right["month_key"] = right["date"].dt.to_period("M")

    merged = (
        left.merge(right.drop(columns=["date"]), on="month_key", how="inner")
        .dropna()
        .reset_index(drop=True)
    )
    if merged.empty:
        raise InputValidationError("No overlapping observations survived single-factor residual merging.")

    y = pd.to_numeric(merged[response_column], errors="coerce").to_numpy(dtype=float)
    factor_value = pd.to_numeric(merged[factor_column], errors="coerce").to_numpy(dtype=float)
    X = np.column_stack([np.ones(len(factor_value)), factor_value])
    beta = np.linalg.pinv(X) @ y
    intercept = float(beta[0])
    factor_beta = float(beta[1])

    merged["intercept"] = intercept
    merged["factor_beta"] = factor_beta
    merged["response_value"] = pd.to_numeric(merged[response_column], errors="coerce")
    merged["factor_value"] = pd.to_numeric(merged[factor_column], errors="coerce")
    merged["factor_component"] = factor_beta * merged[factor_column]
    merged["factor_residual"] = merged["response_value"] - merged["factor_component"]
    merged["model_residual"] = merged["response_value"] - (intercept + merged["factor_component"])

    response_abs = merged["response_value"].abs()
    same_direction = np.sign(merged["response_value"]) == np.sign(
        pd.to_numeric(merged["factor_component"], errors="coerce")
    )
    opposite_direction = np.sign(merged["response_value"]) == -np.sign(
        pd.to_numeric(merged["factor_component"], errors="coerce")
    )
    with np.errstate(divide="ignore", invalid="ignore"):
        merged["signed_absorption_share"] = np.where(
            response_abs > 0.0,
            pd.to_numeric(merged["factor_component"], errors="coerce") / merged["response_value"],
            np.nan,
        )
        merged["matching_absorption_share"] = np.where(
            (response_abs > 0.0) & same_direction,
            pd.to_numeric(merged["factor_component"], errors="coerce").abs() / response_abs,
            0.0,
        )
        merged["offsetting_share"] = np.where(
            (response_abs > 0.0) & opposite_direction,
            pd.to_numeric(merged["factor_component"], errors="coerce").abs() / response_abs,
            0.0,
        )
    return merged


def build_focus_month_absorption_summary(
    residual_frame: pd.DataFrame,
    *,
    focus_months: list[str],
) -> pd.DataFrame:
    """Extract one tidy absorption summary for selected focus months."""

    if residual_frame.empty:
        return pd.DataFrame(
            columns=[
                "focus_month",
                "date",
                "top_bottom_spread",
                "factor_value",
                "factor_component",
                "factor_residual",
                "model_residual",
                "matching_absorption_share",
                "offsetting_share",
            ]
        )

    work = residual_frame.copy()
    work["focus_month"] = work["month_key"].astype(str)
    wanted = {str(month) for month in focus_months}
    work = work.loc[work["focus_month"].isin(wanted)].copy()
    if work.empty:
        return pd.DataFrame(
            columns=[
                "focus_month",
                "date",
                "top_bottom_spread",
                "factor_value",
                "factor_component",
                "factor_residual",
                "model_residual",
                "matching_absorption_share",
                "offsetting_share",
            ]
        )

    return (
        work.loc[
            :,
            [
                "focus_month",
                "date",
                "response_value",
                "factor_value",
                "factor_component",
                "factor_residual",
                "model_residual",
                "matching_absorption_share",
                "offsetting_share",
            ],
        ]
        .rename(columns={"response_value": "top_bottom_spread"})
        .sort_values("date")
        .reset_index(drop=True)
    )


def classify_absorption_regime(
    focus_summary: pd.DataFrame,
    *,
    share_column: str = "matching_absorption_share",
    high_threshold: float = 0.70,
    low_threshold: float = 0.40,
) -> str:
    """Classify whether a crash set is mostly momentum-absorbed or mostly residual."""

    if focus_summary.empty:
        raise InputValidationError("focus_summary cannot be empty for absorption classification.")
    mean_share = float(pd.to_numeric(focus_summary[share_column], errors="coerce").dropna().mean())
    if mean_share >= float(high_threshold):
        return "momentum_absorbed"
    if mean_share <= float(low_threshold):
        return "independent_residual"
    return "mixed"


def _normalize_frame_map(frames_by_name: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Normalize date columns across one labeled monthly-frame map."""

    if not frames_by_name:
        raise InputValidationError("frames_by_name cannot be empty.")
    normalized: dict[str, pd.DataFrame] = {}
    for label, frame in frames_by_name.items():
        work = frame.copy()
        if "date" not in work.columns:
            raise InputValidationError(f"Frame '{label}' is missing the required 'date' column.")
        work["date"] = pd.to_datetime(work["date"]).dt.normalize()
        normalized[str(label)] = work.sort_values("date").reset_index(drop=True)
    return normalized


def build_shared_date_frame_map(frames_by_name: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Filter all labeled monthly frames down to their shared calendar months."""

    normalized = _normalize_frame_map(frames_by_name)
    shared_dates: set[pd.Timestamp] | None = None
    for frame in normalized.values():
        frame_dates = set(pd.DatetimeIndex(frame["date"]))
        shared_dates = frame_dates if shared_dates is None else shared_dates & frame_dates
    if shared_dates is None:
        raise InputValidationError("frames_by_name cannot be empty.")

    return {
        label: frame.loc[frame["date"].isin(shared_dates)].sort_values("date").reset_index(drop=True)
        for label, frame in normalized.items()
    }


def build_frame_overlap_summary(frames_by_name: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Summarize month coverage overlap across labeled monthly frames."""

    normalized = _normalize_frame_map(frames_by_name)
    shared = build_shared_date_frame_map(normalized)
    shared_month_count = int(len(next(iter(shared.values()))["date"].unique())) if shared else 0

    rows: list[dict[str, object]] = []
    for label, frame in normalized.items():
        month_count = int(frame["date"].nunique())
        mean_observation_count = float("nan")
        if "observation_count" in frame.columns and not frame.empty:
            mean_observation_count = float(pd.to_numeric(frame["observation_count"], errors="coerce").mean())
        rows.append(
            {
                "frame_label": str(label),
                "month_count": month_count,
                "shared_month_count": shared_month_count,
                "exclusive_month_count": int(month_count - shared_month_count),
                "shared_month_ratio": float(shared_month_count / month_count) if month_count else 0.0,
                "mean_observation_count": mean_observation_count,
            }
        )
    return pd.DataFrame(rows).sort_values("frame_label").reset_index(drop=True)


def build_horizon_factor_ladder(
    frames_by_name: dict[str, pd.DataFrame],
    factor_frame: pd.DataFrame,
    *,
    response_column: str,
    factor_columns: list[str],
    use_shared_dates: bool = True,
) -> pd.DataFrame:
    """Fit identical factor attributions across multiple horizon frames."""

    normalized = _normalize_frame_map(frames_by_name)
    work_map = build_shared_date_frame_map(normalized) if use_shared_dates else normalized
    rows: list[pd.DataFrame] = []
    for label, frame in work_map.items():
        if frame.empty:
            continue
        attribution = fit_factor_attribution(
            frame,
            factor_frame,
            response_column=response_column,
            factor_columns=factor_columns,
        ).copy()
        attribution.insert(0, "frame_label", str(label))
        attribution.insert(1, "month_count", int(frame["date"].nunique()))
        rows.append(attribution)
    if not rows:
        raise InputValidationError("No factor attributions could be fit from the provided frame map.")
    return pd.concat(rows, ignore_index=True)


def build_spread_distribution_summary(
    monthly_frame: pd.DataFrame,
    *,
    spread_column: str = "top_bottom_spread",
    worst_n: int = 10,
) -> pd.DataFrame:
    """Summarize unconditional spread distribution, tail concentration, and drawdown."""

    if worst_n <= 0:
        raise InputValidationError("worst_n must be positive.")
    work = monthly_frame.loc[:, ["date", spread_column]].copy()
    work["date"] = pd.to_datetime(work["date"]).dt.normalize()
    work[spread_column] = pd.to_numeric(work[spread_column], errors="coerce")
    work = work.dropna(subset=[spread_column]).sort_values("date").reset_index(drop=True)
    if work.empty:
        raise InputValidationError("monthly_frame contains no valid spread observations.")

    series = work[spread_column].astype(float)
    wealth = (1.0 + series).cumprod()
    running_peak = wealth.cummax()
    drawdown = wealth / running_peak - 1.0

    negative_abs = series.loc[series < 0.0].abs()
    negative_sum = float(negative_abs.sum())
    worst_abs = negative_abs.sort_values(ascending=False).reset_index(drop=True)

    def _tail_share(count: int) -> float:
        if negative_sum <= 0.0:
            return 0.0
        return float(worst_abs.head(int(count)).sum() / negative_sum)

    worst_idx = int(series.idxmin())
    max_drawdown_idx = int(drawdown.idxmin())
    return pd.DataFrame(
        [
            {
                "observation_count": int(len(work)),
                "mean_spread": float(series.mean()),
                "std_spread": float(series.std(ddof=1)) if len(series) > 1 else 0.0,
                "skew": float(series.skew()),
                "excess_kurtosis": float(series.kurt()),
                "min_spread": float(series.min()),
                "p01_spread": float(series.quantile(0.01)),
                "p05_spread": float(series.quantile(0.05)),
                "median_spread": float(series.median()),
                "p95_spread": float(series.quantile(0.95)),
                "p99_spread": float(series.quantile(0.99)),
                "max_spread": float(series.max()),
                "negative_month_ratio": float((series < 0.0).mean()),
                "worst_month_date": pd.Timestamp(work.loc[worst_idx, "date"]).strftime("%Y-%m-%d"),
                "worst_month_spread": float(series.min()),
                "worst_1_negative_share": _tail_share(1),
                "worst_3_negative_share": _tail_share(min(3, len(worst_abs))),
                "worst_n_negative_share": _tail_share(min(int(worst_n), len(worst_abs))),
                "max_drawdown_date": pd.Timestamp(work.loc[max_drawdown_idx, "date"]).strftime("%Y-%m-%d"),
                "max_drawdown": float(drawdown.min()),
            }
        ]
    )


def build_factor_focus_pressure_summary(
    focus_summaries_by_factor: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Summarize crash-window absorption pressure across multiple candidate factors."""

    if not focus_summaries_by_factor:
        raise InputValidationError("focus_summaries_by_factor cannot be empty.")

    rows: list[dict[str, object]] = []
    for factor_name, focus_summary in focus_summaries_by_factor.items():
        if focus_summary.empty:
            rows.append(
                {
                    "factor_name": str(factor_name),
                    "focus_month_count": 0,
                    "mean_matching_absorption_share": float("nan"),
                    "mean_offsetting_share": float("nan"),
                    "classification": "no_focus_months",
                }
            )
            continue
        rows.append(
            {
                "factor_name": str(factor_name),
                "focus_month_count": int(len(focus_summary)),
                "mean_matching_absorption_share": float(
                    pd.to_numeric(focus_summary["matching_absorption_share"], errors="coerce").mean()
                ),
                "mean_offsetting_share": float(
                    pd.to_numeric(focus_summary["offsetting_share"], errors="coerce").mean()
                ),
                "classification": classify_absorption_regime(focus_summary),
            }
        )
    return pd.DataFrame(rows).sort_values("factor_name").reset_index(drop=True)


def build_bad_month_cohort_membership(
    monthly_frame: pd.DataFrame,
    *,
    spread_column: str = "top_bottom_spread",
    bad_quantile: float = 0.20,
) -> pd.DataFrame:
    """Assign each month into outer-half, inner-half, or non-bad cohorts."""

    if not 0.0 < float(bad_quantile) < 1.0:
        raise InputValidationError("bad_quantile must be between 0 and 1.")

    work = monthly_frame.loc[:, ["date", spread_column]].copy()
    work["date"] = pd.to_datetime(work["date"]).dt.normalize()
    work[spread_column] = pd.to_numeric(work[spread_column], errors="coerce")
    work = work.dropna(subset=[spread_column]).sort_values("date").reset_index(drop=True)
    if work.empty:
        raise InputValidationError("monthly_frame contains no valid spread observations.")

    bad_count = int(np.ceil(len(work) * float(bad_quantile)))
    bad_count = max(1, bad_count)
    ranking = work.sort_values([spread_column, "date"], ascending=[True, True]).reset_index(drop=True)
    ranking["spread_rank_ascending"] = np.arange(1, len(ranking) + 1, dtype=int)
    ranking["is_bad_month"] = ranking["spread_rank_ascending"] <= bad_count
    outer_count = bad_count // 2
    if bad_count > 0 and outer_count == 0:
        outer_count = 1

    ranking["cohort_label"] = "non_bad"
    ranking.loc[ranking["spread_rank_ascending"] <= outer_count, "cohort_label"] = "outer_half"
    ranking.loc[
        (ranking["spread_rank_ascending"] > outer_count) & (ranking["spread_rank_ascending"] <= bad_count),
        "cohort_label",
    ] = "inner_half"
    ranking["bad_month_rank"] = np.where(ranking["is_bad_month"], ranking["spread_rank_ascending"], np.nan)
    return ranking.sort_values("date").reset_index(drop=True)


def build_bootstrap_null_summary(
    *,
    observed_value: float,
    bootstrap_values: np.ndarray | pd.Series | list[float],
    comparison_name: str,
) -> pd.DataFrame:
    """Summarize one observed statistic against one bootstrap null distribution."""

    clean = pd.to_numeric(pd.Series(bootstrap_values), errors="coerce").dropna()
    if clean.empty:
        raise InputValidationError("bootstrap_values must contain at least one numeric observation.")

    bootstrap_median = float(clean.median())
    return pd.DataFrame(
        [
            {
                "comparison_name": str(comparison_name),
                "observed_value": float(observed_value),
                "bootstrap_observation_count": int(len(clean)),
                "bootstrap_mean": float(clean.mean()),
                "bootstrap_median": bootstrap_median,
                "bootstrap_p05": float(clean.quantile(0.05)),
                "bootstrap_p95": float(clean.quantile(0.95)),
                "observed_to_bootstrap_median_ratio": (
                    float(observed_value) / bootstrap_median if abs(bootstrap_median) > 1e-15 else np.nan
                ),
                "bootstrap_percentile": float((clean <= float(observed_value)).mean()),
            }
        ]
    )


def build_temporal_distribution_summary(
    membership_frame: pd.DataFrame,
    *,
    cohort_column: str = "cohort_label",
) -> pd.DataFrame:
    """Summarize calendar-year distributions by cohort."""

    work = membership_frame.loc[:, ["date", cohort_column]].copy()
    work["date"] = pd.to_datetime(work["date"]).dt.normalize()
    work = work.dropna(subset=["date", cohort_column]).reset_index(drop=True)
    if work.empty:
        raise InputValidationError("membership_frame must contain cohort-labelled dates.")
    work["year"] = work["date"].dt.year.astype(float)

    rows: list[dict[str, object]] = []
    for cohort_label, cohort_frame in work.groupby(cohort_column, sort=True):
        q1 = float(cohort_frame["year"].quantile(0.25))
        q3 = float(cohort_frame["year"].quantile(0.75))
        year_counts = (
            cohort_frame["year"]
            .astype(int)
            .value_counts()
            .sort_index()
            .astype(int)
            .to_dict()
        )
        rows.append(
            {
                "cohort_label": str(cohort_label),
                "month_count": int(len(cohort_frame)),
                "median_year": float(cohort_frame["year"].median()),
                "q1_year": q1,
                "q3_year": q3,
                "year_iqr": float(q3 - q1),
                "min_year": int(cohort_frame["year"].min()),
                "max_year": int(cohort_frame["year"].max()),
                "year_count_map": str(year_counts),
            }
        )
    return pd.DataFrame(rows).sort_values("cohort_label").reset_index(drop=True)


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


def build_leg_concentration_metrics(
    leg_frame: pd.DataFrame,
    *,
    leg_column: str = "leg_label",
    adverse_contribution_column: str = "adverse_contribution",
    loss_floor: float = 0.005,
) -> pd.DataFrame:
    """Compute per-leg HHI and effective N with a small-loss guard."""

    if loss_floor < 0.0:
        raise InputValidationError("loss_floor cannot be negative.")

    work = leg_frame.loc[:, [leg_column, adverse_contribution_column]].copy()
    work[adverse_contribution_column] = pd.to_numeric(work[adverse_contribution_column], errors="coerce")
    work = work.dropna(subset=[leg_column, adverse_contribution_column]).reset_index(drop=True)
    if work.empty:
        raise InputValidationError("leg_frame must contain valid leg labels and adverse contributions.")

    rows: list[dict[str, object]] = []
    for leg_label, leg_rows in work.groupby(leg_column, sort=True):
        total_abs_loss = float(leg_rows[adverse_contribution_column].abs().sum())
        if total_abs_loss < float(loss_floor):
            rows.append(
                {
                    "leg_label": str(leg_label),
                    "observation_count": int(len(leg_rows)),
                    "total_abs_loss": total_abs_loss,
                    "hhi": np.nan,
                    "effective_n": np.nan,
                }
            )
            continue
        shares = leg_rows[adverse_contribution_column].abs() / total_abs_loss
        hhi = float((shares**2).sum())
        rows.append(
            {
                "leg_label": str(leg_label),
                "observation_count": int(len(leg_rows)),
                "total_abs_loss": total_abs_loss,
                "hhi": hhi,
                "effective_n": float(1.0 / hhi) if hhi > 0.0 else np.nan,
            }
        )
    return pd.DataFrame(rows).sort_values("leg_label").reset_index(drop=True)


def _mean_vector_by_month(
    vector_frame: pd.DataFrame,
    *,
    month_column: str,
    key_column: str,
    value_column: str,
) -> pd.DataFrame:
    """Build one date x key matrix from one long vector frame."""

    work = vector_frame.loc[:, [month_column, key_column, value_column]].copy()
    work[month_column] = pd.to_datetime(work[month_column]).dt.normalize()
    work[value_column] = pd.to_numeric(work[value_column], errors="coerce")
    work = work.dropna(subset=[month_column, key_column, value_column])
    if work.empty:
        raise InputValidationError("vector_frame produced an empty month-by-key matrix.")
    return (
        work.groupby([month_column, key_column], sort=True)[value_column]
        .mean()
        .unstack()
        .sort_index()
    )


def _aligned_vector_metric(
    left: pd.Series,
    right: pd.Series,
    *,
    metric_name: str,
) -> float:
    """Evaluate one metric on two aligned vectors."""

    aligned = pd.concat([left.rename("left"), right.rename("right")], axis=1).fillna(0.0)
    if metric_name == "l1_distance":
        return float((aligned["left"] - aligned["right"]).abs().sum())
    if metric_name == "rank_correlation":
        return _safe_correlation(aligned["left"], aligned["right"], method="spearman")
    raise InputValidationError(f"Unsupported metric_name: {metric_name}")


def build_cohort_vector_comparison_summary(
    vector_frame: pd.DataFrame,
    *,
    dimension_name: str,
    metric_name: str,
    metric_direction: str,
    month_column: str = "date",
    cohort_column: str = "cohort_label",
    key_column: str = "vector_key",
    value_column: str = "vector_value",
    outer_label: str = "outer_half",
    inner_label: str = "inner_half",
    non_bad_label: str = "non_bad",
    bootstrap_iterations: int = 5000,
    random_seed: int = 7,
) -> pd.DataFrame:
    """Compare outer-vs-inner cohort vectors against non-bad bootstrap nulls."""

    if bootstrap_iterations <= 0:
        raise InputValidationError("bootstrap_iterations must be positive.")
    if metric_direction not in {"distance", "similarity"}:
        raise InputValidationError("metric_direction must be 'distance' or 'similarity'.")

    work = vector_frame.loc[:, [month_column, cohort_column, key_column, value_column]].copy()
    work[month_column] = pd.to_datetime(work[month_column]).dt.normalize()
    work = work.dropna(subset=[month_column, cohort_column, key_column, value_column]).reset_index(drop=True)
    if work.empty:
        raise InputValidationError("vector_frame must contain valid cohort vectors.")

    outer_matrix = _mean_vector_by_month(
        work.loc[work[cohort_column] == outer_label].copy(),
        month_column=month_column,
        key_column=key_column,
        value_column=value_column,
    )
    inner_matrix = _mean_vector_by_month(
        work.loc[work[cohort_column] == inner_label].copy(),
        month_column=month_column,
        key_column=key_column,
        value_column=value_column,
    )
    non_bad_matrix = _mean_vector_by_month(
        work.loc[work[cohort_column] == non_bad_label].copy(),
        month_column=month_column,
        key_column=key_column,
        value_column=value_column,
    )

    if len(non_bad_matrix) < max(len(outer_matrix), len(inner_matrix)):
        raise InputValidationError("non-bad cohort does not contain enough months for the requested bootstrap sizes.")

    all_keys = outer_matrix.columns.union(inner_matrix.columns).union(non_bad_matrix.columns)
    outer_vector = outer_matrix.reindex(columns=all_keys).mean(axis=0, skipna=True).fillna(0.0)
    inner_vector = inner_matrix.reindex(columns=all_keys).mean(axis=0, skipna=True).fillna(0.0)
    non_bad_matrix = non_bad_matrix.reindex(columns=all_keys)

    outer_inner_metric = _aligned_vector_metric(outer_vector, inner_vector, metric_name=metric_name)
    rng = np.random.default_rng(int(random_seed))
    non_bad_dates = non_bad_matrix.index.to_numpy()

    outer_bootstrap_values: list[float] = []
    inner_bootstrap_values: list[float] = []
    for _ in range(int(bootstrap_iterations)):
        outer_sample = rng.choice(non_bad_dates, size=len(outer_matrix), replace=False)
        inner_sample = rng.choice(non_bad_dates, size=len(inner_matrix), replace=False)
        outer_sample_vector = non_bad_matrix.loc[outer_sample].mean(axis=0, skipna=True).fillna(0.0)
        inner_sample_vector = non_bad_matrix.loc[inner_sample].mean(axis=0, skipna=True).fillna(0.0)
        outer_bootstrap_values.append(
            _aligned_vector_metric(outer_vector, outer_sample_vector, metric_name=metric_name)
        )
        inner_bootstrap_values.append(
            _aligned_vector_metric(inner_vector, inner_sample_vector, metric_name=metric_name)
        )

    outer_bootstrap = pd.to_numeric(pd.Series(outer_bootstrap_values), errors="coerce").dropna()
    inner_bootstrap = pd.to_numeric(pd.Series(inner_bootstrap_values), errors="coerce").dropna()

    if metric_direction == "distance":
        outer_percentile = float((outer_bootstrap <= outer_inner_metric).mean())
        inner_percentile = float((inner_bootstrap <= outer_inner_metric).mean())
    else:
        outer_percentile = float((outer_bootstrap >= outer_inner_metric).mean())
        inner_percentile = float((inner_bootstrap >= outer_inner_metric).mean())

    outer_median = float(outer_bootstrap.median())
    inner_median = float(inner_bootstrap.median())
    return pd.DataFrame(
        [
            {
                "dimension_name": str(dimension_name),
                "metric_name": str(metric_name),
                "metric_direction": str(metric_direction),
                "outer_month_count": int(len(outer_matrix)),
                "inner_month_count": int(len(inner_matrix)),
                "non_bad_month_count": int(len(non_bad_matrix)),
                "bootstrap_iterations": int(bootstrap_iterations),
                "random_seed": int(random_seed),
                "outer_inner_metric": float(outer_inner_metric),
                "outer_non_bad_bootstrap_median": outer_median,
                "outer_non_bad_bootstrap_p05": float(outer_bootstrap.quantile(0.05)),
                "outer_non_bad_bootstrap_p95": float(outer_bootstrap.quantile(0.95)),
                "outer_inner_vs_outer_non_bad_ratio": (
                    float(outer_inner_metric / outer_median) if abs(outer_median) > 1e-15 else np.nan
                ),
                "outer_inner_vs_outer_non_bad_percentile": outer_percentile,
                "inner_non_bad_bootstrap_median": inner_median,
                "inner_non_bad_bootstrap_p05": float(inner_bootstrap.quantile(0.05)),
                "inner_non_bad_bootstrap_p95": float(inner_bootstrap.quantile(0.95)),
                "outer_inner_vs_inner_non_bad_ratio": (
                    float(outer_inner_metric / inner_median) if abs(inner_median) > 1e-15 else np.nan
                ),
                "outer_inner_vs_inner_non_bad_percentile": inner_percentile,
            }
        ]
    )


def build_conditional_spread_summary(
    monthly_frame: pd.DataFrame,
    *,
    spread_column: str = "top_bottom_spread",
) -> pd.DataFrame:
    """Summarize spread behavior across simple market and trailing-signal states."""

    work = monthly_frame.copy()
    work[spread_column] = pd.to_numeric(work[spread_column], errors="coerce")
    work["market_trailing_return"] = pd.to_numeric(work["market_trailing_return"], errors="coerce")
    work["trailing_signal_spread"] = pd.to_numeric(work["trailing_signal_spread"], errors="coerce")

    rows: list[dict[str, object]] = []

    market_ready = work.dropna(subset=[spread_column, "market_trailing_return"]).copy()
    if not market_ready.empty:
        threshold = float(market_ready["market_trailing_return"].median())
        market_ready["bucket"] = np.where(
            market_ready["market_trailing_return"] >= threshold,
            "high_12m_market",
            "low_12m_market",
        )
        for bucket, bucket_frame in market_ready.groupby("bucket", sort=True):
            rows.append(
                {
                    "split_name": "market_state",
                    "bucket": str(bucket),
                    "count": int(len(bucket_frame)),
                    "mean_spread": float(bucket_frame[spread_column].mean()),
                    "median_spread": float(bucket_frame[spread_column].median()),
                }
            )

    signal_ready = work.dropna(subset=[spread_column, "trailing_signal_spread"]).copy()
    if not signal_ready.empty:
        signal_ready["bucket"] = np.where(
            signal_ready["trailing_signal_spread"] > 0.0,
            "positive_trailing_spread",
            "nonpositive_trailing_spread",
        )
        for bucket, bucket_frame in signal_ready.groupby("bucket", sort=True):
            rows.append(
                {
                    "split_name": "signal_state",
                    "bucket": str(bucket),
                    "count": int(len(bucket_frame)),
                    "mean_spread": float(bucket_frame[spread_column].mean()),
                    "median_spread": float(bucket_frame[spread_column].median()),
                }
            )

    return pd.DataFrame(rows)
