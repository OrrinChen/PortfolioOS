"""Run a narrow long-horizon stress study for the US momentum-family candidate."""

from __future__ import annotations

import io
import json
import re
import zipfile
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import yfinance as yf

from portfolio_os.alpha.long_horizon import (
    build_bad_month_cohort_membership,
    build_cohort_vector_comparison_summary,
    build_focus_month_absorption_summary,
    build_factor_focus_pressure_summary,
    build_frame_overlap_summary,
    build_horizon_factor_ladder,
    build_leg_concentration_metrics,
    build_conditional_spread_summary,
    build_month_end_signal_frame,
    build_spread_distribution_summary,
    build_shared_date_frame_map,
    build_temporal_distribution_summary,
    build_single_factor_residual_frame,
    classify_absorption_regime,
    fit_factor_attribution,
)
from portfolio_os.alpha.research import build_alpha_research_frame


REPO_ROOT = Path(__file__).resolve().parents[1]
UNIVERSE_TICKERS_PATH = REPO_ROOT / "data" / "universe" / "us_equity_expanded_tickers.txt"
OUTPUT_ROOT = REPO_ROOT / "outputs"
START_DATE = "2006-05-01"
END_DATE = (date.today() + timedelta(days=1)).isoformat()

_FRENCH_FF3_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_CSV.zip"
_FRENCH_MOM_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_CSV.zip"
_FRENCH_STR_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_ST_Reversal_Factor_CSV.zip"
_FRENCH_LTR_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_LT_Reversal_Factor_CSV.zip"
_AQR_QMJ_URL = "https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/Quality-Minus-Junk-Factors-Monthly.xlsx"
_AQR_BAB_URL = "https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/Betting-Against-Beta-Equity-Factors-Monthly.xlsx"
_HORIZON_DAYS = [5, 10, 15, 21]
CANONICAL_SIGNAL_SPEC = {
    "reversal_lookback_days": 21,
    "momentum_lookback_days": 84,
    "momentum_skip_days": 21,
    "forward_horizon_days": 21,
    "reversal_weight": 0.0,
    "momentum_weight": 1.0,
}


def _load_universe_tickers(path: Path) -> list[str]:
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def _download_close_panel(tickers: list[str]) -> pd.DataFrame:
    frame = yf.download(
        tickers,
        start=START_DATE,
        end=END_DATE,
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=True,
    )
    close_panel = pd.DataFrame(
        {
            ticker: frame[(ticker, "Close")]
            for ticker in tickers
            if (ticker, "Close") in frame.columns
        }
    ).sort_index()
    if close_panel.empty:
        raise RuntimeError("yfinance returned no close data for the requested universe.")
    close_panel.index = pd.to_datetime(close_panel.index).normalize()
    return close_panel


def _build_price_coverage_frame(close_panel: pd.DataFrame, requested_tickers: list[str]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    available = set(close_panel.columns.astype(str))
    for ticker in requested_tickers:
        if ticker not in available:
            rows.append(
                {
                    "ticker": ticker,
                    "available": False,
                    "start_date": None,
                    "end_date": None,
                    "observation_count": 0,
                }
            )
            continue
        series = close_panel[ticker].dropna()
        rows.append(
            {
                "ticker": ticker,
                "available": True,
                "start_date": str(series.index.min().date()),
                "end_date": str(series.index.max().date()),
                "observation_count": int(series.shape[0]),
            }
        )
    return pd.DataFrame(rows).sort_values(["available", "ticker"], ascending=[False, True]).reset_index(drop=True)


def _load_ff3_monthly() -> pd.DataFrame:
    frame = pd.read_csv(_FRENCH_FF3_URL, compression="zip", skiprows=3)
    frame = frame.rename(columns={frame.columns[0]: "date"})
    frame = frame[pd.to_numeric(frame["date"], errors="coerce").notna()].copy()
    frame = frame[frame["date"].astype(int).astype(str).str.len() == 6].copy()
    frame["date"] = pd.to_datetime(frame["date"].astype(int).astype(str), format="%Y%m") + pd.offsets.MonthEnd(0)
    for column in ["Mkt-RF", "SMB", "HML", "RF"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce") / 100.0
    return frame.loc[:, ["date", "Mkt-RF", "SMB", "HML", "RF"]].dropna().reset_index(drop=True)


def _load_french_single_factor_monthly(url: str, *, factor_name: str) -> pd.DataFrame:
    response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    archive = zipfile.ZipFile(io.BytesIO(response.content))
    text = archive.read(archive.namelist()[0]).decode("latin1").splitlines()
    rows: list[tuple[str, float]] = []
    for line in text:
        match = re.match(r"^(\d{6}),\s*([-0-9.]+)\s*$", line)
        if match:
            rows.append((match.group(1), float(match.group(2)) / 100.0))
    frame = pd.DataFrame(rows, columns=["date", factor_name])
    frame["date"] = pd.to_datetime(frame["date"], format="%Y%m") + pd.offsets.MonthEnd(0)
    return frame.dropna().reset_index(drop=True)


def _load_french_momentum_monthly() -> pd.DataFrame:
    return _load_french_single_factor_monthly(_FRENCH_MOM_URL, factor_name="Mom")


def _load_french_short_term_reversal_monthly() -> pd.DataFrame:
    return _load_french_single_factor_monthly(_FRENCH_STR_URL, factor_name="STR")


def _load_french_long_term_reversal_monthly() -> pd.DataFrame:
    return _load_french_single_factor_monthly(_FRENCH_LTR_URL, factor_name="LTR")


def _load_aqr_monthly_factor(url: str, *, sheet_name: str, factor_name: str) -> pd.DataFrame:
    frame = pd.read_excel(url, sheet_name=sheet_name, header=18)
    frame = frame.loc[:, ["DATE", "USA"]].rename(columns={"DATE": "date", "USA": factor_name})
    frame["date"] = pd.to_datetime(frame["date"])
    frame[factor_name] = pd.to_numeric(frame[factor_name], errors="coerce")
    return frame.dropna().reset_index(drop=True)


def _load_style_factor_frame() -> pd.DataFrame:
    ff3 = _load_ff3_monthly()
    mom = _load_french_momentum_monthly()
    str_factor = _load_french_short_term_reversal_monthly()
    ltr_factor = _load_french_long_term_reversal_monthly()
    qmj = _load_aqr_monthly_factor(_AQR_QMJ_URL, sheet_name="QMJ Factors", factor_name="QMJ")
    bab = _load_aqr_monthly_factor(_AQR_BAB_URL, sheet_name="BAB Factors", factor_name="BAB")
    factors = (
        ff3.merge(mom, on="date", how="inner")
        .merge(str_factor, on="date", how="inner")
        .merge(ltr_factor, on="date", how="inner")
        .merge(qmj, on="date", how="inner")
        .merge(bab, on="date", how="inner")
    )
    return factors.sort_values("date").reset_index(drop=True)


def _stack_panel(panel: pd.DataFrame, *, value_name: str) -> pd.DataFrame:
    work = panel.copy()
    work.index = pd.to_datetime(work.index).normalize()
    work.index.name = "date"
    return work.reset_index().melt(id_vars="date", var_name="ticker", value_name=value_name)


def _cross_sectional_quintiles(values: pd.Series) -> pd.Series:
    ranked = values.rank(method="first", pct=True)
    return np.ceil(ranked * 5).clip(1, 5)


def _load_static_label_frame(tickers: list[str]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for ticker in tickers:
        try:
            info = yf.Ticker(ticker).info or {}
        except Exception:
            info = {}
        rows.append(
            {
                "ticker": str(ticker),
                "sector_label": str(info.get("sector") or "Unknown"),
                "industry_label": str(info.get("industry") or "Unknown"),
            }
        )
    return pd.DataFrame(rows).sort_values("ticker").reset_index(drop=True)


def _load_shares_panel(tickers: list[str], price_index: pd.DatetimeIndex) -> pd.DataFrame:
    shares_by_ticker: dict[str, pd.Series] = {}
    for ticker in tickers:
        aligned = pd.Series(index=price_index, dtype=float)
        try:
            series = yf.Ticker(ticker).get_shares_full(start=START_DATE, end=END_DATE)
        except Exception:
            series = None
        if series is None or len(series) == 0:
            shares_by_ticker[str(ticker)] = aligned
            continue
        share_index = pd.DatetimeIndex(pd.to_datetime(series.index))
        if share_index.tz is not None:
            share_index = share_index.tz_convert(None)
        share_index = share_index.normalize()
        normalized = pd.Series(pd.to_numeric(series.to_numpy(), errors="coerce"), index=share_index)
        normalized = normalized.groupby(level=0).last().sort_index()
        aligned = normalized.reindex(price_index, method="ffill")
        shares_by_ticker[str(ticker)] = aligned.astype(float)
    return pd.DataFrame(shares_by_ticker, index=price_index).sort_index()


def _build_operational_cross_section_frame(
    returns_panel: pd.DataFrame,
    close_panel: pd.DataFrame,
    operational_frame: pd.DataFrame,
    static_labels: pd.DataFrame,
    shares_panel: pd.DataFrame,
) -> pd.DataFrame:
    research_frame = build_alpha_research_frame(
        returns_panel,
        reversal_lookback_days=int(CANONICAL_SIGNAL_SPEC["reversal_lookback_days"]),
        momentum_lookback_days=int(CANONICAL_SIGNAL_SPEC["momentum_lookback_days"]),
        momentum_skip_days=int(CANONICAL_SIGNAL_SPEC["momentum_skip_days"]),
        forward_horizon_days=int(CANONICAL_SIGNAL_SPEC["forward_horizon_days"]),
        reversal_weight=float(CANONICAL_SIGNAL_SPEC["reversal_weight"]),
        momentum_weight=float(CANONICAL_SIGNAL_SPEC["momentum_weight"]),
    ).copy()
    research_frame["date"] = pd.to_datetime(research_frame["date"]).dt.normalize()
    month_dates = set(pd.to_datetime(operational_frame["date"]).dt.normalize())
    research_frame = research_frame.loc[research_frame["date"].isin(month_dates)].copy()
    research_frame["quantile"] = (
        research_frame.groupby("date")["alpha_score"].transform(_cross_sectional_quintiles).astype(int)
    )
    research_frame["leg_label"] = np.where(
        research_frame["quantile"] == 5,
        "top",
        np.where(research_frame["quantile"] == 1, "bottom", "middle"),
    )

    shares_long = _stack_panel(shares_panel, value_name="historical_shares")
    market_cap_panel = close_panel.reindex(shares_panel.index) * shares_panel
    market_cap_long = _stack_panel(market_cap_panel, value_name="market_cap_proxy")
    trailing_return_panel = (1.0 + returns_panel).rolling(window=63).apply(np.prod, raw=True) - 1.0
    trailing_vol_panel = returns_panel.rolling(window=63).std()
    trailing_return_long = _stack_panel(trailing_return_panel, value_name="pre_return_63d")
    trailing_vol_long = _stack_panel(trailing_vol_panel, value_name="pre_vol_63d")

    cross_section = research_frame.merge(shares_long, on=["date", "ticker"], how="left")
    cross_section = cross_section.merge(market_cap_long, on=["date", "ticker"], how="left")
    cross_section = cross_section.merge(trailing_return_long, on=["date", "ticker"], how="left")
    cross_section = cross_section.merge(trailing_vol_long, on=["date", "ticker"], how="left")
    cross_section = cross_section.merge(static_labels, on="ticker", how="left")

    cross_section["size_bucket"] = (
        cross_section.groupby("date")["market_cap_proxy"].transform(_cross_sectional_quintiles)
    )
    cross_section["pre_return_bucket"] = (
        cross_section.groupby("date")["pre_return_63d"].transform(_cross_sectional_quintiles)
    )
    cross_section["pre_vol_bucket"] = (
        cross_section.groupby("date")["pre_vol_63d"].transform(_cross_sectional_quintiles)
    )
    cross_section["adverse_contribution"] = np.where(
        cross_section["leg_label"] == "top",
        np.clip(-pd.to_numeric(cross_section["forward_return"], errors="coerce"), 0.0, None),
        np.where(
            cross_section["leg_label"] == "bottom",
            np.clip(pd.to_numeric(cross_section["forward_return"], errors="coerce"), 0.0, None),
            0.0,
        ),
    )
    cross_section["size_bucket"] = cross_section["size_bucket"].map(
        lambda value: f"q{int(value)}" if pd.notna(value) else np.nan
    )
    cross_section["pre_return_bucket"] = cross_section["pre_return_bucket"].map(
        lambda value: f"q{int(value)}" if pd.notna(value) else np.nan
    )
    cross_section["pre_vol_bucket"] = cross_section["pre_vol_bucket"].map(
        lambda value: f"q{int(value)}" if pd.notna(value) else np.nan
    )
    return cross_section.sort_values(["date", "ticker"]).reset_index(drop=True)


def _build_bucket_vector_frame(
    cross_section_frame: pd.DataFrame,
    membership_frame: pd.DataFrame,
    *,
    dimension_column: str,
) -> pd.DataFrame:
    work = cross_section_frame.loc[cross_section_frame["leg_label"].isin(["top", "bottom"])].copy()
    work = work.merge(membership_frame.loc[:, ["date", "cohort_label"]], on="date", how="inner")
    work = work.dropna(subset=[dimension_column]).copy()
    grouped = (
        work.groupby(["date", "cohort_label", dimension_column], sort=True)["adverse_contribution"]
        .sum()
        .reset_index()
    )
    totals = grouped.groupby("date", sort=True)["adverse_contribution"].sum().rename("total_adverse")
    grouped = grouped.merge(totals.reset_index(), on="date", how="left")
    grouped["vector_key"] = grouped[dimension_column].astype(str)
    grouped["vector_value"] = np.where(
        grouped["total_adverse"] > 0.0,
        grouped["adverse_contribution"] / grouped["total_adverse"],
        0.0,
    )
    return grouped.loc[:, ["date", "cohort_label", "vector_key", "vector_value"]].reset_index(drop=True)


def _build_leg_concentration_vector_frames(
    cross_section_frame: pd.DataFrame,
    membership_frame: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    work = cross_section_frame.loc[cross_section_frame["leg_label"].isin(["top", "bottom"])].copy()
    work = work.merge(membership_frame.loc[:, ["date", "cohort_label"]], on="date", how="inner")
    rows_hhi: list[dict[str, object]] = []
    rows_effective_n: list[dict[str, object]] = []
    for (date_value, cohort_label), date_frame in work.groupby(["date", "cohort_label"], sort=True):
        metrics = build_leg_concentration_metrics(
            date_frame.loc[:, ["leg_label", "adverse_contribution"]],
            leg_column="leg_label",
            adverse_contribution_column="adverse_contribution",
            loss_floor=0.005,
        )
        for _, metric_row in metrics.iterrows():
            rows_hhi.append(
                {
                    "date": pd.Timestamp(date_value),
                    "cohort_label": str(cohort_label),
                    "vector_key": f"{metric_row['leg_label']}_hhi",
                    "vector_value": metric_row["hhi"],
                }
            )
            rows_effective_n.append(
                {
                    "date": pd.Timestamp(date_value),
                    "cohort_label": str(cohort_label),
                    "vector_key": f"{metric_row['leg_label']}_effective_n",
                    "vector_value": metric_row["effective_n"],
                }
            )
    return pd.DataFrame(rows_hhi), pd.DataFrame(rows_effective_n)


def _build_long_short_attribution_vector_frame(
    cross_section_frame: pd.DataFrame,
    membership_frame: pd.DataFrame,
) -> pd.DataFrame:
    work = cross_section_frame.loc[cross_section_frame["leg_label"].isin(["top", "bottom"])].copy()
    work = work.merge(membership_frame.loc[:, ["date", "cohort_label"]], on="date", how="inner")
    rows: list[dict[str, object]] = []
    for (date_value, cohort_label), date_frame in work.groupby(["date", "cohort_label"], sort=True):
        top_leg_return = float(date_frame.loc[date_frame["leg_label"] == "top", "forward_return"].mean())
        bottom_leg_return = float(date_frame.loc[date_frame["leg_label"] == "bottom", "forward_return"].mean())
        spread = float(top_leg_return - bottom_leg_return)
        for key, value in (
            ("top_leg_return", top_leg_return),
            ("bottom_leg_return", bottom_leg_return),
            ("spread", spread),
        ):
            rows.append(
                {
                    "date": pd.Timestamp(date_value),
                    "cohort_label": str(cohort_label),
                    "vector_key": key,
                    "vector_value": float(value),
                }
            )
    return pd.DataFrame(rows)


def _build_size_coverage_summary(
    cross_section_frame: pd.DataFrame,
    membership_frame: pd.DataFrame,
) -> pd.DataFrame:
    work = cross_section_frame.loc[cross_section_frame["leg_label"].isin(["top", "bottom"])].copy()
    work = work.merge(membership_frame.loc[:, ["date", "cohort_label"]], on="date", how="inner")
    work["era"] = np.where(work["date"] < pd.Timestamp("2010-01-01"), "pre_2010", "post_2010")
    rows: list[dict[str, object]] = []
    for (cohort_label, era), frame in work.groupby(["cohort_label", "era"], sort=True):
        rows.append(
            {
                "cohort_label": str(cohort_label),
                "era": str(era),
                "row_count": int(len(frame)),
                "historical_shares_coverage_ratio": float(frame["historical_shares"].notna().mean()),
                "market_cap_proxy_coverage_ratio": float(frame["market_cap_proxy"].notna().mean()),
            }
        )
    return pd.DataFrame(rows).sort_values(["cohort_label", "era"]).reset_index(drop=True)


def _safe_cohort_vector_comparison_summary(
    vector_frame: pd.DataFrame,
    *,
    dimension_name: str,
    metric_name: str,
    metric_direction: str,
    bootstrap_iterations: int,
    random_seed: int,
) -> pd.DataFrame:
    try:
        return build_cohort_vector_comparison_summary(
            vector_frame,
            dimension_name=dimension_name,
            metric_name=metric_name,
            metric_direction=metric_direction,
            bootstrap_iterations=bootstrap_iterations,
            random_seed=random_seed,
        )
    except Exception as exc:
        return pd.DataFrame(
            [
                {
                    "dimension_name": str(dimension_name),
                    "metric_name": str(metric_name),
                    "metric_direction": str(metric_direction),
                    "comparison_error": str(exc),
                    "bootstrap_iterations": int(bootstrap_iterations),
                    "random_seed": int(random_seed),
                }
            ]
        )

def _render_beta_table(attribution: pd.DataFrame) -> str:
    table = attribution.loc[:, ["term", "beta", "t_value"]].copy()
    table["beta"] = table["beta"].map(lambda value: f"{value:.4f}")
    table["t_value"] = table["t_value"].map(lambda value: f"{value:.2f}")
    return table.to_markdown(index=False)


def _render_frame_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No rows._"
    work = frame.copy()
    for column in work.columns:
        if pd.api.types.is_float_dtype(work[column]):
            work[column] = work[column].map(lambda value: f"{value:.4f}")
    return work.to_markdown(index=False)


def _mom_beta_lookup(attribution: pd.DataFrame) -> tuple[float, float]:
    mom_row = attribution.loc[attribution["term"] == "Mom"].iloc[0]
    return float(mom_row["beta"]), float(mom_row["t_value"])


def _build_horizon_frame_map(returns_panel: pd.DataFrame) -> dict[str, pd.DataFrame]:
    frame_map: dict[str, pd.DataFrame] = {}
    for horizon_days in _HORIZON_DAYS:
        frame_map[f"{horizon_days}d"] = build_month_end_signal_frame(
            returns_panel,
            reversal_lookback_days=21,
            momentum_lookback_days=84,
            momentum_skip_days=21,
            forward_horizon_days=horizon_days,
            reversal_weight=0.0,
            momentum_weight=1.0,
            quantiles=5,
            min_assets_per_date=20,
            trailing_market_window_days=252,
            trailing_signal_window_months=12,
        )
    return frame_map


def _build_drawdown_path(monthly_frame: pd.DataFrame, *, spread_column: str = "top_bottom_spread") -> pd.DataFrame:
    work = monthly_frame.loc[:, ["date", spread_column]].copy()
    work["date"] = pd.to_datetime(work["date"]).dt.normalize()
    work[spread_column] = pd.to_numeric(work[spread_column], errors="coerce")
    work = work.dropna(subset=[spread_column]).sort_values("date").reset_index(drop=True)
    wealth = (1.0 + work[spread_column]).cumprod()
    running_peak = wealth.cummax()
    work["cumulative_spread_index"] = wealth
    work["running_peak_index"] = running_peak
    work["drawdown"] = wealth / running_peak - 1.0
    return work


def _build_factor_pressure_inputs(
    monthly_frame: pd.DataFrame,
    factor_frame: pd.DataFrame,
    *,
    factor_names: list[str],
    focus_months: list[str],
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    focus_by_factor: dict[str, pd.DataFrame] = {}
    for factor_name in factor_names:
        residual = build_single_factor_residual_frame(
            monthly_frame,
            factor_frame.loc[:, ["date", factor_name]],
            response_column="top_bottom_spread",
            factor_column=factor_name,
        )
        focus_by_factor[factor_name] = build_focus_month_absorption_summary(residual, focus_months=focus_months)
    return focus_by_factor, build_factor_focus_pressure_summary(focus_by_factor)


def _summarize_shared_sample_pair(shared_map: dict[str, pd.DataFrame], *, left_label: str, right_label: str) -> pd.DataFrame:
    left = shared_map[left_label].loc[:, ["date", "observation_count", "top_bottom_spread"]].copy()
    left = left.rename(
        columns={
            "observation_count": f"{left_label}_observation_count",
            "top_bottom_spread": f"{left_label}_top_bottom_spread",
        }
    )
    right = shared_map[right_label].loc[:, ["date", "observation_count", "top_bottom_spread"]].copy()
    right = right.rename(
        columns={
            "observation_count": f"{right_label}_observation_count",
            "top_bottom_spread": f"{right_label}_top_bottom_spread",
        }
    )
    merged = left.merge(right, on="date", how="inner").sort_values("date").reset_index(drop=True)
    if merged.empty:
        return pd.DataFrame(
            [
                {
                    "left_label": left_label,
                    "right_label": right_label,
                    "shared_month_count": 0,
                    "mean_left_observation_count": np.nan,
                    "mean_right_observation_count": np.nan,
                    "spread_correlation": np.nan,
                    "mean_abs_spread_gap": np.nan,
                }
            ]
        )
    return pd.DataFrame(
        [
            {
                "left_label": left_label,
                "right_label": right_label,
                "shared_month_count": int(len(merged)),
                "mean_left_observation_count": float(merged[f"{left_label}_observation_count"].mean()),
                "mean_right_observation_count": float(merged[f"{right_label}_observation_count"].mean()),
                "spread_correlation": float(
                    merged[f"{left_label}_top_bottom_spread"].corr(merged[f"{right_label}_top_bottom_spread"])
                ),
                "mean_abs_spread_gap": float(
                    (merged[f"{left_label}_top_bottom_spread"] - merged[f"{right_label}_top_bottom_spread"]).abs().mean()
                ),
            }
        ]
    )


def _judgment(
    native_spread_attr: pd.DataFrame,
    operational_frame: pd.DataFrame,
    operational_focus_summary: pd.DataFrame,
) -> str:
    mom_row = native_spread_attr.loc[native_spread_attr["term"] == "Mom"].iloc[0]
    absorption_class = classify_absorption_regime(operational_focus_summary)
    focus_2025_10 = operational_frame.loc[operational_frame["date"] == pd.Timestamp("2025-10-31")]
    if (
        not focus_2025_10.empty
        and float(focus_2025_10.iloc[0]["top_bottom_spread"]) < 0.0
        and float(mom_row["t_value"]) >= 2.0
        and absorption_class == "independent_residual"
    ):
        return (
            "The signal still looks momentum-adjacent at its native 5d horizon, but the deployable 21d crash losses "
            "are largely residual to MOM. That means standard momentum crash protection is not yet the right default "
            "template for this package; the current bad months are mostly not being explained away by MOM exposure."
        )
    if float(mom_row["t_value"]) >= 2.0 and absorption_class == "momentum_absorbed":
        return (
            "The signal behaves like a mostly momentum-absorbed package in the selected crash windows, so "
            "standardized vol-managed momentum protection would be the natural next template."
        )
    return (
        "The residual read remains mixed. Treat this as a narrowing study that clarifies what standard momentum can "
        "and cannot explain, not as a final protection prescription."
    )


def _build_summary_payload(
    *,
    coverage: pd.DataFrame,
    native_frame: pd.DataFrame,
    operational_frame: pd.DataFrame,
    operational_mom_attr: pd.DataFrame,
    horizon_overlap_summary: pd.DataFrame,
    shared_sample_pair_summary: pd.DataFrame,
    shared_sample_mom_ladder: pd.DataFrame,
    operational_distribution_summary: pd.DataFrame,
    operational_drawdown_path: pd.DataFrame,
    operational_full_attr: pd.DataFrame,
    operational_factor_pressure_summary: pd.DataFrame,
    native_spread_attr: pd.DataFrame,
    native_top_leg_attr: pd.DataFrame,
    conditional_summary: pd.DataFrame,
    native_mom_residual: pd.DataFrame,
    native_focus_summary: pd.DataFrame,
    native_post2010_mom_attr: pd.DataFrame,
    operational_mom_residual: pd.DataFrame,
    operational_focus_summary: pd.DataFrame,
    operational_post2010_mom_attr: pd.DataFrame,
    bad_month_membership: pd.DataFrame,
    temporal_distribution_summary: pd.DataFrame,
    size_coverage_summary: pd.DataFrame,
    b2_dimension_comparison_summary: pd.DataFrame,
    b2_bootstrap_metadata: dict[str, object],
) -> dict[str, object]:
    focus_dates = {
        "2009-03-31",
        "2009-04-30",
        "2020-04-30",
        "2025-10-31",
        "2026-02-27",
    }
    operational_focus = operational_frame.loc[
        operational_frame["date"].dt.strftime("%Y-%m-%d").isin(focus_dates)
    ].copy()
    mom_beta, mom_t_value = _mom_beta_lookup(native_spread_attr)
    operational_mom_beta, operational_mom_t_value = _mom_beta_lookup(operational_mom_attr)
    native_post2010_beta, native_post2010_t = _mom_beta_lookup(native_post2010_mom_attr)
    operational_post2010_beta, operational_post2010_t = _mom_beta_lookup(operational_post2010_mom_attr)
    cohort_counts = (
        bad_month_membership["cohort_label"].value_counts().reindex(["outer_half", "inner_half", "non_bad"]).fillna(0)
    )
    return {
        "universe_requested_ticker_count": int(len(coverage)),
        "universe_available_ticker_count": int(coverage["available"].sum()),
        "date_range": {
            "start": str(coverage.loc[coverage["available"], "start_date"].min()),
            "end": str(coverage.loc[coverage["available"], "end_date"].max()),
        },
        "native_5d": {
            "month_count": int(len(native_frame)),
            "mean_rank_ic": float(native_frame["rank_ic"].mean()),
            "mean_top_bottom_spread": float(native_frame["top_bottom_spread"].mean()),
            "mom_beta": mom_beta,
            "mom_t_value": mom_t_value,
            "post2010_mom_beta": native_post2010_beta,
            "post2010_mom_t_value": native_post2010_t,
        },
        "operational_21d": {
            "month_count": int(len(operational_frame)),
            "mean_rank_ic": float(operational_frame["rank_ic"].mean()),
            "mean_top_bottom_spread": float(operational_frame["top_bottom_spread"].mean()),
            "worst_month": str(
                operational_frame.loc[operational_frame["top_bottom_spread"].idxmin(), "date"].date()
            ),
            "worst_month_spread": float(operational_frame["top_bottom_spread"].min()),
            "focus_windows": json.loads(
                operational_focus.assign(date=operational_focus["date"].dt.strftime("%Y-%m-%d")).to_json(orient="records")
            ),
            "mom_beta": operational_mom_beta,
            "mom_t_value": operational_mom_t_value,
            "post2010_mom_beta": operational_post2010_beta,
            "post2010_mom_t_value": operational_post2010_t,
        },
        "layer_a_divergence": {
            "frame_overlap_summary": json.loads(horizon_overlap_summary.to_json(orient="records")),
            "shared_sample_pair_summary": json.loads(shared_sample_pair_summary.to_json(orient="records")),
            "shared_sample_mom_ladder": json.loads(shared_sample_mom_ladder.to_json(orient="records")),
        },
        "layer_b_stage_1": {
            "operational_distribution_summary": json.loads(operational_distribution_summary.to_json(orient="records")),
            "operational_factor_attribution": json.loads(operational_full_attr.to_json(orient="records")),
            "operational_factor_pressure_summary": json.loads(
                operational_factor_pressure_summary.to_json(orient="records")
            ),
            "operational_max_drawdown": float(operational_drawdown_path["drawdown"].min()),
            "operational_drawdown_trough_date": str(
                operational_drawdown_path.loc[operational_drawdown_path["drawdown"].idxmin(), "date"].date()
            ),
        },
        "layer_b_stage_2": {
            "bad_month_selection": {
                "ranking_basis": "raw operational 21d top_bottom_spread",
                "bad_quantile": float(b2_bootstrap_metadata["bad_quantile"]),
                "outer_month_count": int(cohort_counts["outer_half"]),
                "inner_month_count": int(cohort_counts["inner_half"]),
                "non_bad_month_count": int(cohort_counts["non_bad"]),
            },
            "bootstrap_metadata": dict(b2_bootstrap_metadata),
            "temporal_distribution_summary": json.loads(temporal_distribution_summary.to_json(orient="records")),
            "size_coverage_summary": json.loads(size_coverage_summary.to_json(orient="records")),
            "dimension_comparison_summary": json.loads(b2_dimension_comparison_summary.to_json(orient="records")),
            "static_label_caveat": (
                "Sector and industry labels are current as of the analysis date and are applied to all historical "
                "months; they ignore historical reclassification and business transitions."
            ),
            "market_cap_proxy_caveat": (
                "Historical size buckets use yfinance adjusted close times get_shares_full. Pre-2010 coverage is a "
                "best-effort proxy rather than a research-grade CRSP market-cap series."
            ),
        },
        "conditional_summary": json.loads(conditional_summary.to_json(orient="records")),
        "mom_residual": {
            "native_focus_months": json.loads(
                native_focus_summary.assign(date=native_focus_summary["date"].dt.strftime("%Y-%m-%d")).to_json(
                    orient="records"
                )
            ),
            "operational_focus_months": json.loads(
                operational_focus_summary.assign(date=operational_focus_summary["date"].dt.strftime("%Y-%m-%d")).to_json(
                    orient="records"
                )
            ),
            "native_classification": classify_absorption_regime(native_focus_summary),
            "operational_classification": classify_absorption_regime(operational_focus_summary),
            "native_mean_matching_absorption": float(native_focus_summary["matching_absorption_share"].mean()),
            "operational_mean_matching_absorption": float(operational_focus_summary["matching_absorption_share"].mean()),
        },
        "judgment": _judgment(native_spread_attr, operational_frame, operational_focus_summary),
        "data_caveat": (
            "This extension uses yfinance-adjusted closes for a fast family-level stress study. "
            "It should be treated as a research proxy until rerun on CRSP-grade history."
        ),
    }


def _render_markdown(
    *,
    coverage: pd.DataFrame,
    native_frame: pd.DataFrame,
    operational_frame: pd.DataFrame,
    operational_mom_attr: pd.DataFrame,
    horizon_overlap_summary: pd.DataFrame,
    shared_sample_pair_summary: pd.DataFrame,
    shared_sample_mom_ladder: pd.DataFrame,
    operational_distribution_summary: pd.DataFrame,
    operational_drawdown_path: pd.DataFrame,
    operational_full_attr: pd.DataFrame,
    operational_factor_pressure_summary: pd.DataFrame,
    native_spread_attr: pd.DataFrame,
    native_top_leg_attr: pd.DataFrame,
    conditional_summary: pd.DataFrame,
    native_focus_summary: pd.DataFrame,
    operational_focus_summary: pd.DataFrame,
    native_post2010_mom_attr: pd.DataFrame,
    operational_post2010_mom_attr: pd.DataFrame,
    bad_month_membership: pd.DataFrame,
    temporal_distribution_summary: pd.DataFrame,
    size_coverage_summary: pd.DataFrame,
    b2_dimension_comparison_summary: pd.DataFrame,
    b2_bootstrap_metadata: dict[str, object],
) -> str:
    focus_dates = ["2009-03-31", "2009-04-30", "2020-04-30", "2025-10-31", "2026-02-27"]
    operational_focus = operational_frame.loc[
        operational_frame["date"].dt.strftime("%Y-%m-%d").isin(focus_dates)
    ].copy()
    worst_operational = operational_frame.nsmallest(10, "top_bottom_spread").copy()
    summary_line = _judgment(native_spread_attr, operational_frame, operational_focus_summary)
    unavailable = coverage.loc[~coverage["available"], "ticker"].astype(str).tolist()
    native_post2010_beta, native_post2010_t = _mom_beta_lookup(native_post2010_mom_attr)
    operational_mom_beta, operational_mom_t = _mom_beta_lookup(operational_mom_attr)
    operational_post2010_beta, operational_post2010_t = _mom_beta_lookup(operational_post2010_mom_attr)
    worst_drawdown = operational_drawdown_path.loc[operational_drawdown_path["drawdown"].idxmin()]
    cohort_counts = (
        bad_month_membership["cohort_label"].value_counts().reindex(["outer_half", "inner_half", "non_bad"]).fillna(0)
    )
    return "\n".join(
        [
            "# US Long-Horizon Signal Extension",
            "",
            "## Scope",
            f"- Universe: {int(len(coverage))} requested tickers from [`us_equity_expanded_tickers.txt`]({UNIVERSE_TICKERS_PATH})",
            f"- Available price history: {int(coverage['available'].sum())}/{int(len(coverage))} tickers",
            f"- Missing tickers from yfinance bulk download: {', '.join(unavailable) if unavailable else 'none'}",
            f"- Price window: {coverage.loc[coverage['available'], 'start_date'].min()} to {coverage.loc[coverage['available'], 'end_date'].max()}",
            "- Data source: yfinance adjusted closes (fast proxy, not CRSP)",
            "",
            "## Layer A: 5d vs 21d Divergence Diagnostics",
            "Frame overlap summary:",
            "",
            _render_frame_table(horizon_overlap_summary),
            "",
            "Shared-sample 5d vs 21d month comparison:",
            "",
            _render_frame_table(shared_sample_pair_summary),
            "",
            "Shared-sample MOM beta ladder by holding horizon:",
            "",
            _render_frame_table(shared_sample_mom_ladder.loc[:, ["frame_label", "month_count", "term", "beta", "t_value"]]),
            "",
            "## Layer B Stage 1: Unconditional Distribution",
            "_Epistemic ceiling: crash-month interpretation is still hypothesis-level because the salient residual tail sample remains very small._",
            "",
            _render_frame_table(operational_distribution_summary),
            "",
            f"- Worst drawdown trough: {pd.Timestamp(worst_drawdown['date']).strftime('%Y-%m-%d')} ({float(worst_drawdown['drawdown']):.4f})",
            "",
            "## Layer B Stage 2: Bad-Month Cohort Decomposition",
            "_Epistemic ceiling: this stage is descriptive only. Any same-type or different-type read should be stated as consistent-with evidence, not as an identified mechanism._",
            "",
            "- Ranking basis: raw operational 21d `top_bottom_spread`",
            f"- Bad-month cohort: worst quintile split into outer={int(cohort_counts['outer_half'])}, inner={int(cohort_counts['inner_half'])}, non-bad={int(cohort_counts['non_bad'])}",
            f"- Bootstrap null: {int(b2_bootstrap_metadata['bootstrap_iterations'])} non-bad resamples without replacement, seed={int(b2_bootstrap_metadata['random_seed'])}",
            "- Sector and industry labels are current as of the analysis date and are applied to all historical months; they ignore historical reclassification and business transitions.",
            "- Historical size buckets use yfinance adjusted close times get_shares_full; pre-2010 coverage remains best-effort rather than CRSP-grade.",
            "",
            "Temporal distribution by cohort:",
            "",
            _render_frame_table(temporal_distribution_summary),
            "",
            "Historical market-cap proxy coverage by cohort and era:",
            "",
            _render_frame_table(size_coverage_summary),
            "",
            "Dimension comparison summary (outer-vs-inner vs non-bad bootstrap):",
            "",
            _render_frame_table(b2_dimension_comparison_summary),
            "",
            "## Native 5d Family Attribution",
            f"- Monthly sampled native-horizon observations: {len(native_frame)}",
            f"- Mean rank IC: {native_frame['rank_ic'].mean():.4f}",
            f"- Mean top-bottom spread: {native_frame['top_bottom_spread'].mean():.4f}",
            f"- MOM-only beta: {_mom_beta_lookup(native_spread_attr)[0]:.4f} (t={_mom_beta_lookup(native_spread_attr)[1]:.2f})",
            f"- MOM-only beta post-2010: {native_post2010_beta:.4f} (t={native_post2010_t:.2f})",
            "",
            "Native 5d top-bottom spread vs MOM / Mkt-RF / SMB / HML / QMJ / BAB:",
            "",
            _render_beta_table(native_spread_attr),
            "",
            "Native 5d top-leg return vs the same factor set:",
            "",
            _render_beta_table(native_top_leg_attr),
            "",
            "## Operational 21d Stress View",
            f"- Monthly sampled operational observations: {len(operational_frame)}",
            f"- Mean rank IC: {operational_frame['rank_ic'].mean():.4f}",
            f"- Mean top-bottom spread: {operational_frame['top_bottom_spread'].mean():.4f}",
            f"- MOM-only beta: {operational_mom_beta:.4f} (t={operational_mom_t:.2f})",
            f"- MOM-only beta post-2010: {operational_post2010_beta:.4f} (t={operational_post2010_t:.2f})",
            "",
            "Operational 21d full factor attribution vs MOM / Mkt-RF / SMB / HML / QMJ / BAB / STR / LTR:",
            "",
            _render_beta_table(operational_full_attr),
            "",
            "Operational 21d factor-pressure summary on focus crash months:",
            "",
            _render_frame_table(operational_factor_pressure_summary),
            "",
            "Selected focus windows:",
            "",
            _render_frame_table(
                operational_focus.loc[
                    :,
                    [
                        "date",
                        "rank_ic",
                        "top_bottom_spread",
                        "top_forward_return",
                        "bottom_forward_return",
                        "market_trailing_return",
                        "trailing_signal_spread",
                    ],
                ]
            ),
            "",
            "Worst operational months by top-bottom spread:",
            "",
            _render_frame_table(
                worst_operational.loc[
                    :,
                    ["date", "rank_ic", "top_bottom_spread", "top_forward_return", "bottom_forward_return"],
                ]
            ),
            "",
            "## Conditional Spread Decomposition",
            "",
            _render_frame_table(conditional_summary),
            "",
            "## Mom Residual Analysis",
            "",
            "Native 5d focus months:",
            "",
            _render_frame_table(native_focus_summary),
            "",
            "Operational 21d focus months:",
            "",
            _render_frame_table(operational_focus_summary),
            "",
            f"- Native mean matching absorption share: {native_focus_summary['matching_absorption_share'].mean():.4f}",
            f"- Operational mean matching absorption share: {operational_focus_summary['matching_absorption_share'].mean():.4f}",
            f"- Native classification: {classify_absorption_regime(native_focus_summary)}",
            f"- Operational classification: {classify_absorption_regime(operational_focus_summary)}",
            "",
            "## Judgment",
            summary_line,
            "",
            "## Caveat",
            "This note is a family-level stress study, not a research-grade final verdict. "
            "The signal definition matches the current `84/21` momentum recipe, but the history comes from yfinance "
            "rather than CRSP. Treat any pre-CRSP conclusion as directional until rerun on institutional data.",
        ]
    )


def main() -> None:
    bootstrap_iterations = 5000
    bootstrap_random_seed = 7
    bad_quantile = 0.20
    requested_tickers = _load_universe_tickers(UNIVERSE_TICKERS_PATH)
    close_panel = _download_close_panel(requested_tickers)
    coverage = _build_price_coverage_frame(close_panel, requested_tickers)
    returns_panel = close_panel.pct_change().dropna(how="all")
    horizon_frame_map = _build_horizon_frame_map(returns_panel)
    native_frame = horizon_frame_map["5d"]
    operational_frame = horizon_frame_map["21d"]

    style_factors = _load_style_factor_frame()
    pressure_factor_names = ["Mom", "Mkt-RF", "SMB", "HML", "QMJ", "BAB", "STR", "LTR"]
    horizon_overlap_summary = build_frame_overlap_summary(horizon_frame_map)
    shared_horizon_frame_map = build_shared_date_frame_map(horizon_frame_map)
    shared_sample_pair_summary = _summarize_shared_sample_pair(
        shared_horizon_frame_map,
        left_label="5d",
        right_label="21d",
    )
    shared_sample_mom_ladder = build_horizon_factor_ladder(
        horizon_frame_map,
        style_factors.loc[:, ["date", "Mom"]],
        response_column="top_bottom_spread",
        factor_columns=["Mom"],
        use_shared_dates=True,
    )
    operational_distribution_summary = build_spread_distribution_summary(
        operational_frame,
        spread_column="top_bottom_spread",
        worst_n=10,
    )
    operational_drawdown_path = _build_drawdown_path(operational_frame, spread_column="top_bottom_spread")
    native_spread_attr = fit_factor_attribution(
        native_frame,
        style_factors,
        response_column="top_bottom_spread",
        factor_columns=["Mom", "Mkt-RF", "SMB", "HML", "QMJ", "BAB"],
    )
    native_top_leg_attr = fit_factor_attribution(
        native_frame,
        style_factors,
        response_column="top_forward_return",
        factor_columns=["Mom", "Mkt-RF", "SMB", "HML", "QMJ", "BAB"],
    )
    operational_mom_attr = fit_factor_attribution(
        operational_frame,
        style_factors.loc[:, ["date", "Mom"]],
        response_column="top_bottom_spread",
        factor_columns=["Mom"],
    )
    native_post2010_mom_attr = fit_factor_attribution(
        native_frame.loc[native_frame["date"] >= pd.Timestamp("2010-01-01")].copy(),
        style_factors.loc[style_factors["date"] >= pd.Timestamp("2010-01-01")].copy(),
        response_column="top_bottom_spread",
        factor_columns=["Mom"],
    )
    operational_post2010_mom_attr = fit_factor_attribution(
        operational_frame.loc[operational_frame["date"] >= pd.Timestamp("2010-01-01")].copy(),
        style_factors.loc[style_factors["date"] >= pd.Timestamp("2010-01-01")].copy(),
        response_column="top_bottom_spread",
        factor_columns=["Mom"],
    )
    native_mom_residual = build_single_factor_residual_frame(
        native_frame,
        style_factors.loc[:, ["date", "Mom"]],
        response_column="top_bottom_spread",
        factor_column="Mom",
    )
    operational_mom_residual = build_single_factor_residual_frame(
        operational_frame,
        style_factors.loc[:, ["date", "Mom"]],
        response_column="top_bottom_spread",
        factor_column="Mom",
    )
    focus_months = ["2009-03", "2022-12", "2025-10"]
    operational_full_attr = fit_factor_attribution(
        operational_frame,
        style_factors,
        response_column="top_bottom_spread",
        factor_columns=pressure_factor_names,
    )
    native_focus_summary = build_focus_month_absorption_summary(native_mom_residual, focus_months=focus_months)
    operational_focus_summary = build_focus_month_absorption_summary(
        operational_mom_residual,
        focus_months=focus_months,
    )
    _, operational_factor_pressure_summary = _build_factor_pressure_inputs(
        operational_frame,
        style_factors,
        factor_names=pressure_factor_names,
        focus_months=focus_months,
    )
    conditional_summary = build_conditional_spread_summary(
        operational_frame,
        spread_column="top_bottom_spread",
    )
    bad_month_membership = build_bad_month_cohort_membership(
        operational_frame,
        spread_column="top_bottom_spread",
        bad_quantile=bad_quantile,
    )
    temporal_distribution_summary = build_temporal_distribution_summary(bad_month_membership)
    static_labels = _load_static_label_frame(requested_tickers)
    shares_panel = _load_shares_panel(requested_tickers, close_panel.index)
    operational_cross_section_frame = _build_operational_cross_section_frame(
        returns_panel=returns_panel,
        close_panel=close_panel,
        operational_frame=operational_frame,
        static_labels=static_labels,
        shares_panel=shares_panel,
    )
    size_coverage_summary = _build_size_coverage_summary(operational_cross_section_frame, bad_month_membership)

    size_vector_frame = _build_bucket_vector_frame(
        operational_cross_section_frame,
        bad_month_membership,
        dimension_column="size_bucket",
    )
    sector_vector_frame = _build_bucket_vector_frame(
        operational_cross_section_frame,
        bad_month_membership,
        dimension_column="sector_label",
    )
    industry_vector_frame = _build_bucket_vector_frame(
        operational_cross_section_frame,
        bad_month_membership,
        dimension_column="industry_label",
    )
    pre_return_vector_frame = _build_bucket_vector_frame(
        operational_cross_section_frame,
        bad_month_membership,
        dimension_column="pre_return_bucket",
    )
    pre_vol_vector_frame = _build_bucket_vector_frame(
        operational_cross_section_frame,
        bad_month_membership,
        dimension_column="pre_vol_bucket",
    )
    leg_hhi_vector_frame, leg_effective_n_vector_frame = _build_leg_concentration_vector_frames(
        operational_cross_section_frame,
        bad_month_membership,
    )
    long_short_attribution_vector_frame = _build_long_short_attribution_vector_frame(
        operational_cross_section_frame,
        bad_month_membership,
    )
    b2_dimension_comparison_summary = pd.concat(
        [
            _safe_cohort_vector_comparison_summary(
                size_vector_frame,
                dimension_name="size_bucket",
                metric_name="l1_distance",
                metric_direction="distance",
                bootstrap_iterations=bootstrap_iterations,
                random_seed=bootstrap_random_seed,
            ),
            _safe_cohort_vector_comparison_summary(
                sector_vector_frame,
                dimension_name="sector_label",
                metric_name="rank_correlation",
                metric_direction="similarity",
                bootstrap_iterations=bootstrap_iterations,
                random_seed=bootstrap_random_seed,
            ),
            _safe_cohort_vector_comparison_summary(
                industry_vector_frame,
                dimension_name="industry_label",
                metric_name="rank_correlation",
                metric_direction="similarity",
                bootstrap_iterations=bootstrap_iterations,
                random_seed=bootstrap_random_seed,
            ),
            _safe_cohort_vector_comparison_summary(
                pre_return_vector_frame,
                dimension_name="pre_return_bucket",
                metric_name="l1_distance",
                metric_direction="distance",
                bootstrap_iterations=bootstrap_iterations,
                random_seed=bootstrap_random_seed,
            ),
            _safe_cohort_vector_comparison_summary(
                pre_vol_vector_frame,
                dimension_name="pre_vol_bucket",
                metric_name="l1_distance",
                metric_direction="distance",
                bootstrap_iterations=bootstrap_iterations,
                random_seed=bootstrap_random_seed,
            ),
            _safe_cohort_vector_comparison_summary(
                leg_hhi_vector_frame,
                dimension_name="leg_hhi",
                metric_name="l1_distance",
                metric_direction="distance",
                bootstrap_iterations=bootstrap_iterations,
                random_seed=bootstrap_random_seed,
            ),
            _safe_cohort_vector_comparison_summary(
                leg_effective_n_vector_frame,
                dimension_name="leg_effective_n",
                metric_name="l1_distance",
                metric_direction="distance",
                bootstrap_iterations=bootstrap_iterations,
                random_seed=bootstrap_random_seed,
            ),
            _safe_cohort_vector_comparison_summary(
                long_short_attribution_vector_frame,
                dimension_name="long_short_attribution",
                metric_name="l1_distance",
                metric_direction="distance",
                bootstrap_iterations=bootstrap_iterations,
                random_seed=bootstrap_random_seed,
            ),
        ],
        ignore_index=True,
    )
    b2_bootstrap_metadata = {
        "bad_quantile": bad_quantile,
        "bootstrap_iterations": bootstrap_iterations,
        "random_seed": bootstrap_random_seed,
        "outer_month_count": int((bad_month_membership["cohort_label"] == "outer_half").sum()),
        "inner_month_count": int((bad_month_membership["cohort_label"] == "inner_half").sum()),
        "non_bad_month_count": int((bad_month_membership["cohort_label"] == "non_bad").sum()),
    }

    summary_payload = _build_summary_payload(
        coverage=coverage,
        native_frame=native_frame,
        operational_frame=operational_frame,
        operational_mom_attr=operational_mom_attr,
        horizon_overlap_summary=horizon_overlap_summary,
        shared_sample_pair_summary=shared_sample_pair_summary,
        shared_sample_mom_ladder=shared_sample_mom_ladder,
        operational_distribution_summary=operational_distribution_summary,
        operational_drawdown_path=operational_drawdown_path,
        operational_full_attr=operational_full_attr,
        operational_factor_pressure_summary=operational_factor_pressure_summary,
        native_spread_attr=native_spread_attr,
        native_top_leg_attr=native_top_leg_attr,
        conditional_summary=conditional_summary,
        native_mom_residual=native_mom_residual,
        native_focus_summary=native_focus_summary,
        native_post2010_mom_attr=native_post2010_mom_attr,
        operational_mom_residual=operational_mom_residual,
        operational_focus_summary=operational_focus_summary,
        operational_post2010_mom_attr=operational_post2010_mom_attr,
        bad_month_membership=bad_month_membership,
        temporal_distribution_summary=temporal_distribution_summary,
        size_coverage_summary=size_coverage_summary,
        b2_dimension_comparison_summary=b2_dimension_comparison_summary,
        b2_bootstrap_metadata=b2_bootstrap_metadata,
    )
    markdown = _render_markdown(
        coverage=coverage,
        native_frame=native_frame,
        operational_frame=operational_frame,
        operational_mom_attr=operational_mom_attr,
        horizon_overlap_summary=horizon_overlap_summary,
        shared_sample_pair_summary=shared_sample_pair_summary,
        shared_sample_mom_ladder=shared_sample_mom_ladder,
        operational_distribution_summary=operational_distribution_summary,
        operational_drawdown_path=operational_drawdown_path,
        operational_full_attr=operational_full_attr,
        operational_factor_pressure_summary=operational_factor_pressure_summary,
        native_spread_attr=native_spread_attr,
        native_top_leg_attr=native_top_leg_attr,
        conditional_summary=conditional_summary,
        native_focus_summary=native_focus_summary,
        operational_focus_summary=operational_focus_summary,
        native_post2010_mom_attr=native_post2010_mom_attr,
        operational_post2010_mom_attr=operational_post2010_mom_attr,
        bad_month_membership=bad_month_membership,
        temporal_distribution_summary=temporal_distribution_summary,
        size_coverage_summary=size_coverage_summary,
        b2_dimension_comparison_summary=b2_dimension_comparison_summary,
        b2_bootstrap_metadata=b2_bootstrap_metadata,
    )

    output_dir = OUTPUT_ROOT / f"us_long_horizon_signal_extension_{date.today().isoformat()}"
    output_dir.mkdir(parents=True, exist_ok=True)
    coverage.to_csv(output_dir / "price_coverage.csv", index=False)
    native_frame.to_csv(output_dir / "native_5d_month_end_signal.csv", index=False)
    operational_frame.to_csv(output_dir / "operational_21d_month_end_signal.csv", index=False)
    horizon_overlap_summary.to_csv(output_dir / "layer_a_horizon_overlap_summary.csv", index=False)
    shared_sample_pair_summary.to_csv(output_dir / "layer_a_shared_sample_pair_summary.csv", index=False)
    shared_sample_mom_ladder.to_csv(output_dir / "layer_a_shared_sample_mom_ladder.csv", index=False)
    operational_distribution_summary.to_csv(output_dir / "layer_b_operational_21d_distribution_summary.csv", index=False)
    operational_drawdown_path.to_csv(output_dir / "layer_b_operational_21d_drawdown_path.csv", index=False)
    operational_full_attr.to_csv(output_dir / "layer_b_operational_21d_full_factor_attribution.csv", index=False)
    operational_factor_pressure_summary.to_csv(
        output_dir / "layer_b_operational_21d_factor_pressure_summary.csv",
        index=False,
    )
    bad_month_membership.to_csv(output_dir / "layer_b2_bad_month_cohort_membership.csv", index=False)
    temporal_distribution_summary.to_csv(output_dir / "layer_b2_temporal_distribution_summary.csv", index=False)
    size_coverage_summary.to_csv(output_dir / "layer_b2_size_coverage_summary.csv", index=False)
    b2_dimension_comparison_summary.to_csv(output_dir / "layer_b2_dimension_comparison_summary.csv", index=False)
    native_spread_attr.to_csv(output_dir / "native_5d_spread_factor_attribution.csv", index=False)
    native_top_leg_attr.to_csv(output_dir / "native_5d_top_leg_factor_attribution.csv", index=False)
    native_mom_residual.to_csv(output_dir / "native_5d_mom_residual.csv", index=False)
    native_focus_summary.to_csv(output_dir / "native_5d_mom_residual_focus_summary.csv", index=False)
    conditional_summary.to_csv(output_dir / "operational_21d_conditional_summary.csv", index=False)
    operational_mom_residual.to_csv(output_dir / "operational_21d_mom_residual.csv", index=False)
    operational_focus_summary.to_csv(output_dir / "operational_21d_mom_residual_focus_summary.csv", index=False)
    (output_dir / "layer_b2_bootstrap_metadata.json").write_text(
        json.dumps(b2_bootstrap_metadata, indent=2),
        encoding="utf-8",
    )
    (output_dir / "us_long_horizon_signal_extension_summary.json").write_text(
        json.dumps(summary_payload, indent=2),
        encoding="utf-8",
    )
    (output_dir / "us_long_horizon_signal_extension.md").write_text(markdown, encoding="utf-8")

    print(
        "US long-horizon signal extension complete:",
        f"native_5d_months={len(native_frame)}",
        f"operational_21d_months={len(operational_frame)}",
        f"mom_beta_5d={_mom_beta_lookup(native_spread_attr)[0]:.4f}",
        f"mom_beta_21d={float(operational_mom_residual['factor_beta'].iloc[0]):.4f}",
        f"shared_sample_mom_beta_21d={float(shared_sample_mom_ladder.loc[(shared_sample_mom_ladder['frame_label'] == '21d') & (shared_sample_mom_ladder['term'] == 'Mom'), 'beta'].iloc[0]):.4f}",
        f"residual_class_21d={classify_absorption_regime(operational_focus_summary)}",
        f"b2_bad_months={int((bad_month_membership['is_bad_month']).sum())}",
    )


if __name__ == "__main__":
    main()
