"""Calibration-family discovery helpers for Alpha Discovery v2."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from portfolio_os.alpha.qualification import (
    build_baseline_monthly_signal_frame,
    build_family_a_monthly_signal_frame,
    build_monthly_forward_return_frame,
)
from portfolio_os.alpha.research import load_alpha_returns_panel
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.storage.snapshots import write_json, write_text

_QUANTILES = 5
_MIN_ASSETS_PER_DATE = 5


@dataclass(frozen=True)
class CalibrationExpressionDefinition:
    expression_id: str
    role: str
    mechanism_id: str
    source_expression_id: str | None
    description: str


@dataclass
class CalibrationRunResult:
    output_dir: Path
    registry_frame: pd.DataFrame
    per_date_frame: pd.DataFrame
    summary_frame: pd.DataFrame
    summary_payload: dict[str, Any]
    note_markdown: str


def build_us_residual_momentum_calibration_registry() -> list[CalibrationExpressionDefinition]:
    """Return the frozen registry for the calibration family."""

    return [
        CalibrationExpressionDefinition(
            expression_id="RM1_MKT_RESIDUAL",
            role="expression",
            mechanism_id="M1_RESIDUAL_CONTINUATION",
            source_expression_id="A1",
            description="Market-residual 84/21 momentum carried onto the monthly decision grid.",
        ),
        CalibrationExpressionDefinition(
            expression_id="RM2_SECTOR_RESIDUAL",
            role="expression",
            mechanism_id="M1_RESIDUAL_CONTINUATION",
            source_expression_id="A2",
            description="Sector-residual 84/21 momentum on the monthly decision grid.",
        ),
        CalibrationExpressionDefinition(
            expression_id="RM3_VOL_MANAGED",
            role="expression",
            mechanism_id="M3_VOL_MANAGED_PERSISTENCE",
            source_expression_id="A3",
            description="Vol-managed residual momentum on the monthly decision grid.",
        ),
        CalibrationExpressionDefinition(
            expression_id="CTRL1_SHUFFLED_PLACEBO",
            role="control",
            mechanism_id="C1_SHUFFLED_PLACEBO",
            source_expression_id="A1",
            description="Per-date shuffled placebo preserving the A1 cross-sectional value distribution.",
        ),
        CalibrationExpressionDefinition(
            expression_id="CTRL2_PRE_WINDOW_PLACEBO",
            role="control",
            mechanism_id="C2_PRE_WINDOW_PLACEBO",
            source_expression_id="A1",
            description="Prior-month residual momentum assigned to the current month as a placebo signal.",
        ),
        CalibrationExpressionDefinition(
            expression_id="CTRL3_BASELINE_MIMIC",
            role="control",
            mechanism_id="C3_BASELINE_MIMIC",
            source_expression_id=None,
            description="Raw 84/21 momentum baseline used as a baseline-mimic control.",
        ),
    ]


def _registry_lookup() -> dict[str, CalibrationExpressionDefinition]:
    return {item.expression_id: item for item in build_us_residual_momentum_calibration_registry()}


def _safe_spearman(left: pd.Series, right: pd.Series) -> float:
    clean = pd.concat(
        [pd.to_numeric(left, errors="coerce"), pd.to_numeric(right, errors="coerce")],
        axis=1,
    ).dropna()
    if len(clean) < 2:
        return float("nan")
    if clean.iloc[:, 0].nunique() < 2 or clean.iloc[:, 1].nunique() < 2:
        return float("nan")
    return float(clean.iloc[:, 0].corr(clean.iloc[:, 1], method="spearman"))


def _mean_t_stat(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    if len(clean) < 2:
        return 0.0
    std = float(clean.std(ddof=1))
    if std <= 0.0:
        return 0.0
    return float(clean.mean() / (std / np.sqrt(float(len(clean)))))


def _empirical_percentile(sample: pd.Series, value: float) -> float:
    clean = pd.to_numeric(sample, errors="coerce").dropna().astype(float)
    if clean.empty or not np.isfinite(value):
        return float("nan")
    return float((clean <= float(value)).mean())


def _quantile_buckets(values: pd.Series, *, quantiles: int = _QUANTILES) -> pd.Series:
    ranked = values.rank(method="first", pct=True)
    return np.ceil(ranked * quantiles).clip(1, quantiles).astype(int)


def build_calibration_signal_frame(
    *,
    returns_panel: pd.DataFrame,
    universe_reference: pd.DataFrame,
    expression_id: str,
    random_seed: int = 0,
) -> pd.DataFrame:
    """Build one calibration-family signal frame for a single expression or control."""

    registry = _registry_lookup()
    if expression_id not in registry:
        raise InputValidationError(f"Unsupported calibration expression_id: {expression_id}")

    if expression_id == "RM1_MKT_RESIDUAL":
        return build_family_a_monthly_signal_frame(
            returns_panel=returns_panel,
            universe_reference=universe_reference,
            candidate_id="A1",
        )
    if expression_id == "RM2_SECTOR_RESIDUAL":
        return build_family_a_monthly_signal_frame(
            returns_panel=returns_panel,
            universe_reference=universe_reference,
            candidate_id="A2",
        )
    if expression_id == "RM3_VOL_MANAGED":
        return build_family_a_monthly_signal_frame(
            returns_panel=returns_panel,
            universe_reference=universe_reference,
            candidate_id="A3",
        )
    if expression_id == "CTRL3_BASELINE_MIMIC":
        return build_baseline_monthly_signal_frame(
            returns_panel=returns_panel,
            universe_reference=universe_reference,
        )

    base_frame = build_family_a_monthly_signal_frame(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
        candidate_id="A1",
    ).copy()
    base_frame["date"] = pd.to_datetime(base_frame["date"]).dt.strftime("%Y-%m-%d")

    if expression_id == "CTRL1_SHUFFLED_PLACEBO":
        shuffled_rows: list[pd.DataFrame] = []
        for offset, (date_value, date_frame) in enumerate(base_frame.groupby("date", sort=True)):
            rng = np.random.default_rng(int(random_seed) + int(offset))
            shuffled = date_frame.copy()
            shuffled["signal_value"] = rng.permutation(shuffled["signal_value"].to_numpy())
            shuffled_rows.append(shuffled)
        return pd.concat(shuffled_rows, ignore_index=True).sort_values(["date", "ticker"]).reset_index(drop=True)

    if expression_id == "CTRL2_PRE_WINDOW_PLACEBO":
        shifted = (
            base_frame.sort_values(["ticker", "date"])
            .assign(signal_value=lambda frame: frame.groupby("ticker")["signal_value"].shift(1))
            .dropna(subset=["signal_value"])
            .sort_values(["date", "ticker"])
            .reset_index(drop=True)
        )
        if shifted.empty:
            raise InputValidationError("Pre-window placebo produced an empty signal frame.")
        return shifted

    raise InputValidationError(f"Unsupported calibration expression_id: {expression_id}")


def build_shuffled_null_distribution(
    *,
    returns_panel: pd.DataFrame,
    universe_reference: pd.DataFrame,
    random_seeds: list[int],
) -> pd.DataFrame:
    """Build one repeated shuffled-placebo null distribution."""

    rows: list[dict[str, Any]] = []
    forward_return_frame = build_monthly_forward_return_frame(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
    )
    for seed in random_seeds:
        signal_frame = build_calibration_signal_frame(
            returns_panel=returns_panel,
            universe_reference=universe_reference,
            expression_id="CTRL1_SHUFFLED_PLACEBO",
            random_seed=int(seed),
        )
        per_date = _build_per_date_metrics(
            signal_frame=signal_frame,
            forward_return_frame=forward_return_frame,
            expression_id="CTRL1_SHUFFLED_PLACEBO",
        )
        rows.append(
            {
                "seed": int(seed),
                "expression_id": "CTRL1_SHUFFLED_PLACEBO",
                "evaluation_month_count": int(len(per_date)),
                "mean_rank_ic": float(pd.to_numeric(per_date["rank_ic"], errors="coerce").mean()) if not per_date.empty else 0.0,
                "rank_ic_t": _mean_t_stat(per_date["rank_ic"]) if not per_date.empty else 0.0,
                "mean_top_bottom_spread": float(pd.to_numeric(per_date["top_bottom_spread"], errors="coerce").mean())
                if not per_date.empty
                else 0.0,
            }
        )
    return pd.DataFrame(rows).sort_values("seed").reset_index(drop=True)


def build_bootstrap_expression_rankings(
    *,
    per_date_frame: pd.DataFrame,
    expression_ids: list[str],
    bootstrap_iterations: int,
    random_seed: int = 0,
) -> pd.DataFrame:
    """Bootstrap expression-level ranking stability from per-date metrics."""

    if bootstrap_iterations <= 0:
        raise InputValidationError("bootstrap_iterations must be positive.")
    work = per_date_frame.loc[per_date_frame["expression_id"].isin(expression_ids)].copy()
    if work.empty:
        return pd.DataFrame(
            columns=[
                "bootstrap_id",
                "expression_id",
                "sampled_month_count",
                "mean_rank_ic",
                "mean_top_bottom_spread",
                "rank_by_rank_ic",
                "rank_by_top_bottom_spread",
            ]
        )
    unique_dates = sorted(str(item) for item in work["date"].dropna().astype(str).unique().tolist())
    if not unique_dates:
        return pd.DataFrame()
    rng = np.random.default_rng(int(random_seed))
    rows: list[dict[str, Any]] = []
    for bootstrap_id in range(int(bootstrap_iterations)):
        sampled_dates = pd.DataFrame({"date": rng.choice(unique_dates, size=len(unique_dates), replace=True)})
        sampled = sampled_dates.merge(work, on="date", how="left")
        grouped = (
            sampled.groupby("expression_id", as_index=False)
            .agg(
                sampled_month_count=("date", "size"),
                mean_rank_ic=("rank_ic", "mean"),
                mean_top_bottom_spread=("top_bottom_spread", "mean"),
            )
            .sort_values(["mean_rank_ic", "mean_top_bottom_spread", "expression_id"], ascending=[False, False, True])
            .reset_index(drop=True)
        )
        grouped["rank_by_rank_ic"] = grouped["mean_rank_ic"].rank(method="first", ascending=False).astype(int)
        grouped["rank_by_top_bottom_spread"] = grouped["mean_top_bottom_spread"].rank(method="first", ascending=False).astype(int)
        grouped["bootstrap_id"] = int(bootstrap_id)
        rows.extend(grouped.to_dict(orient="records"))
    return pd.DataFrame(rows).sort_values(["bootstrap_id", "rank_by_rank_ic", "expression_id"]).reset_index(drop=True)


def build_expression_spread_correlation_matrix(
    *,
    per_date_frame: pd.DataFrame,
    expression_ids: list[str],
) -> pd.DataFrame:
    """Return the per-date spread correlation matrix for expression rows only."""

    work = per_date_frame.loc[per_date_frame["expression_id"].isin(expression_ids), ["date", "expression_id", "top_bottom_spread"]].copy()
    if work.empty:
        return pd.DataFrame(index=expression_ids, columns=expression_ids, dtype=float)
    pivot = (
        work.pivot(index="date", columns="expression_id", values="top_bottom_spread")
        .reindex(columns=expression_ids)
        .sort_index()
    )
    matrix = pivot.corr(method="pearson", min_periods=1).reindex(index=expression_ids, columns=expression_ids)
    for expression_id in expression_ids:
        if expression_id in matrix.index and expression_id in matrix.columns:
            matrix.loc[expression_id, expression_id] = 1.0
    return matrix


def _residualize_against_baseline(signal_values: pd.Series, baseline_values: pd.Series) -> pd.Series:
    signal = pd.to_numeric(signal_values, errors="coerce").astype(float)
    baseline = pd.to_numeric(baseline_values, errors="coerce").astype(float)
    valid = signal.notna() & baseline.notna()
    if valid.sum() < 2:
        return signal
    signal_valid = signal.loc[valid]
    baseline_valid = baseline.loc[valid]
    baseline_centered = baseline_valid - float(baseline_valid.mean())
    denom = float((baseline_centered**2).sum())
    if denom <= 0.0:
        return signal
    signal_centered = signal_valid - float(signal_valid.mean())
    beta = float((signal_centered * baseline_centered).sum() / denom)
    residual = signal.copy()
    residual.loc[valid] = signal_valid - beta * baseline_valid
    return residual


def _build_residualized_signal_frame(
    *,
    signal_frame: pd.DataFrame,
    baseline_frame: pd.DataFrame,
) -> pd.DataFrame:
    baseline_values = baseline_frame.rename(columns={"signal_value": "baseline_signal_value"})
    merged = signal_frame.merge(
        baseline_values.loc[:, ["date", "ticker", "baseline_signal_value"]],
        on=["date", "ticker"],
        how="inner",
    )
    if merged.empty:
        return pd.DataFrame(columns=["date", "ticker", "signal_value", "baseline_signal_value"])
    residualized_rows: list[pd.DataFrame] = []
    for _, date_frame in merged.groupby("date", sort=True):
        residual_frame = date_frame.loc[:, ["date", "ticker", "baseline_signal_value"]].copy()
        residual_frame["signal_value"] = _residualize_against_baseline(
            date_frame["signal_value"],
            date_frame["baseline_signal_value"],
        )
        residualized_rows.append(residual_frame)
    return (
        pd.concat(residualized_rows, ignore_index=True)
        .loc[:, ["date", "ticker", "signal_value", "baseline_signal_value"]]
        .sort_values(["date", "ticker"])
        .reset_index(drop=True)
    )


def _attach_baseline_exposure_terciles(
    *,
    signal_frame: pd.DataFrame,
    baseline_frame: pd.DataFrame,
) -> pd.DataFrame:
    baseline_values = baseline_frame.rename(columns={"signal_value": "baseline_signal_value"})
    merged = signal_frame.merge(
        baseline_values.loc[:, ["date", "ticker", "baseline_signal_value"]],
        on=["date", "ticker"],
        how="inner",
    )
    if merged.empty:
        return pd.DataFrame(
            columns=["date", "ticker", "signal_value", "baseline_signal_value", "baseline_exposure_tercile_id", "baseline_exposure_tercile"]
        )
    rows: list[pd.DataFrame] = []
    label_map = {1: "low", 2: "mid", 3: "high"}
    for _, date_frame in merged.groupby("date", sort=True):
        work = date_frame.copy()
        bucket_ids = _quantile_buckets(work["baseline_signal_value"], quantiles=3)
        work["baseline_exposure_tercile_id"] = bucket_ids
        work["baseline_exposure_tercile"] = bucket_ids.map(label_map)
        rows.append(work)
    return pd.concat(rows, ignore_index=True).sort_values(["date", "ticker"]).reset_index(drop=True)


def build_baseline_residualized_expression_summary(
    *,
    returns_panel: pd.DataFrame,
    universe_reference: pd.DataFrame,
    expression_ids: list[str],
) -> pd.DataFrame:
    """Evaluate live expressions after cross-sectional residualization against the frozen baseline."""

    baseline_frame = build_calibration_signal_frame(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
        expression_id="CTRL3_BASELINE_MIMIC",
    ).rename(columns={"signal_value": "baseline_signal_value"})
    forward_return_frame = build_monthly_forward_return_frame(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
    )

    rows: list[dict[str, Any]] = []
    for expression_id in expression_ids:
        signal_frame = build_calibration_signal_frame(
            returns_panel=returns_panel,
            universe_reference=universe_reference,
            expression_id=expression_id,
        )
        residualized_signal_frame = _build_residualized_signal_frame(
            signal_frame=signal_frame,
            baseline_frame=baseline_frame.rename(columns={"baseline_signal_value": "signal_value"}),
        )
        if residualized_signal_frame.empty:
            rows.append(
                {
                    "expression_id": expression_id,
                    "residualized_evaluation_month_count": 0,
                    "residualized_mean_rank_ic": 0.0,
                    "residualized_rank_ic_t": 0.0,
                    "residualized_mean_top_bottom_spread": 0.0,
                }
            )
            continue
        per_date = _build_per_date_metrics(
            signal_frame=residualized_signal_frame.loc[:, ["date", "ticker", "signal_value"]],
            forward_return_frame=forward_return_frame,
            expression_id=expression_id,
        )
        rows.append(
            {
                "expression_id": expression_id,
                "residualized_evaluation_month_count": int(len(per_date)),
                "residualized_mean_rank_ic": float(pd.to_numeric(per_date["rank_ic"], errors="coerce").mean()) if not per_date.empty else 0.0,
                "residualized_rank_ic_t": _mean_t_stat(per_date["rank_ic"]) if not per_date.empty else 0.0,
                "residualized_mean_top_bottom_spread": float(pd.to_numeric(per_date["top_bottom_spread"], errors="coerce").mean())
                if not per_date.empty
                else 0.0,
            }
        )
    return pd.DataFrame(rows).sort_values("expression_id").reset_index(drop=True)


def build_residualization_placebo_null_distribution(
    *,
    returns_panel: pd.DataFrame,
    universe_reference: pd.DataFrame,
    expression_id: str,
    baseline_expression_id: str,
    random_seeds: list[int],
) -> pd.DataFrame:
    """Measure how strong residualization can look under a per-date shuffled null."""

    signal_frame = build_calibration_signal_frame(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
        expression_id=expression_id,
    )
    baseline_frame = build_calibration_signal_frame(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
        expression_id=baseline_expression_id,
    )
    forward_return_frame = build_monthly_forward_return_frame(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
    )

    rows: list[dict[str, Any]] = []
    grouped_frames = list(signal_frame.groupby("date", sort=True))
    for seed in random_seeds:
        shuffled_rows: list[pd.DataFrame] = []
        for offset, (_, date_frame) in enumerate(grouped_frames):
            rng = np.random.default_rng(int(seed) + int(offset))
            shuffled_frame = date_frame.loc[:, ["date", "ticker"]].copy()
            shuffled_frame["signal_value"] = rng.permutation(date_frame["signal_value"].to_numpy())
            shuffled_rows.append(shuffled_frame)
        shuffled_signal_frame = (
            pd.concat(shuffled_rows, ignore_index=True).sort_values(["date", "ticker"]).reset_index(drop=True)
            if shuffled_rows
            else pd.DataFrame(columns=["date", "ticker", "signal_value"])
        )
        residualized_signal_frame = _build_residualized_signal_frame(
            signal_frame=shuffled_signal_frame,
            baseline_frame=baseline_frame,
        )
        per_date = _build_per_date_metrics(
            signal_frame=residualized_signal_frame.loc[:, ["date", "ticker", "signal_value"]],
            forward_return_frame=forward_return_frame,
            expression_id=expression_id,
        )
        rows.append(
            {
                "seed": int(seed),
                "expression_id": expression_id,
                "baseline_expression_id": baseline_expression_id,
                "evaluation_month_count": int(len(per_date)),
                "residualized_mean_rank_ic": float(pd.to_numeric(per_date["rank_ic"], errors="coerce").mean())
                if not per_date.empty
                else 0.0,
                "residualized_rank_ic_t": _mean_t_stat(per_date["rank_ic"]) if not per_date.empty else 0.0,
                "residualized_mean_top_bottom_spread": float(
                    pd.to_numeric(per_date["top_bottom_spread"], errors="coerce").mean()
                )
                if not per_date.empty
                else 0.0,
            }
        )
    return pd.DataFrame(rows).sort_values("seed").reset_index(drop=True)


def build_baseline_exposure_tercile_decomposition(
    *,
    returns_panel: pd.DataFrame,
    universe_reference: pd.DataFrame,
    expression_id: str,
    baseline_expression_id: str,
) -> pd.DataFrame:
    """Decompose residualized strength by baseline exposure tercile."""

    signal_frame = build_calibration_signal_frame(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
        expression_id=expression_id,
    )
    baseline_frame = build_calibration_signal_frame(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
        expression_id=baseline_expression_id,
    )
    residualized_signal_frame = _build_residualized_signal_frame(
        signal_frame=signal_frame,
        baseline_frame=baseline_frame,
    )
    if residualized_signal_frame.empty:
        return pd.DataFrame(
            columns=[
                "expression_id",
                "baseline_expression_id",
                "baseline_exposure_tercile",
                "evaluation_month_count",
                "mean_rank_ic",
                "rank_ic_t",
                "mean_top_bottom_spread",
                "mean_observation_count",
            ]
        )
    tercile_signal_frame = _attach_baseline_exposure_terciles(
        signal_frame=residualized_signal_frame.loc[:, ["date", "ticker", "signal_value"]],
        baseline_frame=baseline_frame,
    )

    forward_return_frame = build_monthly_forward_return_frame(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
    )
    rows: list[dict[str, Any]] = []
    for bucket_id, bucket_label in [(1, "low"), (2, "mid"), (3, "high")]:
        bucket_signal_frame = tercile_signal_frame.loc[
            tercile_signal_frame["baseline_exposure_tercile_id"] == bucket_id,
            ["date", "ticker", "signal_value"],
        ].copy()
        per_date = _build_per_date_metrics(
            signal_frame=bucket_signal_frame,
            forward_return_frame=forward_return_frame,
            expression_id=expression_id,
        )
        rows.append(
            {
                "expression_id": expression_id,
                "baseline_expression_id": baseline_expression_id,
                "baseline_exposure_tercile": bucket_label,
                "evaluation_month_count": int(len(per_date)),
                "mean_rank_ic": float(pd.to_numeric(per_date["rank_ic"], errors="coerce").mean()) if not per_date.empty else 0.0,
                "rank_ic_t": _mean_t_stat(per_date["rank_ic"]) if not per_date.empty else 0.0,
                "mean_top_bottom_spread": float(pd.to_numeric(per_date["top_bottom_spread"], errors="coerce").mean())
                if not per_date.empty
                else 0.0,
                "mean_observation_count": float(pd.to_numeric(per_date["observation_count"], errors="coerce").mean())
                if not per_date.empty
                else 0.0,
            }
        )
    decomposition = pd.DataFrame(rows)
    decomposition["baseline_exposure_tercile"] = pd.Categorical(
        decomposition["baseline_exposure_tercile"],
        categories=["low", "mid", "high"],
        ordered=True,
    )
    return decomposition.sort_values("baseline_exposure_tercile").reset_index(drop=True)


def build_exposure_conditioned_residualization_placebo_null_distribution(
    *,
    returns_panel: pd.DataFrame,
    universe_reference: pd.DataFrame,
    expression_id: str,
    baseline_expression_id: str,
    random_seeds: list[int],
) -> pd.DataFrame:
    """Build a residualization placebo null that preserves per-date baseline-exposure terciles."""

    signal_frame = build_calibration_signal_frame(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
        expression_id=expression_id,
    )
    baseline_frame = build_calibration_signal_frame(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
        expression_id=baseline_expression_id,
    )
    signal_with_exposure = _attach_baseline_exposure_terciles(
        signal_frame=signal_frame,
        baseline_frame=baseline_frame,
    )
    forward_return_frame = build_monthly_forward_return_frame(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
    )

    rows: list[dict[str, Any]] = []
    grouped_dates = list(signal_with_exposure.groupby("date", sort=True))
    for seed in random_seeds:
        conditioned_rows: list[pd.DataFrame] = []
        for offset, (_, date_frame) in enumerate(grouped_dates):
            bucket_rows: list[pd.DataFrame] = []
            for bucket_id, bucket_frame in date_frame.groupby("baseline_exposure_tercile_id", sort=True):
                rng = np.random.default_rng(int(seed) + int(offset) * 10 + int(bucket_id))
                shuffled_bucket = bucket_frame.loc[:, ["date", "ticker", "signal_value"]].copy()
                shuffled_bucket["signal_value"] = rng.permutation(bucket_frame["signal_value"].to_numpy())
                bucket_rows.append(shuffled_bucket)
            conditioned_rows.append(pd.concat(bucket_rows, ignore_index=True))
        conditioned_signal_frame = (
            pd.concat(conditioned_rows, ignore_index=True).sort_values(["date", "ticker"]).reset_index(drop=True)
            if conditioned_rows
            else pd.DataFrame(columns=["date", "ticker", "signal_value"])
        )
        residualized_signal_frame = _build_residualized_signal_frame(
            signal_frame=conditioned_signal_frame,
            baseline_frame=baseline_frame,
        )
        residualized_with_exposure = _attach_baseline_exposure_terciles(
            signal_frame=residualized_signal_frame.loc[:, ["date", "ticker", "signal_value"]],
            baseline_frame=baseline_frame,
        )

        overall_per_date = _build_per_date_metrics(
            signal_frame=residualized_signal_frame.loc[:, ["date", "ticker", "signal_value"]],
            forward_return_frame=forward_return_frame,
            expression_id=expression_id,
        )
        rows.append(
            {
                "seed": int(seed),
                "expression_id": expression_id,
                "baseline_expression_id": baseline_expression_id,
                "baseline_exposure_tercile": "all",
                "evaluation_month_count": int(len(overall_per_date)),
                "residualized_mean_rank_ic": float(pd.to_numeric(overall_per_date["rank_ic"], errors="coerce").mean())
                if not overall_per_date.empty
                else 0.0,
                "residualized_rank_ic_t": _mean_t_stat(overall_per_date["rank_ic"]) if not overall_per_date.empty else 0.0,
                "residualized_mean_top_bottom_spread": float(
                    pd.to_numeric(overall_per_date["top_bottom_spread"], errors="coerce").mean()
                )
                if not overall_per_date.empty
                else 0.0,
            }
        )
        for bucket_label in ["low", "mid", "high"]:
            bucket_signal_frame = residualized_with_exposure.loc[
                residualized_with_exposure["baseline_exposure_tercile"] == bucket_label,
                ["date", "ticker", "signal_value"],
            ].copy()
            per_date = _build_per_date_metrics(
                signal_frame=bucket_signal_frame,
                forward_return_frame=forward_return_frame,
                expression_id=expression_id,
            )
            rows.append(
                {
                    "seed": int(seed),
                    "expression_id": expression_id,
                    "baseline_expression_id": baseline_expression_id,
                    "baseline_exposure_tercile": bucket_label,
                    "evaluation_month_count": int(len(per_date)),
                    "residualized_mean_rank_ic": float(pd.to_numeric(per_date["rank_ic"], errors="coerce").mean())
                    if not per_date.empty
                    else 0.0,
                    "residualized_rank_ic_t": _mean_t_stat(per_date["rank_ic"]) if not per_date.empty else 0.0,
                    "residualized_mean_top_bottom_spread": float(
                        pd.to_numeric(per_date["top_bottom_spread"], errors="coerce").mean()
                    )
                    if not per_date.empty
                    else 0.0,
                }
            )
    conditioned_null = pd.DataFrame(rows)
    conditioned_null["baseline_exposure_tercile"] = pd.Categorical(
        conditioned_null["baseline_exposure_tercile"],
        categories=["all", "low", "mid", "high"],
        ordered=True,
    )
    return conditioned_null.sort_values(["seed", "baseline_exposure_tercile"]).reset_index(drop=True)


def build_baseline_exposure_tercile_null_comparison(
    *,
    observed_decomposition_frame: pd.DataFrame,
    conditioned_null_frame: pd.DataFrame,
) -> pd.DataFrame:
    """Compare observed tercile reads against the exposure-conditioned placebo null."""

    rows: list[dict[str, Any]] = []
    for bucket_label in ["low", "mid", "high"]:
        observed = observed_decomposition_frame.loc[
            observed_decomposition_frame["baseline_exposure_tercile"] == bucket_label
        ]
        if observed.empty:
            continue
        observed_row = observed.iloc[0]
        sample = conditioned_null_frame.loc[
            conditioned_null_frame["baseline_exposure_tercile"] == bucket_label
        ]
        rows.append(
            {
                "baseline_exposure_tercile": bucket_label,
                "observed_rank_ic_t": float(observed_row["rank_ic_t"]),
                "observed_rank_ic_t_null_percentile": _empirical_percentile(
                    sample["residualized_rank_ic_t"],
                    float(observed_row["rank_ic_t"]),
                ),
                "null_rank_ic_t_median": float(pd.to_numeric(sample["residualized_rank_ic_t"], errors="coerce").median())
                if not sample.empty
                else float("nan"),
                "observed_mean_top_bottom_spread": float(observed_row["mean_top_bottom_spread"]),
                "observed_spread_null_percentile": _empirical_percentile(
                    sample["residualized_mean_top_bottom_spread"],
                    float(observed_row["mean_top_bottom_spread"]),
                ),
                "null_mean_top_bottom_spread_median": float(
                    pd.to_numeric(sample["residualized_mean_top_bottom_spread"], errors="coerce").median()
                )
                if not sample.empty
                else float("nan"),
            }
        )
    comparison = pd.DataFrame(rows)
    comparison["baseline_exposure_tercile"] = pd.Categorical(
        comparison["baseline_exposure_tercile"],
        categories=["low", "mid", "high"],
        ordered=True,
    )
    return comparison.sort_values("baseline_exposure_tercile").reset_index(drop=True)


def _build_per_date_metrics(
    *,
    signal_frame: pd.DataFrame,
    forward_return_frame: pd.DataFrame,
    expression_id: str,
) -> pd.DataFrame:
    merged = signal_frame.merge(forward_return_frame.loc[:, ["date", "ticker", "forward_return"]], on=["date", "ticker"], how="inner")
    rows: list[dict[str, Any]] = []
    for date_value, date_frame in merged.groupby("date", sort=True):
        clean = date_frame.dropna(subset=["signal_value", "forward_return"]).copy()
        if len(clean) < _MIN_ASSETS_PER_DATE:
            continue
        buckets = _quantile_buckets(clean["signal_value"])
        top_return = clean.loc[buckets == _QUANTILES, "forward_return"].mean()
        bottom_return = clean.loc[buckets == 1, "forward_return"].mean()
        rows.append(
            {
                "expression_id": expression_id,
                "date": str(date_value),
                "observation_count": int(len(clean)),
                "coverage_ratio": float(len(clean) / clean["ticker"].nunique()) if len(clean) else 0.0,
                "rank_ic": _safe_spearman(clean["signal_value"], clean["forward_return"]),
                "top_bottom_spread": float(top_return - bottom_return) if pd.notna(top_return) and pd.notna(bottom_return) else float("nan"),
            }
        )
    return pd.DataFrame(rows)


def _render_calibration_note(
    *,
    summary_frame: pd.DataFrame,
    registry_frame: pd.DataFrame,
    spread_corr_frame: pd.DataFrame,
    rm3_placebo_null_frame: pd.DataFrame,
    rm3_conditioned_placebo_null_frame: pd.DataFrame,
    rm3_tercile_decomposition_frame: pd.DataFrame,
    rm3_tercile_null_comparison_frame: pd.DataFrame,
) -> str:
    expression_rows = summary_frame.loc[summary_frame["role"] == "expression"].sort_values(
        ["mean_rank_ic", "rank_ic_t"], ascending=[False, False]
    )
    control_rows = summary_frame.loc[summary_frame["role"] == "control"].sort_values(
        ["mean_rank_ic", "rank_ic_t"], ascending=[False, False]
    )
    best_expression = expression_rows.iloc[0] if not expression_rows.empty else None

    lines = [
        "# US Residual Momentum Calibration Note",
        "",
        "## Purpose",
        "",
        "This run evaluates the calibration family as a discovery-machine check, not as a winner search.",
        "",
        "## Registry",
        "",
        f"- expressions: {int((registry_frame['role'] == 'expression').sum())}",
        f"- controls: {int((registry_frame['role'] == 'control').sum())}",
        "",
        "## Best Expression Read",
        "",
    ]
    if best_expression is None:
        lines.extend(["- no expression produced evaluable months", ""])
    else:
        off_diag_abs_max = float("nan")
        if not spread_corr_frame.empty:
            corr_values = spread_corr_frame.to_numpy(dtype=float)
            if corr_values.size:
                mask = ~np.eye(corr_values.shape[0], dtype=bool)
                off_diag = np.abs(corr_values[mask])
                finite = off_diag[np.isfinite(off_diag)]
                if finite.size:
                    off_diag_abs_max = float(finite.max())
        lines.extend(
            [
                f"- best expression: `{best_expression['expression_id']}`",
                f"- mean rank IC: `{best_expression['mean_rank_ic']:.4f}`",
                f"- rank IC t-stat: `{best_expression['rank_ic_t']:.4f}`",
                f"- mean top-bottom spread: `{best_expression['mean_top_bottom_spread']:.4%}`",
                f"- shuffled-null mean-rank-IC percentile: `{float(best_expression['shuffle_null_mean_rank_ic_percentile']):.2%}`",
                f"- shuffled-null rank-IC-t percentile: `{float(best_expression['shuffle_null_rank_ic_t_percentile']):.2%}`",
                f"- bootstrap top-1 frequency (rank IC): `{float(best_expression['bootstrap_top1_frequency_rank_ic']):.2%}`",
                f"- baseline-residualized rank IC t-stat: `{float(best_expression['baseline_residualized_rank_ic_t']):.4f}`",
                f"- baseline-residualized mean top-bottom spread: `{float(best_expression['baseline_residualized_mean_top_bottom_spread']):.4%}`",
                (
                    f"- max absolute pairwise spread correlation across live expressions: `{off_diag_abs_max:.4f}`"
                    if np.isfinite(off_diag_abs_max)
                    else "- max absolute pairwise spread correlation across live expressions: `n/a`"
                ),
                "",
            ]
        )
    if not rm3_placebo_null_frame.empty:
        lines.extend(
            [
                "## RM3 Residualization Diagnostic",
                "",
                f"- live residualized `rank_ic_t`: `{float(summary_frame.loc[summary_frame['expression_id'] == 'RM3_VOL_MANAGED', 'baseline_residualized_rank_ic_t'].iloc[0]):.4f}`",
                f"- placebo-null percentile for residualized `rank_ic_t`: `{_empirical_percentile(rm3_placebo_null_frame['residualized_rank_ic_t'], float(summary_frame.loc[summary_frame['expression_id'] == 'RM3_VOL_MANAGED', 'baseline_residualized_rank_ic_t'].iloc[0])):.2%}`",
                (
                    f"- exposure-conditioned placebo-null percentile for residualized `rank_ic_t`: "
                    f"`{_empirical_percentile(rm3_conditioned_placebo_null_frame.loc[rm3_conditioned_placebo_null_frame['baseline_exposure_tercile'] == 'all', 'residualized_rank_ic_t'], float(summary_frame.loc[summary_frame['expression_id'] == 'RM3_VOL_MANAGED', 'baseline_residualized_rank_ic_t'].iloc[0])):.2%}`"
                    if not rm3_conditioned_placebo_null_frame.empty
                    else "- exposure-conditioned placebo-null percentile for residualized `rank_ic_t`: `n/a`"
                ),
                "",
            ]
        )
    if not rm3_tercile_decomposition_frame.empty:
        lines.extend(
            [
                "### RM3 Baseline-Exposure Terciles",
                "",
            ]
        )
        for row in rm3_tercile_decomposition_frame.itertuples(index=False):
            lines.append(
                f"- `{row.baseline_exposure_tercile}`: rank_ic_t={float(row.rank_ic_t):.4f}, "
                f"spread={float(row.mean_top_bottom_spread):.4%}, months={int(row.evaluation_month_count)}"
            )
        lines.append("")
    if not rm3_tercile_null_comparison_frame.empty:
        lines.extend(
            [
                "### RM3 Tercile Null Comparison",
                "",
            ]
        )
        for row in rm3_tercile_null_comparison_frame.itertuples(index=False):
            lines.append(
                f"- `{row.baseline_exposure_tercile}`: rank_ic_t null percentile={float(row.observed_rank_ic_t_null_percentile):.2%}, "
                f"spread null percentile={float(row.observed_spread_null_percentile):.2%}"
            )
        lines.append("")
    lines.extend(
        [
            "## Control Read",
            "",
            "Controls in this first slice are expected to be weak or unstable. The goal here is to ensure the control path exists and can be compared directly against the live expressions.",
            "",
            "## Interpretation Boundary",
            "",
            "- this artifact is a calibration-family setup run, not a family closeout",
            "- a strong expression read here does not yet imply a winner",
            "- the next justified step is deeper calibration execution and adversarial comparison, not primary-family mining",
            "",
        ]
    )
    if not control_rows.empty:
        lines.append("## Control Snapshot")
        lines.append("")
        for row in control_rows.itertuples(index=False):
            lines.append(
                f"- `{row.expression_id}`: mean_rank_ic={float(row.mean_rank_ic):.4f}, "
                f"rank_ic_t={float(row.rank_ic_t):.4f}, spread={float(row.mean_top_bottom_spread):.4%}"
            )
        lines.append("")
    return "\n".join(lines)


def run_us_residual_momentum_calibration(
    *,
    returns_panel: pd.DataFrame,
    universe_reference: pd.DataFrame,
    output_dir: str | Path,
    random_seed: int = 0,
) -> CalibrationRunResult:
    """Run the first calibration-family slice and write artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    registry = build_us_residual_momentum_calibration_registry()
    registry_frame = pd.DataFrame(asdict(item) for item in registry)
    shuffle_null_frame = build_shuffled_null_distribution(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
        random_seeds=list(range(100)),
    )
    forward_return_frame = build_monthly_forward_return_frame(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
    )

    per_date_frames: list[pd.DataFrame] = []
    summary_rows: list[dict[str, Any]] = []
    for item in registry:
        signal_frame = build_calibration_signal_frame(
            returns_panel=returns_panel,
            universe_reference=universe_reference,
            expression_id=item.expression_id,
            random_seed=random_seed,
        )
        per_date = _build_per_date_metrics(
            signal_frame=signal_frame,
            forward_return_frame=forward_return_frame,
            expression_id=item.expression_id,
        )
        per_date_frames.append(per_date)
        summary_rows.append(
            {
                **asdict(item),
                "evaluation_month_count": int(len(per_date)),
                "coverage_median": float(pd.to_numeric(per_date["observation_count"], errors="coerce").median())
                if not per_date.empty
                else 0.0,
                "mean_rank_ic": float(pd.to_numeric(per_date["rank_ic"], errors="coerce").mean()) if not per_date.empty else 0.0,
                "rank_ic_t": _mean_t_stat(per_date["rank_ic"]) if not per_date.empty else 0.0,
                "mean_top_bottom_spread": float(pd.to_numeric(per_date["top_bottom_spread"], errors="coerce").mean())
                if not per_date.empty
                else 0.0,
            }
        )

    per_date_frame = pd.concat(per_date_frames, ignore_index=True) if per_date_frames else pd.DataFrame()
    summary_frame = pd.DataFrame(summary_rows).sort_values(
        ["role", "mean_rank_ic", "rank_ic_t", "expression_id"],
        ascending=[True, False, False, True],
    ).reset_index(drop=True)
    summary_frame["shuffle_null_mean_rank_ic_percentile"] = summary_frame["mean_rank_ic"].map(
        lambda value: _empirical_percentile(shuffle_null_frame["mean_rank_ic"], float(value))
    )
    summary_frame["shuffle_null_rank_ic_t_percentile"] = summary_frame["rank_ic_t"].map(
        lambda value: _empirical_percentile(shuffle_null_frame["rank_ic_t"], float(value))
    )
    expression_ids = registry_frame.loc[registry_frame["role"] == "expression", "expression_id"].astype(str).tolist()
    bootstrap_frame = build_bootstrap_expression_rankings(
        per_date_frame=per_date_frame,
        expression_ids=expression_ids,
        bootstrap_iterations=500,
        random_seed=random_seed,
    )
    spread_corr_frame = build_expression_spread_correlation_matrix(
        per_date_frame=per_date_frame,
        expression_ids=expression_ids,
    )
    bootstrap_top1_frequency = (
        bootstrap_frame.loc[bootstrap_frame["rank_by_rank_ic"] == 1]
        .groupby("expression_id")
        .size()
        .div(float(bootstrap_frame["bootstrap_id"].nunique()) if not bootstrap_frame.empty else 1.0)
        if not bootstrap_frame.empty
        else pd.Series(dtype=float)
    )
    summary_frame["bootstrap_top1_frequency_rank_ic"] = (
        summary_frame["expression_id"].map(bootstrap_top1_frequency).fillna(0.0)
    )
    residualized_summary = build_baseline_residualized_expression_summary(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
        expression_ids=expression_ids,
    )
    rm3_placebo_null_frame = build_residualization_placebo_null_distribution(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
        expression_id="RM3_VOL_MANAGED",
        baseline_expression_id="CTRL3_BASELINE_MIMIC",
        random_seeds=list(range(100)),
    )
    rm3_conditioned_placebo_null_frame = build_exposure_conditioned_residualization_placebo_null_distribution(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
        expression_id="RM3_VOL_MANAGED",
        baseline_expression_id="CTRL3_BASELINE_MIMIC",
        random_seeds=list(range(100)),
    )
    rm3_tercile_decomposition_frame = build_baseline_exposure_tercile_decomposition(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
        expression_id="RM3_VOL_MANAGED",
        baseline_expression_id="CTRL3_BASELINE_MIMIC",
    )
    rm3_tercile_null_comparison_frame = build_baseline_exposure_tercile_null_comparison(
        observed_decomposition_frame=rm3_tercile_decomposition_frame,
        conditioned_null_frame=rm3_conditioned_placebo_null_frame,
    )
    summary_frame = summary_frame.merge(
        residualized_summary.rename(
            columns={
                "residualized_evaluation_month_count": "baseline_residualized_evaluation_month_count",
                "residualized_mean_rank_ic": "baseline_residualized_mean_rank_ic",
                "residualized_rank_ic_t": "baseline_residualized_rank_ic_t",
                "residualized_mean_top_bottom_spread": "baseline_residualized_mean_top_bottom_spread",
            }
        ),
        on="expression_id",
        how="left",
    )

    note_markdown = _render_calibration_note(
        summary_frame=summary_frame,
        registry_frame=registry_frame,
        spread_corr_frame=spread_corr_frame,
        rm3_placebo_null_frame=rm3_placebo_null_frame,
        rm3_conditioned_placebo_null_frame=rm3_conditioned_placebo_null_frame,
        rm3_tercile_decomposition_frame=rm3_tercile_decomposition_frame,
        rm3_tercile_null_comparison_frame=rm3_tercile_null_comparison_frame,
    )
    rm3_live_residualized_rank_ic_t = float(
        summary_frame.loc[
            summary_frame["expression_id"] == "RM3_VOL_MANAGED",
            "baseline_residualized_rank_ic_t",
        ].iloc[0]
    )
    summary_payload = {
        "family_id": "US_RESIDUAL_MOMENTUM_CALIBRATION",
        "registry_count": int(len(registry_frame)),
        "expression_count": int((registry_frame["role"] == "expression").sum()),
        "control_count": int((registry_frame["role"] == "control").sum()),
        "shuffle_null_seed_count": int(len(shuffle_null_frame)),
        "bootstrap_iteration_count": int(bootstrap_frame["bootstrap_id"].nunique()) if not bootstrap_frame.empty else 0,
        "residualized_expression_count": int(len(residualized_summary)),
        "rm3_residualized_rank_ic_t_null_percentile": _empirical_percentile(
            rm3_placebo_null_frame["residualized_rank_ic_t"],
            rm3_live_residualized_rank_ic_t,
        ),
        "rm3_residualized_rank_ic_t_exposure_conditioned_null_percentile": _empirical_percentile(
            rm3_conditioned_placebo_null_frame.loc[
                rm3_conditioned_placebo_null_frame["baseline_exposure_tercile"] == "all",
                "residualized_rank_ic_t",
            ],
            rm3_live_residualized_rank_ic_t,
        ),
        "best_expression_id": str(summary_frame.loc[summary_frame["role"] == "expression"].iloc[0]["expression_id"])
        if not summary_frame.loc[summary_frame["role"] == "expression"].empty
        else None,
    }

    registry_frame.to_csv(output_path / "registry.csv", index=False)
    per_date_frame.to_csv(output_path / "per_date_metrics.csv", index=False)
    summary_frame.to_csv(output_path / "summary.csv", index=False)
    shuffle_null_frame.to_csv(output_path / "shuffle_null_distribution.csv", index=False)
    bootstrap_frame.to_csv(output_path / "bootstrap_expression_rankings.csv", index=False)
    spread_corr_frame.to_csv(output_path / "expression_spread_correlation.csv")
    residualized_summary.to_csv(output_path / "residualized_vs_baseline_summary.csv", index=False)
    rm3_placebo_null_frame.to_csv(output_path / "residualization_placebo_null_distribution.csv", index=False)
    rm3_conditioned_placebo_null_frame.to_csv(
        output_path / "residualization_placebo_null_distribution_exposure_conditioned.csv",
        index=False,
    )
    rm3_tercile_decomposition_frame.to_csv(output_path / "baseline_exposure_tercile_decomposition.csv", index=False)
    rm3_tercile_null_comparison_frame.to_csv(
        output_path / "baseline_exposure_tercile_null_comparison.csv",
        index=False,
    )
    write_json(output_path / "summary.json", summary_payload)
    write_text(output_path / "note.md", note_markdown)

    return CalibrationRunResult(
        output_dir=output_path,
        registry_frame=registry_frame,
        per_date_frame=per_date_frame,
        summary_frame=summary_frame,
        summary_payload=summary_payload,
        note_markdown=note_markdown,
    )


def run_us_residual_momentum_calibration_from_files(
    *,
    returns_file: str | Path,
    universe_reference_file: str | Path,
    output_dir: str | Path,
    random_seed: int = 0,
) -> CalibrationRunResult:
    """File-backed wrapper for the calibration run."""

    returns_panel = load_alpha_returns_panel(returns_file)
    universe_reference = pd.read_csv(universe_reference_file)
    return run_us_residual_momentum_calibration(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
        output_dir=output_dir,
        random_seed=random_seed,
    )
