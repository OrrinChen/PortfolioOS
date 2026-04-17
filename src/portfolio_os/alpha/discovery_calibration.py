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
        lines.extend(
            [
                f"- best expression: `{best_expression['expression_id']}`",
                f"- mean rank IC: `{best_expression['mean_rank_ic']:.4f}`",
                f"- rank IC t-stat: `{best_expression['rank_ic_t']:.4f}`",
                f"- mean top-bottom spread: `{best_expression['mean_top_bottom_spread']:.4%}`",
                f"- shuffled-null mean-rank-IC percentile: `{float(best_expression['shuffle_null_mean_rank_ic_percentile']):.2%}`",
                f"- shuffled-null rank-IC-t percentile: `{float(best_expression['shuffle_null_rank_ic_t_percentile']):.2%}`",
                "",
            ]
        )
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

    note_markdown = _render_calibration_note(summary_frame=summary_frame, registry_frame=registry_frame)
    summary_payload = {
        "family_id": "US_RESIDUAL_MOMENTUM_CALIBRATION",
        "registry_count": int(len(registry_frame)),
        "expression_count": int((registry_frame["role"] == "expression").sum()),
        "control_count": int((registry_frame["role"] == "control").sum()),
        "shuffle_null_seed_count": int(len(shuffle_null_frame)),
        "best_expression_id": str(summary_frame.loc[summary_frame["role"] == "expression"].iloc[0]["expression_id"])
        if not summary_frame.loc[summary_frame["role"] == "expression"].empty
        else None,
    }

    registry_frame.to_csv(output_path / "registry.csv", index=False)
    per_date_frame.to_csv(output_path / "per_date_metrics.csv", index=False)
    summary_frame.to_csv(output_path / "summary.csv", index=False)
    shuffle_null_frame.to_csv(output_path / "shuffle_null_distribution.csv", index=False)
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
