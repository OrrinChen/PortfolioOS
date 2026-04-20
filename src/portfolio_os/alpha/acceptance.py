"""Phase 1 alpha acceptance-gate workflow."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from portfolio_os.alpha.report import render_alpha_acceptance_note
from portfolio_os.alpha.research import build_alpha_ic_frame, build_alpha_research_frame, load_alpha_returns_panel
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.storage.snapshots import file_metadata, write_json, write_text


_PRIMARY_SIGNAL_NAME = "blended_alpha"
_BASELINE_RECIPE_NAME = "equal_weight_momentum_6_1"


@dataclass(frozen=True)
class AlphaRecipeConfig:
    """Deterministic factor recipe configuration for one acceptance run."""

    recipe_name: str
    reversal_lookback_days: int
    momentum_lookback_days: int
    momentum_skip_days: int
    forward_horizon_days: int
    reversal_weight: float
    momentum_weight: float
    quantiles: int = 5
    min_assets_per_date: int = 20


@dataclass
class AlphaRecipeEvaluation:
    """One evaluated recipe before slice-level summarization."""

    recipe: AlphaRecipeConfig
    signal_frame: pd.DataFrame
    ic_frame: pd.DataFrame


@dataclass
class AlphaAcceptanceResult:
    """Top-level acceptance gate outputs."""

    returns_file: Path
    output_dir: Path
    summary_frame: pd.DataFrame
    decision_payload: dict[str, object]
    manifest_payload: dict[str, object]
    note_markdown: str


def default_round_one_recipes() -> list[AlphaRecipeConfig]:
    """Return the fixed Phase 1 first-round recipe set."""

    return [
        AlphaRecipeConfig("equal_weight_momentum_6_1", 21, 126, 21, 5, 0.0, 1.0),
        AlphaRecipeConfig("momentum_heavy_10_90", 21, 126, 21, 5, 0.1, 0.9),
        AlphaRecipeConfig("momentum_heavy_25_75", 21, 126, 21, 5, 0.25, 0.75),
        AlphaRecipeConfig("current_50_50", 21, 126, 21, 5, 0.5, 0.5),
        AlphaRecipeConfig("alt_momentum_4_1", 21, 84, 21, 5, 0.0, 1.0),
    ]


def build_alpha_recipe_result(
    returns_panel: pd.DataFrame,
    *,
    recipe: AlphaRecipeConfig,
) -> AlphaRecipeEvaluation:
    """Evaluate one recipe with the existing alpha research engine."""

    signal_frame = build_alpha_research_frame(
        returns_panel,
        reversal_lookback_days=recipe.reversal_lookback_days,
        momentum_lookback_days=recipe.momentum_lookback_days,
        momentum_skip_days=recipe.momentum_skip_days,
        forward_horizon_days=recipe.forward_horizon_days,
        reversal_weight=recipe.reversal_weight,
        momentum_weight=recipe.momentum_weight,
    )
    ic_frame = build_alpha_ic_frame(
        signal_frame,
        min_assets_per_date=recipe.min_assets_per_date,
        quantiles=recipe.quantiles,
    )
    primary_ic_frame = (
        ic_frame.loc[ic_frame["signal_name"] == _PRIMARY_SIGNAL_NAME]
        .copy()
        .sort_values("date")
        .reset_index(drop=True)
    )
    if primary_ic_frame.empty:
        raise InputValidationError(
            f"Recipe {recipe.recipe_name} produced no primary blended-alpha evaluation dates."
        )
    return AlphaRecipeEvaluation(
        recipe=recipe,
        signal_frame=signal_frame.copy(),
        ic_frame=primary_ic_frame,
    )


def _intersect_evaluation_dates(recipe_ic_frames: list[pd.DataFrame]) -> list[str]:
    """Return the sorted intersection of evaluation dates across recipes."""

    date_sets = [set(frame["date"].tolist()) for frame in recipe_ic_frames if not frame.empty]
    if not date_sets:
        return []
    return sorted(set.intersection(*date_sets))


def _split_common_dates(common_dates: list[str]) -> tuple[list[str], list[str]]:
    """Split aligned evaluation dates into development and holdout slices."""

    if len(common_dates) < 2:
        return [], []
    split_index = int(len(common_dates) * 0.6)
    split_index = min(max(split_index, 1), len(common_dates) - 1)
    return common_dates[:split_index], common_dates[split_index:]


def _top_quantile_weights(
    frame: pd.DataFrame,
    *,
    score_column: str,
    quantiles: int,
) -> pd.Series:
    """Create one equal-weight top-quantile long basket."""

    if frame.empty:
        return pd.Series(dtype=float)
    ranks = np.ceil(frame[score_column].rank(method="first", pct=True) * quantiles).clip(1, quantiles)
    winners = frame.loc[ranks == quantiles, "ticker"].astype(str).tolist()
    if not winners:
        return pd.Series(dtype=float)
    weight = 1.0 / float(len(winners))
    return pd.Series({ticker: weight for ticker in winners}, dtype=float).sort_index()


def _compute_mean_monthly_factor_turnover(
    signal_frame: pd.DataFrame,
    *,
    score_column: str,
    quantiles: int,
) -> float:
    """Estimate average monthly factor turnover from top-quantile baskets."""

    if signal_frame.empty:
        return float("nan")
    work = signal_frame.loc[:, ["date", "ticker", score_column]].copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.dropna(subset=["date"]).sort_values(["date", "ticker"]).reset_index(drop=True)
    if work.empty:
        return float("nan")

    month_end_dates = (
        work.groupby(work["date"].dt.to_period("M"))["date"]
        .max()
        .sort_values()
        .tolist()
    )
    if len(month_end_dates) < 2:
        return float("nan")

    baskets: list[pd.Series] = []
    for date_value in month_end_dates:
        date_frame = work.loc[work["date"] == date_value].copy()
        baskets.append(_top_quantile_weights(date_frame, score_column=score_column, quantiles=quantiles))

    turnovers: list[float] = []
    for previous, current in zip(baskets, baskets[1:]):
        tickers = previous.index.union(current.index)
        previous_aligned = previous.reindex(tickers, fill_value=0.0)
        current_aligned = current.reindex(tickers, fill_value=0.0)
        turnovers.append(0.5 * float((current_aligned - previous_aligned).abs().sum()))
    if not turnovers:
        return float("nan")
    return float(np.mean(turnovers))


def _slice_metric_row(
    *,
    round_number: int,
    recipe: AlphaRecipeConfig,
    slice_name: str,
    common_evaluation_dates: list[str],
    slice_dates: list[str],
    signal_frame: pd.DataFrame,
    ic_frame: pd.DataFrame,
    baseline_recipe_name: str,
) -> dict[str, object]:
    """Build one flattened summary row for one recipe/slice pair."""

    slice_signal_frame = signal_frame.loc[signal_frame["date"].isin(slice_dates)].copy()
    slice_ic_frame = ic_frame.loc[ic_frame["date"].isin(slice_dates)].copy()
    mean_monthly_factor_turnover = _compute_mean_monthly_factor_turnover(
        slice_signal_frame,
        score_column="alpha_score",
        quantiles=recipe.quantiles,
    )
    return {
        "round_number": int(round_number),
        "recipe_name": recipe.recipe_name,
        "slice_name": slice_name,
        "is_baseline": recipe.recipe_name == baseline_recipe_name,
        "common_evaluation_date_count": int(len(common_evaluation_dates)),
        "evaluation_date_count": int(len(slice_ic_frame)),
        "mean_ic": float(slice_ic_frame["ic"].mean()) if not slice_ic_frame.empty else 0.0,
        "mean_rank_ic": float(slice_ic_frame["rank_ic"].mean()) if not slice_ic_frame.empty else 0.0,
        "positive_rank_ic_ratio": float((slice_ic_frame["rank_ic"] > 0.0).mean()) if not slice_ic_frame.empty else 0.0,
        "mean_top_bottom_spread": float(slice_ic_frame["top_bottom_spread"].mean()) if not slice_ic_frame.empty else 0.0,
        "mean_monthly_factor_turnover": float(mean_monthly_factor_turnover),
        "reversal_lookback_days": int(recipe.reversal_lookback_days),
        "momentum_lookback_days": int(recipe.momentum_lookback_days),
        "momentum_skip_days": int(recipe.momentum_skip_days),
        "forward_horizon_days": int(recipe.forward_horizon_days),
        "reversal_weight": float(recipe.reversal_weight),
        "momentum_weight": float(recipe.momentum_weight),
        "quantiles": int(recipe.quantiles),
        "min_assets_per_date": int(recipe.min_assets_per_date),
    }


def _sort_recipe_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Apply deterministic recipe ranking for development or holdout comparisons."""

    return frame.sort_values(
        by=["mean_rank_ic", "positive_rank_ic_ratio", "mean_top_bottom_spread", "recipe_name"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)


def _evaluate_relative_gate(candidate_row: pd.Series, baseline_row: pd.Series) -> bool:
    """Check whether one challenger beats the current baseline on holdout."""

    return (
        float(candidate_row["mean_rank_ic"]) > float(baseline_row["mean_rank_ic"])
        and float(candidate_row["mean_top_bottom_spread"]) > float(baseline_row["mean_top_bottom_spread"])
        and float(candidate_row["positive_rank_ic_ratio"]) >= float(baseline_row["positive_rank_ic_ratio"]) - 0.02
    )


def _evaluate_absolute_gate(summary_row: pd.Series) -> bool:
    """Check Phase 1 minimum viability thresholds on one slice row."""

    turnover = float(summary_row["mean_monthly_factor_turnover"])
    return (
        float(summary_row["mean_rank_ic"]) >= 0.01
        and float(summary_row["positive_rank_ic_ratio"]) >= 0.52
        and float(summary_row["mean_top_bottom_spread"]) > 0.0
        and int(summary_row["evaluation_date_count"]) >= 40
        and np.isfinite(turnover)
        and turnover <= 0.8
    )


def _bounded_round_three_lookbacks(parent_lookback: int) -> list[int]:
    """Return the round-three local lookback neighborhood."""

    values = {
        max(63, int(parent_lookback) - 21),
        int(parent_lookback),
        min(189, int(parent_lookback) + 21),
    }
    return sorted(values)


def _expand_round_recipes(
    *,
    round_number: int,
    parent_recipes: list[AlphaRecipeConfig],
    tested_recipe_names: set[str],
) -> list[AlphaRecipeConfig]:
    """Expand the search space deterministically around top development recipes."""

    candidates: list[AlphaRecipeConfig] = []
    if round_number == 2:
        lookback_grid = (84, 126, 168)
        skip_grid = (10, 21)
        reversal_grid = (0.0, 0.1, 0.25)
    elif round_number == 3:
        skip_grid = (5, 10, 21)
        reversal_grid = (0.0, 0.05, 0.1, 0.25)
    else:
        raise InputValidationError(f"Unsupported expansion round {round_number}.")

    for parent in parent_recipes:
        lookback_values = lookback_grid if round_number == 2 else _bounded_round_three_lookbacks(parent.momentum_lookback_days)
        for lookback in lookback_values:
            for skip in skip_grid:
                for reversal_weight in reversal_grid:
                    momentum_weight = 1.0 - float(reversal_weight)
                    recipe_name = (
                        f"mom_{int(lookback)}_skip_{int(skip)}_rev_{int(round(float(reversal_weight) * 100)):02d}"
                    )
                    recipe = AlphaRecipeConfig(
                        recipe_name=recipe_name,
                        reversal_lookback_days=21,
                        momentum_lookback_days=int(lookback),
                        momentum_skip_days=int(skip),
                        forward_horizon_days=5,
                        reversal_weight=float(reversal_weight),
                        momentum_weight=float(momentum_weight),
                    )
                    if recipe.recipe_name in tested_recipe_names:
                        continue
                    if recipe.recipe_name in {item.recipe_name for item in candidates}:
                        continue
                    candidates.append(recipe)
    return candidates


def _snapshot_metadata(returns_file: Path) -> dict[str, Any]:
    """Collect snapshot-path metadata for manifest and decision artifacts."""

    metadata = {"returns_file": file_metadata(returns_file)}
    manifest_path = returns_file.parent / "risk_inputs_manifest.json"
    if manifest_path.exists():
        metadata["risk_inputs_manifest"] = file_metadata(manifest_path)
    factor_exposure_path = returns_file.parent / "factor_exposure.csv"
    if factor_exposure_path.exists():
        metadata["factor_exposure"] = file_metadata(factor_exposure_path)
    return metadata


def _build_acceptance_decision_payload(
    *,
    returns_file: Path,
    baseline_recipe_name: str,
    final_round_number: int,
    completed_rounds: list[dict[str, object]],
    accepted_row: pd.Series | None,
    acceptance_mode: str | None,
    stop_reason: str,
) -> dict[str, object]:
    """Build the final machine-readable acceptance decision."""

    accepted_recipe_name = None if accepted_row is None else str(accepted_row["recipe_name"])
    accepted_holdout_metrics = None
    if accepted_row is not None:
        accepted_holdout_metrics = {
            "mean_ic": float(accepted_row["mean_ic"]),
            "mean_rank_ic": float(accepted_row["mean_rank_ic"]),
            "positive_rank_ic_ratio": float(accepted_row["positive_rank_ic_ratio"]),
            "mean_top_bottom_spread": float(accepted_row["mean_top_bottom_spread"]),
            "evaluation_date_count": int(accepted_row["evaluation_date_count"]),
            "mean_monthly_factor_turnover": float(accepted_row["mean_monthly_factor_turnover"]),
        }
    status = "accepted" if accepted_row is not None else "rejected_but_infrastructure_complete"
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "acceptance_mode": acceptance_mode,
        "accepted_recipe_name": accepted_recipe_name,
        "baseline_recipe_name": baseline_recipe_name,
        "final_round_number": int(final_round_number),
        "completed_round_count": int(len(completed_rounds)),
        "stop_reason": stop_reason,
        "snapshot": _snapshot_metadata(returns_file),
        "accepted_holdout_metrics": accepted_holdout_metrics,
        "rounds": completed_rounds,
        "next_recommended_action": (
            "phase_1_5_expected_return_integration" if accepted_row is not None else "close_phase_1_without_optimizer_integration"
        ),
    }


def _build_manifest_payload(
    *,
    returns_file: Path,
    output_dir: Path,
    max_rounds: int,
    summary_frame: pd.DataFrame,
    decision_payload: dict[str, object],
) -> dict[str, object]:
    """Build one archival manifest for the acceptance run."""

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "returns_file": str(returns_file),
        "output_dir": str(output_dir),
        "max_rounds": int(max_rounds),
        "snapshot": _snapshot_metadata(returns_file),
        "recipe_registry": sorted(summary_frame["recipe_name"].unique().tolist()),
        "summary_row_count": int(len(summary_frame)),
        "final_status": str(decision_payload["status"]),
        "acceptance_mode": decision_payload.get("acceptance_mode"),
    }


def run_alpha_acceptance_gate(
    *,
    returns_file: str | Path,
    output_dir: str | Path,
    recipe_configs: list[AlphaRecipeConfig] | None = None,
    max_rounds: int = 3,
) -> AlphaAcceptanceResult:
    """Run the full Phase 1 alpha acceptance gate on one returns snapshot."""

    if int(max_rounds) <= 0:
        raise InputValidationError("max_rounds must be positive.")

    returns_path = Path(returns_file)
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    returns_panel = load_alpha_returns_panel(returns_path)
    baseline_recipe_name = _BASELINE_RECIPE_NAME
    current_round_recipes = list(recipe_configs or default_round_one_recipes())
    if not current_round_recipes:
        raise InputValidationError("At least one alpha recipe is required.")
    if baseline_recipe_name not in {item.recipe_name for item in current_round_recipes}:
        baseline_recipe_name = current_round_recipes[0].recipe_name

    tested_recipe_names: set[str] = set()
    summary_rows: list[dict[str, object]] = []
    completed_rounds: list[dict[str, object]] = []
    accepted_row: pd.Series | None = None
    acceptance_mode: str | None = None
    stop_reason = "max_rounds_exhausted"
    final_round_number = 0

    for round_number in range(1, int(max_rounds) + 1):
        final_round_number = round_number
        if not current_round_recipes:
            stop_reason = "no_new_valid_recipes"
            break

        evaluations = [build_alpha_recipe_result(returns_panel, recipe=recipe) for recipe in current_round_recipes]
        common_dates = _intersect_evaluation_dates([item.ic_frame for item in evaluations])
        development_dates, holdout_dates = _split_common_dates(common_dates)
        if len(holdout_dates) < 40:
            stop_reason = "insufficient_holdout_window"
            completed_rounds.append(
                {
                    "round_number": int(round_number),
                    "recipe_names": [item.recipe.recipe_name for item in evaluations],
                    "common_evaluation_date_count": int(len(common_dates)),
                    "development_date_count": int(len(development_dates)),
                    "holdout_date_count": int(len(holdout_dates)),
                    "stop_reason": stop_reason,
                }
            )
            break

        round_rows: list[dict[str, object]] = []
        for evaluation in evaluations:
            round_rows.append(
                _slice_metric_row(
                    round_number=round_number,
                    recipe=evaluation.recipe,
                    slice_name="development",
                    common_evaluation_dates=common_dates,
                    slice_dates=development_dates,
                    signal_frame=evaluation.signal_frame,
                    ic_frame=evaluation.ic_frame,
                    baseline_recipe_name=baseline_recipe_name,
                )
            )
            round_rows.append(
                _slice_metric_row(
                    round_number=round_number,
                    recipe=evaluation.recipe,
                    slice_name="holdout",
                    common_evaluation_dates=common_dates,
                    slice_dates=holdout_dates,
                    signal_frame=evaluation.signal_frame,
                    ic_frame=evaluation.ic_frame,
                    baseline_recipe_name=baseline_recipe_name,
                )
            )
        round_frame = pd.DataFrame(round_rows)
        summary_rows.extend(round_rows)

        holdout_frame = round_frame.loc[round_frame["slice_name"] == "holdout"].copy().reset_index(drop=True)
        development_frame = round_frame.loc[round_frame["slice_name"] == "development"].copy().reset_index(drop=True)
        if holdout_frame.empty:
            stop_reason = "missing_holdout_rows"
            break
        baseline_holdout = holdout_frame.loc[holdout_frame["recipe_name"] == baseline_recipe_name]
        if baseline_holdout.empty:
            raise InputValidationError(f"Baseline recipe {baseline_recipe_name} is missing from holdout results.")
        baseline_row = baseline_holdout.iloc[0]

        holdout_frame["absolute_gate_pass"] = holdout_frame.apply(_evaluate_absolute_gate, axis=1)
        holdout_frame["relative_gate_pass"] = False
        for idx, row in holdout_frame.iterrows():
            if bool(row["is_baseline"]):
                continue
            holdout_frame.at[idx, "relative_gate_pass"] = _evaluate_relative_gate(row, baseline_row)

        completed_rounds.append(
            {
                "round_number": int(round_number),
                "recipe_names": [item.recipe.recipe_name for item in evaluations],
                "common_evaluation_date_count": int(len(common_dates)),
                "development_date_count": int(len(development_dates)),
                "holdout_date_count": int(len(holdout_dates)),
                "development_ranking": _sort_recipe_rows(development_frame)[["recipe_name", "mean_rank_ic"]].to_dict(orient="records"),
            }
        )

        accepted_challengers = _sort_recipe_rows(
            holdout_frame.loc[~holdout_frame["is_baseline"] & holdout_frame["relative_gate_pass"] & holdout_frame["absolute_gate_pass"]].copy()
        )
        if not accepted_challengers.empty:
            accepted_row = accepted_challengers.iloc[0]
            acceptance_mode = "accepted_by_relative_and_absolute_gates"
            stop_reason = "accepted_challenger"
            break

        baseline_absolute_pass = bool(holdout_frame.loc[holdout_frame["recipe_name"] == baseline_recipe_name, "absolute_gate_pass"].iloc[0])
        if baseline_absolute_pass:
            accepted_row = holdout_frame.loc[holdout_frame["recipe_name"] == baseline_recipe_name].iloc[0]
            acceptance_mode = "accepted_as_baseline"
            stop_reason = "accepted_baseline"
            break

        tested_recipe_names.update(recipe.recipe_name for recipe in current_round_recipes)
        if round_number >= int(max_rounds):
            stop_reason = "max_rounds_exhausted"
            break

        top_parents = _sort_recipe_rows(development_frame).head(2)["recipe_name"].tolist()
        parent_recipes = [evaluation.recipe for evaluation in evaluations if evaluation.recipe.recipe_name in top_parents]
        current_round_recipes = _expand_round_recipes(
            round_number=round_number + 1,
            parent_recipes=parent_recipes,
            tested_recipe_names=tested_recipe_names,
        )
        if not current_round_recipes:
            stop_reason = "no_new_valid_recipes"
            break

    summary_frame = pd.DataFrame(summary_rows).sort_values(
        by=["round_number", "slice_name", "recipe_name"]
    ).reset_index(drop=True)
    decision_payload = _build_acceptance_decision_payload(
        returns_file=returns_path,
        baseline_recipe_name=baseline_recipe_name,
        final_round_number=final_round_number,
        completed_rounds=completed_rounds,
        accepted_row=accepted_row,
        acceptance_mode=acceptance_mode,
        stop_reason=stop_reason,
    )
    note_markdown = render_alpha_acceptance_note(decision_payload, summary_frame=summary_frame)
    manifest_payload = _build_manifest_payload(
        returns_file=returns_path,
        output_dir=output_root,
        max_rounds=max_rounds,
        summary_frame=summary_frame,
        decision_payload=decision_payload,
    )

    summary_frame.to_csv(output_root / "alpha_sweep_summary.csv", index=False)
    write_json(output_root / "alpha_sweep_manifest.json", manifest_payload)
    write_json(output_root / "alpha_acceptance_decision.json", decision_payload)
    write_text(output_root / "alpha_acceptance_note.md", note_markdown)

    return AlphaAcceptanceResult(
        returns_file=returns_path,
        output_dir=output_root,
        summary_frame=summary_frame,
        decision_payload=decision_payload,
        manifest_payload=manifest_payload,
        note_markdown=note_markdown,
    )
