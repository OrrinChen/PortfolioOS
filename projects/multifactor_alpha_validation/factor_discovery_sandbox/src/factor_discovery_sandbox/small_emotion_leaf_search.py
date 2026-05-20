"""E0.5 greedy leaf search for small-cap emotion pockets."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

import numpy as np
import pandas as pd

from .small_emotion_d2 import _data_coverage_report, _prepare_price_panel, _read_csv
from .small_emotion_exploratory_sweep import EXPLORATORY_GUARDS
from .small_emotion_sharpening_sweep import _add_sharpening_features, _event_label_panel


@dataclass(frozen=True)
class SmallEmotionLeafSearchResult:
    """Output from E0.5 greedy leaf search."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


@dataclass(frozen=True)
class _Predicate:
    predicate_id: str
    description: str
    apply: Callable[[pd.DataFrame], pd.Series]


def run_small_emotion_leaf_search(
    *,
    price_panel_path: str | Path,
    benchmark_panel_path: str | Path,
    delisting_path: str | Path,
    output_dir: str | Path,
    mechanisms: Iterable[str] = (
        "up_shock_reversal",
        "up_shock_continuation",
        "down_shock_reversal",
        "down_shock_continuation",
    ),
    windows: Iterable[str] = ("post_1_5", "post_1_10", "post_6_22", "post_1_22"),
    base_shock_threshold: float = 0.05,
    base_volume_spike_threshold: float = 1.5,
    adv_min_dollars: float = 250_000.0,
    max_depth: int = 3,
    beam_width: int = 8,
    min_events: int = 50,
    min_event_months: int = 3,
    min_history_observations: int = 60,
    small_cap_min_market_cap: float = 50_000_000.0,
    small_cap_max_market_cap: float = 5_000_000_000.0,
    top_n: int = 50,
    max_rows: int | None = 750_000,
) -> SmallEmotionLeafSearchResult:
    """Run a greedy decision-leaf search over event filters."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    prices = _read_csv(Path(price_panel_path), nrows=max_rows)
    benchmark = _read_csv(Path(benchmark_panel_path))
    delistings = _read_csv(Path(delisting_path))
    coverage = _data_coverage_report(
        prices=prices,
        benchmark=benchmark,
        delistings=delistings,
        price_panel_path=Path(price_panel_path),
        benchmark_panel_path=Path(benchmark_panel_path),
        delisting_path=Path(delisting_path),
        max_rows=max_rows,
    )
    artifacts["data_coverage_report"].write_text(
        json.dumps({**coverage, **EXPLORATORY_GUARDS}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if coverage["data_status"] != "available":
        tree = pd.DataFrame(columns=_tree_columns())
        best = pd.DataFrame(columns=_best_columns())
        summary = _summary(tree=tree, best=best, coverage=coverage, max_depth=max_depth, beam_width=beam_width)
        _write_outputs(artifacts, tree, best, summary)
        return SmallEmotionLeafSearchResult(summary=summary, artifacts=artifacts)

    prepared = _prepare_price_panel(
        prices,
        benchmark,
        min_history_observations=min_history_observations,
        min_adv_dollars=float(adv_min_dollars),
        small_cap_min_market_cap=small_cap_min_market_cap,
        small_cap_max_market_cap=small_cap_max_market_cap,
        large_cap_min_market_cap=5_000_000_000.0,
    )
    enriched = _add_sharpening_features(prepared)
    event_labels = _event_label_panel(
        enriched,
        min_shock_threshold=float(base_shock_threshold),
        min_volume_spike=float(base_volume_spike_threshold),
        windows=list(windows),
    )
    tree = _leaf_search(
        event_labels=event_labels,
        mechanisms=list(mechanisms),
        windows=list(windows),
        base_shock_threshold=float(base_shock_threshold),
        base_volume_spike_threshold=float(base_volume_spike_threshold),
        adv_min_dollars=float(adv_min_dollars),
        max_depth=int(max_depth),
        beam_width=int(beam_width),
        min_events=int(min_events),
        min_event_months=int(min_event_months),
    )
    best = _best_leaves(tree, top_n=top_n)
    summary = _summary(tree=tree, best=best, coverage=coverage, max_depth=max_depth, beam_width=beam_width)
    _write_outputs(artifacts, tree, best, summary)
    return SmallEmotionLeafSearchResult(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "data_coverage_report": output_path / "data_coverage_report.json",
        "leaf_search_tree": output_path / "leaf_search_tree.csv",
        "best_leaf_candidates": output_path / "best_leaf_candidates.csv",
        "leaf_overfit_disclosure": output_path / "leaf_overfit_disclosure.json",
        "leaf_candidate_to_freeze_next": output_path / "leaf_candidate_to_freeze_next.json",
        "leaf_search_summary": output_path / "leaf_search_summary.json",
        "leaf_search_report": output_path / "leaf_search_report.md",
    }


def _leaf_search(
    *,
    event_labels: pd.DataFrame,
    mechanisms: list[str],
    windows: list[str],
    base_shock_threshold: float,
    base_volume_spike_threshold: float,
    adv_min_dollars: float,
    max_depth: int,
    beam_width: int,
    min_events: int,
    min_event_months: int,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    node_id = 0
    predicates = _predicate_library()
    frontier: list[dict[str, object]] = []
    for mechanism in mechanisms:
        for window in windows:
            base = _base_frame(
                event_labels,
                mechanism=mechanism,
                window=window,
                shock_threshold=base_shock_threshold,
                volume_threshold=base_volume_spike_threshold,
                adv_min=adv_min_dollars,
            )
            node_id += 1
            row = _node_row(
                node_id=node_id,
                parent_node_id=0,
                depth=0,
                mechanism=mechanism,
                window=window,
                path_predicates=[],
                added_predicate="root",
                frame=base,
                min_events=min_events,
                min_event_months=min_event_months,
            )
            rows.append(row)
            if row["eligible_for_leaf_review"]:
                frontier.append({"node_id": node_id, "frame": base, "path": [], "mechanism": mechanism, "window": window})

    for depth in range(1, int(max_depth) + 1):
        candidates: list[dict[str, object]] = []
        for node in frontier:
            used = set(node["path"])
            for predicate in predicates:
                if predicate.predicate_id in used:
                    continue
                frame = node["frame"]
                mask = predicate.apply(frame)
                child = frame[mask.fillna(False)].copy()
                node_id += 1
                path = [*node["path"], predicate.predicate_id]
                row = _node_row(
                    node_id=node_id,
                    parent_node_id=int(node["node_id"]),
                    depth=depth,
                    mechanism=str(node["mechanism"]),
                    window=str(node["window"]),
                    path_predicates=path,
                    added_predicate=predicate.predicate_id,
                    frame=child,
                    min_events=min_events,
                    min_event_months=min_event_months,
                )
                rows.append(row)
                if row["eligible_for_leaf_review"]:
                    candidates.append(
                        {
                            "node_id": node_id,
                            "frame": child,
                            "path": path,
                            "mechanism": node["mechanism"],
                            "window": node["window"],
                            "leaf_score": row["leaf_score"],
                        }
                    )
        if not candidates:
            break
        candidates.sort(key=lambda item: float(item["leaf_score"]), reverse=True)
        frontier = candidates[: int(beam_width)]
    if not rows:
        return pd.DataFrame(columns=_tree_columns())
    tree = pd.DataFrame(rows)
    return tree[_tree_columns()]


def _base_frame(
    event_labels: pd.DataFrame,
    *,
    mechanism: str,
    window: str,
    shock_threshold: float,
    volume_threshold: float,
    adv_min: float,
) -> pd.DataFrame:
    if event_labels.empty:
        return event_labels.copy()
    frame = event_labels[
        event_labels["window"].eq(window)
        & event_labels["label_status"].eq("observed")
        & event_labels["abnormal_volume"].ge(float(volume_threshold))
        & event_labels["adv20"].ge(float(adv_min))
    ].copy()
    if mechanism in {"up_shock_reversal", "up_shock_continuation"}:
        frame = frame[frame["shock_return"].ge(float(shock_threshold))]
    elif mechanism in {"down_shock_reversal", "down_shock_continuation"}:
        frame = frame[frame["shock_return"].le(-float(shock_threshold))]
    else:
        return frame.iloc[0:0].copy()
    frame["directional_return"] = _directional_series(frame, mechanism)
    return frame


def _directional_series(frame: pd.DataFrame, mechanism: str) -> pd.Series:
    abnormal = pd.to_numeric(frame["abnormal_return"], errors="coerce")
    if mechanism in {"up_shock_continuation", "down_shock_reversal"}:
        return abnormal
    if mechanism in {"up_shock_reversal", "down_shock_continuation"}:
        return -abnormal
    return pd.Series(np.nan, index=frame.index, dtype="float64")


def _predicate_library() -> list[_Predicate]:
    return [
        _ge("shock_ge_8pct", "abs_shock_return", 0.08),
        _ge("shock_ge_10pct", "abs_shock_return", 0.10),
        _ge("shock_ge_15pct", "abs_shock_return", 0.15),
        _ge("shock_ge_20pct", "abs_shock_return", 0.20),
        _ge("volume_ge_2x", "abnormal_volume", 2.0),
        _ge("volume_ge_3x", "abnormal_volume", 3.0),
        _ge("volume_ge_5x", "abnormal_volume", 5.0),
        _ge("prior5_ge_0", "prior_5d_return", 0.0),
        _ge("prior5_ge_10pct", "prior_5d_return", 0.10),
        _ge("prior5_ge_20pct", "prior_5d_return", 0.20),
        _ge("prior5_ge_50pct", "prior_5d_return", 0.50),
        _le("prior5_le_0", "prior_5d_return", 0.0),
        _ge("prior20_ge_0", "prior_20d_return", 0.0),
        _ge("prior20_ge_20pct", "prior_20d_return", 0.20),
        _ge("prior20_ge_50pct", "prior_20d_return", 0.50),
        _le("prior20_le_0", "prior_20d_return", 0.0),
        _eq("mcap_micro", "market_cap_bucket", "micro"),
        _eq("mcap_small", "market_cap_bucket", "small"),
        _eq("liquidity_low", "liquidity_bucket", "low"),
        _bool("weak_liquidity", "weak_liquidity"),
        _eq("spread_wide", "spread_bucket", "wide"),
        _eq("spread_tight", "spread_bucket", "tight"),
        _eq("regime_market_up", "market_regime", "market_up_20d"),
        _eq("regime_market_down", "market_regime", "market_down_20d"),
        _eq("regime_high_vol", "market_regime", "market_high_vol"),
        _isin("price_under_20", "low_price_bucket", {"under_5", "under_10", "under_20"}),
        _isin("price_under_10", "low_price_bucket", {"under_5", "under_10"}),
        _eq("price_under_5", "low_price_bucket", "under_5"),
        _ge("close_top_quartile", "close_location", 0.75),
        _le("close_lower_half", "close_location", 0.50),
        _ge("open_to_close_ge_5pct", "open_to_close_return", 0.05),
        _le("open_to_close_le_minus_5pct", "open_to_close_return", -0.05),
    ]


def _ge(predicate_id: str, column: str, value: float) -> _Predicate:
    return _Predicate(predicate_id, f"{column} >= {value}", lambda frame: pd.to_numeric(frame[column], errors="coerce").ge(value))


def _le(predicate_id: str, column: str, value: float) -> _Predicate:
    return _Predicate(predicate_id, f"{column} <= {value}", lambda frame: pd.to_numeric(frame[column], errors="coerce").le(value))


def _eq(predicate_id: str, column: str, value: object) -> _Predicate:
    return _Predicate(predicate_id, f"{column} == {value}", lambda frame: frame[column].eq(value))


def _isin(predicate_id: str, column: str, values: set[object]) -> _Predicate:
    return _Predicate(predicate_id, f"{column} in {sorted(values)}", lambda frame: frame[column].isin(values))


def _bool(predicate_id: str, column: str) -> _Predicate:
    return _Predicate(predicate_id, f"{column} is true", lambda frame: frame[column].fillna(False).astype(bool))


def _node_row(
    *,
    node_id: int,
    parent_node_id: int,
    depth: int,
    mechanism: str,
    window: str,
    path_predicates: list[str],
    added_predicate: str,
    frame: pd.DataFrame,
    min_events: int,
    min_event_months: int,
) -> dict[str, object]:
    directional = pd.to_numeric(frame.get("directional_return", pd.Series(dtype="float64")), errors="coerce").dropna()
    event_count = int(len(directional))
    event_months = int(frame.loc[directional.index, "event_month"].nunique()) if event_count else 0
    issuer_count = int(frame.loc[directional.index, "asset_id"].nunique()) if event_count else 0
    mean_directional = float(directional.mean()) if event_count else np.nan
    median_directional = float(directional.median()) if event_count else np.nan
    hit_rate = float((directional > 0.0).mean()) if event_count else np.nan
    leaf_score = (
        float(mean_directional * np.log1p(event_count) * np.sqrt(max(event_months, 1)))
        if event_count and pd.notna(mean_directional)
        else np.nan
    )
    eligible = bool(
        event_count >= int(min_events)
        and event_months >= int(min_event_months)
        and pd.notna(mean_directional)
        and mean_directional > 0.0
    )
    return {
        "schema_version": "small_emotion_leaf_search_tree.v1",
        "stage": "E0-SMALL-EMOTION-05",
        "node_id": int(node_id),
        "parent_node_id": int(parent_node_id),
        "depth": int(depth),
        "mechanism": mechanism,
        "window": window,
        "path_predicates": " & ".join(path_predicates),
        "added_predicate": added_predicate,
        "active_event_count": event_count,
        "event_month_count": event_months,
        "issuer_count": issuer_count,
        "mean_directional_return": mean_directional,
        "median_directional_return": median_directional,
        "hit_rate": hit_rate,
        "leaf_score": leaf_score,
        "eligible_for_leaf_review": eligible,
        "selection_status": "aggressive_leaf_search_only",
        **EXPLORATORY_GUARDS,
    }


def _best_leaves(tree: pd.DataFrame, *, top_n: int) -> pd.DataFrame:
    if tree.empty:
        return pd.DataFrame(columns=_best_columns())
    eligible = tree[tree["eligible_for_leaf_review"].astype(bool)].copy()
    if eligible.empty:
        return pd.DataFrame(columns=_best_columns())
    eligible = eligible.sort_values(
        ["mean_directional_return", "leaf_score", "active_event_count"],
        ascending=[False, False, False],
        na_position="last",
    ).head(int(top_n))
    eligible = eligible.reset_index(drop=True)
    eligible["leaf_rank"] = range(1, len(eligible) + 1)
    return eligible[_best_columns()]


def _summary(
    *,
    tree: pd.DataFrame,
    best: pd.DataFrame,
    coverage: dict[str, object],
    max_depth: int,
    beam_width: int,
) -> dict[str, object]:
    top = best.iloc[0].to_dict() if not best.empty else {}
    return {
        "schema_version": "small_emotion_leaf_search_summary.v1",
        "stage": "E0-SMALL-EMOTION-05",
        "candidate_id": "small_cap_emotion_leaf_search",
        "purpose": "greedy_leaf_search_for_strong_in_sample_pockets",
        "tree_node_count": int(len(tree)),
        "best_leaf_count": int(len(best)),
        "leaf_candidate_found": bool(top),
        "top_leaf": _json_safe(top),
        "max_depth": int(max_depth),
        "beam_width": int(beam_width),
        "data_status": coverage.get("data_status"),
        "exploratory_only": True,
        "overfit_search_allowed": True,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        **EXPLORATORY_GUARDS,
    }


def _write_outputs(
    artifacts: dict[str, Path],
    tree: pd.DataFrame,
    best: pd.DataFrame,
    summary: dict[str, object],
) -> None:
    tree.to_csv(artifacts["leaf_search_tree"], index=False)
    best.to_csv(artifacts["best_leaf_candidates"], index=False)
    artifacts["leaf_overfit_disclosure"].write_text(
        json.dumps(_overfit_disclosure(tree, best), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["leaf_candidate_to_freeze_next"].write_text(
        json.dumps(_candidate_to_freeze_next(best), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["leaf_search_summary"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["leaf_search_report"].write_text(_report(summary, best), encoding="utf-8")


def _overfit_disclosure(tree: pd.DataFrame, best: pd.DataFrame) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_leaf_search_overfit_disclosure.v1",
        "stage": "E0-SMALL-EMOTION-05",
        "purpose": "greedy_leaf_search_for_strong_in_sample_pockets",
        "selection_bias_risk": "extreme",
        "tree_node_count": int(len(tree)),
        "reported_best_leaf_count": int(len(best)),
        "requires_freeze_before_q1": True,
        "leaf_search_results_are_not_alpha_evidence": True,
        **EXPLORATORY_GUARDS,
    }


def _candidate_to_freeze_next(best: pd.DataFrame) -> dict[str, object]:
    top = best.iloc[0].to_dict() if not best.empty else {}
    return {
        "schema_version": "small_emotion_leaf_candidate_to_freeze_next.v1",
        "stage": "E0-SMALL-EMOTION-05",
        "candidate_found": bool(top),
        "candidate": _json_safe(top),
        "recommendation": "chunk_replay_then_manual_d3_charter" if top else "no_leaf_candidate_found",
        "measurement_spec_written": False,
        "formula_score_written": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "expected_return_panel_written": False,
        "optimizer_entry_allowed": False,
        "alpha_registry_update_allowed": False,
        "production_approval_claimed": False,
        **EXPLORATORY_GUARDS,
    }


def _report(summary: dict[str, object], best: pd.DataFrame) -> str:
    lines = [
        "# E0-SMALL-EMOTION-05 Leaf Search",
        "",
        "This is an aggressive in-sample greedy leaf search. It is not alpha evidence and opens no Q1/Q2/downstream workflow.",
        "",
        f"- tree_node_count: {summary['tree_node_count']}",
        f"- best_leaf_count: {summary['best_leaf_count']}",
        f"- leaf_candidate_found: {summary['leaf_candidate_found']}",
        "",
        "## Best Leaves",
    ]
    if best.empty:
        lines.append("- none")
    else:
        for row in best.head(10).itertuples(index=False):
            lines.append(
                "- "
                f"rank={row.leaf_rank}, depth={row.depth}, mechanism={row.mechanism}, "
                f"window={row.window}, mean={row.mean_directional_return:.6f}, "
                f"hit_rate={row.hit_rate:.6f}, events={row.active_event_count}, "
                f"months={row.event_month_count}, path={row.path_predicates}"
            )
    lines.append("")
    return "\n".join(lines)


def _tree_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "node_id",
        "parent_node_id",
        "depth",
        "mechanism",
        "window",
        "path_predicates",
        "added_predicate",
        "active_event_count",
        "event_month_count",
        "issuer_count",
        "mean_directional_return",
        "median_directional_return",
        "hit_rate",
        "leaf_score",
        "eligible_for_leaf_review",
        "selection_status",
        *EXPLORATORY_GUARDS.keys(),
    ]


def _best_columns() -> list[str]:
    return [*_tree_columns(), "leaf_rank"]


def _json_safe(value: dict[str, object]) -> dict[str, object]:
    safe: dict[str, object] = {}
    for key, item in value.items():
        if isinstance(item, (np.bool_, bool)):
            safe[str(key)] = bool(item)
        elif isinstance(item, (np.integer, int)):
            safe[str(key)] = int(item)
        elif isinstance(item, (np.floating, float)):
            safe[str(key)] = None if pd.isna(item) else float(item)
        elif pd.isna(item):
            safe[str(key)] = None
        else:
            safe[str(key)] = item
    return safe
