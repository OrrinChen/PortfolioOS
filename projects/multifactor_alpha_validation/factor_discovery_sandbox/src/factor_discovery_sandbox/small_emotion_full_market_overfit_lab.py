"""E1 full-market overfit lab for shock-conditioned emotion/liquidity pockets."""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

import numpy as np
import pandas as pd

from .small_emotion_d2 import _data_coverage_report, _read_csv
from .small_emotion_exploratory_sweep import EXPLORATORY_GUARDS
from .small_emotion_q1_oos import Q1_PRICE_COLUMNS, _add_q1_sharpening_features, _prepare_q1_price_panel, _read_q1_price_csv
from .small_emotion_sharpening_sweep import SHARPEN_WINDOWS


STAGE = "E1-SMALL-EMOTION-FULL-MARKET-OVERFIT"
UNIVERSE_SCOPE = "full_market_common_stock_research_universe"


@dataclass(frozen=True)
class SmallEmotionFullMarketOverfitLabResult:
    """Output from the E1 full-market exploratory overfit lab."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


@dataclass(frozen=True)
class _Predicate:
    predicate_id: str
    description: str
    apply: Callable[[pd.DataFrame], pd.Series]


def run_small_emotion_full_market_overfit_lab(
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
    shock_thresholds: Iterable[float] = (0.05, 0.08, 0.10),
    volume_spike_thresholds: Iterable[float] = (1.0, 1.5, 2.0),
    adv_min_dollars: Iterable[float] = (250_000.0,),
    max_depth: int = 4,
    beam_width: int = 16,
    min_events: int = 50,
    min_event_months: int = 6,
    min_history_observations: int = 60,
    top_n: int = 50,
    max_rows: int | None = None,
    feature_cache_dir: str | Path | None = None,
    cache_only: bool = False,
    force_rebuild_cache: bool = False,
    feature_cache_shards: int = 32,
    feature_cache_chunk_rows: int = 250_000,
    excluded_predicates: Iterable[str] = (),
    exclude_stale_price_events: bool = False,
) -> SmallEmotionFullMarketOverfitLabResult:
    """Search full-market historical pockets in-sample without validation claims."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)
    windows_list = list(windows)
    mechanisms_list = list(mechanisms)
    shock_thresholds_list = list(shock_thresholds)
    volume_thresholds_list = list(volume_spike_thresholds)
    adv_min_list = list(adv_min_dollars)
    excluded_predicates_list = sorted({str(value).strip() for value in excluded_predicates if str(value).strip()})
    cache_path = Path(feature_cache_dir) if feature_cache_dir is not None else None
    cache_config = _cache_config(
        price_panel_path=Path(price_panel_path),
        benchmark_panel_path=Path(benchmark_panel_path),
        delisting_path=Path(delisting_path),
        windows=windows_list,
        min_shock_threshold=min(float(value) for value in shock_thresholds_list),
        min_volume_spike=min(float(value) for value in volume_thresholds_list),
        min_adv_dollars=min(float(value) for value in adv_min_list),
        min_history_observations=min_history_observations,
        max_rows=max_rows,
    )

    cached = None if force_rebuild_cache else _read_feature_cache(cache_path, cache_config)
    built_feature_cache = False
    if cached is not None:
        event_labels, coverage = cached
        feature_cache_status = "cache_hit"
        artifacts["data_coverage_report"].write_text(
            json.dumps({**coverage, **_boundary_flags()}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    else:
        feature_cache_status = "not_configured" if cache_path is None else "cache_miss"

    if cached is None and cache_path is not None and max_rows is None:
        event_labels, coverage = _build_feature_cache_by_asset_shards(
            price_panel_path=Path(price_panel_path),
            benchmark_panel_path=Path(benchmark_panel_path),
            delisting_path=Path(delisting_path),
            cache_path=cache_path,
            config=cache_config,
            windows=windows_list,
            min_shock_threshold=min(float(value) for value in shock_thresholds_list),
            min_volume_spike=min(float(value) for value in volume_thresholds_list),
            min_adv_dollars=min(float(value) for value in adv_min_list),
            min_history_observations=min_history_observations,
            feature_cache_shards=feature_cache_shards,
            feature_cache_chunk_rows=feature_cache_chunk_rows,
        )
        feature_cache_status = "written_cache_only" if cache_only else "cache_written"
        artifacts["data_coverage_report"].write_text(
            json.dumps({**coverage, **_boundary_flags()}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        built_feature_cache = True

    if cached is None and not built_feature_cache:
        prices = _read_q1_price_csv(Path(price_panel_path), nrows=max_rows)
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
            json.dumps({**coverage, **_boundary_flags()}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if coverage["data_status"] != "available":
            empty = pd.DataFrame(columns=_grid_columns())
            top = pd.DataFrame(columns=_top_columns())
            tail = pd.DataFrame(columns=_tail_columns())
            cost = pd.DataFrame(columns=_cost_columns())
            summary = _summary(
                grid=empty,
                top=top,
                coverage=coverage,
                max_depth=max_depth,
                beam_width=beam_width,
                search_count=0,
                feature_cache_status=feature_cache_status,
                cache_only=cache_only,
                excluded_predicates=excluded_predicates_list,
                candidate_filter_report=_candidate_filter_report(empty, empty, exclude_stale_price_events=exclude_stale_price_events),
            )
            _write_outputs(artifacts, empty, top, tail, cost, summary)
            return SmallEmotionFullMarketOverfitLabResult(summary=summary, artifacts=artifacts)

        min_adv = min(float(value) for value in adv_min_list)
        prepared = _prepare_q1_price_panel(
            prices,
            benchmark,
            min_history_observations=min_history_observations,
            min_adv_dollars=min_adv,
            small_cap_min_market_cap=0.0,
            small_cap_max_market_cap=10_000_000_000_000.0,
            large_cap_min_market_cap=5_000_000_000.0,
            universe_scope="full_market",
            exclude_stale_price_events=False,
        )
        enriched = _add_full_market_features(_add_q1_sharpening_features(prepared))
        event_labels = _event_label_panel(
            enriched,
            min_shock_threshold=min(float(value) for value in shock_thresholds_list),
            min_volume_spike=min(float(value) for value in volume_thresholds_list),
            windows=windows_list,
        )
        if cache_path is not None:
            _write_feature_cache(cache_path, event_labels, coverage=coverage, config=cache_config)
            feature_cache_status = "written_cache_only" if cache_only else "cache_written"

    if cache_only:
        empty = pd.DataFrame(columns=_grid_columns())
        top = pd.DataFrame(columns=_top_columns())
        tail = pd.DataFrame(columns=_tail_columns())
        cost = pd.DataFrame(columns=_cost_columns())
        event_labels_for_summary = event_labels if "event_labels" in locals() else pd.DataFrame(columns=_event_label_columns())
        summary = _summary(
            grid=empty,
            top=top,
            coverage=coverage,
            max_depth=max_depth,
            beam_width=beam_width,
            search_count=0,
            feature_cache_status=feature_cache_status,
            cache_only=True,
            excluded_predicates=excluded_predicates_list,
            candidate_filter_report=_candidate_filter_report(
                event_labels_for_summary,
                event_labels_for_summary,
                exclude_stale_price_events=exclude_stale_price_events,
            ),
        )
        _write_outputs(artifacts, empty, top, tail, cost, summary)
        return SmallEmotionFullMarketOverfitLabResult(summary=summary, artifacts=artifacts)

    event_labels_for_search, candidate_filter_report = _apply_candidate_event_filters(
        event_labels,
        exclude_stale_price_events=exclude_stale_price_events,
    )
    grid, frame_lookup, search_count = _leaf_search(
        event_labels=event_labels_for_search,
        mechanisms=mechanisms_list,
        windows=windows_list,
        shock_thresholds=shock_thresholds_list,
        volume_spike_thresholds=volume_thresholds_list,
        adv_min_dollars=adv_min_list,
        max_depth=int(max_depth),
        beam_width=int(beam_width),
        min_events=int(min_events),
        min_event_months=int(min_event_months),
        excluded_predicates=excluded_predicates_list,
    )
    top = _top_pockets(grid, top_n=int(top_n))
    tail = _tail_concentration_audit(top, frame_lookup)
    top = _attach_tail_status(top, tail)
    cost = _cost_liquidity_audit(top, frame_lookup)
    summary = _summary(
        grid=grid,
        top=top,
        coverage=coverage,
        max_depth=max_depth,
        beam_width=beam_width,
        search_count=search_count,
        feature_cache_status=feature_cache_status,
        cache_only=False,
        excluded_predicates=excluded_predicates_list,
        candidate_filter_report=candidate_filter_report,
    )
    _write_outputs(artifacts, grid, top, tail, cost, summary)
    return SmallEmotionFullMarketOverfitLabResult(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "data_coverage_report": output_path / "data_coverage_report.json",
        "full_market_overfit_grid": output_path / "full_market_overfit_grid.csv",
        "top_50_overfit_pockets": output_path / "top_50_overfit_pockets.csv",
        "tail_concentration_audit": output_path / "tail_concentration_audit.csv",
        "cost_liquidity_audit": output_path / "cost_liquidity_audit.csv",
        "overfit_disclosure": output_path / "overfit_disclosure.json",
        "best_pocket_spec_draft": output_path / "best_pocket_spec_draft.json",
        "full_market_overfit_summary": output_path / "full_market_overfit_summary.json",
        "full_market_overfit_report": output_path / "full_market_overfit_report.md",
    }


def _cache_config(
    *,
    price_panel_path: Path,
    benchmark_panel_path: Path,
    delisting_path: Path,
    windows: list[str],
    min_shock_threshold: float,
    min_volume_spike: float,
    min_adv_dollars: float,
    min_history_observations: int,
    max_rows: int | None,
) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_full_market_feature_cache_config.v1",
        "stage": STAGE,
        "price_panel_path": str(price_panel_path),
        "benchmark_panel_path": str(benchmark_panel_path),
        "delisting_path": str(delisting_path),
        "windows": list(windows),
        "min_shock_threshold": float(min_shock_threshold),
        "min_volume_spike": float(min_volume_spike),
        "min_adv_dollars": float(min_adv_dollars),
        "min_history_observations": int(min_history_observations),
        "max_rows": None if max_rows is None else int(max_rows),
    }


def _read_feature_cache(
    cache_path: Path | None,
    config: dict[str, object],
) -> tuple[pd.DataFrame, dict[str, object]] | None:
    if cache_path is None:
        return None
    manifest_path = cache_path / "feature_cache_manifest.json"
    if not manifest_path.exists():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if manifest.get("config") != config:
        return None
    frames: list[pd.DataFrame] = []
    for window in config.get("windows", []):
        event_path = cache_path / _cache_event_label_file(str(window))
        if not event_path.exists():
            return None
        frames.append(pd.read_csv(event_path))
    event_labels = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=_event_label_columns())
    coverage = manifest.get("coverage", {})
    return event_labels, coverage if isinstance(coverage, dict) else {}


def _write_feature_cache(
    cache_path: Path,
    event_labels: pd.DataFrame,
    *,
    coverage: dict[str, object],
    config: dict[str, object],
) -> None:
    cache_path.mkdir(parents=True, exist_ok=True)
    windows = [str(window) for window in config.get("windows", [])]
    for window in windows:
        window_frame = event_labels[event_labels["window"].astype(str).eq(window)].copy() if not event_labels.empty else event_labels.copy()
        window_frame.to_csv(cache_path / _cache_event_label_file(window), index=False)
    manifest = {
        "schema_version": "small_emotion_full_market_feature_cache_manifest.v1",
        "stage": STAGE,
        "config": config,
        "coverage": coverage,
        "windows": windows,
        "event_label_row_count": int(len(event_labels)),
        "cache_files": [_cache_event_label_file(window) for window in windows],
        **_boundary_flags(),
    }
    (cache_path / "feature_cache_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _cache_event_label_file(window: str) -> str:
    safe = "".join(char if char.isalnum() or char in {"_", "-"} else "_" for char in window)
    return f"event_labels_{safe}.csv"


def _build_feature_cache_by_asset_shards(
    *,
    price_panel_path: Path,
    benchmark_panel_path: Path,
    delisting_path: Path,
    cache_path: Path,
    config: dict[str, object],
    windows: list[str],
    min_shock_threshold: float,
    min_volume_spike: float,
    min_adv_dollars: float,
    min_history_observations: int,
    feature_cache_shards: int,
    feature_cache_chunk_rows: int,
) -> tuple[pd.DataFrame, dict[str, object]]:
    benchmark = _read_csv(benchmark_panel_path)
    delistings = _read_csv(delisting_path)
    coverage: dict[str, object] = {
        "schema_version": "small_emotion_full_market_feature_cache_coverage.v1",
        "data_status": "available",
        "price_panel_path": str(price_panel_path),
        "benchmark_panel_path": str(benchmark_panel_path),
        "delisting_path": str(delisting_path),
        "max_rows": None,
        "feature_cache_mode": "asset_hash_sharded",
        "feature_cache_shards": int(feature_cache_shards),
        "feature_cache_chunk_rows": int(feature_cache_chunk_rows),
        "benchmark_row_count": int(len(benchmark)),
        "delisting_row_count": int(len(delistings)),
    }
    if not price_panel_path.exists() or benchmark.empty or not delisting_path.exists():
        coverage["data_status"] = "missing_inputs"
        empty = pd.DataFrame(columns=_event_label_columns())
        _write_feature_cache(cache_path, empty, coverage=coverage, config=config)
        return empty, coverage

    shard_count = max(1, int(feature_cache_shards))
    chunk_rows = max(1, int(feature_cache_chunk_rows))
    shard_dir = cache_path / "_price_shards"
    if shard_dir.exists():
        shutil.rmtree(shard_dir)
    shard_dir.mkdir(parents=True, exist_ok=True)
    available = list(pd.read_csv(price_panel_path, nrows=0).columns)
    usecols = [column for column in available if column in Q1_PRICE_COLUMNS]
    headers_written = [False] * shard_count
    total_rows = 0
    for chunk in pd.read_csv(price_panel_path, usecols=usecols or None, chunksize=chunk_rows):
        if chunk.empty:
            continue
        if "asset_id" not in chunk.columns:
            if "permno" in chunk.columns:
                chunk["asset_id"] = chunk["permno"].astype(str)
            elif "ticker" in chunk.columns:
                chunk["asset_id"] = chunk["ticker"].astype(str)
            else:
                chunk["asset_id"] = ""
        chunk["asset_id"] = chunk["asset_id"].astype(str)
        buckets = pd.util.hash_pandas_object(chunk["asset_id"], index=False).mod(shard_count).astype(int)
        total_rows += int(len(chunk))
        for bucket in range(shard_count):
            shard = chunk.loc[buckets.eq(bucket)]
            if shard.empty:
                continue
            shard.to_csv(
                shard_dir / f"price_shard_{bucket:03d}.csv",
                mode="a",
                header=not headers_written[bucket],
                index=False,
            )
            headers_written[bucket] = True

    label_frames: list[pd.DataFrame] = []
    for shard_path in sorted(shard_dir.glob("price_shard_*.csv")):
        prices = pd.read_csv(shard_path)
        if prices.empty:
            continue
        prepared = _prepare_q1_price_panel(
            prices,
            benchmark,
            min_history_observations=min_history_observations,
            min_adv_dollars=float(min_adv_dollars),
            small_cap_min_market_cap=0.0,
            small_cap_max_market_cap=10_000_000_000_000.0,
            large_cap_min_market_cap=5_000_000_000.0,
            universe_scope="full_market",
            exclude_stale_price_events=False,
        )
        enriched = _add_full_market_features(_add_q1_sharpening_features(prepared))
        labels = _event_label_panel(
            enriched,
            min_shock_threshold=float(min_shock_threshold),
            min_volume_spike=float(min_volume_spike),
            windows=windows,
        )
        if not labels.empty:
            label_frames.append(labels)
    event_labels = pd.concat(label_frames, ignore_index=True) if label_frames else pd.DataFrame(columns=_event_label_columns())
    coverage["price_row_count"] = int(total_rows)
    coverage["event_label_row_count"] = int(len(event_labels))
    coverage["event_label_windows"] = sorted(event_labels["window"].astype(str).unique().tolist()) if not event_labels.empty else []
    _write_feature_cache(cache_path, event_labels, coverage=coverage, config=config)
    shutil.rmtree(shard_dir, ignore_errors=True)
    return event_labels, coverage


def _add_full_market_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    market_cap = pd.to_numeric(out["market_cap"], errors="coerce")
    out["full_market_size_bucket"] = np.select(
        [
            market_cap < 300_000_000.0,
            market_cap < 2_000_000_000.0,
            market_cap < 10_000_000_000.0,
            market_cap < 50_000_000_000.0,
        ],
        ["micro", "small", "mid", "large"],
        default="mega",
    )
    out["sector"] = out["sector"] if "sector" in out.columns else ""
    out["industry"] = out["industry"] if "industry" in out.columns else ""
    return out


def _event_label_panel(
    frame: pd.DataFrame,
    *,
    min_shock_threshold: float,
    min_volume_spike: float,
    windows: list[str],
) -> pd.DataFrame:
    active = frame[
        frame["coverage_state"].eq("active_view")
        & frame["shock_abs"].ge(float(min_shock_threshold))
        & frame["abnormal_volume"].ge(float(min_volume_spike))
    ].copy()
    if active.empty:
        return pd.DataFrame(columns=_event_label_columns())
    active_index = active.index
    rows: list[dict[str, object]] = []
    asset_log_returns = np.log1p(pd.to_numeric(frame["return"], errors="coerce").clip(lower=-0.999999))
    benchmark_log_returns = np.log1p(pd.to_numeric(frame["benchmark_return"], errors="coerce").clip(lower=-0.999999))
    groups = frame["asset_id"].astype(str)
    for window in windows:
        if window not in SHARPEN_WINDOWS:
            continue
        offsets = SHARPEN_WINDOWS[window]
        asset_return = _window_compounded_return_by_group(asset_log_returns, groups, start=offsets[0], end=offsets[1])
        benchmark_return = _window_compounded_return_by_group(benchmark_log_returns, groups, start=offsets[0], end=offsets[1])
        window_frame = active.copy()
        window_frame["window"] = window
        window_frame["asset_return"] = asset_return.loc[active_index].to_numpy()
        window_frame["benchmark_return"] = benchmark_return.loc[active_index].to_numpy()
        window_frame["abnormal_return"] = window_frame["asset_return"] - window_frame["benchmark_return"]
        window_frame["label_status"] = np.where(
            window_frame["asset_return"].notna() & window_frame["benchmark_return"].notna(),
            "observed",
            "unavailable_missing_return_window",
        )
        rows.append(window_frame)
    if not rows:
        return pd.DataFrame(columns=_event_label_columns())
    panel = pd.concat(rows, ignore_index=True)
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce").dt.date.astype(str)
    panel["event_month"] = pd.to_datetime(panel["date"], errors="coerce").dt.strftime("%Y-%m")
    panel = panel.rename(columns={"return": "shock_return", "shock_abs": "abs_shock_return"})
    for key, value in _boundary_flags().items():
        panel[key] = value
    return panel[_event_label_columns()]


def _window_compounded_return_by_group(values: pd.Series, groups: pd.Series, *, start: int, end: int) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    window_len = int(end - start + 1)
    if window_len <= 0:
        return pd.Series(np.nan, index=numeric.index, dtype="float64")
    if start < 1:
        raise ValueError("E1 full-market window cache only supports post-event windows with start >= 1")

    group_keys = groups.astype(str)
    filled = numeric.fillna(0.0)
    valid = numeric.notna().astype(int)
    cumulative = filled.groupby(group_keys, sort=False).cumsum()
    valid_cumulative = valid.groupby(group_keys, sort=False).cumsum()
    end_prefix = cumulative.groupby(group_keys, sort=False).shift(-int(end))
    start_prefix = cumulative.groupby(group_keys, sort=False).shift(-int(start - 1))
    end_valid_prefix = valid_cumulative.groupby(group_keys, sort=False).shift(-int(end))
    start_valid_prefix = valid_cumulative.groupby(group_keys, sort=False).shift(-int(start - 1))
    valid_count = end_valid_prefix - start_valid_prefix
    sums = end_prefix - start_prefix
    sums = sums.where(valid_count.eq(window_len))
    return np.expm1(sums)


def _leaf_search(
    *,
    event_labels: pd.DataFrame,
    mechanisms: list[str],
    windows: list[str],
    shock_thresholds: list[float],
    volume_spike_thresholds: list[float],
    adv_min_dollars: list[float],
    max_depth: int,
    beam_width: int,
    min_events: int,
    min_event_months: int,
    excluded_predicates: Iterable[str] = (),
) -> tuple[pd.DataFrame, dict[int, pd.DataFrame], int]:
    rows: list[dict[str, object]] = []
    frame_lookup: dict[int, pd.DataFrame] = {}
    predicates = _predicate_library(excluded_predicates=excluded_predicates)
    frontier: list[dict[str, object]] = []
    node_id = 0
    search_count = 0
    for mechanism in mechanisms:
        for window in windows:
            for shock in shock_thresholds:
                for volume in volume_spike_thresholds:
                    for adv in adv_min_dollars:
                        base = _base_frame(
                            event_labels,
                            mechanism=mechanism,
                            window=window,
                            shock_threshold=float(shock),
                            volume_threshold=float(volume),
                            adv_min=float(adv),
                        )
                        node_id += 1
                        search_count += 1
                        row = _node_row(
                            node_id=node_id,
                            parent_node_id=0,
                            depth=0,
                            mechanism=mechanism,
                            window=window,
                            shock_threshold=float(shock),
                            volume_spike_threshold=float(volume),
                            adv_min_dollars=float(adv),
                            path_predicates=[],
                            added_predicate="root",
                            frame=base,
                            min_events=min_events,
                            min_event_months=min_event_months,
                        )
                        rows.append(row)
                        if row["eligible_for_overfit_review"]:
                            frame_lookup[node_id] = base
                            frontier.append(
                                {
                                    "node_id": node_id,
                                    "frame": base,
                                    "path": [],
                                    "mechanism": mechanism,
                                    "window": window,
                                    "shock_threshold": float(shock),
                                    "volume_spike_threshold": float(volume),
                                    "adv_min_dollars": float(adv),
                                }
                            )

    for depth in range(1, int(max_depth) + 1):
        candidates: list[dict[str, object]] = []
        for node in frontier:
            used = set(node["path"])
            for predicate in predicates:
                if predicate.predicate_id in used:
                    continue
                mask = predicate.apply(node["frame"])
                child = node["frame"][mask.fillna(False)].copy()
                node_id += 1
                search_count += 1
                path = [*node["path"], predicate.predicate_id]
                row = _node_row(
                    node_id=node_id,
                    parent_node_id=int(node["node_id"]),
                    depth=depth,
                    mechanism=str(node["mechanism"]),
                    window=str(node["window"]),
                    shock_threshold=float(node["shock_threshold"]),
                    volume_spike_threshold=float(node["volume_spike_threshold"]),
                    adv_min_dollars=float(node["adv_min_dollars"]),
                    path_predicates=path,
                    added_predicate=predicate.predicate_id,
                    frame=child,
                    min_events=min_events,
                    min_event_months=min_event_months,
                )
                rows.append(row)
                if row["eligible_for_overfit_review"]:
                    frame_lookup[node_id] = child
                    candidates.append(
                        {
                            "node_id": node_id,
                            "frame": child,
                            "path": path,
                            "mechanism": node["mechanism"],
                            "window": node["window"],
                            "shock_threshold": node["shock_threshold"],
                            "volume_spike_threshold": node["volume_spike_threshold"],
                            "adv_min_dollars": node["adv_min_dollars"],
                            "overfit_score": row["overfit_score"],
                        }
                    )
        if not candidates:
            break
        candidates.sort(key=lambda item: float(item["overfit_score"]), reverse=True)
        frontier = candidates[: int(beam_width)]
    if not rows:
        return pd.DataFrame(columns=_grid_columns()), frame_lookup, search_count
    grid = pd.DataFrame(rows)
    grid = grid.sort_values(
        ["eligible_for_overfit_review", "mean_directional_return", "overfit_score", "active_event_count"],
        ascending=[False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)
    grid["search_rank"] = range(1, len(grid) + 1)
    return grid[_grid_columns()], frame_lookup, search_count


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


def _apply_candidate_event_filters(
    event_labels: pd.DataFrame,
    *,
    exclude_stale_price_events: bool,
) -> tuple[pd.DataFrame, dict[str, object]]:
    if event_labels.empty:
        return event_labels.copy(), _candidate_filter_report(
            event_labels,
            event_labels,
            exclude_stale_price_events=exclude_stale_price_events,
        )
    filtered = event_labels.copy()
    if exclude_stale_price_events:
        stale_or_zero = _stale_price_event_mask(filtered)
        filtered = filtered[~stale_or_zero].copy()
    return filtered.reset_index(drop=True), _candidate_filter_report(
        event_labels,
        filtered,
        exclude_stale_price_events=exclude_stale_price_events,
    )


def _candidate_filter_report(
    before: pd.DataFrame,
    after: pd.DataFrame,
    *,
    exclude_stale_price_events: bool,
) -> dict[str, object]:
    return {
        "exclude_stale_price_events": bool(exclude_stale_price_events),
        "candidate_event_row_count_before_filter": int(len(before)),
        "candidate_event_row_count_after_filter": int(len(after)),
        "candidate_event_row_count_removed_by_stale_price_filter": int(len(before) - len(after))
        if exclude_stale_price_events
        else 0,
    }


def _stale_price_event_mask(frame: pd.DataFrame) -> pd.Series:
    stale = pd.to_numeric(frame.get("stale_roll_5", pd.Series(0, index=frame.index)), errors="coerce").fillna(0).ge(1)
    zero_volume = frame.get("zero_volume", pd.Series(False, index=frame.index)).astype(str).str.lower().isin({"true", "1", "yes"})
    return stale | zero_volume


def _directional_series(frame: pd.DataFrame, mechanism: str) -> pd.Series:
    abnormal = pd.to_numeric(frame["abnormal_return"], errors="coerce")
    if mechanism in {"up_shock_continuation", "down_shock_reversal"}:
        return abnormal
    if mechanism in {"up_shock_reversal", "down_shock_continuation"}:
        return -abnormal
    return pd.Series(np.nan, index=frame.index, dtype="float64")


def _predicate_library(*, excluded_predicates: Iterable[str] = ()) -> list[_Predicate]:
    excluded = {str(value).strip() for value in excluded_predicates if str(value).strip()}
    predicates = [
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
        _le("prior5_le_0", "prior_5d_return", 0.0),
        _ge("prior20_ge_0", "prior_20d_return", 0.0),
        _ge("prior20_ge_20pct", "prior_20d_return", 0.20),
        _le("prior20_le_0", "prior_20d_return", 0.0),
        _eq("size_micro", "full_market_size_bucket", "micro"),
        _eq("size_small", "full_market_size_bucket", "small"),
        _eq("size_mid", "full_market_size_bucket", "mid"),
        _eq("size_large", "full_market_size_bucket", "large"),
        _eq("size_mega", "full_market_size_bucket", "mega"),
        _eq("liquidity_low", "liquidity_bucket", "low"),
        _eq("liquidity_high", "liquidity_bucket", "high"),
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
    if not excluded:
        return predicates
    return [predicate for predicate in predicates if predicate.predicate_id not in excluded]


def _node_row(
    *,
    node_id: int,
    parent_node_id: int,
    depth: int,
    mechanism: str,
    window: str,
    shock_threshold: float,
    volume_spike_threshold: float,
    adv_min_dollars: float,
    path_predicates: list[str],
    added_predicate: str,
    frame: pd.DataFrame,
    min_events: int,
    min_event_months: int,
) -> dict[str, object]:
    directional = pd.to_numeric(frame.get("directional_return", pd.Series(dtype="float64")), errors="coerce").dropna()
    active_events = int(len(directional))
    active = frame.loc[directional.index] if active_events else frame.iloc[0:0]
    event_months = int(active["event_month"].nunique()) if active_events else 0
    issuer_count = int(active["asset_id"].nunique()) if active_events else 0
    mean_directional = float(directional.mean()) if active_events else np.nan
    median_directional = float(directional.median()) if active_events else np.nan
    std_directional = float(directional.std(ddof=1)) if active_events > 1 else np.nan
    t_stat = float(mean_directional / (std_directional / np.sqrt(active_events))) if active_events > 1 and std_directional > 0 else np.nan
    hit_rate = float((directional > 0.0).mean()) if active_events else np.nan
    winsorized = directional.clip(lower=directional.quantile(0.05), upper=directional.quantile(0.95)) if active_events else directional
    winsorized_mean = float(winsorized.mean()) if active_events else np.nan
    tail_penalty = _tail_penalty(active, directional)
    overfit_score = (
        float(mean_directional * np.sqrt(active_events) * np.sqrt(max(event_months, 1)) * max(hit_rate, 0.01) * tail_penalty)
        if active_events and pd.notna(mean_directional) and pd.notna(hit_rate)
        else np.nan
    )
    eligible = bool(
        active_events >= int(min_events)
        and event_months >= int(min_event_months)
        and pd.notna(mean_directional)
        and mean_directional > 0.0
    )
    return {
        "schema_version": "small_emotion_full_market_overfit_grid.v1",
        "stage": STAGE,
        "node_id": int(node_id),
        "parent_node_id": int(parent_node_id),
        "depth": int(depth),
        "mechanism": mechanism,
        "window": window,
        "shock_threshold": float(shock_threshold),
        "volume_spike_threshold": float(volume_spike_threshold),
        "adv_min_dollars": float(adv_min_dollars),
        "path_predicates": " & ".join(path_predicates),
        "added_predicate": added_predicate,
        "active_event_count": active_events,
        "event_month_count": event_months,
        "issuer_count": issuer_count,
        "event_set_hash": _event_set_hash(active),
        "mean_directional_return": mean_directional,
        "winsorized_mean_directional_return": winsorized_mean,
        "median_directional_return": median_directional,
        "t_stat": t_stat,
        "hit_rate": hit_rate,
        "overfit_score": overfit_score,
        "eligible_for_overfit_review": eligible,
        "selection_status": "exploratory_full_market_overfit_only",
        "search_rank": 0,
        **_boundary_flags(),
    }


def _top_pockets(grid: pd.DataFrame, *, top_n: int) -> pd.DataFrame:
    if grid.empty:
        return pd.DataFrame(columns=_top_columns())
    eligible = grid[grid["eligible_for_overfit_review"].astype(bool)].copy()
    if eligible.empty:
        return pd.DataFrame(columns=_top_columns())
    eligible["canonical_path_predicates"] = eligible["path_predicates"].map(_canonical_path)
    eligible = eligible.drop_duplicates(
        [
            "mechanism",
            "window",
            "shock_threshold",
            "volume_spike_threshold",
            "adv_min_dollars",
            "canonical_path_predicates",
        ],
        keep="first",
    )
    eligible = eligible.drop_duplicates("event_set_hash", keep="first")
    eligible = eligible.sort_values(
        ["mean_directional_return", "overfit_score", "active_event_count"],
        ascending=[False, False, False],
        na_position="last",
    ).head(int(top_n))
    eligible = eligible.reset_index(drop=True)
    eligible["pocket_rank"] = range(1, len(eligible) + 1)
    eligible["tail_concentration_status"] = "pending_audit"
    return eligible[_top_columns()]


def _canonical_path(value: object) -> str:
    return " & ".join(sorted(part.strip() for part in str(value or "").split("&") if part.strip()))


def _attach_tail_status(top: pd.DataFrame, tail: pd.DataFrame) -> pd.DataFrame:
    if top.empty or tail.empty:
        return top
    status = tail[["node_id", "tail_concentration_status"]].drop_duplicates("node_id")
    patched = top.drop(columns=["tail_concentration_status"], errors="ignore").merge(status, on="node_id", how="left")
    patched["tail_concentration_status"] = patched["tail_concentration_status"].fillna("audit_unavailable")
    return patched[_top_columns()]


def _tail_concentration_audit(top: pd.DataFrame, frame_lookup: dict[int, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for row in top.itertuples(index=False):
        frame = frame_lookup.get(int(row.node_id), pd.DataFrame()).copy()
        directional = pd.to_numeric(frame.get("directional_return", pd.Series(dtype="float64")), errors="coerce").dropna()
        active = frame.loc[directional.index] if not directional.empty else frame.iloc[0:0]
        abs_directional = directional.abs()
        top5_share = float(abs_directional.nlargest(min(5, len(abs_directional))).sum() / abs_directional.sum()) if abs_directional.sum() else np.nan
        issuer_share = _max_share(active, "asset_id")
        month_share = _max_share(active, "event_month")
        sector_share = _max_share(active, "sector")
        raw_mean = float(directional.mean()) if not directional.empty else np.nan
        winsorized_mean = float(directional.clip(directional.quantile(0.05), directional.quantile(0.95)).mean()) if len(directional) else np.nan
        status = "pass" if all(_not_too_concentrated(value) for value in [top5_share, issuer_share, month_share, sector_share]) else "review_tail_concentration"
        rows.append(
            {
                "schema_version": "small_emotion_full_market_tail_concentration_audit.v1",
                "stage": STAGE,
                "node_id": int(row.node_id),
                "pocket_rank": int(row.pocket_rank),
                "active_event_count": int(len(directional)),
                "raw_mean_directional_return": raw_mean,
                "winsorized_mean_directional_return": winsorized_mean,
                "median_directional_return": float(directional.median()) if len(directional) else np.nan,
                "hit_rate": float((directional > 0.0).mean()) if len(directional) else np.nan,
                "top5_abs_directional_return_share": top5_share,
                "issuer_concentration_max_share": issuer_share,
                "month_concentration_max_share": month_share,
                "sector_concentration_max_share": sector_share,
                "tail_concentration_status": status,
                **_boundary_flags(),
            }
        )
    return pd.DataFrame(rows, columns=_tail_columns())


def _cost_liquidity_audit(top: pd.DataFrame, frame_lookup: dict[int, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for row in top.itertuples(index=False):
        frame = frame_lookup.get(int(row.node_id), pd.DataFrame()).copy()
        spread = pd.to_numeric(frame.get("bid_ask_spread", pd.Series(dtype="float64")), errors="coerce")
        adv = pd.to_numeric(frame.get("adv20", pd.Series(dtype="float64")), errors="coerce")
        adv_25k = (25_000.0 / adv.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
        adv_100k = (100_000.0 / adv.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
        spread_p95 = float(spread.quantile(0.95)) if spread.notna().any() else np.nan
        slippage_stress = spread * 0.75
        slippage_p95 = float(slippage_stress.quantile(0.95)) if slippage_stress.notna().any() else np.nan
        status = (
            "review_cost_liquidity"
            if (pd.notna(spread_p95) and spread_p95 > 0.05)
            or (pd.notna(slippage_p95) and slippage_p95 > 0.04)
            or (adv_100k.notna().any() and float(adv_100k.quantile(0.95)) > 0.5)
            else "pass"
        )
        rows.append(
            {
                "schema_version": "small_emotion_full_market_cost_liquidity_audit.v1",
                "stage": STAGE,
                "node_id": int(row.node_id),
                "pocket_rank": int(row.pocket_rank),
                "active_event_count": int(len(frame)),
                "adv_participation_25k_p95": float(adv_25k.quantile(0.95)) if adv_25k.notna().any() else np.nan,
                "adv_participation_100k_p95": float(adv_100k.quantile(0.95)) if adv_100k.notna().any() else np.nan,
                "spread_proxy_p95": spread_p95,
                "slippage_stress_p95": slippage_p95,
                "cost_liquidity_status": status,
                **_boundary_flags(),
            }
        )
    return pd.DataFrame(rows, columns=_cost_columns())


def _tail_penalty(active: pd.DataFrame, directional: pd.Series) -> float:
    if directional.empty:
        return 0.0
    top5_share = float(directional.abs().nlargest(min(5, len(directional))).sum() / directional.abs().sum()) if directional.abs().sum() else 1.0
    month_share = _max_share(active, "event_month")
    concentration = max(value for value in [top5_share, month_share] if pd.notna(value))
    return float(max(0.10, 1.0 - min(concentration, 0.90)))


def _event_set_hash(frame: pd.DataFrame) -> str:
    if frame.empty:
        return ""
    keys = [
        f"{row.asset_id}|{row.date}|{row.window}"
        for row in frame[["asset_id", "date", "window"]].drop_duplicates().sort_values(["asset_id", "date", "window"]).itertuples(index=False)
    ]
    return hashlib.sha256("\n".join(keys).encode("utf-8")).hexdigest()


def _max_share(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return np.nan
    counts = frame[column].astype(str).value_counts(dropna=False)
    return float(counts.iloc[0] / counts.sum()) if int(counts.sum()) else np.nan


def _not_too_concentrated(value: float) -> bool:
    return bool(pd.isna(value) or value <= 0.50)


def _summary(
    *,
    grid: pd.DataFrame,
    top: pd.DataFrame,
    coverage: dict[str, object],
    max_depth: int,
    beam_width: int,
    search_count: int,
    feature_cache_status: str,
    cache_only: bool,
    excluded_predicates: list[str],
    candidate_filter_report: dict[str, object],
) -> dict[str, object]:
    top_candidate = top.iloc[0].to_dict() if not top.empty else {}
    return {
        "schema_version": "small_emotion_full_market_overfit_lab_summary.v1",
        "stage": STAGE,
        "candidate_id": "small_emotion_full_market_overfit_lab",
        "purpose": "find_strong_in_sample_full_market_pockets_before_any_freeze",
        "universe_scope": UNIVERSE_SCOPE,
        "grid_row_count": int(len(grid)),
        "search_burden_row_count": int(search_count),
        "top_pocket_count": int(len(top)),
        "overfit_pocket_found": bool(top_candidate),
        "top_pocket": _json_safe(top_candidate),
        "max_depth": int(max_depth),
        "beam_width": int(beam_width),
        "feature_cache_status": feature_cache_status,
        "cache_only": bool(cache_only),
        "excluded_predicates": list(excluded_predicates),
        **candidate_filter_report,
        "data_status": coverage.get("data_status"),
        **_boundary_flags(),
    }


def _write_outputs(
    artifacts: dict[str, Path],
    grid: pd.DataFrame,
    top: pd.DataFrame,
    tail: pd.DataFrame,
    cost: pd.DataFrame,
    summary: dict[str, object],
) -> None:
    grid.to_csv(artifacts["full_market_overfit_grid"], index=False)
    top.to_csv(artifacts["top_50_overfit_pockets"], index=False)
    tail.to_csv(artifacts["tail_concentration_audit"], index=False)
    cost.to_csv(artifacts["cost_liquidity_audit"], index=False)
    artifacts["overfit_disclosure"].write_text(
        json.dumps(_overfit_disclosure(grid, top), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["best_pocket_spec_draft"].write_text(
        json.dumps(_best_pocket_spec_draft(top), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["full_market_overfit_summary"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["full_market_overfit_report"].write_text(_report(summary, top, tail, cost), encoding="utf-8")


def _overfit_disclosure(grid: pd.DataFrame, top: pd.DataFrame) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_full_market_overfit_disclosure.v1",
        "stage": STAGE,
        "purpose": "exploratory overfit lab",
        "selection_bias_risk": "extreme",
        "search_burden_row_count": int(len(grid)),
        "reported_top_pocket_count": int(len(top)),
        "requires_freeze_before_q1": True,
        "results_are_not_alpha_evidence": True,
        **_boundary_flags(),
    }


def _best_pocket_spec_draft(top: pd.DataFrame) -> dict[str, object]:
    candidate = _json_safe(top.iloc[0].to_dict()) if not top.empty else {}
    return {
        "schema_version": "small_emotion_full_market_best_pocket_spec_draft.v1",
        "stage": STAGE,
        "candidate_found": bool(candidate),
        "candidate": candidate,
        "recommendation": "freeze_candidate_before_q1" if candidate else "no_overfit_pocket_found",
        "draft_only": True,
        "measurement_spec_written": False,
        "formula_score_written": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "expected_return_panel_written": False,
        "optimizer_entry_allowed": False,
        "alpha_registry_update_allowed": False,
        "production_approval_claimed": False,
        **_boundary_flags(),
    }


def _report(summary: dict[str, object], top: pd.DataFrame, tail: pd.DataFrame, cost: pd.DataFrame) -> str:
    lines = [
        "# E1-SMALL-EMOTION Full-Market Overfit Lab",
        "",
        "This is an exploratory overfit lab. It is not alpha evidence, does not write a MeasurementSpec, and opens no Q1/Q2/downstream workflow.",
        "",
        f"- universe_scope: {summary['universe_scope']}",
        f"- grid_row_count: {summary['grid_row_count']}",
        f"- search_burden_row_count: {summary['search_burden_row_count']}",
        f"- top_pocket_count: {summary['top_pocket_count']}",
        f"- overfit_pocket_found: {summary['overfit_pocket_found']}",
        f"- feature_cache_status: {summary['feature_cache_status']}",
        f"- cache_only: {summary['cache_only']}",
        "",
        "## Best Overfit Pockets",
    ]
    if top.empty:
        lines.append("- none")
    else:
        for row in top.head(10).itertuples(index=False):
            lines.append(
                "- "
                f"rank={row.pocket_rank}, mechanism={row.mechanism}, window={row.window}, "
                f"mean={row.mean_directional_return:.6f}, t={row.t_stat:.3f}, hit={row.hit_rate:.3f}, "
                f"events={row.active_event_count}, months={row.event_month_count}, path={row.path_predicates}"
            )
    if not tail.empty:
        lines.extend(["", "## Tail Audit", f"- worst_top5_share: {tail['top5_abs_directional_return_share'].max():.6f}"])
    if not cost.empty:
        lines.extend(["", "## Cost/Liquidity Audit", f"- worst_spread_p95: {cost['spread_proxy_p95'].max():.6f}"])
    lines.append("")
    return "\n".join(lines)


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


def _float_or_nan(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def _json_safe(values: dict[str, object]) -> dict[str, object]:
    safe: dict[str, object] = {}
    for key, value in values.items():
        if isinstance(value, (np.bool_, bool)):
            safe[key] = bool(value)
        elif isinstance(value, (np.integer, int)):
            safe[key] = int(value)
        elif isinstance(value, (np.floating, float)):
            safe[key] = None if pd.isna(value) else float(value)
        elif isinstance(value, float) and pd.isna(value):
            safe[key] = None
        elif pd.isna(value):
            safe[key] = None
        else:
            safe[key] = value
    return safe


def _boundary_flags() -> dict[str, object]:
    return {
        **EXPLORATORY_GUARDS,
        "not_alpha_evidence": True,
        "formula_score_written": False,
        "measurement_spec_written": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "expected_return_panel_written": False,
        "optimizer_entry_allowed": False,
        "portfolio_construction_allowed": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
        "no_view_not_zero_alpha": True,
    }


def _event_label_columns() -> list[str]:
    return [
        "asset_id",
        "ticker",
        "date",
        "event_month",
        "shock_return",
        "abs_shock_return",
        "abnormal_volume",
        "prior_5d_return",
        "prior_20d_return",
        "close_location",
        "open_to_close_return",
        "low_price_bucket",
        "market_cap",
        "full_market_size_bucket",
        "adv20",
        "liquidity_bucket",
        "weak_liquidity",
        "bid_ask_spread",
        "spread_bucket",
        "sector",
        "industry",
        "market_regime",
        "stale_roll_5",
        "zero_volume",
        "window",
        "asset_return",
        "benchmark_return",
        "abnormal_return",
        "label_status",
        *_boundary_flags().keys(),
    ]


def _grid_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "node_id",
        "parent_node_id",
        "depth",
        "mechanism",
        "window",
        "shock_threshold",
        "volume_spike_threshold",
        "adv_min_dollars",
        "path_predicates",
        "added_predicate",
        "active_event_count",
        "event_month_count",
        "issuer_count",
        "event_set_hash",
        "mean_directional_return",
        "winsorized_mean_directional_return",
        "median_directional_return",
        "t_stat",
        "hit_rate",
        "overfit_score",
        "eligible_for_overfit_review",
        "selection_status",
        "search_rank",
        *_boundary_flags().keys(),
    ]


def _top_columns() -> list[str]:
    return [*_grid_columns(), "pocket_rank", "tail_concentration_status"]


def _tail_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "node_id",
        "pocket_rank",
        "active_event_count",
        "raw_mean_directional_return",
        "winsorized_mean_directional_return",
        "median_directional_return",
        "hit_rate",
        "top5_abs_directional_return_share",
        "issuer_concentration_max_share",
        "month_concentration_max_share",
        "sector_concentration_max_share",
        "tail_concentration_status",
        *_boundary_flags().keys(),
    ]


def _cost_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "node_id",
        "pocket_rank",
        "active_event_count",
        "adv_participation_25k_p95",
        "adv_participation_100k_p95",
        "spread_proxy_p95",
        "slippage_stress_p95",
        "cost_liquidity_status",
        *_boundary_flags().keys(),
    ]
