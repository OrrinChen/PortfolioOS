"""Chunked replay for one sharpened E0 small-emotion pocket."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .small_emotion_exploratory_sweep import EXPLORATORY_GUARDS
from .small_emotion_sharpening_sweep import run_small_emotion_sharpening_sweep


@dataclass(frozen=True)
class SmallEmotionSharpenedTopPocketReplayResult:
    """Focused chunked replay output for a sharpened E0 pocket."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_emotion_sharpened_top_pocket_replay(
    *,
    price_chunk_paths: Iterable[str | Path],
    benchmark_panel_path: str | Path,
    delisting_path: str | Path,
    output_dir: str | Path,
    mechanism: str,
    shock_threshold: float,
    volume_spike_threshold: float,
    prior_5d_min_return: float | None,
    prior_20d_min_return: float | None,
    close_location_filter: str,
    low_price_filter: str,
    market_cap_bucket: str,
    liquidity_filter: str,
    spread_filter: str,
    regime_filter: str,
    adv_min_dollars: float,
    window: str,
    min_history_observations: int = 60,
    minimum_observed_chunks: int = 3,
    minimum_positive_chunks: int = 3,
    minimum_aggregate_events: int = 50,
    refresh: bool = False,
) -> SmallEmotionSharpenedTopPocketReplayResult:
    """Replay a sharpened candidate over chunked price panels."""

    chunks = [Path(path) for path in price_chunk_paths]
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    chunk_root = output_path / "chunks"
    chunk_root.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    manifest_rows: list[dict[str, object]] = []
    metric_rows: list[dict[str, object]] = []
    for index, chunk_path in enumerate(chunks, start=1):
        chunk_output = chunk_root / f"chunk_{index:04d}"
        summary_path = chunk_output / "sharpening_sweep_summary.json"
        if summary_path.exists() and not refresh:
            chunk_status = "resumed"
            chunk_summary = json.loads(summary_path.read_text(encoding="utf-8"))
        else:
            result = run_small_emotion_sharpening_sweep(
                price_panel_path=chunk_path,
                benchmark_panel_path=benchmark_panel_path,
                delisting_path=delisting_path,
                output_dir=chunk_output,
                shock_thresholds=[float(shock_threshold)],
                volume_spike_thresholds=[float(volume_spike_threshold)],
                prior_5d_min_returns=[prior_5d_min_return],
                prior_20d_min_returns=[prior_20d_min_return],
                close_location_filters=[close_location_filter],
                low_price_filters=[low_price_filter],
                market_cap_buckets=[market_cap_bucket],
                liquidity_filters=[liquidity_filter],
                spread_filters=[spread_filter],
                regime_filters=[regime_filter],
                windows=[window],
                mechanisms=[mechanism],
                adv_min_dollars=[float(adv_min_dollars)],
                min_history_observations=min_history_observations,
                min_events=1,
                top_n=1,
                max_rows=None,
            )
            chunk_status = "computed"
            chunk_summary = result.summary
        manifest_rows.append(_manifest_row(index, chunk_path, chunk_output, chunk_status, chunk_summary))
        metric_rows.append(
            _metric_row(
                index=index,
                chunk_path=chunk_path,
                grid_path=chunk_output / "sharpening_sweep_grid.csv",
                mechanism=mechanism,
                shock_threshold=shock_threshold,
                volume_spike_threshold=volume_spike_threshold,
                prior_5d_min_return=prior_5d_min_return,
                prior_20d_min_return=prior_20d_min_return,
                close_location_filter=close_location_filter,
                low_price_filter=low_price_filter,
                market_cap_bucket=market_cap_bucket,
                liquidity_filter=liquidity_filter,
                spread_filter=spread_filter,
                regime_filter=regime_filter,
                adv_min_dollars=adv_min_dollars,
                window=window,
            )
        )

    manifest = pd.DataFrame(manifest_rows)
    metrics = pd.DataFrame(metric_rows)
    summary = _summary(
        manifest=manifest,
        metrics=metrics,
        chunks=chunks,
        mechanism=mechanism,
        shock_threshold=shock_threshold,
        volume_spike_threshold=volume_spike_threshold,
        prior_5d_min_return=prior_5d_min_return,
        prior_20d_min_return=prior_20d_min_return,
        close_location_filter=close_location_filter,
        low_price_filter=low_price_filter,
        market_cap_bucket=market_cap_bucket,
        liquidity_filter=liquidity_filter,
        spread_filter=spread_filter,
        regime_filter=regime_filter,
        adv_min_dollars=adv_min_dollars,
        window=window,
        minimum_observed_chunks=minimum_observed_chunks,
        minimum_positive_chunks=minimum_positive_chunks,
        minimum_aggregate_events=minimum_aggregate_events,
    )
    freeze_review = _freeze_review(summary)

    manifest.to_csv(artifacts["sharpened_top_pocket_chunk_manifest"], index=False)
    metrics.to_csv(artifacts["sharpened_top_pocket_chunk_metrics"], index=False)
    artifacts["sharpened_top_pocket_replay_summary"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["sharpened_candidate_freeze_review"].write_text(
        json.dumps(freeze_review, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["sharpened_top_pocket_replay_report"].write_text(_report(summary, metrics), encoding="utf-8")
    return SmallEmotionSharpenedTopPocketReplayResult(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "sharpened_top_pocket_chunk_manifest": output_path / "sharpened_top_pocket_chunk_manifest.csv",
        "sharpened_top_pocket_chunk_metrics": output_path / "sharpened_top_pocket_chunk_metrics.csv",
        "sharpened_top_pocket_replay_summary": output_path / "sharpened_top_pocket_replay_summary.json",
        "sharpened_candidate_freeze_review": output_path / "sharpened_candidate_freeze_review.json",
        "sharpened_top_pocket_replay_report": output_path / "sharpened_top_pocket_replay_report.md",
    }


def _manifest_row(
    index: int,
    chunk_path: Path,
    chunk_output: Path,
    chunk_status: str,
    chunk_summary: dict[str, object],
) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_sharpened_top_pocket_chunk_manifest.v1",
        "stage": "E0-SMALL-EMOTION-04A",
        "chunk_index": index,
        "chunk_path": str(chunk_path),
        "chunk_output_dir": str(chunk_output),
        "chunk_status": chunk_status,
        "chunk_data_status": chunk_summary.get("data_status", "unavailable"),
        "chunk_grid_row_count": int(chunk_summary.get("grid_row_count", 0) or 0),
        "chunk_candidate_found": bool(chunk_summary.get("explosive_candidate_found", False)),
        **EXPLORATORY_GUARDS,
    }


def _metric_row(
    *,
    index: int,
    chunk_path: Path,
    grid_path: Path,
    mechanism: str,
    shock_threshold: float,
    volume_spike_threshold: float,
    prior_5d_min_return: float | None,
    prior_20d_min_return: float | None,
    close_location_filter: str,
    low_price_filter: str,
    market_cap_bucket: str,
    liquidity_filter: str,
    spread_filter: str,
    regime_filter: str,
    adv_min_dollars: float,
    window: str,
) -> dict[str, object]:
    base = {
        "schema_version": "small_emotion_sharpened_top_pocket_chunk_metrics.v1",
        "stage": "E0-SMALL-EMOTION-04A",
        "chunk_index": index,
        "chunk_path": str(chunk_path),
        "mechanism": mechanism,
        "shock_threshold": float(shock_threshold),
        "volume_spike_threshold": float(volume_spike_threshold),
        "prior_5d_min_return": "" if prior_5d_min_return is None else float(prior_5d_min_return),
        "prior_20d_min_return": "" if prior_20d_min_return is None else float(prior_20d_min_return),
        "close_location_filter": close_location_filter,
        "low_price_filter": low_price_filter,
        "market_cap_bucket": market_cap_bucket,
        "liquidity_filter": liquidity_filter,
        "spread_filter": spread_filter,
        "regime_filter": regime_filter,
        "adv_min_dollars": float(adv_min_dollars),
        "window": window,
        "active_event_count": 0,
        "event_month_count": 0,
        "issuer_count": 0,
        "mean_directional_return": np.nan,
        "median_directional_return": np.nan,
        "hit_rate": np.nan,
        "gross_explosive_score": np.nan,
        "chunk_observed": False,
        "chunk_positive": False,
        **EXPLORATORY_GUARDS,
    }
    if not grid_path.exists():
        return base
    grid = pd.read_csv(grid_path)
    if grid.empty:
        return base
    row = grid.iloc[0]
    active_event_count = int(row.get("active_event_count", 0) or 0)
    mean_directional = _float_or_nan(row.get("mean_directional_return"))
    base.update(
        {
            "active_event_count": active_event_count,
            "event_month_count": int(row.get("event_month_count", 0) or 0),
            "issuer_count": int(row.get("issuer_count", 0) or 0),
            "mean_directional_return": mean_directional,
            "median_directional_return": _float_or_nan(row.get("median_directional_return")),
            "hit_rate": _float_or_nan(row.get("hit_rate")),
            "gross_explosive_score": _float_or_nan(row.get("gross_explosive_score")),
            "chunk_observed": bool(active_event_count > 0 and pd.notna(mean_directional)),
            "chunk_positive": bool(active_event_count > 0 and pd.notna(mean_directional) and mean_directional > 0.0),
        }
    )
    return base


def _summary(
    *,
    manifest: pd.DataFrame,
    metrics: pd.DataFrame,
    chunks: list[Path],
    mechanism: str,
    shock_threshold: float,
    volume_spike_threshold: float,
    prior_5d_min_return: float | None,
    prior_20d_min_return: float | None,
    close_location_filter: str,
    low_price_filter: str,
    market_cap_bucket: str,
    liquidity_filter: str,
    spread_filter: str,
    regime_filter: str,
    adv_min_dollars: float,
    window: str,
    minimum_observed_chunks: int,
    minimum_positive_chunks: int,
    minimum_aggregate_events: int,
) -> dict[str, object]:
    aggregate_events = int(metrics["active_event_count"].sum()) if "active_event_count" in metrics else 0
    observed_chunks = int(metrics["chunk_observed"].fillna(False).sum()) if "chunk_observed" in metrics else 0
    positive_chunks = int(metrics["chunk_positive"].fillna(False).sum()) if "chunk_positive" in metrics else 0
    weighted_mean = _weighted_mean(metrics, "mean_directional_return", "active_event_count")
    weighted_hit_rate = _weighted_mean(metrics, "hit_rate", "active_event_count")
    candidate_ready = bool(
        observed_chunks >= int(minimum_observed_chunks)
        and positive_chunks >= int(minimum_positive_chunks)
        and aggregate_events >= int(minimum_aggregate_events)
        and pd.notna(weighted_mean)
        and weighted_mean > 0.0
    )
    if candidate_ready:
        decision = "candidate_stable_enough_for_manual_d3_freeze_review"
    elif observed_chunks < int(minimum_observed_chunks) or aggregate_events < int(minimum_aggregate_events):
        decision = "hold_insufficient_full_replay_sample"
    else:
        decision = "candidate_unstable_do_not_freeze"
    return {
        "schema_version": "small_emotion_sharpened_top_pocket_replay_summary.v1",
        "stage": "E0-SMALL-EMOTION-04A",
        "candidate_id": "small_cap_sharpened_up_shock_reversal_replay",
        "mechanism": mechanism,
        "shock_threshold": float(shock_threshold),
        "volume_spike_threshold": float(volume_spike_threshold),
        "prior_5d_min_return": "" if prior_5d_min_return is None else float(prior_5d_min_return),
        "prior_20d_min_return": "" if prior_20d_min_return is None else float(prior_20d_min_return),
        "close_location_filter": close_location_filter,
        "low_price_filter": low_price_filter,
        "market_cap_bucket": market_cap_bucket,
        "liquidity_filter": liquidity_filter,
        "spread_filter": spread_filter,
        "regime_filter": regime_filter,
        "adv_min_dollars": float(adv_min_dollars),
        "window": window,
        "chunk_count": int(len(chunks)),
        "observed_chunk_count": observed_chunks,
        "positive_chunk_count": positive_chunks,
        "aggregate_active_event_count": aggregate_events,
        "weighted_mean_directional_return": weighted_mean,
        "weighted_hit_rate": weighted_hit_rate,
        "overall_decision": decision,
        "exploratory_only": True,
        "measurement_spec_written": False,
        "formula_score_written": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        **EXPLORATORY_GUARDS,
    }


def _freeze_review(summary: dict[str, object]) -> dict[str, object]:
    ready = summary["overall_decision"] == "candidate_stable_enough_for_manual_d3_freeze_review"
    return {
        "schema_version": "small_emotion_sharpened_top_pocket_freeze_review.v1",
        "stage": "E0-SMALL-EMOTION-04A",
        "candidate_id": summary["candidate_id"],
        "overall_decision": summary["overall_decision"],
        "candidate_ready_for_manual_d3_freeze_review": ready,
        "candidate_can_be_reviewed_for_d3_freeze": ready,
        "candidate": {
            "mechanism": summary["mechanism"],
            "shock_threshold": summary["shock_threshold"],
            "volume_spike_threshold": summary["volume_spike_threshold"],
            "prior_5d_min_return": summary["prior_5d_min_return"],
            "prior_20d_min_return": summary["prior_20d_min_return"],
            "close_location_filter": summary["close_location_filter"],
            "low_price_filter": summary["low_price_filter"],
            "market_cap_bucket": summary["market_cap_bucket"],
            "liquidity_filter": summary["liquidity_filter"],
            "spread_filter": summary["spread_filter"],
            "regime_filter": summary["regime_filter"],
            "stale_filter": "not_used_in_sharpened_candidate",
            "adv_min_dollars": summary["adv_min_dollars"],
            "window": summary["window"],
        },
        "requires_manual_freeze_before_q1": True,
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


def _report(summary: dict[str, object], metrics: pd.DataFrame) -> str:
    lines = [
        "# E0-SMALL-EMOTION-04A Sharpened Top Pocket Replay",
        "",
        "This replay checks whether one aggressively selected in-sample pocket survives chunked/full replay. It is not alpha evidence and opens no downstream workflow.",
        "",
        f"- overall_decision: {summary['overall_decision']}",
        f"- aggregate_active_event_count: {summary['aggregate_active_event_count']}",
        f"- weighted_mean_directional_return: {summary['weighted_mean_directional_return']}",
        f"- weighted_hit_rate: {summary['weighted_hit_rate']}",
        f"- observed_chunk_count: {summary['observed_chunk_count']}",
        f"- positive_chunk_count: {summary['positive_chunk_count']}",
        "",
        "## Chunk Metrics",
    ]
    if metrics.empty:
        lines.append("- none")
    else:
        for row in metrics.itertuples(index=False):
            lines.append(
                "- "
                f"chunk={row.chunk_index}, events={row.active_event_count}, "
                f"mean={row.mean_directional_return}, hit_rate={row.hit_rate}, "
                f"positive={row.chunk_positive}"
            )
    lines.append("")
    return "\n".join(lines)


def _weighted_mean(frame: pd.DataFrame, value_col: str, weight_col: str) -> float:
    if frame.empty or value_col not in frame or weight_col not in frame:
        return np.nan
    values = pd.to_numeric(frame[value_col], errors="coerce")
    weights = pd.to_numeric(frame[weight_col], errors="coerce").fillna(0.0)
    valid = values.notna() & weights.gt(0.0)
    if not valid.any():
        return np.nan
    return float(np.average(values[valid], weights=weights[valid]))


def _float_or_nan(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return np.nan
