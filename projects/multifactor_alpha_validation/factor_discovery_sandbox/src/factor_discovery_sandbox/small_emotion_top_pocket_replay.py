"""Chunked replay for a selected E0 small-emotion top pocket."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .small_emotion_exploratory_sweep import EXPLORATORY_GUARDS, run_small_emotion_exploratory_sweep


@dataclass(frozen=True)
class SmallEmotionTopPocketReplayResult:
    """Focused chunked replay output for one exploratory pocket."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_emotion_top_pocket_chunked_replay(
    *,
    price_chunk_paths: Iterable[str | Path],
    benchmark_panel_path: str | Path,
    delisting_path: str | Path,
    output_dir: str | Path,
    mechanism: str,
    shock_threshold: float,
    volume_spike_threshold: float,
    market_cap_bucket: str,
    liquidity_filter: str,
    stale_filter: str,
    adv_min_dollars: float,
    window: str,
    min_history_observations: int = 60,
    minimum_observed_chunks: int = 3,
    minimum_positive_chunks: int = 3,
    minimum_aggregate_events: int = 500,
    refresh: bool = False,
) -> SmallEmotionTopPocketReplayResult:
    """Replay a single E0 candidate over chunked price panels."""

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
        summary_path = chunk_output / "exploratory_sweep_summary.json"
        if summary_path.exists() and not refresh:
            chunk_status = "resumed"
            chunk_summary = json.loads(summary_path.read_text(encoding="utf-8"))
        else:
            result = run_small_emotion_exploratory_sweep(
                price_panel_path=chunk_path,
                benchmark_panel_path=benchmark_panel_path,
                delisting_path=delisting_path,
                output_dir=chunk_output,
                shock_thresholds=[float(shock_threshold)],
                volume_spike_thresholds=[float(volume_spike_threshold)],
                windows=[window],
                mechanisms=[mechanism],
                market_cap_buckets=[market_cap_bucket],
                liquidity_filters=[liquidity_filter],
                stale_filters=[stale_filter],
                adv_min_dollars=[float(adv_min_dollars)],
                min_history_observations=min_history_observations,
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
                grid_path=chunk_output / "parameter_sweep_grid.csv",
                mechanism=mechanism,
                shock_threshold=shock_threshold,
                volume_spike_threshold=volume_spike_threshold,
                market_cap_bucket=market_cap_bucket,
                liquidity_filter=liquidity_filter,
                stale_filter=stale_filter,
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
        market_cap_bucket=market_cap_bucket,
        liquidity_filter=liquidity_filter,
        stale_filter=stale_filter,
        adv_min_dollars=adv_min_dollars,
        window=window,
        minimum_observed_chunks=minimum_observed_chunks,
        minimum_positive_chunks=minimum_positive_chunks,
        minimum_aggregate_events=minimum_aggregate_events,
    )
    freeze_review = _freeze_review(summary)

    manifest.to_csv(artifacts["top_pocket_chunk_manifest"], index=False)
    metrics.to_csv(artifacts["top_pocket_chunk_metrics"], index=False)
    artifacts["top_pocket_replay_summary"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["candidate_freeze_review"].write_text(
        json.dumps(freeze_review, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["top_pocket_replay_report"].write_text(_report(summary, metrics), encoding="utf-8")
    return SmallEmotionTopPocketReplayResult(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "top_pocket_chunk_manifest": output_path / "top_pocket_chunk_manifest.csv",
        "top_pocket_chunk_metrics": output_path / "top_pocket_chunk_metrics.csv",
        "top_pocket_replay_summary": output_path / "top_pocket_replay_summary.json",
        "candidate_freeze_review": output_path / "candidate_freeze_review.json",
        "top_pocket_replay_report": output_path / "top_pocket_replay_report.md",
    }


def _manifest_row(
    index: int,
    chunk_path: Path,
    chunk_output: Path,
    chunk_status: str,
    chunk_summary: dict[str, object],
) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_top_pocket_chunk_manifest.v1",
        "stage": "E0-SMALL-EMOTION-02A",
        "chunk_index": index,
        "chunk_path": str(chunk_path),
        "chunk_output_dir": str(chunk_output),
        "chunk_status": chunk_status,
        "chunk_data_status": chunk_summary.get("data_status", "unavailable"),
        "chunk_grid_row_count": int(chunk_summary.get("grid_row_count", 0) or 0),
        "chunk_candidate_found": bool(chunk_summary.get("candidate_found_for_possible_freeze", False)),
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
    market_cap_bucket: str,
    liquidity_filter: str,
    stale_filter: str,
    adv_min_dollars: float,
    window: str,
) -> dict[str, object]:
    base = {
        "schema_version": "small_emotion_top_pocket_chunk_metrics.v1",
        "stage": "E0-SMALL-EMOTION-02A",
        "chunk_index": index,
        "chunk_path": str(chunk_path),
        "mechanism": mechanism,
        "shock_threshold": float(shock_threshold),
        "volume_spike_threshold": float(volume_spike_threshold),
        "market_cap_bucket": market_cap_bucket,
        "liquidity_filter": liquidity_filter,
        "stale_filter": stale_filter,
        "adv_min_dollars": float(adv_min_dollars),
        "window": window,
        "active_event_count": 0,
        "event_month_count": 0,
        "issuer_count": 0,
        "mean_directional_return": np.nan,
        "median_directional_return": np.nan,
        "hit_rate": np.nan,
        "exploratory_score": np.nan,
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
            "exploratory_score": _float_or_nan(row.get("exploratory_score")),
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
    market_cap_bucket: str,
    liquidity_filter: str,
    stale_filter: str,
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
        "schema_version": "small_emotion_top_pocket_replay_summary.v1",
        "stage": "E0-SMALL-EMOTION-02A",
        "source_mode": "chunked_price_panels",
        "chunk_count": int(len(chunks)),
        "computed_chunk_count": int(manifest["chunk_status"].eq("computed").sum()) if "chunk_status" in manifest else 0,
        "resumed_chunk_count": int(manifest["chunk_status"].eq("resumed").sum()) if "chunk_status" in manifest else 0,
        "mechanism": mechanism,
        "shock_threshold": float(shock_threshold),
        "volume_spike_threshold": float(volume_spike_threshold),
        "market_cap_bucket": market_cap_bucket,
        "liquidity_filter": liquidity_filter,
        "stale_filter": stale_filter,
        "adv_min_dollars": float(adv_min_dollars),
        "window": window,
        "aggregate_active_event_count": aggregate_events,
        "observed_chunk_count": observed_chunks,
        "positive_chunk_count": positive_chunks,
        "weighted_mean_directional_return": weighted_mean,
        "weighted_hit_rate": weighted_hit_rate,
        "minimum_observed_chunks": int(minimum_observed_chunks),
        "minimum_positive_chunks": int(minimum_positive_chunks),
        "minimum_aggregate_events": int(minimum_aggregate_events),
        "candidate_can_be_reviewed_for_d3_freeze": candidate_ready,
        "overall_decision": decision,
        "d3_charter_written": False,
        "d3_auto_open_allowed": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        **EXPLORATORY_GUARDS,
    }


def _freeze_review(summary: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_candidate_freeze_review.v1",
        "stage": "E0-SMALL-EMOTION-02A",
        "candidate_can_be_reviewed_for_d3_freeze": bool(summary["candidate_can_be_reviewed_for_d3_freeze"]),
        "overall_decision": summary["overall_decision"],
        "candidate": {
            "mechanism": summary["mechanism"],
            "shock_threshold": summary["shock_threshold"],
            "volume_spike_threshold": summary["volume_spike_threshold"],
            "market_cap_bucket": summary["market_cap_bucket"],
            "liquidity_filter": summary["liquidity_filter"],
            "stale_filter": summary["stale_filter"],
            "adv_min_dollars": summary["adv_min_dollars"],
            "window": summary["window"],
        },
        "manual_next_step": (
            "write_explicit_D3_charter_for_this_one_candidate"
            if summary["candidate_can_be_reviewed_for_d3_freeze"]
            else "do_not_freeze_this_candidate"
        ),
        "d3_charter_written": False,
        "measurement_spec_written": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "expected_return_panel_written": False,
        "optimizer_entry_allowed": False,
        "alpha_registry_update_allowed": False,
        "production_approval_claimed": False,
        **EXPLORATORY_GUARDS,
    }


def _weighted_mean(frame: pd.DataFrame, value_column: str, weight_column: str) -> float:
    if frame.empty or value_column not in frame or weight_column not in frame:
        return float("nan")
    values = pd.to_numeric(frame[value_column], errors="coerce")
    weights = pd.to_numeric(frame[weight_column], errors="coerce").fillna(0.0)
    mask = values.notna() & weights.gt(0.0)
    if not mask.any():
        return float("nan")
    return float(np.average(values[mask], weights=weights[mask]))


def _float_or_nan(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def _report(summary: dict[str, object], metrics: pd.DataFrame) -> str:
    lines = [
        "# E0-SMALL-EMOTION-02A Top-Pocket Chunked Replay",
        "",
        "This is a focused full/chunk replay of one exploratory in-sample pocket. It is not alpha evidence and does not write a D3 charter, MeasurementSpec, Q1, Q2, optimizer, portfolio, Alpha Registry, paper, broker, order, live, or production artifact.",
        "",
        f"- decision: {summary['overall_decision']}",
        f"- candidate_can_be_reviewed_for_d3_freeze: {summary['candidate_can_be_reviewed_for_d3_freeze']}",
        f"- aggregate_active_event_count: {summary['aggregate_active_event_count']}",
        f"- observed_chunk_count: {summary['observed_chunk_count']}",
        f"- positive_chunk_count: {summary['positive_chunk_count']}",
        f"- weighted_mean_directional_return: {summary['weighted_mean_directional_return']}",
        "",
        "## Chunk Metrics",
    ]
    if metrics.empty:
        lines.append("- no chunk metrics")
    else:
        for row in metrics.itertuples(index=False):
            lines.append(
                "- "
                f"chunk={row.chunk_index}, events={row.active_event_count}, "
                f"mean_directional={row.mean_directional_return}, positive={row.chunk_positive}"
            )
    lines.append("")
    return "\n".join(lines)
