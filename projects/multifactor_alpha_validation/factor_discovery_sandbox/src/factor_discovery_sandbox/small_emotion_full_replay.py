"""Chunked full replay for D2-SMALL-EMOTION observability."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from .small_emotion_d2 import GUARDS, run_small_emotion_d2_observability


@dataclass(frozen=True)
class SmallEmotionFullReplayResult:
    """Chunked D2 full replay output."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_emotion_chunked_full_replay(
    *,
    price_chunk_paths: Iterable[str | Path],
    benchmark_panel_path: str | Path,
    delisting_path: str | Path,
    output_dir: str | Path,
    minimum_subset_events: int = 50,
    minimum_event_month_count: int = 12,
    minimum_label_coverage_share: float = 0.70,
    min_history_observations: int = 60,
    min_adv_dollars: float = 250_000.0,
    minimum_observable_chunks: int = 2,
    refresh: bool = False,
) -> SmallEmotionFullReplayResult:
    """Run D2-SMALL-EMOTION over chunked local price panels and aggregate guards."""

    chunks = [Path(path) for path in price_chunk_paths]
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)
    chunk_root = output_path / "chunks"
    chunk_root.mkdir(parents=True, exist_ok=True)

    manifest_rows: list[dict[str, object]] = []
    subset_frames: list[pd.DataFrame] = []
    for index, chunk_path in enumerate(chunks, start=1):
        chunk_output = chunk_root / f"chunk_{index:04d}"
        summary_path = chunk_output / "d2_small_emotion_summary.json"
        if summary_path.exists() and not refresh:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            status = "resumed"
        else:
            result = run_small_emotion_d2_observability(
                price_panel_path=chunk_path,
                benchmark_panel_path=benchmark_panel_path,
                delisting_path=delisting_path,
                output_dir=chunk_output,
                minimum_subset_events=minimum_subset_events,
                minimum_event_month_count=minimum_event_month_count,
                minimum_label_coverage_share=minimum_label_coverage_share,
                min_history_observations=min_history_observations,
                min_adv_dollars=min_adv_dollars,
            )
            summary = result.summary
            status = "computed"
        manifest_rows.append(_chunk_manifest_row(index, chunk_path, chunk_output, status, summary))
        subset_path = chunk_output / "subset_counts.csv"
        diagnostics_path = chunk_output / "continuation_reversal_diagnostics.csv"
        placebo_path = chunk_output / "placebo_report.csv"
        if subset_path.exists() and diagnostics_path.exists() and placebo_path.exists():
            subset_frames.append(_subset_guard_frame(subset_path, diagnostics_path, placebo_path, index))

    chunk_manifest = pd.DataFrame(manifest_rows)
    subset_aggregate = _aggregate_subset_guards(subset_frames)
    decision, allow_d3 = _aggregate_decision(
        chunk_manifest=chunk_manifest,
        subset_aggregate=subset_aggregate,
        minimum_observable_chunks=minimum_observable_chunks,
    )
    summary = _summary(
        decision=decision,
        allow_d3=allow_d3,
        chunks=chunks,
        chunk_manifest=chunk_manifest,
        subset_aggregate=subset_aggregate,
        minimum_observable_chunks=minimum_observable_chunks,
    )

    chunk_manifest.to_csv(artifacts["chunk_manifest"], index=False)
    subset_aggregate.to_csv(artifacts["subset_guard_aggregate"], index=False)
    artifacts["full_replay_decision"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["full_replay_report"].write_text(_report(summary, subset_aggregate), encoding="utf-8")
    return SmallEmotionFullReplayResult(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "chunk_manifest": output_path / "chunk_manifest.csv",
        "subset_guard_aggregate": output_path / "subset_guard_aggregate.csv",
        "full_replay_decision": output_path / "full_replay_decision.json",
        "full_replay_report": output_path / "full_replay_report.md",
    }


def _chunk_manifest_row(
    index: int,
    chunk_path: Path,
    chunk_output: Path,
    status: str,
    summary: dict[str, object],
) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_full_replay_chunk_manifest.v1",
        "stage": "D2-SMALL-EMOTION-01A",
        "chunk_index": index,
        "chunk_path": str(chunk_path),
        "chunk_output_dir": str(chunk_output),
        "chunk_status": status,
        "chunk_decision": summary.get("overall_decision", "unavailable"),
        "allow_d3_charter_for": "|".join(summary.get("allow_d3_charter_for", []) or []),
        "price_row_count": int(summary.get("price_row_count", 0) or 0),
        "event_count": int(summary.get("event_count", 0) or 0),
        "active_event_count": int(summary.get("active_event_count", 0) or 0),
        "car_row_count": int(summary.get("car_row_count", 0) or 0),
        **GUARDS,
    }


def _subset_guard_frame(
    subset_path: Path,
    diagnostics_path: Path,
    placebo_path: Path,
    chunk_index: int,
) -> pd.DataFrame:
    subsets = pd.read_csv(subset_path)
    diagnostics = pd.read_csv(diagnostics_path)
    placebo = pd.read_csv(placebo_path)
    rows: list[dict[str, object]] = []
    for row in subsets.itertuples(index=False):
        subset = str(row.event_subset)
        if subset == "no_view_guard_row":
            continue
        diag = diagnostics[diagnostics["event_subset"].eq(subset)]
        place = placebo[placebo["event_subset"].eq(subset)]
        direction_ok = bool(
            not diag.empty
            and bool(diag["live_post_direction_matches_preregistered_mechanism"].iloc[0])
        )
        pre_dominates = bool(
            not diag.empty
            and bool(diag["pre_event_dominates_post"].iloc[0])
        )
        placebo_dominates = bool(place["placebo_dominates_live"].fillna(False).any()) if not place.empty else False
        stale_dominates = bool(
            place[place["placebo_name"].eq("stale_price_matched")]["placebo_dominates_live"].fillna(False).any()
        ) if not place.empty else False
        subset_guard_passed = bool(direction_ok and not pre_dominates and not placebo_dominates)
        rows.append(
            {
                "schema_version": "small_emotion_full_replay_subset_guard.v1",
                "stage": "D2-SMALL-EMOTION-01A",
                "chunk_index": chunk_index,
                "event_subset": subset,
                "active_event_count": int(getattr(row, "active_event_count", 0)),
                "event_month_count": int(getattr(row, "event_month_count", 0)),
                "label_coverage_share": float(getattr(row, "label_coverage_share", 0.0)),
                "direction_ok": direction_ok,
                "pre_event_dominates_post": pre_dominates,
                "placebo_dominates_live": placebo_dominates,
                "stale_placebo_dominates_live": stale_dominates,
                "subset_guard_passed": subset_guard_passed,
                **GUARDS,
            }
        )
    return pd.DataFrame(rows)


def _aggregate_subset_guards(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame(columns=_subset_aggregate_columns())
    combined = pd.concat(frames, ignore_index=True)
    grouped = (
        combined.groupby("event_subset", dropna=False)
        .agg(
            chunk_count=("chunk_index", "nunique"),
            active_event_count=("active_event_count", "sum"),
            max_event_month_count=("event_month_count", "max"),
            mean_label_coverage_share=("label_coverage_share", "mean"),
            direction_ok_chunk_count=("direction_ok", "sum"),
            pre_event_dominates_chunk_count=("pre_event_dominates_post", "sum"),
            placebo_dominates_chunk_count=("placebo_dominates_live", "sum"),
            stale_placebo_dominates_chunk_count=("stale_placebo_dominates_live", "sum"),
            subset_guard_passed_chunk_count=("subset_guard_passed", "sum"),
        )
        .reset_index()
    )
    grouped["schema_version"] = "small_emotion_full_replay_subset_guard_aggregate.v1"
    grouped["stage"] = "D2-SMALL-EMOTION-01A"
    grouped["subset_guard_passed"] = grouped["subset_guard_passed_chunk_count"] > 0
    for key, value in GUARDS.items():
        grouped[key] = value
    return grouped[_subset_aggregate_columns()]


def _subset_aggregate_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "event_subset",
        "chunk_count",
        "active_event_count",
        "max_event_month_count",
        "mean_label_coverage_share",
        "direction_ok_chunk_count",
        "pre_event_dominates_chunk_count",
        "placebo_dominates_chunk_count",
        "stale_placebo_dominates_chunk_count",
        "subset_guard_passed_chunk_count",
        "subset_guard_passed",
        *GUARDS.keys(),
    ]


def _aggregate_decision(
    *,
    chunk_manifest: pd.DataFrame,
    subset_aggregate: pd.DataFrame,
    minimum_observable_chunks: int,
) -> tuple[str, list[str]]:
    if chunk_manifest.empty:
        return "blocked_data_coverage", []
    if subset_aggregate.empty:
        return "hold_insufficient_sample", []
    eligible = subset_aggregate[
        subset_aggregate["subset_guard_passed_chunk_count"] >= int(minimum_observable_chunks)
    ].copy()
    if eligible.empty:
        if subset_aggregate["placebo_dominates_chunk_count"].fillna(0).gt(0).any():
            return "blocked_placebo_dominance", []
        return "hold_insufficient_sample", []
    eligible = eligible.sort_values(["subset_guard_passed_chunk_count", "active_event_count"], ascending=False)
    subset = str(eligible["event_subset"].iloc[0])
    if subset == "panic_overreaction_candidate":
        return "observable_panic_reversal", ["panic_overreaction_reversal"]
    if subset == "fomo_continuation_candidate":
        return "observable_fomo_continuation", ["fomo_continuation"]
    if subset == "liquidity_vacuum_reversal_candidate":
        return "observable_liquidity_vacuum_reversal", ["liquidity_vacuum_reversal"]
    return "mixed_narrow_scope", []


def _summary(
    *,
    decision: str,
    allow_d3: list[str],
    chunks: list[Path],
    chunk_manifest: pd.DataFrame,
    subset_aggregate: pd.DataFrame,
    minimum_observable_chunks: int,
) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_full_replay_summary.v1",
        "stage": "D2-SMALL-EMOTION-01A",
        "source_mode": "chunked_price_panels",
        "chunk_count": int(len(chunks)),
        "computed_chunk_count": int(chunk_manifest["chunk_status"].eq("computed").sum()) if "chunk_status" in chunk_manifest else 0,
        "resumed_chunk_count": int(chunk_manifest["chunk_status"].eq("resumed").sum()) if "chunk_status" in chunk_manifest else 0,
        "minimum_observable_chunks": int(minimum_observable_chunks),
        "overall_decision": decision,
        "allow_d3_charter_for": allow_d3[:1],
        "aggregate_active_event_count": int(subset_aggregate["active_event_count"].sum()) if "active_event_count" in subset_aggregate else 0,
        **GUARDS,
    }


def _report(summary: dict[str, object], subset_aggregate: pd.DataFrame) -> str:
    lines = [
        "# D2-SMALL-EMOTION-01A Chunked Full Replay",
        "",
        "This is no-formula observability only. It is not alpha evidence and does not open Q1, Q2, optimizer, portfolio, Alpha Registry, paper, broker, order, live, or production workflows.",
        "",
        f"- decision: {summary['overall_decision']}",
        f"- allowed D3 charter: {summary['allow_d3_charter_for']}",
        f"- chunks: {summary['chunk_count']}",
        f"- computed chunks: {summary['computed_chunk_count']}",
        f"- resumed chunks: {summary['resumed_chunk_count']}",
        "",
        "## Subset Guards",
    ]
    if subset_aggregate.empty:
        lines.append("- no observed subset rows")
    else:
        for row in subset_aggregate.itertuples(index=False):
            lines.append(
                "- "
                f"{row.event_subset}: passed_chunks={row.subset_guard_passed_chunk_count}, "
                f"active_events={row.active_event_count}, "
                f"placebo_dominates_chunks={row.placebo_dominates_chunk_count}, "
                f"stale_dominates_chunks={row.stale_placebo_dominates_chunk_count}"
            )
    lines.append("")
    return "\n".join(lines)
