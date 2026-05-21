"""Small-emotion Q2 robustness profile audit.

This module audits concentration, temporal breadth, horizon decay, candidate
overlap, and bootstrap stability for Q2-completed small-emotion candidates. It
does not reselect pockets, modify frozen formulas, write orders, build
production portfolio artifacts, update Alpha Registry, or open paper/live
workflows.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd

from portfolio_os.provenance.hashing import canonical_json, hash_payload, sha256_file


STAGE = "Q2-SMALL-EMOTION-07"
Q2_COMPLETE_DECISION = "completed_q2_execution_survival"
PASS_DECISION = "robustness_profile_passed"


@dataclass(frozen=True)
class SmallEmotionQ2RobustnessAuditResult:
    """Written robustness audit artifacts and summary."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_emotion_q2_robustness_audit(
    *,
    q2_complete_dir: str | Path,
    q1_event_panels: Mapping[str, str | Path],
    q1_window_panels: Mapping[str, str | Path],
    output_dir: str | Path,
    minimum_event_count: int = 100,
    minimum_event_month_count: int = 24,
    bootstrap_trials: int = 500,
    random_seed: int = 12345,
) -> SmallEmotionQ2RobustnessAuditResult:
    """Run Q2 robustness audit for completed small-emotion candidates."""

    q2_complete_path = Path(q2_complete_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)
    complete_matrix = _read_csv(q2_complete_path / "small_emotion_q2_complete_matrix.csv")

    primary_frames: dict[str, pd.DataFrame] = {}
    robustness_rows: list[dict[str, object]] = []
    horizon_frames: list[pd.DataFrame] = []
    concentration_rows: list[dict[str, object]] = []
    bootstrap_rows: list[dict[str, object]] = []

    for candidate_name in _candidate_order(complete_matrix, q1_event_panels, q1_window_panels):
        complete_row = _first_row(complete_matrix, candidate_name)
        if str(complete_row.get("q2_complete_decision")) != Q2_COMPLETE_DECISION:
            robustness_rows.append(_blocked_row(candidate_name, complete_row, "blocked_q2_incomplete"))
            continue
        event_path = Path(q1_event_panels.get(candidate_name, ""))
        window_path = Path(q1_window_panels.get(candidate_name, ""))
        if not event_path.exists() or not window_path.exists():
            robustness_rows.append(_blocked_row(candidate_name, complete_row, "blocked_missing_q1_artifacts"))
            continue

        event_panel = _read_csv(event_path)
        window_panel = _read_csv(window_path)
        primary_window = _primary_window(complete_row, window_panel)
        primary = _primary_event_panel(
            candidate_name=candidate_name,
            complete_row=complete_row,
            event_panel=event_panel,
            window_panel=window_panel,
            primary_window=primary_window,
        )
        primary_frames[candidate_name] = primary
        horizon_frames.append(_horizon_decay(candidate_name, complete_row, window_panel))
        concentration_rows.extend(_concentration_rows(candidate_name, complete_row, primary))
        bootstrap_rows.append(_bootstrap_row(candidate_name, complete_row, primary, bootstrap_trials, random_seed))
        robustness_rows.append(
            _robustness_row(
                candidate_name=candidate_name,
                complete_row=complete_row,
                primary=primary,
                minimum_event_count=minimum_event_count,
                minimum_event_month_count=minimum_event_month_count,
            )
        )

    robustness_matrix = pd.DataFrame(robustness_rows, columns=_robustness_columns())
    horizon_decay = _concat(horizon_frames, _horizon_columns())
    concentration_matrix = pd.DataFrame(concentration_rows, columns=_concentration_columns())
    overlap_matrix = _overlap_matrix(primary_frames)
    bootstrap_matrix = pd.DataFrame(bootstrap_rows, columns=_bootstrap_columns())
    summary = _summary(robustness_matrix, overlap_matrix, q2_complete_path)
    manifest = _manifest(summary, artifacts, q2_complete_path, q1_event_panels, q1_window_panels)

    robustness_matrix.to_csv(artifacts["robustness_matrix"], index=False)
    horizon_decay.to_csv(artifacts["horizon_decay_matrix"], index=False)
    concentration_matrix.to_csv(artifacts["concentration_matrix"], index=False)
    overlap_matrix.to_csv(artifacts["overlap_matrix"], index=False)
    bootstrap_matrix.to_csv(artifacts["bootstrap_matrix"], index=False)
    artifacts["summary"].write_text(canonical_json(summary) + "\n", encoding="utf-8")
    artifacts["manifest"].write_text(canonical_json(manifest) + "\n", encoding="utf-8")
    artifacts["report"].write_text(_report(summary, robustness_matrix, overlap_matrix), encoding="utf-8")
    return SmallEmotionQ2RobustnessAuditResult(summary=summary, artifacts=artifacts)


def _primary_event_panel(
    *,
    candidate_name: str,
    complete_row: dict[str, object],
    event_panel: pd.DataFrame,
    window_panel: pd.DataFrame,
    primary_window: str,
) -> pd.DataFrame:
    events = event_panel.copy()
    windows = window_panel.copy()
    if "signal_state" in events.columns:
        events = events[events["signal_state"].astype(str).str.lower().eq("active")].copy()
    if "label_status" in windows.columns:
        windows = windows[windows["label_status"].astype(str).eq("observed")].copy()
    if primary_window and "window" in windows.columns:
        windows = windows[windows["window"].astype(str).eq(primary_window)].copy()
    if events.empty or windows.empty:
        return pd.DataFrame(columns=_primary_columns())

    for frame in [events, windows]:
        frame["event_id"] = frame["event_id"].astype(str)
        frame["ticker"] = frame["ticker"].astype(str).str.upper()
    merged = events.merge(
        windows.loc[
            :,
            [
                "event_id",
                "ticker",
                "date",
                "event_month",
                "window",
                "asset_return",
                "benchmark_return",
                "abnormal_return",
                "directional_return",
            ],
        ],
        on="event_id",
        how="inner",
        suffixes=("", "_label"),
    )
    if "ticker_label" in merged.columns:
        merged = merged[merged["ticker"].eq(merged["ticker_label"])].copy()
    result = pd.DataFrame(
        {
            "candidate_name": candidate_name,
            "measurement_spec_id": str(complete_row.get("measurement_spec_id", "")),
            "measurement_spec_hash": str(complete_row.get("measurement_spec_hash", "")),
            "event_id": merged["event_id"].astype(str),
            "ticker": merged["ticker"].astype(str),
            "date": pd.to_datetime(merged["date"]).dt.strftime("%Y-%m-%d"),
            "event_month": merged["event_month"].astype(str),
            "primary_window": merged["window"].astype(str),
            "directional_return": _numeric(merged, "directional_return"),
            "abs_directional_return": _numeric(merged, "directional_return").abs(),
            "sector": merged.get("sector", pd.Series(["unknown"] * len(merged))).astype(str),
            "industry": merged.get("industry", pd.Series(["unknown"] * len(merged))).astype(str),
            "market_cap_bucket": merged.get("market_cap_bucket", pd.Series(["unknown"] * len(merged))).astype(str),
            "liquidity_bucket": merged.get("liquidity_bucket", pd.Series(["unknown"] * len(merged))).astype(str),
            "spread_bucket": merged.get("spread_bucket", pd.Series(["unknown"] * len(merged))).astype(str),
            "no_view_not_zero_alpha": True,
        }
    )
    return result.sort_values(["candidate_name", "date", "event_id"]).reset_index(drop=True).loc[:, _primary_columns()]


def _robustness_row(
    *,
    candidate_name: str,
    complete_row: dict[str, object],
    primary: pd.DataFrame,
    minimum_event_count: int,
    minimum_event_month_count: int,
) -> dict[str, object]:
    values = pd.to_numeric(primary.get("directional_return", pd.Series(dtype=float)), errors="coerce").dropna()
    event_count = int(len(values))
    event_month_count = int(primary["event_month"].nunique()) if not primary.empty and "event_month" in primary else 0
    monthly = primary.groupby("event_month", observed=False)["directional_return"].mean() if not primary.empty else pd.Series(dtype=float)
    positive_month_share = float((monthly > 0.0).mean()) if not monthly.empty else math.nan
    top_1_share = _top_abs_share(values, 1)
    top_5_share = _top_abs_share(values, 5)
    largest_issuer_share = _largest_group_share(primary, "ticker")
    largest_month_share = _largest_group_share(primary, "event_month")
    largest_sector_share = _largest_group_share(primary, "sector")
    t_stat = _t_stat(values)
    decision = _decision(
        event_count=event_count,
        event_month_count=event_month_count,
        t_stat=t_stat,
        hit_rate=_hit_rate(values),
        positive_month_share=positive_month_share,
        top_5_share=top_5_share,
        largest_issuer_share=largest_issuer_share,
        largest_month_share=largest_month_share,
        minimum_event_count=minimum_event_count,
        minimum_event_month_count=minimum_event_month_count,
    )
    return {
        "schema_version": "small_emotion_q2_robustness_matrix.v1",
        "stage": STAGE,
        "candidate_name": candidate_name,
        "measurement_spec_id": str(complete_row.get("measurement_spec_id", "")),
        "measurement_spec_hash": str(complete_row.get("measurement_spec_hash", "")),
        "primary_window": _first_value(primary, "primary_window"),
        "event_count": event_count,
        "event_month_count": event_month_count,
        "mean_directional_return": _mean(values),
        "median_directional_return": _median(values),
        "t_stat": t_stat,
        "hit_rate": _hit_rate(values),
        "positive_month_share": positive_month_share,
        "top_1_abs_return_share": top_1_share,
        "top_5_abs_return_share": top_5_share,
        "largest_issuer_event_share": largest_issuer_share,
        "largest_month_event_share": largest_month_share,
        "largest_sector_event_share": largest_sector_share,
        "audit_decision": decision,
        "drawdown_ignored_by_user_request": True,
        "orders_written": False,
        "portfolio_construction_artifact_written": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
        "no_view_not_zero_alpha": True,
    }


def _horizon_decay(candidate_name: str, complete_row: dict[str, object], window_panel: pd.DataFrame) -> pd.DataFrame:
    if window_panel.empty:
        return pd.DataFrame(columns=_horizon_columns())
    rows = window_panel.copy()
    rows = rows[rows.get("label_status", "observed").astype(str).eq("observed")].copy()
    rows["directional_return"] = _numeric(rows, "directional_return")
    grouped = (
        rows.groupby("window", observed=False)
        .agg(
            event_count=("event_id", "size"),
            event_month_count=("event_month", "nunique"),
            mean_directional_return=("directional_return", "mean"),
            median_directional_return=("directional_return", "median"),
            hit_rate=("directional_return", lambda values: _hit_rate(pd.Series(values))),
        )
        .reset_index()
        .sort_values("window")
    )
    grouped.insert(0, "schema_version", "small_emotion_q2_horizon_decay_matrix.v1")
    grouped.insert(1, "stage", STAGE)
    grouped.insert(2, "candidate_name", candidate_name)
    grouped.insert(3, "measurement_spec_id", str(complete_row.get("measurement_spec_id", "")))
    grouped.insert(4, "measurement_spec_hash", str(complete_row.get("measurement_spec_hash", "")))
    grouped["no_view_not_zero_alpha"] = True
    return grouped.loc[:, _horizon_columns()]


def _concentration_rows(candidate_name: str, complete_row: dict[str, object], primary: pd.DataFrame) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for column in ["ticker", "event_month", "sector", "industry", "market_cap_bucket", "liquidity_bucket", "spread_bucket"]:
        if primary.empty or column not in primary.columns:
            continue
        grouped = (
            primary.groupby(column, observed=False)
            .agg(event_count=("event_id", "size"), abs_return=("abs_directional_return", "sum"))
            .reset_index()
            .sort_values(["event_count", "abs_return"], ascending=False)
        )
        total_count = float(grouped["event_count"].sum())
        total_abs = float(grouped["abs_return"].sum())
        top = grouped.iloc[0]
        rows.append(
            {
                "schema_version": "small_emotion_q2_concentration_matrix.v1",
                "stage": STAGE,
                "candidate_name": candidate_name,
                "measurement_spec_id": str(complete_row.get("measurement_spec_id", "")),
                "measurement_spec_hash": str(complete_row.get("measurement_spec_hash", "")),
                "concentration_type": column,
                "top_bucket": str(top[column]),
                "top_bucket_event_count": int(top["event_count"]),
                "top_bucket_share": float(top["event_count"] / total_count) if total_count > 0 else math.nan,
                "top_bucket_abs_return_share": float(top["abs_return"] / total_abs) if total_abs > 0 else math.nan,
                "bucket_count": int(len(grouped)),
                "no_view_not_zero_alpha": True,
            }
        )
    return rows


def _bootstrap_row(
    candidate_name: str,
    complete_row: dict[str, object],
    primary: pd.DataFrame,
    bootstrap_trials: int,
    random_seed: int,
) -> dict[str, object]:
    values = pd.to_numeric(primary.get("directional_return", pd.Series(dtype=float)), errors="coerce").dropna().to_numpy()
    if len(values) == 0 or bootstrap_trials <= 0:
        means = np.array([], dtype=float)
    else:
        rng = np.random.default_rng(random_seed + abs(hash(candidate_name)) % 1_000_000)
        draws = rng.choice(values, size=(bootstrap_trials, len(values)), replace=True)
        means = draws.mean(axis=1)
    return {
        "schema_version": "small_emotion_q2_bootstrap_matrix.v1",
        "stage": STAGE,
        "candidate_name": candidate_name,
        "measurement_spec_id": str(complete_row.get("measurement_spec_id", "")),
        "measurement_spec_hash": str(complete_row.get("measurement_spec_hash", "")),
        "event_count": int(len(values)),
        "bootstrap_trials": int(bootstrap_trials),
        "bootstrap_mean": float(means.mean()) if len(means) else math.nan,
        "ci_05": float(np.quantile(means, 0.05)) if len(means) else math.nan,
        "ci_95": float(np.quantile(means, 0.95)) if len(means) else math.nan,
        "positive_bootstrap_share": float((means > 0.0).mean()) if len(means) else math.nan,
        "random_seed": int(random_seed),
        "no_view_not_zero_alpha": True,
    }


def _overlap_matrix(primary_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    names = sorted(primary_frames)
    for left_idx, left_name in enumerate(names):
        for right_name in names[left_idx + 1 :]:
            left = primary_frames[left_name]
            right = primary_frames[right_name]
            left_events = set(left.get("event_id", pd.Series(dtype=str)).astype(str).tolist())
            right_events = set(right.get("event_id", pd.Series(dtype=str)).astype(str).tolist())
            left_ticker_dates = set((row["ticker"], row["date"]) for row in left.loc[:, ["ticker", "date"]].to_dict("records")) if not left.empty else set()
            right_ticker_dates = set((row["ticker"], row["date"]) for row in right.loc[:, ["ticker", "date"]].to_dict("records")) if not right.empty else set()
            event_jaccard = _jaccard(left_events, right_events)
            ticker_date_jaccard = _jaccard(left_ticker_dates, right_ticker_dates)
            rows.append(
                {
                    "schema_version": "small_emotion_q2_overlap_matrix.v1",
                    "stage": STAGE,
                    "left_candidate": left_name,
                    "right_candidate": right_name,
                    "left_event_count": len(left_events),
                    "right_event_count": len(right_events),
                    "event_overlap_count": len(left_events & right_events),
                    "event_jaccard": event_jaccard,
                    "ticker_date_overlap_count": len(left_ticker_dates & right_ticker_dates),
                    "ticker_date_jaccard": ticker_date_jaccard,
                    "redundancy_flag": event_jaccard >= 0.80 or ticker_date_jaccard >= 0.80,
                    "no_view_not_zero_alpha": True,
                }
            )
    return pd.DataFrame(rows, columns=_overlap_columns())


def _decision(
    *,
    event_count: int,
    event_month_count: int,
    t_stat: float,
    hit_rate: float,
    positive_month_share: float,
    top_5_share: float,
    largest_issuer_share: float,
    largest_month_share: float,
    minimum_event_count: int,
    minimum_event_month_count: int,
) -> str:
    if event_count < minimum_event_count or event_month_count < minimum_event_month_count:
        return "hold_insufficient_sample"
    if not _is_number(t_stat) or t_stat <= 2.0 or not _is_number(hit_rate) or hit_rate <= 0.55:
        return "robustness_profile_weak"
    if _is_number(positive_month_share) and positive_month_share < 0.55:
        return "temporal_breadth_failed"
    if (_is_number(top_5_share) and top_5_share > 0.50) or (_is_number(largest_issuer_share) and largest_issuer_share > 0.25):
        return "tail_or_issuer_concentration_failed"
    if _is_number(largest_month_share) and largest_month_share > 0.25:
        return "month_concentration_failed"
    return PASS_DECISION


def _blocked_row(candidate_name: str, complete_row: dict[str, object], decision: str) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_q2_robustness_matrix.v1",
        "stage": STAGE,
        "candidate_name": candidate_name,
        "measurement_spec_id": str(complete_row.get("measurement_spec_id", "")),
        "measurement_spec_hash": str(complete_row.get("measurement_spec_hash", "")),
        "primary_window": "",
        "event_count": 0,
        "event_month_count": 0,
        "mean_directional_return": math.nan,
        "median_directional_return": math.nan,
        "t_stat": math.nan,
        "hit_rate": math.nan,
        "positive_month_share": math.nan,
        "top_1_abs_return_share": math.nan,
        "top_5_abs_return_share": math.nan,
        "largest_issuer_event_share": math.nan,
        "largest_month_event_share": math.nan,
        "largest_sector_event_share": math.nan,
        "audit_decision": decision,
        "drawdown_ignored_by_user_request": True,
        "orders_written": False,
        "portfolio_construction_artifact_written": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
        "no_view_not_zero_alpha": True,
    }


def _summary(robustness_matrix: pd.DataFrame, overlap_matrix: pd.DataFrame, q2_complete_dir: Path) -> dict[str, object]:
    decisions = robustness_matrix["audit_decision"].astype(str) if "audit_decision" in robustness_matrix else pd.Series(dtype=str)
    redundancy_count = int(overlap_matrix["redundancy_flag"].astype(bool).sum()) if "redundancy_flag" in overlap_matrix else 0
    return {
        "schema_version": "small_emotion_q2_robustness_audit_summary.v1",
        "stage": STAGE,
        "candidate_count": int(len(robustness_matrix)),
        "robustness_passed_count": int(decisions.eq(PASS_DECISION).sum()),
        "robustness_blocked_or_failed_count": int((decisions != PASS_DECISION).sum()),
        "redundant_pair_count": redundancy_count,
        "q2_complete_dir": str(q2_complete_dir),
        "drawdown_ignored_by_user_request": True,
        "orders_written": False,
        "portfolio_construction_artifact_written": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
        "no_view_not_zero_alpha": True,
    }


def _manifest(
    summary: dict[str, object],
    artifacts: dict[str, Path],
    q2_complete_dir: Path,
    q1_event_panels: Mapping[str, str | Path],
    q1_window_panels: Mapping[str, str | Path],
) -> dict[str, object]:
    payload = {
        "schema_version": "small_emotion_q2_robustness_audit_manifest.v1",
        "stage": STAGE,
        "summary": summary,
        "input_artifact_hashes": {
            "q2_complete_matrix": _hash_if_exists(q2_complete_dir / "small_emotion_q2_complete_matrix.csv"),
            "q1_event_panels": {name: _hash_if_exists(Path(path)) for name, path in sorted(q1_event_panels.items())},
            "q1_window_panels": {name: _hash_if_exists(Path(path)) for name, path in sorted(q1_window_panels.items())},
        },
        "output_artifacts": {key: str(path) for key, path in artifacts.items()},
        "orders_written": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _report(summary: dict[str, object], robustness: pd.DataFrame, overlap: pd.DataFrame) -> str:
    lines = [
        "# Q2-SMALL-EMOTION-07 Robustness Profile Audit",
        "",
        "This is a Q2 robustness profile audit only. It checks concentration, temporal breadth, horizon decay, candidate overlap, and bootstrap stability for already Q2-completed candidates. It does not modify frozen formulas, write orders, update Alpha Registry, open paper/live/broker/order workflows, or claim production approval.",
        "",
        "## Boundary",
        "",
        "- Q2 robustness profile audit only",
        "- drawdown: observed elsewhere and ignored in this gate by explicit user request",
        "- orders: not written",
        "- broker/order/live paths: closed",
        "- production approval: not claimed",
        "",
        "## Summary",
        "",
        f"- candidate_count: `{summary['candidate_count']}`",
        f"- robustness_passed_count: `{summary['robustness_passed_count']}`",
        f"- robustness_blocked_or_failed_count: `{summary['robustness_blocked_or_failed_count']}`",
        f"- redundant_pair_count: `{summary['redundant_pair_count']}`",
        "",
        "| candidate | decision | events | months | mean | t-stat | hit rate | positive month share | top5 abs share |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in robustness.to_dict("records"):
        lines.append(
            "| {candidate} | {decision} | {events} | {months} | {mean} | {tstat} | {hit} | {month} | {top5} |".format(
                candidate=row.get("candidate_name", ""),
                decision=row.get("audit_decision", ""),
                events=row.get("event_count", ""),
                months=row.get("event_month_count", ""),
                mean=_fmt(row.get("mean_directional_return")),
                tstat=_fmt(row.get("t_stat")),
                hit=_fmt(row.get("hit_rate")),
                month=_fmt(row.get("positive_month_share")),
                top5=_fmt(row.get("top_5_abs_return_share")),
            )
        )
    if not overlap.empty:
        lines.extend(["", "## Candidate Overlap", "", "| left | right | event_jaccard | redundancy |", "|---|---|---:|---|"])
        for row in overlap.to_dict("records"):
            lines.append(
                f"| {row.get('left_candidate', '')} | {row.get('right_candidate', '')} | {_fmt(row.get('event_jaccard'))} | {row.get('redundancy_flag', '')} |"
            )
    return "\n".join(lines) + "\n"


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "robustness_matrix": output_path / "small_emotion_q2_robustness_matrix.csv",
        "horizon_decay_matrix": output_path / "small_emotion_q2_horizon_decay_matrix.csv",
        "concentration_matrix": output_path / "small_emotion_q2_concentration_matrix.csv",
        "overlap_matrix": output_path / "small_emotion_q2_overlap_matrix.csv",
        "bootstrap_matrix": output_path / "small_emotion_q2_bootstrap_matrix.csv",
        "summary": output_path / "small_emotion_q2_robustness_audit_summary.json",
        "manifest": output_path / "small_emotion_q2_robustness_audit_manifest.json",
        "report": output_path / "small_emotion_q2_robustness_audit_report.md",
    }


def _candidate_order(
    complete_matrix: pd.DataFrame,
    q1_event_panels: Mapping[str, str | Path],
    q1_window_panels: Mapping[str, str | Path],
) -> list[str]:
    names: list[str] = []
    if not complete_matrix.empty and "candidate_name" in complete_matrix.columns:
        names.extend(str(name) for name in complete_matrix["candidate_name"].dropna().tolist())
    names.extend(str(name) for name in q1_event_panels)
    names.extend(str(name) for name in q1_window_panels)
    ordered: list[str] = []
    seen: set[str] = set()
    for name in names:
        if name and name not in seen:
            ordered.append(name)
            seen.add(name)
    return ordered


def _primary_window(complete_row: dict[str, object], window_panel: pd.DataFrame) -> str:
    spec = str(complete_row.get("measurement_spec_id", ""))
    for window in ["post_1_44", "post_1_22", "post_1_10", "post_1_5", "post_6_22"]:
        if window in spec:
            return window
    if "window" not in window_panel.columns:
        return ""
    post = window_panel["window"].dropna().astype(str)
    post = post[post.str.startswith("post_")]
    return str(post.mode().iloc[0]) if not post.empty else ""


def _first_row(frame: pd.DataFrame, candidate_name: str) -> dict[str, object]:
    if frame.empty or "candidate_name" not in frame.columns:
        return {}
    rows = frame[frame["candidate_name"].astype(str).eq(candidate_name)]
    return rows.iloc[0].to_dict() if not rows.empty else {}


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series([pd.NA] * len(frame), index=frame.index, dtype="Float64")
    return pd.to_numeric(frame[column], errors="coerce").astype("Float64")


def _mean(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    return float(numeric.mean()) if not numeric.empty else math.nan


def _median(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    return float(numeric.median()) if not numeric.empty else math.nan


def _hit_rate(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    return float((numeric > 0.0).mean()) if not numeric.empty else math.nan


def _t_stat(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if len(numeric) < 2:
        return math.nan
    std = float(numeric.std(ddof=1))
    return float(numeric.mean() / (std / math.sqrt(len(numeric)))) if std > 0.0 else math.nan


def _top_abs_share(values: pd.Series, n: int) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna().abs().sort_values(ascending=False)
    total = float(numeric.sum())
    return float(numeric.head(n).sum() / total) if total > 0.0 else math.nan


def _largest_group_share(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return math.nan
    counts = frame.groupby(column, observed=False)["event_id"].size()
    total = float(counts.sum())
    return float(counts.max() / total) if total > 0.0 else math.nan


def _first_value(frame: pd.DataFrame, column: str) -> str:
    if frame.empty or column not in frame.columns:
        return ""
    values = frame[column].dropna().astype(str)
    return "" if values.empty else str(values.iloc[0])


def _jaccard(left: set[object], right: set[object]) -> float:
    union = left | right
    return float(len(left & right) / len(union)) if union else math.nan


def _is_number(value: object) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number)


def _fmt(value: object) -> str:
    return "" if not _is_number(value) else f"{float(value):.6f}"


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _hash_if_exists(path: Path) -> str:
    return sha256_file(path) if path.exists() else "missing"


def _concat(frames: list[pd.DataFrame], columns: list[str]) -> pd.DataFrame:
    return pd.concat(frames, ignore_index=True).reindex(columns=columns) if frames else pd.DataFrame(columns=columns)


def _primary_columns() -> list[str]:
    return [
        "candidate_name",
        "measurement_spec_id",
        "measurement_spec_hash",
        "event_id",
        "ticker",
        "date",
        "event_month",
        "primary_window",
        "directional_return",
        "abs_directional_return",
        "sector",
        "industry",
        "market_cap_bucket",
        "liquidity_bucket",
        "spread_bucket",
        "no_view_not_zero_alpha",
    ]


def _robustness_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "measurement_spec_id",
        "measurement_spec_hash",
        "primary_window",
        "event_count",
        "event_month_count",
        "mean_directional_return",
        "median_directional_return",
        "t_stat",
        "hit_rate",
        "positive_month_share",
        "top_1_abs_return_share",
        "top_5_abs_return_share",
        "largest_issuer_event_share",
        "largest_month_event_share",
        "largest_sector_event_share",
        "audit_decision",
        "drawdown_ignored_by_user_request",
        "orders_written",
        "portfolio_construction_artifact_written",
        "alpha_registry_update_allowed",
        "paper_ready",
        "live_ready",
        "broker_order_path_opened",
        "production_approval_claimed",
        "no_view_not_zero_alpha",
    ]


def _horizon_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "measurement_spec_id",
        "measurement_spec_hash",
        "window",
        "event_count",
        "event_month_count",
        "mean_directional_return",
        "median_directional_return",
        "hit_rate",
        "no_view_not_zero_alpha",
    ]


def _concentration_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "measurement_spec_id",
        "measurement_spec_hash",
        "concentration_type",
        "top_bucket",
        "top_bucket_event_count",
        "top_bucket_share",
        "top_bucket_abs_return_share",
        "bucket_count",
        "no_view_not_zero_alpha",
    ]


def _overlap_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "left_candidate",
        "right_candidate",
        "left_event_count",
        "right_event_count",
        "event_overlap_count",
        "event_jaccard",
        "ticker_date_overlap_count",
        "ticker_date_jaccard",
        "redundancy_flag",
        "no_view_not_zero_alpha",
    ]


def _bootstrap_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "measurement_spec_id",
        "measurement_spec_hash",
        "event_count",
        "bootstrap_trials",
        "bootstrap_mean",
        "ci_05",
        "ci_95",
        "positive_bootstrap_share",
        "random_seed",
        "no_view_not_zero_alpha",
    ]
