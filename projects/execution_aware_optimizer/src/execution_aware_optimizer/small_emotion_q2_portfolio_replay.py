"""Small-emotion Q2 portfolio quant replay.

This module builds a local event-portfolio replay from candidates that already
completed the small-emotion Q2 execution-survival chain. It is a historical
portfolio quant diagnostic only: no orders, broker paths, live workflow,
Alpha Registry update, or production approval.
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


STAGE = "Q2-SMALL-EMOTION-05"
PASS_DECISION = "portfolio_replay_completed"
Q2_COMPLETE_DECISION = "completed_q2_execution_survival"
DEFAULT_NOTIONAL_USD = 25_000.0
PARTICIPATION_LIMIT = 0.10
SPREAD_LIMIT = 0.20


@dataclass(frozen=True)
class SmallEmotionQ2PortfolioReplayResult:
    """Written portfolio replay artifacts and summary."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_emotion_q2_portfolio_replay(
    *,
    q2_complete_dir: str | Path,
    q2_intake_dir: str | Path,
    q1_window_panels: Mapping[str, str | Path],
    output_dir: str | Path,
    notional_usd: float = DEFAULT_NOTIONAL_USD,
    minimum_event_count: int = 100,
    minimum_event_month_count: int = 24,
) -> SmallEmotionQ2PortfolioReplayResult:
    """Run portfolio quant replay for Q2-completed small-emotion candidates."""

    complete_path = Path(q2_complete_dir)
    intake_path = Path(q2_intake_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    q2_complete = _read_csv(complete_path / "small_emotion_q2_complete_matrix.csv")
    q2_expected = _read_csv(intake_path / "small_emotion_q2_expected_return_panel.csv")
    q2_candidate_matrix = _read_csv(intake_path / "small_emotion_q2_candidate_matrix.csv")

    replay_rows: list[dict[str, object]] = []
    event_frames: list[pd.DataFrame] = []
    monthly_frames: list[pd.DataFrame] = []
    nav_frames: list[pd.DataFrame] = []
    cost_rows: list[dict[str, object]] = []
    policy_rows: list[dict[str, object]] = []

    for candidate_name in _candidate_order(q2_complete, q2_expected):
        complete_row = _first_row(q2_complete, candidate_name)
        candidate_row = _first_row(q2_candidate_matrix, candidate_name)
        primary_window = str(candidate_row.get("primary_window") or complete_row.get("primary_window") or "")
        q1_panel_path = Path(q1_window_panels.get(candidate_name, ""))
        if str(complete_row.get("q2_complete_decision")) != Q2_COMPLETE_DECISION:
            replay_rows.append(_blocked_row(candidate_name, complete_row, "blocked_q2_incomplete"))
            continue
        if not q1_panel_path.exists():
            replay_rows.append(_blocked_row(candidate_name, complete_row, "blocked_missing_q1_window_panel"))
            continue

        q1_panel = _read_csv(q1_panel_path)
        events = _build_event_replay_panel(
            candidate_name=candidate_name,
            primary_window=primary_window,
            expected_panel=_candidate_rows(q2_expected, candidate_name),
            q1_panel=q1_panel,
            notional_usd=notional_usd,
        )
        if events.empty:
            replay_rows.append(_blocked_row(candidate_name, complete_row, "blocked_missing_observed_labels"))
            continue

        monthly = _monthly_returns(events)
        nav = _nav_curve(candidate_name, monthly)
        costs = _cost_attribution(candidate_name, events)
        policies = _policy_rows(
            candidate_name=candidate_name,
            events=events,
            monthly=monthly,
            nav=nav,
            minimum_event_count=minimum_event_count,
            minimum_event_month_count=minimum_event_month_count,
        )
        decision = _decision(events, monthly, policies)

        event_frames.append(events)
        monthly_frames.append(monthly)
        nav_frames.append(nav)
        cost_rows.append(costs)
        policy_rows.extend(policies)
        replay_rows.append(
            {
                "schema_version": "small_emotion_q2_portfolio_replay_matrix.v1",
                "stage": STAGE,
                "candidate_name": candidate_name,
                "measurement_spec_id": str(complete_row.get("measurement_spec_id", "")),
                "measurement_spec_hash": str(complete_row.get("measurement_spec_hash", "")),
                "primary_window": primary_window,
                "event_count": int(len(events)),
                "event_month_count": int(events["event_month"].nunique()),
                "gross_total_return": _total_return(nav, "gross_nav"),
                "net_total_return": _total_return(nav, "net_nav"),
                "net_hit_rate": _hit_rate(events["net_directional_return"]),
                "max_drawdown": _min_or_zero(nav["drawdown"]),
                "total_cost_return": float(events["cost_return"].sum()),
                "max_participation": _max_or_zero(events["participation"]),
                "portfolio_replay_decision": decision,
                "portfolio_quant_replay_run": True,
                "orders_written": False,
                "portfolio_construction_artifact_written": False,
                "alpha_registry_update_allowed": False,
                "paper_ready": False,
                "live_ready": False,
                "broker_order_path_opened": False,
                "production_approval_claimed": False,
                "no_view_not_zero_alpha": True,
            }
        )

    replay_matrix = pd.DataFrame(replay_rows, columns=_replay_matrix_columns())
    event_panel = _concat(event_frames, _event_columns())
    monthly_returns = _concat(monthly_frames, _monthly_columns())
    nav_curve = _concat(nav_frames, _nav_columns())
    cost_attribution = pd.DataFrame(cost_rows, columns=_cost_columns())
    policy_gate = pd.DataFrame(policy_rows, columns=_policy_columns())
    summary = _summary(replay_matrix, complete_path, intake_path)
    manifest = _manifest(summary, artifacts, complete_path, intake_path, q1_window_panels)

    replay_matrix.to_csv(artifacts["replay_matrix"], index=False)
    event_panel.to_csv(artifacts["event_panel"], index=False)
    monthly_returns.to_csv(artifacts["monthly_returns"], index=False)
    nav_curve.to_csv(artifacts["nav_curve"], index=False)
    cost_attribution.to_csv(artifacts["cost_attribution"], index=False)
    policy_gate.to_csv(artifacts["policy_gate"], index=False)
    artifacts["summary"].write_text(canonical_json(summary) + "\n", encoding="utf-8")
    artifacts["manifest"].write_text(canonical_json(manifest) + "\n", encoding="utf-8")
    artifacts["report"].write_text(_report(summary, replay_matrix), encoding="utf-8")

    return SmallEmotionQ2PortfolioReplayResult(summary=summary, artifacts=artifacts)


def _build_event_replay_panel(
    *,
    candidate_name: str,
    primary_window: str,
    expected_panel: pd.DataFrame,
    q1_panel: pd.DataFrame,
    notional_usd: float,
) -> pd.DataFrame:
    expected = expected_panel.copy()
    if expected.empty:
        return pd.DataFrame(columns=_event_columns())
    if "signal_state" in expected.columns:
        expected = expected[expected["signal_state"].astype(str).str.lower().eq("active")].copy()
    expected["event_id"] = expected["event_id"].astype(str)
    expected["symbol"] = expected["symbol"].astype(str).str.upper()
    q1 = q1_panel.copy()
    q1 = q1[q1["label_status"].astype(str).eq("observed")].copy()
    if primary_window:
        q1 = q1[q1["window"].astype(str).eq(primary_window)].copy()
    q1["event_id"] = q1["event_id"].astype(str)
    q1["ticker"] = q1["ticker"].astype(str).str.upper()

    merged = expected.merge(
        q1.loc[:, ["event_id", "ticker", "date", "event_month", "window", "directional_return"]],
        on="event_id",
        how="inner",
        suffixes=("", "_q1"),
    )
    if merged.empty:
        return pd.DataFrame(columns=_event_columns())
    merged = merged[merged["symbol"].eq(merged["ticker"])].copy()
    if merged.empty:
        return pd.DataFrame(columns=_event_columns())

    dollar_volume = _numeric(merged, "dollar_volume")
    adv20 = _numeric(merged, "adv20")
    close = _numeric(merged, "adjusted_close")
    volume = _numeric(merged, "volume")
    fallback_dollar_volume = close * volume
    effective_dollar_volume = dollar_volume.where(dollar_volume.gt(0), adv20)
    effective_dollar_volume = effective_dollar_volume.where(effective_dollar_volume.gt(0), fallback_dollar_volume)
    effective_dollar_volume = effective_dollar_volume.clip(lower=1.0)
    participation = float(notional_usd) / effective_dollar_volume
    spread = _numeric(merged, "bid_ask_spread").fillna(0.0).clip(lower=0.0)
    cost_return = spread + 0.20 * participation
    directional = _numeric(merged, "directional_return")
    result = pd.DataFrame(
        {
            "schema_version": "small_emotion_q2_portfolio_event_panel.v1",
            "stage": STAGE,
            "candidate_name": candidate_name,
            "event_id": merged["event_id"].astype(str),
            "symbol": merged["symbol"].astype(str),
            "date": pd.to_datetime(merged["date"]).dt.strftime("%Y-%m-%d"),
            "event_month": merged["event_month"].astype(str),
            "primary_window": merged["window"].astype(str),
            "directional_return": directional.astype(float),
            "cost_return": cost_return.astype(float),
            "net_directional_return": (directional - cost_return).astype(float),
            "participation": participation.astype(float),
            "bid_ask_spread": spread.astype(float),
            "notional_usd": float(notional_usd),
            "no_view_not_zero_alpha": True,
        }
    )
    return result.sort_values(["candidate_name", "date", "event_id"]).reset_index(drop=True)


def _monthly_returns(events: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        events.groupby(["candidate_name", "event_month"], observed=False)
        .agg(
            event_count=("event_id", "size"),
            gross_event_return=("directional_return", "mean"),
            net_event_return=("net_directional_return", "mean"),
            avg_cost_return=("cost_return", "mean"),
            avg_participation=("participation", "mean"),
            hit_rate=("net_directional_return", lambda values: _hit_rate(values)),
        )
        .reset_index()
        .sort_values(["candidate_name", "event_month"])
    )
    grouped.insert(0, "schema_version", "small_emotion_q2_portfolio_monthly_returns.v1")
    grouped.insert(1, "stage", STAGE)
    return grouped.loc[:, _monthly_columns()]


def _nav_curve(candidate_name: str, monthly: pd.DataFrame) -> pd.DataFrame:
    gross_nav = 1.0
    net_nav = 1.0
    rows: list[dict[str, object]] = []
    peak = 1.0
    for row in monthly.sort_values("event_month").to_dict("records"):
        gross_nav *= 1.0 + float(row["gross_event_return"])
        net_nav *= 1.0 + float(row["net_event_return"])
        peak = max(peak, net_nav)
        rows.append(
            {
                "schema_version": "small_emotion_q2_portfolio_nav_curve.v1",
                "stage": STAGE,
                "candidate_name": candidate_name,
                "event_month": row["event_month"],
                "gross_nav": gross_nav,
                "net_nav": net_nav,
                "drawdown": net_nav / peak - 1.0 if peak > 0.0 else 0.0,
                "event_count": int(row["event_count"]),
            }
        )
    return pd.DataFrame(rows, columns=_nav_columns())


def _cost_attribution(candidate_name: str, events: pd.DataFrame) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_q2_portfolio_cost_attribution.v1",
        "stage": STAGE,
        "candidate_name": candidate_name,
        "event_count": int(len(events)),
        "total_cost_return": float(events["cost_return"].sum()),
        "avg_cost_return": float(events["cost_return"].mean()) if not events.empty else math.nan,
        "max_cost_return": _max_or_zero(events["cost_return"]),
        "max_participation": _max_or_zero(events["participation"]),
        "avg_bid_ask_spread": float(events["bid_ask_spread"].mean()) if not events.empty else math.nan,
        "orders_written": False,
    }


def _policy_rows(
    *,
    candidate_name: str,
    events: pd.DataFrame,
    monthly: pd.DataFrame,
    nav: pd.DataFrame,
    minimum_event_count: int,
    minimum_event_month_count: int,
) -> list[dict[str, object]]:
    max_spread = _max_or_zero(events["bid_ask_spread"])
    max_participation = _max_or_zero(events["participation"])
    max_drawdown = _min_or_zero(nav["drawdown"])
    net_total_return = _total_return(nav, "net_nav")
    cvar_5 = _cvar(monthly["net_event_return"], 0.05)
    checks = [
        ("minimum_event_count", len(events), minimum_event_count, ">="),
        ("minimum_event_month_count", events["event_month"].nunique(), minimum_event_month_count, ">="),
        ("spread_limit", max_spread, SPREAD_LIMIT, "<="),
        ("participation_limit", max_participation, PARTICIPATION_LIMIT, "<="),
        ("net_total_return_positive", net_total_return, 0.0, ">"),
        ("max_drawdown_observed", max_drawdown, math.nan, "observed"),
        ("cvar_5_observed", cvar_5, math.nan, "observed"),
    ]
    return [
        {
            "schema_version": "small_emotion_q2_portfolio_policy_gate.v1",
            "stage": STAGE,
            "candidate_name": candidate_name,
            "policy_name": name,
            "observed_value": float(value) if _is_number(value) else value,
            "limit_value": float(limit) if _is_number(limit) else limit,
            "comparison": comparison,
            "status": _policy_status(value, limit, comparison),
            "portfolio_quant_replay_run": True,
            "no_view_not_zero_alpha": True,
        }
        for name, value, limit, comparison in checks
    ]


def _decision(events: pd.DataFrame, monthly: pd.DataFrame, policies: list[dict[str, object]]) -> str:
    failed = {str(row["policy_name"]) for row in policies if row["status"] == "fail"}
    if "minimum_event_count" in failed or "minimum_event_month_count" in failed:
        return "hold_insufficient_sample"
    if "spread_limit" in failed or "participation_limit" in failed:
        return "blocked_cost_liquidity"
    if "net_total_return_positive" in failed:
        return "portfolio_replay_negative"
    if events.empty or monthly.empty:
        return "blocked_missing_labels"
    return PASS_DECISION


def _blocked_row(candidate_name: str, complete_row: dict[str, object], decision: str) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_q2_portfolio_replay_matrix.v1",
        "stage": STAGE,
        "candidate_name": candidate_name,
        "measurement_spec_id": str(complete_row.get("measurement_spec_id", "")),
        "measurement_spec_hash": str(complete_row.get("measurement_spec_hash", "")),
        "primary_window": "",
        "event_count": 0,
        "event_month_count": 0,
        "gross_total_return": math.nan,
        "net_total_return": math.nan,
        "net_hit_rate": math.nan,
        "max_drawdown": math.nan,
        "total_cost_return": math.nan,
        "max_participation": math.nan,
        "portfolio_replay_decision": decision,
        "portfolio_quant_replay_run": False,
        "orders_written": False,
        "portfolio_construction_artifact_written": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
        "no_view_not_zero_alpha": True,
    }


def _summary(matrix: pd.DataFrame, q2_complete_dir: Path, q2_intake_dir: Path) -> dict[str, object]:
    decisions = matrix["portfolio_replay_decision"].astype(str) if "portfolio_replay_decision" in matrix.columns else pd.Series(dtype=str)
    return {
        "schema_version": "small_emotion_q2_portfolio_replay_summary.v1",
        "stage": STAGE,
        "candidate_count": int(len(matrix)),
        "portfolio_replay_completed_count": int(decisions.eq(PASS_DECISION).sum()),
        "portfolio_replay_blocked_count": int((decisions != PASS_DECISION).sum()),
        "q2_complete_dir": str(q2_complete_dir),
        "q2_intake_dir": str(q2_intake_dir),
        "portfolio_quant_replay_run": bool(decisions.eq(PASS_DECISION).any()),
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
    q2_intake_dir: Path,
    q1_window_panels: Mapping[str, str | Path],
) -> dict[str, object]:
    q1_hashes = {name: _hash_if_exists(Path(path)) for name, path in sorted(q1_window_panels.items())}
    payload = {
        "schema_version": "small_emotion_q2_portfolio_replay_manifest.v1",
        "stage": STAGE,
        "summary": summary,
        "input_artifact_hashes": {
            "q2_complete_matrix": _hash_if_exists(q2_complete_dir / "small_emotion_q2_complete_matrix.csv"),
            "q2_expected_return_panel": _hash_if_exists(q2_intake_dir / "small_emotion_q2_expected_return_panel.csv"),
            "q2_candidate_matrix": _hash_if_exists(q2_intake_dir / "small_emotion_q2_candidate_matrix.csv"),
            "q1_window_panels": q1_hashes,
        },
        "output_artifacts": {key: str(path) for key, path in artifacts.items()},
        "orders_written": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "replay_matrix": output_path / "small_emotion_q2_portfolio_replay_matrix.csv",
        "event_panel": output_path / "small_emotion_q2_portfolio_event_panel.csv",
        "monthly_returns": output_path / "small_emotion_q2_portfolio_monthly_returns.csv",
        "nav_curve": output_path / "small_emotion_q2_portfolio_nav_curve.csv",
        "cost_attribution": output_path / "small_emotion_q2_portfolio_cost_attribution.csv",
        "policy_gate": output_path / "small_emotion_q2_portfolio_policy_gate.csv",
        "summary": output_path / "small_emotion_q2_portfolio_replay_summary.json",
        "manifest": output_path / "small_emotion_q2_portfolio_replay_manifest.json",
        "report": output_path / "small_emotion_q2_portfolio_replay_report.md",
    }


def _report(summary: dict[str, object], matrix: pd.DataFrame) -> str:
    lines = [
        "# Q2-SMALL-EMOTION-05 Portfolio Quant Replay",
        "",
        "This is a Q2 portfolio quant replay only. It uses completed Q2 candidates, observed Q1 primary-window event returns, and Q2 cost/capacity fields to build event-portfolio diagnostics. It does not write orders, update Alpha Registry, open paper/live/broker/order workflows, or claim production approval.",
        "",
        "## Boundary",
        "",
        "- Q2 portfolio quant replay only",
        "- orders: not written",
        "- broker/order/live paths: closed",
        "- production approval: not claimed",
        "",
        "## Summary",
        "",
        f"- candidate_count: `{summary['candidate_count']}`",
        f"- portfolio_replay_completed_count: `{summary['portfolio_replay_completed_count']}`",
        f"- portfolio_replay_blocked_count: `{summary['portfolio_replay_blocked_count']}`",
        "",
        "| candidate | decision | events | months | net total return | max drawdown | hit rate |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for row in matrix.to_dict("records"):
        lines.append(
            "| {candidate} | {decision} | {events} | {months} | {ret} | {dd} | {hit} |".format(
                candidate=row.get("candidate_name", ""),
                decision=row.get("portfolio_replay_decision", ""),
                events=row.get("event_count", ""),
                months=row.get("event_month_count", ""),
                ret=_fmt(row.get("net_total_return")),
                dd=_fmt(row.get("max_drawdown")),
                hit=_fmt(row.get("net_hit_rate")),
            )
        )
    return "\n".join(lines) + "\n"


def _candidate_order(*frames: pd.DataFrame) -> list[str]:
    names: list[str] = []
    for frame in frames:
        if not frame.empty and "candidate_name" in frame.columns:
            names.extend(str(name) for name in frame["candidate_name"].dropna().tolist())
    ordered: list[str] = []
    seen: set[str] = set()
    for name in names:
        if name and name not in seen:
            ordered.append(name)
            seen.add(name)
    return ordered


def _first_row(frame: pd.DataFrame, candidate_name: str) -> dict[str, object]:
    rows = _candidate_rows(frame, candidate_name)
    return rows.iloc[0].to_dict() if not rows.empty else {}


def _candidate_rows(frame: pd.DataFrame, candidate_name: str) -> pd.DataFrame:
    if frame.empty or "candidate_name" not in frame.columns:
        return pd.DataFrame()
    return frame[frame["candidate_name"].astype(str).eq(candidate_name)].copy()


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series([pd.NA] * len(frame), index=frame.index, dtype="Float64")
    return pd.to_numeric(frame[column], errors="coerce").astype("Float64")


def _policy_status(value: object, limit: object, comparison: str) -> str:
    if comparison == "observed":
        return "pass" if _is_number(value) else "unavailable"
    if not _is_number(value) or not _is_number(limit):
        return "unavailable"
    observed = float(value)
    threshold = float(limit)
    if comparison == ">=":
        return "pass" if observed >= threshold else "fail"
    if comparison == ">":
        return "pass" if observed > threshold else "fail"
    if comparison == "<=":
        return "pass" if observed <= threshold else "fail"
    return "unavailable"


def _total_return(nav: pd.DataFrame, column: str) -> float:
    if nav.empty or column not in nav.columns:
        return math.nan
    values = pd.to_numeric(nav[column], errors="coerce").dropna()
    if values.empty:
        return math.nan
    return float(values.iloc[-1] - 1.0)


def _hit_rate(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    return float((numeric > 0).mean()) if not numeric.empty else math.nan


def _cvar(values: pd.Series, alpha: float) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return math.nan
    cutoff = float(numeric.quantile(alpha))
    tail = numeric.loc[numeric <= cutoff]
    return float(tail.mean()) if not tail.empty else math.nan


def _max_or_zero(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    return float(numeric.max()) if not numeric.empty else 0.0


def _min_or_zero(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    return float(numeric.min()) if not numeric.empty else 0.0


def _is_number(value: object) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number)


def _fmt(value: object) -> str:
    return "" if not _is_number(value) else f"{float(value):.6f}"


def _concat(frames: list[pd.DataFrame], columns: list[str]) -> pd.DataFrame:
    return pd.concat(frames, ignore_index=True).reindex(columns=columns) if frames else pd.DataFrame(columns=columns)


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _hash_if_exists(path: Path) -> str:
    return sha256_file(path) if path.exists() else "missing"


def _replay_matrix_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "measurement_spec_id",
        "measurement_spec_hash",
        "primary_window",
        "event_count",
        "event_month_count",
        "gross_total_return",
        "net_total_return",
        "net_hit_rate",
        "max_drawdown",
        "total_cost_return",
        "max_participation",
        "portfolio_replay_decision",
        "portfolio_quant_replay_run",
        "orders_written",
        "portfolio_construction_artifact_written",
        "alpha_registry_update_allowed",
        "paper_ready",
        "live_ready",
        "broker_order_path_opened",
        "production_approval_claimed",
        "no_view_not_zero_alpha",
    ]


def _event_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "event_id",
        "symbol",
        "date",
        "event_month",
        "primary_window",
        "directional_return",
        "cost_return",
        "net_directional_return",
        "participation",
        "bid_ask_spread",
        "notional_usd",
        "no_view_not_zero_alpha",
    ]


def _monthly_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "event_month",
        "event_count",
        "gross_event_return",
        "net_event_return",
        "avg_cost_return",
        "avg_participation",
        "hit_rate",
    ]


def _nav_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "event_month",
        "gross_nav",
        "net_nav",
        "drawdown",
        "event_count",
    ]


def _cost_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "event_count",
        "total_cost_return",
        "avg_cost_return",
        "max_cost_return",
        "max_participation",
        "avg_bid_ask_spread",
        "orders_written",
    ]


def _policy_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "policy_name",
        "observed_value",
        "limit_value",
        "comparison",
        "status",
        "portfolio_quant_replay_run",
        "no_view_not_zero_alpha",
    ]
