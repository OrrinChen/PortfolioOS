"""D2-SMALL-EMOTION-01B no-formula shock-direction remap audit."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError


GUARDS: dict[str, object] = {
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
    "not_alpha_evidence": True,
    "no_view_not_zero_alpha": True,
}

MECHANISMS: tuple[dict[str, object], ...] = (
    {
        "mechanism": "up_shock_continuation",
        "source_subset": "fomo_continuation_candidate",
        "direction_sign": 1.0,
        "d3_charter": "up_shock_continuation",
    },
    {
        "mechanism": "up_shock_reversal",
        "source_subset": "fomo_continuation_candidate",
        "direction_sign": -1.0,
        "d3_charter": "up_shock_reversal",
    },
    {
        "mechanism": "down_shock_reversal",
        "source_subset": "panic_overreaction_candidate",
        "direction_sign": 1.0,
        "d3_charter": "down_shock_reversal",
    },
    {
        "mechanism": "down_shock_continuation",
        "source_subset": "panic_overreaction_candidate",
        "direction_sign": -1.0,
        "d3_charter": "down_shock_continuation",
    },
)


@dataclass(frozen=True)
class SmallEmotionDirectionRemapResult:
    """Output from the small-cap emotion shock-direction remap audit."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_emotion_direction_remap_audit(
    *,
    input_dir: str | Path,
    output_dir: str | Path,
    minimum_subset_events: int = 50,
    minimum_event_month_count: int = 12,
    minimum_label_coverage_share: float = 0.70,
) -> SmallEmotionDirectionRemapResult:
    """Run a no-formula remap audit over existing D2 small-emotion artifacts."""

    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    counts = _read_csv(input_path / "subset_counts.csv")
    car = _read_csv(input_path / "car_window_panel.csv")
    placebo = _read_csv(input_path / "placebo_report.csv")
    stale_guard = _read_csv(input_path / "stale_price_guard_report.csv")
    capacity_guard = _read_csv(input_path / "adv_capacity_guard_report.csv")

    missing_inputs = [
        name
        for name, frame in {
            "subset_counts.csv": counts,
            "car_window_panel.csv": car,
            "placebo_report.csv": placebo,
        }.items()
        if frame.empty
    ]
    if missing_inputs:
        grid = _empty_grid()
        placebo_audit = _empty_placebo_audit()
        decision = "blocked_data_coverage"
        allow_d3: list[str] = []
    else:
        grid, placebo_audit = _build_remap_grid(
            counts=counts,
            car=car,
            placebo=placebo,
            capacity_guard=capacity_guard,
            minimum_subset_events=minimum_subset_events,
            minimum_event_month_count=minimum_event_month_count,
            minimum_label_coverage_share=minimum_label_coverage_share,
        )
        decision, allow_d3 = _decision(grid, capacity_guard)

    _write_frame(grid, artifacts["shock_direction_remap_grid"])
    _write_frame(placebo_audit, artifacts["shock_direction_placebo_audit"])

    summary = _summary(
        decision=decision,
        allow_d3=allow_d3,
        input_path=input_path,
        missing_inputs=missing_inputs,
        grid=grid,
        placebo_audit=placebo_audit,
        stale_guard=stale_guard,
        capacity_guard=capacity_guard,
    )
    artifacts["shock_direction_remap_decision"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["shock_direction_remap_report"].write_text(
        _report(summary, grid, placebo_audit),
        encoding="utf-8",
    )
    return SmallEmotionDirectionRemapResult(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "shock_direction_remap_grid": output_path / "shock_direction_remap_grid.csv",
        "shock_direction_placebo_audit": output_path / "shock_direction_placebo_audit.csv",
        "shock_direction_remap_decision": output_path / "shock_direction_remap_decision.json",
        "shock_direction_remap_report": output_path / "shock_direction_remap_report.md",
    }


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()


def _build_remap_grid(
    *,
    counts: pd.DataFrame,
    car: pd.DataFrame,
    placebo: pd.DataFrame,
    capacity_guard: pd.DataFrame,
    minimum_subset_events: int,
    minimum_event_month_count: int,
    minimum_label_coverage_share: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    placebo_rows: list[dict[str, object]] = []
    capacity_fatal = _capacity_guard_fatal(capacity_guard)
    for mechanism in MECHANISMS:
        name = str(mechanism["mechanism"])
        source_subset = str(mechanism["source_subset"])
        sign = float(mechanism["direction_sign"])
        count_row = counts[counts["event_subset"].eq(source_subset)]
        post = _car_value(car, source_subset, "post_1_22") * sign
        pre = _car_value(car, source_subset, "pre_5_1") * sign
        active_events = _count_value(count_row, "active_event_count")
        event_months = _count_value(count_row, "event_month_count")
        label_coverage = _count_float(count_row, "label_coverage_share")
        sample_passed = (
            active_events >= int(minimum_subset_events)
            and event_months >= int(minimum_event_month_count)
            and label_coverage >= float(minimum_label_coverage_share)
        )
        direction_ok = bool(pd.notna(post) and post > 0.0)
        pre_dominates = bool(direction_ok and pd.notna(pre) and pre > post)
        mechanism_placebo = _mechanism_placebo_audit(
            mechanism=name,
            source_subset=source_subset,
            sign=sign,
            live_directional_return=post,
            placebo=placebo,
        )
        placebo_dominates = bool(mechanism_placebo["placebo_dominates_live"].fillna(False).any())
        stale_placebo_dominates = bool(
            mechanism_placebo[
                mechanism_placebo["placebo_name"].eq("stale_price_matched")
            ]["placebo_dominates_live"]
            .fillna(False)
            .any()
        )
        eligible = bool(
            sample_passed
            and direction_ok
            and not pre_dominates
            and not placebo_dominates
            and not capacity_fatal
        )
        rows.append(
            {
                "schema_version": "small_emotion_direction_remap_grid.v1",
                "stage": "D2-SMALL-EMOTION-01B",
                "mechanism": name,
                "source_subset": source_subset,
                "active_event_count": active_events,
                "event_month_count": event_months,
                "label_coverage_share": label_coverage,
                "post_1_22_directional_return": post,
                "pre_5_1_directional_return": pre,
                "sample_contract_passed": sample_passed,
                "direction_matches_preregistered_mechanism": direction_ok,
                "pre_event_dominates_post": pre_dominates,
                "placebo_dominates_live": placebo_dominates,
                "stale_placebo_dominates_live": stale_placebo_dominates,
                "capacity_guard_fatal": capacity_fatal,
                "eligible_for_d3_charter": eligible,
                **GUARDS,
            }
        )
        placebo_rows.extend(mechanism_placebo.to_dict("records"))
    return pd.DataFrame(rows), pd.DataFrame(placebo_rows)


def _mechanism_placebo_audit(
    *,
    mechanism: str,
    source_subset: str,
    sign: float,
    live_directional_return: float,
    placebo: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    subset = placebo[placebo["event_subset"].eq(source_subset)]
    for row in subset.itertuples(index=False):
        value = sign * _float(getattr(row, "placebo_directional_return", np.nan))
        dominates = bool(
            pd.notna(value)
            and pd.notna(live_directional_return)
            and live_directional_return > 0.0
            and value >= live_directional_return
        )
        rows.append(
            {
                "schema_version": "small_emotion_direction_placebo_audit.v1",
                "stage": "D2-SMALL-EMOTION-01B",
                "mechanism": mechanism,
                "source_subset": source_subset,
                "placebo_name": str(getattr(row, "placebo_name", "")),
                "live_directional_return": live_directional_return,
                "transformed_placebo_directional_return": value,
                "placebo_dominates_live": dominates,
                **GUARDS,
            }
        )
    return pd.DataFrame(rows)


def _car_value(car: pd.DataFrame, source_subset: str, window: str) -> float:
    row = car[
        car["event_subset"].eq(source_subset)
        & car["window"].eq(window)
        & car["label_status"].eq("observed")
    ]
    if row.empty:
        return np.nan
    if "mean_directional_return" in row:
        return _float(row["mean_directional_return"].iloc[0])
    return _float(row["mean_abnormal_return"].iloc[0])


def _count_value(row: pd.DataFrame, column: str) -> int:
    if row.empty or column not in row:
        return 0
    return int(pd.to_numeric(row[column], errors="coerce").fillna(0).iloc[0])


def _count_float(row: pd.DataFrame, column: str) -> float:
    if row.empty or column not in row:
        return 0.0
    return float(pd.to_numeric(row[column], errors="coerce").fillna(0.0).iloc[0])


def _capacity_guard_fatal(capacity_guard: pd.DataFrame) -> bool:
    if capacity_guard.empty or "capacity_guard_fatal" not in capacity_guard:
        return False
    value = capacity_guard["capacity_guard_fatal"].iloc[0]
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return bool(value)


def _float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return np.nan


def _decision(grid: pd.DataFrame, capacity_guard: pd.DataFrame) -> tuple[str, list[str]]:
    if grid.empty:
        return "blocked_data_coverage", []
    if _capacity_guard_fatal(capacity_guard):
        return "blocked_cost_liquidity", []
    eligible = grid[grid["eligible_for_d3_charter"].eq(True)].copy()
    if not eligible.empty:
        eligible = eligible.sort_values("post_1_22_directional_return", ascending=False)
        mechanism = str(eligible["mechanism"].iloc[0])
        return f"observable_{mechanism}", [mechanism]
    sampled = grid[grid["sample_contract_passed"].eq(True)]
    if sampled.empty:
        return "hold_insufficient_sample", []
    direction_ok = sampled[sampled["direction_matches_preregistered_mechanism"].eq(True)]
    if not direction_ok.empty and direction_ok["placebo_dominates_live"].fillna(False).any():
        return "blocked_placebo_dominance", []
    if not direction_ok.empty and direction_ok["pre_event_dominates_post"].fillna(False).all():
        return "blocked_pre_event_dominance", []
    return "not_observable", []


def _summary(
    *,
    decision: str,
    allow_d3: list[str],
    input_path: Path,
    missing_inputs: list[str],
    grid: pd.DataFrame,
    placebo_audit: pd.DataFrame,
    stale_guard: pd.DataFrame,
    capacity_guard: pd.DataFrame,
) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_direction_remap_summary.v1",
        "stage": "D2-SMALL-EMOTION-01B",
        "candidate_id": "small_cap_shock_direction_remap_observability",
        "source_input_dir": str(input_path),
        "source_d2_modified": False,
        "missing_inputs": missing_inputs,
        "overall_decision": decision,
        "allow_d3_charter_for": allow_d3[:1],
        "mechanism_count": int(len(grid)),
        "placebo_audit_row_count": int(len(placebo_audit)),
        "global_stale_guard_reported": bool(
            not stale_guard.empty
            and "stale_placebo_dominates_live" in stale_guard
            and _boolish(stale_guard["stale_placebo_dominates_live"].iloc[0])
        ),
        "capacity_guard_fatal": _capacity_guard_fatal(capacity_guard),
        **GUARDS,
    }


def _boolish(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return bool(value)


def _report(summary: Mapping[str, object], grid: pd.DataFrame, placebo_audit: pd.DataFrame) -> str:
    lines = [
        "# D2-SMALL-EMOTION-01B Shock Direction Remap Audit",
        "",
        "This is a no-formula remap audit only.",
        "It is not alpha evidence and it does not write a MeasurementSpec, expected-return panel, Q1, Q2, optimizer, portfolio, Alpha Registry, paper, broker, order, or production artifact.",
        "",
        f"- decision: {summary['overall_decision']}",
        f"- allowed D3 charter: {summary['allow_d3_charter_for']}",
        f"- source D2 modified: {str(summary['source_d2_modified']).lower()}",
        f"- global stale guard reported: {str(summary['global_stale_guard_reported']).lower()}",
        "",
        "## Mechanism Grid",
    ]
    if grid.empty:
        lines.append("- no rows")
    else:
        for row in grid.itertuples(index=False):
            lines.append(
                "- "
                f"{row.mechanism}: post_1_22={row.post_1_22_directional_return:.6f}, "
                f"sample={str(row.sample_contract_passed).lower()}, "
                f"direction={str(row.direction_matches_preregistered_mechanism).lower()}, "
                f"pre_dominates={str(row.pre_event_dominates_post).lower()}, "
                f"placebo_dominates={str(row.placebo_dominates_live).lower()}, "
                f"eligible={str(row.eligible_for_d3_charter).lower()}"
            )
    lines.extend(
        [
            "",
            "## Placebo Rows",
            f"- rows: {len(placebo_audit)}",
            "",
        ]
    )
    return "\n".join(lines)


def _write_frame(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False)


def _empty_grid() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "schema_version",
            "stage",
            "mechanism",
            "source_subset",
            "active_event_count",
            "event_month_count",
            "label_coverage_share",
            "post_1_22_directional_return",
            "pre_5_1_directional_return",
            "sample_contract_passed",
            "direction_matches_preregistered_mechanism",
            "pre_event_dominates_post",
            "placebo_dominates_live",
            "stale_placebo_dominates_live",
            "capacity_guard_fatal",
            "eligible_for_d3_charter",
            *GUARDS.keys(),
        ]
    )


def _empty_placebo_audit() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "schema_version",
            "stage",
            "mechanism",
            "source_subset",
            "placebo_name",
            "live_directional_return",
            "transformed_placebo_directional_return",
            "placebo_dominates_live",
            *GUARDS.keys(),
        ]
    )
