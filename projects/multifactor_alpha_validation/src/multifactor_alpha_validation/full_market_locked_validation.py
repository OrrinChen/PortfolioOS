from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from multifactor_alpha_validation.full_market_sweep import (
    _add_template_scores,
    _build_feature_and_label_panel,
    _profile_metrics,
    _read_returns,
    _select_by_date_quantile,
)


@dataclass(frozen=True)
class FullMarketLockedValidationResult:
    locked_candidate_path: str
    by_split_path: str
    placebo_report_path: str
    summary_path: str
    report_path: str
    validation_status: str
    decision_label: str


_BY_SPLIT_COLUMNS = [
    "schema_version",
    "split",
    "start_date",
    "end_date",
    "sample_count",
    "mean_return",
    "t_stat",
    "hit_rate",
    "month_breadth",
    "issuer_breadth",
    "top10_abs_return_concentration",
    "profile_score",
    "not_alpha_evidence",
]
_PLACEBO_COLUMNS = [
    "schema_version",
    "placebo_type",
    "split",
    "sample_count",
    "mean_return",
    "t_stat",
    "hit_rate",
    "month_breadth",
    "issuer_breadth",
    "top10_abs_return_concentration",
    "profile_score",
    "not_alpha_evidence",
]
_FEATURE_BY_LEAF_ID = {
    "momentum_5d": "momentum_5d",
    "momentum_10d": "momentum_10d",
    "momentum_20d": "momentum_20d",
    "momentum_40d": "momentum_40d",
    "reversal_1d": "reversal_1d",
    "reversal_5d": "reversal_5d",
    "reversal_10d": "reversal_10d",
    "reversal_20d": "reversal_20d",
    "low_vol_10d": "low_vol_10d",
    "low_vol_20d": "low_vol_20d",
    "high_vol_10d": "high_vol_10d",
    "up_shock_reversal": "up_shock_reversal_score",
    "down_shock_rebound": "down_shock_rebound_score",
    "up_5d_shock_reversal": "up_5d_shock_reversal_score",
    "down_5d_shock_rebound": "down_5d_shock_rebound_score",
}


def run_full_market_locked_validation(
    returns_panel_path: Path,
    candidate: dict[str, object],
    output_dir: Path,
    random_seed: int = 17,
) -> FullMarketLockedValidationResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    locked_candidate_path = output_dir / "locked_candidate.json"
    by_split_path = output_dir / "locked_validation_by_split.csv"
    placebo_path = output_dir / "locked_validation_placebo_report.csv"
    summary_path = output_dir / "locked_validation_summary.json"
    report_path = output_dir / "locked_validation_report.md"

    locked_candidate = _lock_candidate(candidate)
    raw_returns = _read_returns(returns_panel_path)
    if raw_returns.empty:
        by_split = pd.DataFrame(columns=_BY_SPLIT_COLUMNS)
        placebo = pd.DataFrame(columns=_PLACEBO_COLUMNS)
        summary = _blocked_summary(returns_panel_path, locked_candidate, "missing_or_invalid_returns_panel")
        _write_artifacts(locked_candidate_path, by_split_path, placebo_path, summary_path, report_path, locked_candidate, by_split, placebo, summary)
        return _result(locked_candidate_path, by_split_path, placebo_path, summary_path, report_path, "blocked", "blocked_data_coverage")

    panel = _build_feature_and_label_panel(raw_returns)
    work, _templates = _add_template_scores(panel)
    feature = _candidate_feature(locked_candidate, work)
    label = _candidate_label(locked_candidate)
    if feature is None or label not in work.columns:
        by_split = pd.DataFrame(columns=_BY_SPLIT_COLUMNS)
        placebo = pd.DataFrame(columns=_PLACEBO_COLUMNS)
        summary = _blocked_summary(returns_panel_path, locked_candidate, "invalid_locked_candidate")
        _write_artifacts(locked_candidate_path, by_split_path, placebo_path, summary_path, report_path, locked_candidate, by_split, placebo, summary)
        return _result(locked_candidate_path, by_split_path, placebo_path, summary_path, report_path, "blocked", "blocked_data_coverage")

    split_dates = _chronological_thirds(work["date"])
    selected = _locked_selection(work, feature, label, locked_candidate)
    by_split = _by_split_metrics(selected, split_dates)
    test_selected = _split_frame(selected, split_dates["test"])
    placebo = _test_placebos(work, test_selected, label, split_dates["test"], random_seed=random_seed)
    decision_label = _decision_label(by_split, placebo)
    summary = _summary(
        returns_panel_path=returns_panel_path,
        locked_candidate=locked_candidate,
        by_split=by_split,
        placebo=placebo,
        decision_label=decision_label,
        raw_returns=raw_returns,
        feature=feature,
        label=label,
    )
    _write_artifacts(locked_candidate_path, by_split_path, placebo_path, summary_path, report_path, locked_candidate, by_split, placebo, summary)
    validation_status = "blocked" if decision_label.startswith("blocked") else "evaluated"
    return _result(locked_candidate_path, by_split_path, placebo_path, summary_path, report_path, validation_status, decision_label)


def _lock_candidate(candidate: dict[str, object]) -> dict[str, Any]:
    return {
        "schema_version": "full_market_locked_candidate.v1",
        "candidate_id": str(candidate.get("candidate_id", "")),
        "search_kind": str(candidate.get("search_kind", "leaf")),
        "window": str(candidate.get("window", "post_1_22")),
        "side": str(candidate.get("side", "top")),
        "quantile": float(candidate.get("quantile", 0.8)),
        "feature_id": str(candidate.get("feature_id", "")),
        "formula_modified": False,
        "threshold_modified": False,
        "not_alpha_evidence": True,
    }


def _candidate_feature(candidate: dict[str, Any], work: pd.DataFrame) -> str | None:
    search_kind = str(candidate["search_kind"])
    candidate_id = str(candidate["candidate_id"])
    if search_kind == "template":
        feature = f"template_{candidate_id}"
        return feature if feature in work.columns else None
    feature_id = str(candidate.get("feature_id") or "")
    feature = feature_id if feature_id in work.columns else _FEATURE_BY_LEAF_ID.get(candidate_id, candidate_id)
    return feature if feature in work.columns else None


def _candidate_label(candidate: dict[str, Any]) -> str:
    try:
        window = int(str(candidate["window"]).split("_")[-1])
    except (TypeError, ValueError):
        window = 22
    return f"label_{window}d"


def _chronological_thirds(dates: pd.Series) -> dict[str, pd.Index]:
    unique_dates = pd.Index(pd.Series(pd.to_datetime(dates, errors="coerce")).dropna().drop_duplicates().sort_values())
    chunks = np.array_split(unique_dates, 3)
    names = ["train", "validation", "test"]
    return {name: pd.Index(chunks[index]) for index, name in enumerate(names)}


def _locked_selection(work: pd.DataFrame, feature: str, label: str, candidate: dict[str, Any]) -> pd.DataFrame:
    active = _select_by_date_quantile(work, feature, str(candidate["side"]), float(candidate["quantile"]))
    selected = work.loc[active, ["date", "instrument_id", label]].rename(columns={label: "label"}).copy()
    selected["label"] = pd.to_numeric(selected["label"], errors="coerce")
    return selected.dropna(subset=["date", "instrument_id", "label"]).sort_values(["date", "instrument_id"]).reset_index(drop=True)


def _by_split_metrics(selected: pd.DataFrame, split_dates: dict[str, pd.Index]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for split, dates in split_dates.items():
        frame = _split_frame(selected, dates)
        metrics = _locked_metrics(frame)
        rows.append(
            {
                "schema_version": "full_market_locked_validation_split.v1",
                "split": split,
                "start_date": _date_bound(dates, "min"),
                "end_date": _date_bound(dates, "max"),
                **metrics,
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows, columns=_BY_SPLIT_COLUMNS)


def _split_frame(frame: pd.DataFrame, dates: pd.Index) -> pd.DataFrame:
    if frame.empty or len(dates) == 0:
        return pd.DataFrame(columns=["date", "instrument_id", "label"])
    return frame[frame["date"].isin(dates)].copy()


def _locked_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    metrics = _profile_metrics(frame)
    return {
        "sample_count": int(metrics["sample_count"]),
        "mean_return": float(metrics["mean_return"]),
        "t_stat": float(metrics["t_stat"]),
        "hit_rate": float(metrics["hit_rate"]),
        "month_breadth": int(metrics["month_breadth"]),
        "issuer_breadth": int(metrics["issuer_breadth"]),
        "top10_abs_return_concentration": float(metrics["top10_abs_return_concentration"]),
        "profile_score": _profile_score(metrics),
    }


def _test_placebos(
    work: pd.DataFrame,
    test_selected: pd.DataFrame,
    label: str,
    test_dates: pd.Index,
    *,
    random_seed: int,
) -> pd.DataFrame:
    if test_selected.empty:
        return pd.DataFrame(columns=_PLACEBO_COLUMNS)
    rng = np.random.default_rng(random_seed)
    labels = test_selected["label"].to_numpy(dtype=float)
    test_universe = (
        work.loc[work["date"].isin(test_dates), ["date", "instrument_id", label]]
        .rename(columns={label: "label"})
        .dropna(subset=["date", "instrument_id", "label"])
        .sort_values(["date", "instrument_id"])
        .reset_index(drop=True)
    )
    random_sample = (
        test_universe.sample(n=min(len(test_selected), len(test_universe)), replace=False, random_state=random_seed)
        if not test_universe.empty
        else pd.DataFrame(columns=["date", "instrument_id", "label"])
    )
    rows: list[dict[str, Any]] = []
    placebo_frames = {
        "same_coverage_random": random_sample,
        "selected_label_permutation": test_selected.assign(label=rng.permutation(labels)),
        "shifted_date": test_selected.assign(label=test_selected["label"].shift(5).to_numpy(dtype=float)),
    }
    for placebo_type, frame in placebo_frames.items():
        metrics = _locked_metrics(frame)
        rows.append(
            {
                "schema_version": "full_market_locked_validation_placebo.v1",
                "placebo_type": placebo_type,
                "split": "test",
                **metrics,
                "not_alpha_evidence": True,
            }
        )
    return pd.DataFrame(rows, columns=_PLACEBO_COLUMNS)


def _decision_label(by_split: pd.DataFrame, placebo: pd.DataFrame) -> str:
    validation = _split_row(by_split, "validation")
    test = _split_row(by_split, "test")
    if validation is None or test is None:
        return "hold_insufficient_sample"
    if int(validation["sample_count"]) < 10 or int(test["sample_count"]) < 10:
        return "hold_insufficient_sample"
    if int(validation["month_breadth"]) < 2 or int(test["month_breadth"]) < 2:
        return "hold_insufficient_sample"
    primary_metrics_pass = (
        float(validation["mean_return"]) > 0
        and float(test["mean_return"]) > 0
        and float(validation["t_stat"]) > 1.0
        and float(test["t_stat"]) > 1.0
        and float(validation["hit_rate"]) > 0.52
        and float(test["hit_rate"]) > 0.52
    )
    best_placebo_profile = float(placebo["profile_score"].max()) if not placebo.empty else 0.0
    if primary_metrics_pass and float(test["profile_score"]) >= best_placebo_profile:
        return "locked_validation_passed"
    if primary_metrics_pass and float(test["profile_score"]) < best_placebo_profile:
        return "blocked_placebo_dominance"
    return "locked_validation_failed"


def _profile_score(metrics: dict[str, Any]) -> float:
    concentration_penalty = 1.0 + max(float(metrics["top10_abs_return_concentration"]), 0.0)
    score = (
        float(metrics["mean_return"])
        * max(float(metrics["hit_rate"]), 0.01)
        * max(float(metrics["t_stat"]), 0.0)
        * max(int(metrics["month_breadth"]), 1) ** 0.5
        / concentration_penalty
    )
    return round(float(score), 10)


def _split_row(by_split: pd.DataFrame, split: str) -> pd.Series | None:
    rows = by_split[by_split["split"].eq(split)]
    if rows.empty:
        return None
    return rows.iloc[0]


def _summary(
    *,
    returns_panel_path: Path,
    locked_candidate: dict[str, Any],
    by_split: pd.DataFrame,
    placebo: pd.DataFrame,
    decision_label: str,
    raw_returns: pd.DataFrame,
    feature: str,
    label: str,
) -> dict[str, Any]:
    return {
        "schema_version": "full_market_locked_validation_summary.v1",
        "validation_status": "blocked" if decision_label.startswith("blocked") else "evaluated",
        "decision_label": decision_label,
        "returns_panel_path": str(returns_panel_path),
        "locked_candidate": locked_candidate,
        "feature_column": feature,
        "label_column": label,
        "return_row_count": int(len(raw_returns)),
        "instrument_count": int(raw_returns["instrument_id"].nunique()),
        "split_metrics": _records(by_split),
        "placebo_metrics": _records(placebo),
        "measurement_spec_written": False,
        "d3_charter_allowed": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "or_optimizer_used": False,
        "alpha_registry_update_allowed": False,
        "production_approval": False,
        "expected_return_panel_written": False,
        "not_alpha_evidence": True,
    }


def _blocked_summary(returns_panel_path: Path, locked_candidate: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "schema_version": "full_market_locked_validation_summary.v1",
        "validation_status": "blocked",
        "decision_label": "blocked_data_coverage",
        "unavailable_reason": reason,
        "returns_panel_path": str(returns_panel_path),
        "locked_candidate": locked_candidate,
        "split_metrics": [],
        "placebo_metrics": [],
        "measurement_spec_written": False,
        "d3_charter_allowed": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "or_optimizer_used": False,
        "alpha_registry_update_allowed": False,
        "production_approval": False,
        "expected_return_panel_written": False,
        "not_alpha_evidence": True,
    }


def _write_artifacts(
    locked_candidate_path: Path,
    by_split_path: Path,
    placebo_path: Path,
    summary_path: Path,
    report_path: Path,
    locked_candidate: dict[str, Any],
    by_split: pd.DataFrame,
    placebo: pd.DataFrame,
    summary: dict[str, Any],
) -> None:
    locked_candidate_path.write_text(json.dumps(locked_candidate, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    by_split.to_csv(by_split_path, index=False)
    placebo.to_csv(placebo_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_path.write_text(_render_report(summary), encoding="utf-8")


def _render_report(summary: dict[str, Any]) -> str:
    lines = [
        "# Full-Market Locked Validation",
        "",
        "This locked validation is diagnostic only. It does not write a Measurement Spec and it is not alpha evidence.",
        "",
        "Q2 remains closed. The locked candidate is not routed to optimizer, Alpha Registry, paper/live, broker/order, or production approval paths.",
        "",
        f"Decision label: `{summary['decision_label']}`",
        "",
    ]
    for row in summary.get("split_metrics", []):
        lines.append(
            f"- {row['split']}: sample `{row['sample_count']}`, mean `{row['mean_return']}`, "
            f"t-stat `{row['t_stat']}`, hit rate `{row['hit_rate']}`."
        )
    return "\n".join(lines) + "\n"


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records"))


def _date_bound(dates: pd.Index, bound: str) -> str:
    if len(dates) == 0:
        return ""
    value = dates.min() if bound == "min" else dates.max()
    return pd.Timestamp(value).date().isoformat()


def _result(
    locked_candidate_path: Path,
    by_split_path: Path,
    placebo_path: Path,
    summary_path: Path,
    report_path: Path,
    validation_status: str,
    decision_label: str,
) -> FullMarketLockedValidationResult:
    return FullMarketLockedValidationResult(
        locked_candidate_path=str(locked_candidate_path),
        by_split_path=str(by_split_path),
        placebo_report_path=str(placebo_path),
        summary_path=str(summary_path),
        report_path=str(report_path),
        validation_status=validation_status,
        decision_label=decision_label,
    )
