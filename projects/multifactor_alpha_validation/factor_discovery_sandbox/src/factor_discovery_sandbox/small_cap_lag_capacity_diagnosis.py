"""FD-S4.1 small-cap lag / capacity dominance diagnosis runner."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError

from .small_cap_capacity_diagnostics import (
    FIXED_WEIGHTING_SCHEMES,
    build_capacity_bucket_diagnostics,
    build_cost_drag_decomposition,
    build_holding_period_sensitivity,
    build_weighting_scheme_comparison,
)
from .small_cap_data_admission import GUARDS
from .small_cap_temporal_diagnostics import (
    build_lag_construction_audit,
    build_signal_decay_grid,
    build_signal_variants,
    build_temporal_update_component_diagnostics,
)


@dataclass(frozen=True)
class FDSmallCapLagCapacityDiagnosisResult:
    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_cap_lag_capacity_diagnosis(
    family_output_dir: str | Path,
    report_path: str | Path,
) -> FDSmallCapLagCapacityDiagnosisResult:
    """Run FD-S4.1 diagnostics from cached small-cap family panels."""

    output_dir = Path(family_output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_file = Path(report_path)
    report_file.parent.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_dir, report_file)

    signal_panel = _read_csv(output_dir / "monthly_signal_panel_cache.csv")
    target = _read_csv(output_dir / "forward_target_panel_cache.csv")
    signal_variants = build_signal_variants(signal_panel)
    lag_audit = build_lag_construction_audit(signal_variants)
    lag_decay = build_signal_decay_grid(signal_variants, target)
    update_component = build_temporal_update_component_diagnostics(signal_variants, target)
    holding = build_holding_period_sensitivity(signal_panel, target)
    capacity = build_capacity_bucket_diagnostics(signal_panel, target)
    weighting = build_weighting_scheme_comparison(signal_panel, target)
    cost = build_cost_drag_decomposition(signal_panel, target)
    decision = _decision(lag_decay, holding, capacity, weighting, cost, update_component)

    lag_decay.to_csv(artifacts["lag_decay_grid"], index=False)
    holding.to_csv(artifacts["holding_period_sensitivity"], index=False)
    capacity.to_csv(artifacts["capacity_bucket_diagnostics"], index=False)
    weighting.to_csv(artifacts["weighting_scheme_comparison"], index=False)
    cost.to_csv(artifacts["cost_drag_decomposition"], index=False)
    lag_audit.to_csv(artifacts["lag_construction_audit"], index=False)
    update_component.to_csv(artifacts["temporal_update_component_diagnostics"], index=False)
    artifacts["small_cap_dominance_decision"].write_text(
        json.dumps(decision, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["dominance_report"].write_text(
        _render_report(decision, lag_decay, holding, capacity, weighting, cost),
        encoding="utf-8",
    )

    summary = {
        "schema_version": "fd_small_cap_lag_capacity_diagnosis_summary.v1",
        "stage": "FD-S4.1",
        "decision_label": decision["decision_label"],
        "fixed_weighting_scheme_count": len(FIXED_WEIGHTING_SCHEMES),
        **GUARDS,
    }
    return FDSmallCapLagCapacityDiagnosisResult(summary=summary, artifacts=artifacts)


def _decision(
    lag_decay: pd.DataFrame,
    holding: pd.DataFrame,
    capacity: pd.DataFrame,
    weighting: pd.DataFrame,
    cost: pd.DataFrame,
    update_component: pd.DataFrame,
) -> dict[str, object]:
    slow_pass = _slow_signal_pass(lag_decay, holding)
    capacity_pass = _capacity_pass(capacity, weighting)
    cost_blocked = _cost_blocked(cost, weighting)
    temporal_noise = _temporal_noise(lag_decay, update_component)
    if cost_blocked:
        label = "diagnostic_only_cost_blocked"
    elif slow_pass and capacity_pass:
        label = "revise_to_slow_capacity_filtered_candidate"
    elif slow_pass:
        label = "revise_to_slow_signal_candidate"
    elif capacity_pass:
        label = "revise_to_capacity_filtered_candidate"
    elif temporal_noise:
        label = "reject_temporal_noise"
    else:
        label = "close_family"
    return {
        "schema_version": "fd_small_cap_dominance_decision.v1",
        "stage": "FD-S4.1",
        "decision_label": label,
        "slow_signal_condition_passed": slow_pass,
        "capacity_filter_condition_passed": capacity_pass,
        "temporal_noise_detected": temporal_noise,
        "cost_blocked": cost_blocked,
        "fixed_weighting_schemes_only": True,
        "learned_weighting_used": False,
        "rolling_icir_used": False,
        "ridge_weighting_used": False,
        "recommended_next_action": "diagnostic_only_do_not_promote",
        **GUARDS,
    }


def _slow_signal_pass(lag_decay: pd.DataFrame, holding: pd.DataFrame) -> bool:
    if lag_decay.empty or holding.empty:
        return False
    slow = lag_decay[
        lag_decay["signal_variant"].isin(["lag_1m_signal", "rolling_3m_mean_signal", "rolling_3m_median_signal"])
    ]
    slow_spread = pd.to_numeric(slow["spread_3m"], errors="coerce").mean()
    live_spread = pd.to_numeric(
        lag_decay.loc[lag_decay["signal_variant"] == "live_signal", "spread_3m"], errors="coerce"
    ).mean()
    monthly = holding[holding["rebalance_frequency"] == "monthly"]
    hold_3 = pd.to_numeric(monthly.loc[monthly["holding_period_months"] == 3, "net_spread"], errors="coerce").mean()
    hold_1 = pd.to_numeric(monthly.loc[monthly["holding_period_months"] == 1, "net_spread"], errors="coerce").mean()
    placebo_ok = not slow["placebo_status"].astype(str).str.contains("failed").any()
    survival_ok = pd.to_numeric(slow["subperiod_survival_rate"], errors="coerce").fillna(0.0).mean() >= 0.5
    return bool(slow_spread > live_spread and hold_3 > hold_1 and placebo_ok and survival_ok)


def _capacity_pass(capacity: pd.DataFrame, weighting: pd.DataFrame) -> bool:
    if capacity.empty or weighting.empty:
        return False
    net = pd.to_numeric(weighting["net_spread"], errors="coerce")
    equal = weighting.loc[weighting["weighting_scheme"] == "equal_weight", "net_spread"]
    cap_schemes = weighting[weighting["weighting_scheme"].str.contains("adv|capacity|value", regex=True)]
    capacity_best = pd.to_numeric(cap_schemes["net_spread"], errors="coerce").max()
    equal_value = pd.to_numeric(equal, errors="coerce").mean()
    return bool(pd.notna(capacity_best) and pd.notna(equal_value) and capacity_best > max(equal_value, 0.0) and net.max() > 0)


def _cost_blocked(cost: pd.DataFrame, weighting: pd.DataFrame) -> bool:
    net_cost = pd.to_numeric(cost.get("net_spread", pd.Series(dtype=float)), errors="coerce").dropna()
    net_weighting = pd.to_numeric(weighting.get("net_spread", pd.Series(dtype=float)), errors="coerce").dropna()
    if net_cost.empty and net_weighting.empty:
        return False
    return bool((net_cost <= 0).all() and (net_weighting <= 0).all())


def _temporal_noise(lag_decay: pd.DataFrame, update_component: pd.DataFrame) -> bool:
    if lag_decay.empty:
        return False
    lag1 = pd.to_numeric(lag_decay.loc[lag_decay["signal_variant"] == "lag_1m_signal", "spread_1m"], errors="coerce").mean()
    lag2 = pd.to_numeric(lag_decay.loc[lag_decay["signal_variant"] == "lag_2m_signal", "spread_1m"], errors="coerce").mean()
    lag3 = pd.to_numeric(lag_decay.loc[lag_decay["signal_variant"] == "lag_3m_signal", "spread_1m"], errors="coerce").mean()
    update_ic = pd.to_numeric(
        update_component.get("update_component_rank_ic_1m", pd.Series(dtype=float)),
        errors="coerce",
    ).mean()
    unstable_lags = pd.notna(lag1) and (pd.isna(lag2) or pd.isna(lag3) or lag2 < lag1 or lag3 < lag2)
    return bool(unstable_lags or (pd.notna(update_ic) and update_ic < 0))


def _render_report(
    decision: Mapping[str, object],
    lag_decay: pd.DataFrame,
    holding: pd.DataFrame,
    capacity: pd.DataFrame,
    weighting: pd.DataFrame,
    cost: pd.DataFrame,
) -> str:
    lines = [
        "# FD-S4.1 Small-Cap Lag / Capacity Dominance Diagnosis",
        "",
        "not alpha evidence",
        "allocator entry: blocked",
        "Q1 entry: blocked",
        "Q2 entry: blocked",
        "Alpha Registry update: blocked",
        "production approval: not claimed",
        "",
        f"- decision: {decision['decision_label']}",
        f"- learned weighting used: {str(decision['learned_weighting_used']).lower()}",
        "",
        "## Signal Decay",
    ]
    for row in lag_decay.itertuples(index=False):
        lines.append(f"- {row.signal_variant}: spread_1m={_fmt(row.spread_1m)}, spread_3m={_fmt(row.spread_3m)}")
    lines.append("")
    lines.append("## Holding / Rebalance")
    for row in holding.itertuples(index=False):
        lines.append(
            f"- {row.rebalance_frequency} {int(row.holding_period_months)}m: "
            f"gross={_fmt(row.gross_spread)}, net={_fmt(row.net_spread)}"
        )
    lines.append("")
    lines.append("## Capacity")
    if capacity.empty:
        lines.append("- capacity bucket diagnostics: unavailable")
    else:
        for row in capacity.itertuples(index=False):
            lines.append(
                f"- {row.bucket_type}/{row.bucket_label}: "
                f"gross={_fmt(row.gross_spread)}, net={_fmt(row.net_spread)}, "
                f"cost_drag={_fmt(row.cost_drag)}, active_count={int(row.active_count)}"
            )
    lines.append("")
    lines.append("## Fixed Weighting")
    if weighting.empty:
        lines.append("- fixed weighting diagnostics: unavailable")
    else:
        for row in weighting.itertuples(index=False):
            lines.append(
                f"- {row.weighting_scheme}: gross={_fmt(row.gross_spread)}, "
                f"net={_fmt(row.net_spread)}, learned_weighting=false"
            )
    if not cost.empty:
        row = cost.iloc[0]
        lines.append("")
        lines.append("## Cost Drag")
        lines.append(f"- gross spread: {_fmt(row.get('gross_spread'))}")
        lines.append(f"- net spread: {_fmt(row.get('net_spread'))}")
    lines.append("")
    return "\n".join(lines)


def _fmt(value: object) -> str:
    return f"{float(value):.6f}" if pd.notna(value) else "unavailable"


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()


def _artifact_paths(output_dir: Path, report_file: Path) -> dict[str, Path]:
    return {
        "lag_decay_grid": output_dir / "lag_decay_grid.csv",
        "holding_period_sensitivity": output_dir / "holding_period_sensitivity.csv",
        "capacity_bucket_diagnostics": output_dir / "capacity_bucket_diagnostics.csv",
        "weighting_scheme_comparison": output_dir / "weighting_scheme_comparison.csv",
        "cost_drag_decomposition": output_dir / "cost_drag_decomposition.csv",
        "lag_construction_audit": output_dir / "lag_construction_audit.csv",
        "temporal_update_component_diagnostics": output_dir / "temporal_update_component_diagnostics.csv",
        "small_cap_dominance_decision": output_dir / "small_cap_dominance_decision.json",
        "dominance_report": report_file,
    }
