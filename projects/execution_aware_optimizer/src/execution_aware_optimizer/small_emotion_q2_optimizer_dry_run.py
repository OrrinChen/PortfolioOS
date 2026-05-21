"""Small-emotion Q2 optimizer adapter dry-run.

This module connects the Q2 execution-survival optimizer input probe to the
local PortfolioOS optimizer input shape and observes constraint responses. It
does not write orders, build a portfolio construction artifact, open broker
paths, or claim production approval.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

from portfolio_os.domain.models import PortfolioState
from portfolio_os.optimizer.solver import solve_rebalance_problem
from portfolio_os.provenance.hashing import canonical_json, hash_payload, sha256_file
from portfolio_os.utils.config import AppConfig, load_app_config


STAGE = "Q2-SMALL-EMOTION-03"
DEFAULT_CONFIG_PATH = Path("config/us_expanded_alpha_phase_1_5.yaml")
DEFAULT_CONSTRAINTS_PATH = Path("config/constraints/us_public_fund.yaml")
DEFAULT_EXECUTION_PATH = Path("config/execution/conservative.yaml")


@dataclass(frozen=True)
class SmallEmotionQ2OptimizerDryRunResult:
    """Written optimizer dry-run artifacts and summary."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_small_emotion_q2_optimizer_dry_run(
    *,
    q2_survival_dir: str | Path,
    output_dir: str | Path,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    constraints_path: str | Path = DEFAULT_CONSTRAINTS_PATH,
    execution_path: str | Path = DEFAULT_EXECUTION_PATH,
    alpha_weight: float = 8.0,
    diagnostic_sleeve_weight: float = 0.30,
) -> SmallEmotionQ2OptimizerDryRunResult:
    """Run local optimizer dry-run for Q2-surviving small-emotion candidates."""

    survival_path = Path(q2_survival_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    probe = _read_csv(survival_path / "small_emotion_q2_optimizer_input_probe.csv")
    survival_matrix = _read_csv(survival_path / "small_emotion_q2_execution_survival_matrix.csv")
    response_rows: list[dict[str, object]] = []
    constraint_rows: list[dict[str, object]] = []
    snapshot_frames: list[pd.DataFrame] = []

    config = _load_optimizer_config(config_path, constraints_path, execution_path, alpha_weight=alpha_weight)
    for candidate_name in _candidate_order(survival_matrix, probe):
        candidate_status = _survival_status(survival_matrix, candidate_name)
        candidate_probe = _candidate_probe(probe, candidate_name)
        if candidate_status != "execution_survival_passed":
            response_rows.append(_skipped_response_row(candidate_name, candidate_status))
            constraint_rows.extend(_skipped_constraint_rows(candidate_name, candidate_status))
            continue
        if candidate_probe.empty:
            response_rows.append(_skipped_response_row(candidate_name, "missing_optimizer_probe_rows"))
            constraint_rows.extend(_skipped_constraint_rows(candidate_name, "missing_optimizer_probe_rows"))
            continue

        base_universe = _build_universe(candidate_probe, config, diagnostic_sleeve_weight=diagnostic_sleeve_weight)
        snapshot_frames.append(_snapshot(candidate_name, "live_panel", base_universe))
        for panel_name, sign in [
            ("live_panel", 1.0),
            ("sign_flipped_panel", -1.0),
            ("zero_alpha_panel", 0.0),
        ]:
            universe = base_universe.copy()
            universe["expected_return"] = universe["expected_return"].astype(float) * sign
            if sign == 0.0:
                universe["expected_return"] = 0.0
                universe["expected_return_source"] = "zero_alpha_control"
            row, constraints = _evaluate_panel(
                candidate_name=candidate_name,
                panel_name=panel_name,
                universe=universe,
                config=config,
            )
            response_rows.append(row)
            constraint_rows.extend(constraints)

    response_matrix = pd.DataFrame(response_rows, columns=_response_columns())
    constraint_response = pd.DataFrame(constraint_rows, columns=_constraint_columns())
    optimizer_input_snapshot = (
        pd.concat(snapshot_frames, ignore_index=True)
        if snapshot_frames
        else pd.DataFrame(columns=_snapshot_columns())
    )
    summary = _summary(response_matrix, q2_survival_dir=survival_path, config_path=Path(config_path))
    manifest = _manifest(summary, artifacts, q2_survival_dir=survival_path, config_path=Path(config_path))

    response_matrix.to_csv(artifacts["optimizer_response_matrix"], index=False)
    constraint_response.to_csv(artifacts["constraint_response"], index=False)
    optimizer_input_snapshot.to_csv(artifacts["optimizer_input_snapshot"], index=False)
    artifacts["summary"].write_text(canonical_json(summary) + "\n", encoding="utf-8")
    artifacts["manifest"].write_text(canonical_json(manifest) + "\n", encoding="utf-8")
    artifacts["report"].write_text(_report(summary, response_matrix), encoding="utf-8")
    return SmallEmotionQ2OptimizerDryRunResult(summary=summary, artifacts=artifacts)


def _load_optimizer_config(
    config_path: str | Path,
    constraints_path: str | Path,
    execution_path: str | Path,
    *,
    alpha_weight: float,
) -> AppConfig:
    portfolio_state = PortfolioState(
        account_id="q2_optimizer_dry_run_local",
        as_of_date="2021-01-29",
        available_cash=500_000.0,
        min_cash_buffer=0.0,
        account_type="diagnostic",
    )
    config = load_app_config(
        default_path=config_path,
        constraints_path=constraints_path,
        execution_path=execution_path,
        portfolio_state=portfolio_state,
    )
    objective = config.objective_weights.model_copy(
        update={
            "alpha_weight": float(alpha_weight),
            "risk_term": 0.0,
            "tracking_error": 0.0,
            "transaction_cost": 1.0,
            "target_deviation": 0.0,
            "transaction_fee": 0.0,
            "turnover_penalty": 0.0,
            "slippage_penalty": 0.0,
        }
    )
    risk_model = config.risk_model.model_copy(update={"enabled": False})
    return config.model_copy(update={"objective_weights": objective, "risk_model": risk_model})


def _build_universe(probe: pd.DataFrame, config: AppConfig, *, diagnostic_sleeve_weight: float) -> pd.DataFrame:
    work = probe.copy()
    work["ticker"] = work["ticker"].astype(str).str.upper()
    work["expected_return"] = pd.to_numeric(work["expected_return"], errors="raise").astype(float)
    work["estimated_price"] = pd.to_numeric(work["close"], errors="raise").astype(float)
    work["adv_shares"] = pd.to_numeric(work["adv_shares"], errors="raise").astype(float)
    if "sector" not in work.columns:
        work["sector"] = "Technology"
    work["sector"] = work["sector"].fillna("Technology").astype(str).replace({"": "Technology"})
    work = (
        work.groupby("ticker", as_index=False)
        .agg(
            expected_return=("expected_return", "mean"),
            estimated_price=("estimated_price", "median"),
            adv_shares=("adv_shares", "median"),
            sector=("sector", "first"),
        )
    )
    n = max(1, len(work))
    pre_trade_nav = 500_000.0
    n = max(1, len(work))
    sleeve_weight = min(float(diagnostic_sleeve_weight), 0.30)
    weight = min(sleeve_weight / n, float(config.effective_single_name_limit) / 2.0)
    work["quantity"] = (pre_trade_nav * weight) / work["estimated_price"]
    work["target_weight"] = 0.0
    work["industry"] = work.get("sector", "Technology")
    work["industry"] = work["industry"].fillna("Technology").astype(str).replace({"": "Technology"})
    work["tradable"] = True
    work["upper_limit_hit"] = False
    work["lower_limit_hit"] = False
    work["blacklist_buy"] = False
    work["blacklist_sell"] = False
    work["current_notional"] = work["quantity"].astype(float) * work["estimated_price"].astype(float)
    work["current_weight"] = work["current_notional"] / pre_trade_nav
    work["manager_aggregate_qty"] = 0.0
    work["issuer_total_shares"] = 0.0
    work["decision_horizon_days"] = 22.0
    work["expected_return_source"] = "small_emotion_q2_optimizer_probe"
    columns = [
        "ticker",
        "quantity",
        "estimated_price",
        "adv_shares",
        "target_weight",
        "current_notional",
        "current_weight",
        "industry",
        "tradable",
        "upper_limit_hit",
        "lower_limit_hit",
        "blacklist_buy",
        "blacklist_sell",
        "manager_aggregate_qty",
        "issuer_total_shares",
        "decision_horizon_days",
        "expected_return",
        "expected_return_source",
    ]
    return work.loc[:, columns].sort_values("ticker").reset_index(drop=True)


def _evaluate_panel(
    *,
    candidate_name: str,
    panel_name: str,
    universe: pd.DataFrame,
    config: AppConfig,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    try:
        result = solve_rebalance_problem(universe, config)
    except Exception as exc:  # noqa: BLE001 - deterministic unavailable row
        return _error_response_row(candidate_name, panel_name, str(exc)), _error_constraint_rows(candidate_name, panel_name, str(exc))

    current = pd.Series(result.current_weights, dtype=float)
    post = pd.Series(result.post_trade_weights, dtype=float).reindex(current.index).fillna(0.0)
    delta = post - current
    expected = universe.set_index("ticker")["expected_return"].astype(float).reindex(current.index).fillna(0.0)
    pre_trade_nav = float(result.pre_trade_nav)
    turnover = float(result.gross_traded_notional / pre_trade_nav) if pre_trade_nav > 0.0 else 0.0
    max_abs_trade_notional = _max_abs_trade_notional(result)
    max_participation = _max_participation(result, universe)
    max_post_weight = float(post.max()) if not post.empty else 0.0
    response = {
        "schema_version": "small_emotion_q2_optimizer_response_matrix.v1",
        "stage": STAGE,
        "candidate_name": candidate_name,
        "panel_name": panel_name,
        "optimizer_dry_run_status": "observed",
        "optimizer_status": str(result.status),
        "objective_value": float(result.objective_value),
        "alpha_reward_share": _alpha_reward_share(result.objective_decomposition),
        "gross_traded_notional": float(result.gross_traded_notional),
        "turnover": turnover,
        "max_turnover_limit": float(config.constraints.max_turnover),
        "max_abs_trade_notional": max_abs_trade_notional,
        "max_participation": max_participation,
        "participation_limit": float(config.constraints.participation_limit),
        "max_post_trade_weight": max_post_weight,
        "single_name_limit": float(config.effective_single_name_limit),
        "constraint_residual_max": float(result.constraint_residual_max or 0.0),
        "net_weight_change": float(delta.sum()),
        "expected_return_weight_change_alignment": _safe_spearman(expected, delta),
        "actual_local_optimizer_run": True,
        "orders_written": False,
        "portfolio_construction_allowed": False,
        "no_view_not_zero_alpha": True,
    }
    constraints = [
        _constraint_row(candidate_name, panel_name, "max_turnover", turnover, config.constraints.max_turnover, "<="),
        _constraint_row(candidate_name, panel_name, "participation_limit", max_participation, config.constraints.participation_limit, "<="),
        _constraint_row(candidate_name, panel_name, "single_name_limit", max_post_weight, config.effective_single_name_limit, "<="),
    ]
    return response, constraints


def _max_abs_trade_notional(result: object) -> float:
    values = [abs(float(instruction.quantity) * float(instruction.estimated_price)) for instruction in result.instructions]
    return max(values) if values else 0.0


def _max_participation(result: object, universe: pd.DataFrame) -> float:
    adv = universe.set_index("ticker")["adv_shares"].astype(float)
    values: list[float] = []
    for instruction in result.instructions:
        adv_value = float(adv.get(str(instruction.ticker), 0.0))
        if adv_value > 0.0:
            values.append(abs(float(instruction.quantity)) / adv_value)
    return max(values) if values else 0.0


def _constraint_row(
    candidate_name: str,
    panel_name: str,
    constraint_name: str,
    observed_value: float,
    limit: float,
    comparison: str,
) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_q2_optimizer_constraint_response.v1",
        "stage": STAGE,
        "candidate_name": candidate_name,
        "panel_name": panel_name,
        "constraint_name": constraint_name,
        "observed_value": observed_value,
        "limit": float(limit),
        "comparison": comparison,
        "status": "pass" if observed_value <= float(limit) + 1e-9 else "fail",
        "actual_local_optimizer_run": True,
        "no_view_not_zero_alpha": True,
    }


def _candidate_order(survival_matrix: pd.DataFrame, probe: pd.DataFrame) -> list[str]:
    names: list[str] = []
    if "candidate_name" in survival_matrix.columns:
        names.extend([str(name) for name in survival_matrix["candidate_name"].dropna().tolist()])
    if "candidate_name" in probe.columns:
        names.extend([str(name) for name in probe["candidate_name"].dropna().tolist()])
    ordered: list[str] = []
    seen: set[str] = set()
    for name in names:
        if name and name not in seen:
            ordered.append(name)
            seen.add(name)
    return ordered


def _survival_status(survival_matrix: pd.DataFrame, candidate_name: str) -> str:
    if survival_matrix.empty or "candidate_name" not in survival_matrix.columns:
        return "missing_survival_matrix"
    rows = survival_matrix[survival_matrix["candidate_name"].astype(str).eq(candidate_name)]
    if rows.empty:
        return "missing_survival_row"
    return str(rows.iloc[0].get("survival_decision", "unknown"))


def _candidate_probe(probe: pd.DataFrame, candidate_name: str) -> pd.DataFrame:
    if probe.empty or "candidate_name" not in probe.columns:
        return pd.DataFrame()
    rows = probe[probe["candidate_name"].astype(str).eq(candidate_name)].copy()
    if "optimizer_input_probe_status" in rows.columns:
        rows = rows[rows["optimizer_input_probe_status"].astype(str).eq("staged_optimizer_input_ready")]
    return rows


def _skipped_response_row(candidate_name: str, reason: str) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_q2_optimizer_response_matrix.v1",
        "stage": STAGE,
        "candidate_name": candidate_name,
        "panel_name": "not_run",
        "optimizer_dry_run_status": "skipped_not_execution_survival_passed",
        "optimizer_status": "not_run",
        "objective_value": math.nan,
        "alpha_reward_share": math.nan,
        "gross_traded_notional": 0.0,
        "turnover": 0.0,
        "max_turnover_limit": math.nan,
        "max_abs_trade_notional": 0.0,
        "max_participation": 0.0,
        "participation_limit": math.nan,
        "max_post_trade_weight": 0.0,
        "single_name_limit": math.nan,
        "constraint_residual_max": math.nan,
        "net_weight_change": 0.0,
        "expected_return_weight_change_alignment": 0.0,
        "actual_local_optimizer_run": False,
        "orders_written": False,
        "portfolio_construction_allowed": False,
        "no_view_not_zero_alpha": True,
        "skip_reason": reason,
    }


def _skipped_constraint_rows(candidate_name: str, reason: str) -> list[dict[str, object]]:
    return [
        {
            "schema_version": "small_emotion_q2_optimizer_constraint_response.v1",
            "stage": STAGE,
            "candidate_name": candidate_name,
            "panel_name": "not_run",
            "constraint_name": name,
            "observed_value": math.nan,
            "limit": math.nan,
            "comparison": "not_run",
            "status": "not_run",
            "actual_local_optimizer_run": False,
            "no_view_not_zero_alpha": True,
            "skip_reason": reason,
        }
        for name in ["max_turnover", "participation_limit", "single_name_limit"]
    ]


def _error_response_row(candidate_name: str, panel_name: str, reason: str) -> dict[str, object]:
    row = _skipped_response_row(candidate_name, reason)
    row["panel_name"] = panel_name
    row["optimizer_dry_run_status"] = "optimizer_unavailable"
    return row


def _error_constraint_rows(candidate_name: str, panel_name: str, reason: str) -> list[dict[str, object]]:
    rows = _skipped_constraint_rows(candidate_name, reason)
    for row in rows:
        row["panel_name"] = panel_name
        row["status"] = "unavailable"
    return rows


def _snapshot(candidate_name: str, panel_name: str, universe: pd.DataFrame) -> pd.DataFrame:
    snapshot = universe.copy()
    snapshot["schema_version"] = "small_emotion_q2_optimizer_input_snapshot.v1"
    snapshot["stage"] = STAGE
    snapshot["candidate_name"] = candidate_name
    snapshot["panel_name"] = panel_name
    snapshot["actual_local_optimizer_run"] = True
    snapshot["orders_written"] = False
    for column in _snapshot_columns():
        if column not in snapshot.columns:
            snapshot[column] = ""
    return snapshot.loc[:, _snapshot_columns()]


def _summary(response_matrix: pd.DataFrame, *, q2_survival_dir: Path, config_path: Path) -> dict[str, object]:
    observed_candidates = set(
        response_matrix.loc[response_matrix["optimizer_dry_run_status"].eq("observed"), "candidate_name"].astype(str)
    )
    return {
        "schema_version": "small_emotion_q2_optimizer_dry_run_summary.v1",
        "stage": STAGE,
        "candidate_count": int(response_matrix["candidate_name"].astype(str).nunique()) if not response_matrix.empty else 0,
        "optimizer_observed_candidate_count": int(len(observed_candidates)),
        "response_row_count": int(len(response_matrix)),
        "q2_survival_dir": str(q2_survival_dir),
        "config_path": str(config_path),
        "actual_local_optimizer_run": int(len(observed_candidates)) > 0,
        "orders_written": False,
        "portfolio_construction_allowed": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
        "no_view_not_zero_alpha": True,
    }


def _manifest(summary: dict[str, object], artifacts: dict[str, Path], *, q2_survival_dir: Path, config_path: Path) -> dict[str, object]:
    input_hashes = {
        "q2_survival_summary": _hash_if_exists(q2_survival_dir / "small_emotion_q2_survival_summary.json"),
        "q2_survival_matrix": _hash_if_exists(q2_survival_dir / "small_emotion_q2_execution_survival_matrix.csv"),
        "optimizer_probe": _hash_if_exists(q2_survival_dir / "small_emotion_q2_optimizer_input_probe.csv"),
        "config": _hash_if_exists(config_path),
    }
    payload = {
        "schema_version": "small_emotion_q2_optimizer_dry_run_manifest.v1",
        "stage": STAGE,
        "summary": summary,
        "input_artifact_hashes": input_hashes,
        "output_artifacts": {key: str(path) for key, path in artifacts.items()},
        "actual_local_optimizer_run": summary["actual_local_optimizer_run"],
        "orders_written": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
    }
    payload["content_hash"] = hash_payload(payload)
    return payload


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "optimizer_response_matrix": output_path / "small_emotion_q2_optimizer_response_matrix.csv",
        "constraint_response": output_path / "small_emotion_q2_optimizer_constraint_response.csv",
        "optimizer_input_snapshot": output_path / "small_emotion_q2_optimizer_input_snapshot.csv",
        "summary": output_path / "small_emotion_q2_optimizer_dry_run_summary.json",
        "manifest": output_path / "small_emotion_q2_optimizer_dry_run_manifest.json",
        "report": output_path / "small_emotion_q2_optimizer_dry_run_report.md",
    }


def _response_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "panel_name",
        "optimizer_dry_run_status",
        "optimizer_status",
        "objective_value",
        "alpha_reward_share",
        "gross_traded_notional",
        "turnover",
        "max_turnover_limit",
        "max_abs_trade_notional",
        "max_participation",
        "participation_limit",
        "max_post_trade_weight",
        "single_name_limit",
        "constraint_residual_max",
        "net_weight_change",
        "expected_return_weight_change_alignment",
        "actual_local_optimizer_run",
        "orders_written",
        "portfolio_construction_allowed",
        "no_view_not_zero_alpha",
        "skip_reason",
    ]


def _constraint_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "panel_name",
        "constraint_name",
        "observed_value",
        "limit",
        "comparison",
        "status",
        "actual_local_optimizer_run",
        "no_view_not_zero_alpha",
        "skip_reason",
    ]


def _snapshot_columns() -> list[str]:
    return [
        "schema_version",
        "stage",
        "candidate_name",
        "panel_name",
        "ticker",
        "quantity",
        "estimated_price",
        "adv_shares",
        "target_weight",
        "current_weight",
        "industry",
        "tradable",
        "expected_return",
        "expected_return_source",
        "decision_horizon_days",
        "actual_local_optimizer_run",
        "orders_written",
    ]


def _report(summary: dict[str, object], response_matrix: pd.DataFrame) -> str:
    lines = [
        "# Q2-SMALL-EMOTION-03 Optimizer Adapter Dry-Run",
        "",
        "This is a local Q2 optimizer adapter dry-run. It connects the staged optimizer probe panel to the PortfolioOS optimizer input shape and observes constraint response only. It does not write orders, build a portfolio construction artifact, update Alpha Registry, open paper/live/broker/order workflows, or claim production approval.",
        "",
        f"- candidate_count: `{summary['candidate_count']}`",
        f"- optimizer_observed_candidate_count: `{summary['optimizer_observed_candidate_count']}`",
        f"- response_row_count: `{summary['response_row_count']}`",
        "",
        "| candidate | panel | status | optimizer_status | turnover | net_weight_change | max_participation |",
        "|---|---|---|---|---:|---:|---:|",
    ]
    for row in response_matrix.to_dict("records"):
        lines.append(
            "| {candidate} | {panel} | {status} | {opt} | {turnover} | {net} | {participation} |".format(
                candidate=row.get("candidate_name", ""),
                panel=row.get("panel_name", ""),
                status=row.get("optimizer_dry_run_status", ""),
                opt=row.get("optimizer_status", ""),
                turnover=_fmt(row.get("turnover")),
                net=_fmt(row.get("net_weight_change")),
                participation=_fmt(row.get("max_participation")),
            )
        )
    return "\n".join(lines) + "\n"


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _hash_if_exists(path: Path) -> str:
    return sha256_file(path) if path.exists() else "missing"


def _alpha_reward_share(objective_decomposition: dict[str, object]) -> float:
    components = objective_decomposition.get("components", {}) if isinstance(objective_decomposition, dict) else {}
    alpha = components.get("alpha_reward", {}) if isinstance(components, dict) else {}
    if isinstance(alpha, dict):
        return float(abs(alpha.get("weighted_value") or alpha.get("share_abs_weighted") or 0.0))
    return 0.0


def _safe_spearman(left: pd.Series, right: pd.Series) -> float:
    pair = pd.concat([left, right], axis=1).dropna()
    if len(pair) < 2:
        return 0.0
    if pair.iloc[:, 0].nunique() <= 1 or pair.iloc[:, 1].nunique() <= 1:
        return 0.0
    value = float(pair.iloc[:, 0].corr(pair.iloc[:, 1], method="spearman"))
    return value if np.isfinite(value) else 0.0


def _fmt(value: object) -> str:
    try:
        observed = float(value)
    except (TypeError, ValueError):
        return ""
    return "" if math.isnan(observed) else f"{observed:.6f}"
