"""Scenario manifest loading and scenario comparison execution."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from portfolio_os.compliance.findings import summarize_findings
from portfolio_os.compliance.posttrade import run_posttrade_checks
from portfolio_os.compliance.pretrade import collect_data_quality_findings
from portfolio_os.data.import_profiles import ImportProfile, load_import_profile
from portfolio_os.data.loaders import read_yaml
from portfolio_os.data.market import load_market_snapshot, market_to_frame
from portfolio_os.data.portfolio import build_portfolio_frame, load_holdings, load_portfolio_state, load_target_weights
from portfolio_os.data.reference import load_reference_snapshot, reference_to_frame
from portfolio_os.data.universe import build_universe_frame
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.domain.models import ScenarioMetrics
from portfolio_os.explain.summary import build_summary
from portfolio_os.optimizer.rebalancer import RebalanceRun, run_rebalance
from portfolio_os.storage.snapshots import file_metadata
from portfolio_os.utils.config import AppConfig, load_app_config


# Transparent scenario scoring rule: lower is better.
# The weights favor target fit first, then cost, then turnover, while still
# penalizing blocked or unresolved control outcomes.
SCENARIO_SCORE_WEIGHTS: dict[str, float] = {
    "target_deviation_after": 0.35,
    "estimated_total_cost": 0.25,
    "turnover": 0.15,
    "blocked_trade_count": 0.15,
    "blocking_finding_count": 0.10,
}

SCENARIO_TIEBREAK_WEIGHTS: dict[str, float] = {
    "blocked_trade_count": 0.5,
    "estimated_total_cost": 0.3,
    "turnover": 0.2,
}
SCENARIO_TIEBREAK_SCALE = 0.02
SCENARIO_SEQUENCE_TIEBREAK_EPS = 0.01


class ScenarioBaseInputs(BaseModel):
    """Base input files shared across all scenarios in one manifest."""

    holdings: str
    target: str
    market: str
    reference: str
    portfolio_state: str
    config: str


class ScenarioOverrides(BaseModel):
    """White-listed per-scenario overrides."""

    model_config = ConfigDict(extra="forbid")

    max_turnover: float | None = None
    single_name_max_weight: float | None = None
    min_order_notional: float | None = None
    participation_limit: float | None = None
    min_cash_buffer: float | None = None


class ScenarioDefinition(BaseModel):
    """One what-if scenario."""

    id: str
    label: str
    constraints: str
    execution_profile: str
    overrides: ScenarioOverrides = Field(default_factory=ScenarioOverrides)


class ScenarioManifest(BaseModel):
    """Scenario manifest for a single input snapshot."""

    name: str
    description: str | None = None
    base_inputs: ScenarioBaseInputs
    scenarios: list[ScenarioDefinition] = Field(default_factory=list)


@dataclass
class ScenarioRun:
    """One scenario execution result and supporting context."""

    scenario: ScenarioDefinition
    constraints_path: Path
    execution_profile_path: Path
    config: AppConfig
    rebalance_run: RebalanceRun
    summary: dict[str, Any]
    score: float


@dataclass
class ScenarioSuiteResult:
    """Result of a full scenario comparison run."""

    manifest: ScenarioManifest
    manifest_path: Path
    base_input_paths: dict[str, Path]
    import_profile_path: Path | None
    scenario_runs: list[ScenarioRun]
    scenario_comparison_payload: dict[str, Any]
    scenario_comparison_markdown: str
    decision_pack_markdown: str


def resolve_manifest_path(path_text: str, *, manifest_dir: Path, cwd: Path) -> Path:
    """Resolve a manifest-relative or cwd-relative path."""

    raw_path = Path(path_text)
    if raw_path.is_absolute():
        return raw_path
    manifest_candidate = (manifest_dir / raw_path).resolve()
    if manifest_candidate.exists():
        return manifest_candidate
    cwd_candidate = (cwd / raw_path).resolve()
    if cwd_candidate.exists():
        return cwd_candidate
    return manifest_candidate


def load_scenario_manifest(path: str | Path) -> ScenarioManifest:
    """Load and validate the scenario manifest."""

    payload = read_yaml(path)
    try:
        manifest = ScenarioManifest.model_validate(payload)
    except ValidationError as exc:
        raise InputValidationError(f"Invalid scenario manifest: {exc}") from exc
    if not manifest.scenarios:
        raise InputValidationError("Scenario manifest must define at least one scenario.")
    return manifest


def resolve_base_input_paths(manifest_path: str | Path, manifest: ScenarioManifest) -> dict[str, Path]:
    """Resolve the shared base input paths for a scenario suite."""

    manifest_dir = Path(manifest_path).resolve().parent
    cwd = Path.cwd().resolve()
    paths = {
        name: resolve_manifest_path(value, manifest_dir=manifest_dir, cwd=cwd)
        for name, value in manifest.base_inputs.model_dump(mode="json").items()
    }
    missing = [name for name, path in paths.items() if not path.exists()]
    if missing:
        raise InputValidationError(
            f"Scenario manifest base_inputs contain missing file(s): {', '.join(missing)}"
        )
    return paths


def _load_base_snapshot(
    base_input_paths: dict[str, Path],
    *,
    import_profile: ImportProfile | None = None,
) -> tuple[pd.DataFrame, Any]:
    """Load the shared working universe and portfolio state."""

    holdings = load_holdings(base_input_paths["holdings"], import_profile=import_profile)
    targets = load_target_weights(base_input_paths["target"], import_profile=import_profile)
    portfolio_state = load_portfolio_state(base_input_paths["portfolio_state"])
    portfolio_frame = build_portfolio_frame(holdings, targets)
    required_tickers = portfolio_frame["ticker"].tolist()
    market_frame = market_to_frame(
        load_market_snapshot(base_input_paths["market"], required_tickers, import_profile=import_profile)
    )
    reference_frame = reference_to_frame(
        load_reference_snapshot(base_input_paths["reference"], required_tickers, import_profile=import_profile)
    )
    universe = build_universe_frame(portfolio_frame, market_frame, reference_frame, portfolio_state)
    return universe, portfolio_state


def build_app_config_for_scenario(
    *,
    base_input_paths: dict[str, Path],
    portfolio_state,
    scenario: ScenarioDefinition,
    manifest_path: str | Path,
) -> tuple[AppConfig, Path, Path]:
    """Load config and apply the allowed override set for one scenario."""

    manifest_dir = Path(manifest_path).resolve().parent
    cwd = Path.cwd().resolve()
    constraints_path = resolve_manifest_path(scenario.constraints, manifest_dir=manifest_dir, cwd=cwd)
    execution_profile_path = resolve_manifest_path(
        scenario.execution_profile,
        manifest_dir=manifest_dir,
        cwd=cwd,
    )
    app_config = load_app_config(
        default_path=base_input_paths["config"],
        constraints_path=constraints_path,
        execution_path=execution_profile_path,
        portfolio_state=portfolio_state,
    )
    overrides = scenario.overrides.model_dump(mode="json", exclude_none=True)
    for key, value in overrides.items():
        if key == "min_cash_buffer":
            app_config.portfolio_state.min_cash_buffer = float(value)
        else:
            setattr(app_config.constraints, key, float(value))
    return app_config, constraints_path, execution_profile_path


def build_scenario_metrics(run: ScenarioRun) -> ScenarioMetrics:
    """Convert one scenario run into the comparison metric schema."""

    summary = run.summary
    finding_summary = summarize_findings(run.rebalance_run.findings)
    return ScenarioMetrics(
        scenario_id=run.scenario.id,
        scenario_label=run.scenario.label,
        constraints_template=run.scenario.constraints,
        execution_profile=run.scenario.execution_profile,
        pre_trade_nav=float(run.rebalance_run.pre_trade_nav),
        cash_before=float(run.rebalance_run.cash_before),
        cash_after=float(run.rebalance_run.cash_after),
        target_deviation_after=float(summary["target_deviation_after"]),
        gross_traded_notional=float(run.rebalance_run.basket.gross_traded_notional),
        turnover=float(summary["turnover"]),
        estimated_fee_total=float(run.rebalance_run.basket.total_fee),
        estimated_slippage_total=float(run.rebalance_run.basket.total_slippage),
        estimated_total_cost=float(run.rebalance_run.basket.total_cost),
        buy_order_count=int(summary["buy_order_count"]),
        sell_order_count=int(summary["sell_order_count"]),
        blocked_trade_count=int(summary["blocked_trade_count"]),
        blocking_finding_count=int(summary["blocking_finding_count"]),
        warning_finding_count=int(summary["warning_count"]),
        data_quality_finding_count=int(finding_summary["category_counts"].get("data_quality", 0)),
        regulatory_finding_count=int(finding_summary["category_counts"].get("regulatory", 0)),
        tradability_finding_count=int(finding_summary["category_counts"].get("tradability", 0)),
    )


def normalize_metric_map(metric_values: dict[str, float]) -> dict[str, float]:
    """Min-max normalize a metric dictionary for scoring."""

    values = np.asarray(list(metric_values.values()), dtype=float)
    if values.size == 0:
        return {}
    min_value = float(np.min(values))
    max_value = float(np.max(values))
    if abs(max_value - min_value) < 1e-12:
        return {key: 0.0 for key in metric_values}
    return {
        key: float((value - min_value) / (max_value - min_value))
        for key, value in metric_values.items()
    }


def score_scenarios(
    metrics: list[ScenarioMetrics],
) -> tuple[dict[str, float], list[dict[str, Any]], dict[str, dict[str, float]]]:
    """Score scenarios under the transparent workflow scoring rule."""

    normalized_by_metric = {
        metric_name: normalize_metric_map(
            {metric.scenario_id: float(getattr(metric, metric_name)) for metric in metrics}
        )
        for metric_name in SCENARIO_SCORE_WEIGHTS
    }
    tie_break_normalized = {
        metric_name: normalize_metric_map(
            {metric.scenario_id: float(getattr(metric, metric_name)) for metric in metrics}
        )
        for metric_name in SCENARIO_TIEBREAK_WEIGHTS
    }
    scores: dict[str, float] = {}
    score_components: dict[str, dict[str, float]] = {}
    scenario_order = {metric.scenario_id: idx for idx, metric in enumerate(metrics)}
    for metric in metrics:
        scenario_id = metric.scenario_id
        weighted_components: dict[str, float] = {}
        for metric_name, weight in SCENARIO_SCORE_WEIGHTS.items():
            weighted_components[metric_name] = float(weight * normalized_by_metric[metric_name][scenario_id])
        base_score = float(sum(weighted_components.values()))
        tie_break_penalty = float(
            SCENARIO_TIEBREAK_SCALE
            * sum(
                SCENARIO_TIEBREAK_WEIGHTS[metric_name] * tie_break_normalized[metric_name][scenario_id]
                for metric_name in SCENARIO_TIEBREAK_WEIGHTS
            )
        )
        sequence_penalty = float(SCENARIO_SEQUENCE_TIEBREAK_EPS * scenario_order.get(scenario_id, 0))
        effective_score = base_score + tie_break_penalty + sequence_penalty
        score_components[scenario_id] = {
            **weighted_components,
            "_base_score": base_score,
            "_tie_break_penalty": tie_break_penalty,
            "_sequence_penalty": sequence_penalty,
            "_effective_score": effective_score,
        }
        scores[scenario_id] = float(effective_score)
    ranking = [
        {
            "scenario_id": scenario_id,
            "score": score_components[scenario_id]["_effective_score"],
            "base_score": score_components[scenario_id]["_base_score"],
            "tie_break_penalty": score_components[scenario_id]["_tie_break_penalty"],
        }
        for scenario_id, _ in sorted(
            scores.items(),
            key=lambda item: (
                item[1],
                score_components[item[0]]["blocked_trade_count"],
                score_components[item[0]]["estimated_total_cost"],
                score_components[item[0]]["turnover"],
                item[0],
            ),
        )
    ]
    for rank, row in enumerate(ranking, start=1):
        row["rank"] = rank
    return scores, ranking, score_components


def top_changed_tickers(
    scenario_runs: list[ScenarioRun],
    *,
    reference_scenario_id: str,
    candidate_scenario_id: str,
    top_n: int = 3,
) -> list[str]:
    """Return the tickers whose signed order notionals changed the most."""

    run_by_id = {run.scenario.id: run for run in scenario_runs}
    reference_run = run_by_id[reference_scenario_id]
    candidate_run = run_by_id[candidate_scenario_id]
    prices = reference_run.rebalance_run.universe.set_index("ticker")["estimated_price"].to_dict()

    def _signed_order_map(run: ScenarioRun) -> dict[str, float]:
        signed_map: dict[str, float] = {ticker: 0.0 for ticker in prices}
        for order in run.rebalance_run.orders:
            signed_quantity = float(order.quantity if order.side.value == "BUY" else -order.quantity)
            signed_map[order.ticker] = signed_quantity
        return signed_map

    reference_orders = _signed_order_map(reference_run)
    candidate_orders = _signed_order_map(candidate_run)
    ticker_changes: list[tuple[str, float]] = []
    for ticker, price in prices.items():
        change = abs(candidate_orders.get(ticker, 0.0) - reference_orders.get(ticker, 0.0)) * float(price)
        ticker_changes.append((ticker, change))
    ticker_changes.sort(key=lambda item: item[1], reverse=True)
    return [ticker for ticker, change in ticker_changes[:top_n] if change > 0]


def explain_scenario_tradeoff(
    *,
    recommended_run: ScenarioRun,
    candidate_run: ScenarioRun,
) -> str:
    """Return a simple explanation of why a scenario differs from the recommended one."""

    reasons: list[str] = []
    candidate_config = candidate_run.config
    recommended_config = recommended_run.config
    if candidate_config.constraints.max_turnover != recommended_config.constraints.max_turnover:
        if candidate_config.constraints.max_turnover < recommended_config.constraints.max_turnover:
            reasons.append("a tighter turnover cap")
        else:
            reasons.append("a looser turnover cap")
    if candidate_config.effective_single_name_limit != recommended_config.effective_single_name_limit:
        if candidate_config.effective_single_name_limit < recommended_config.effective_single_name_limit:
            reasons.append("a tighter single-name cap")
        else:
            reasons.append("a looser single-name cap")
    if candidate_config.constraints.participation_limit != recommended_config.constraints.participation_limit:
        reasons.append("a different participation cap")
    if candidate_config.portfolio_state.min_cash_buffer != recommended_config.portfolio_state.min_cash_buffer:
        reasons.append("a different cash buffer requirement")
    if candidate_config.constraints.min_order_notional != recommended_config.constraints.min_order_notional:
        reasons.append("a different minimum order threshold")
    if candidate_config.execution.urgency != recommended_config.execution.urgency:
        reasons.append("a different execution urgency profile")

    changed_tickers = top_changed_tickers(
        [recommended_run, candidate_run],
        reference_scenario_id=recommended_run.scenario.id,
        candidate_scenario_id=candidate_run.scenario.id,
    )
    if changed_tickers:
        reasons.append(f"the largest order changes were in {', '.join(changed_tickers)}")
    if not reasons:
        return "This scenario is operationally close to the recommended one and mostly changes the reporting posture."
    return "This scenario differs because of " + ", ".join(reasons) + "."


def classify_scenario_positioning(
    metric: ScenarioMetrics,
    *,
    recommended_scenario_id: str,
    labels: dict[str, str],
) -> str:
    """Return a one-line scenario positioning string."""

    if metric.scenario_id == recommended_scenario_id:
        return "Recommended balance under the current workflow scoring rule."
    if metric.scenario_id == labels["lowest_cost_scenario"]:
        return "Cheapest scenario on estimated execution cost."
    if metric.scenario_id == labels["lowest_turnover_scenario"]:
        return "Lowest-turnover scenario under the current snapshot."
    if metric.scenario_id == labels["fewest_blocked_trades_scenario"]:
        return "Scenario with the fewest blocked-trade outcomes."
    if metric.scenario_id == labels["best_target_fit_scenario"]:
        return "Scenario with the closest target fit after rebalancing."
    return "Alternative scenario for comparing policy trade-offs."


def build_scenario_comparison_payload(
    *,
    manifest: ScenarioManifest,
    manifest_path: str | Path,
    base_input_paths: dict[str, Path],
    scenario_runs: list[ScenarioRun],
    import_profile_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build the JSON comparison payload for a scenario suite."""

    metrics = [build_scenario_metrics(run) for run in scenario_runs]
    scores, ranking, score_components = score_scenarios(metrics)
    recommended_scenario_id = ranking[0]["scenario_id"]
    lowest_cost_scenario = min(metrics, key=lambda item: item.estimated_total_cost).scenario_id
    lowest_turnover_scenario = min(metrics, key=lambda item: item.turnover).scenario_id
    fewest_blocked_trades_scenario = min(metrics, key=lambda item: (item.blocked_trade_count, item.blocking_finding_count)).scenario_id
    best_target_fit_scenario = min(metrics, key=lambda item: item.target_deviation_after).scenario_id
    labels = {
        "recommended_scenario": recommended_scenario_id,
        "lowest_cost_scenario": lowest_cost_scenario,
        "lowest_turnover_scenario": lowest_turnover_scenario,
        "fewest_blocked_trades_scenario": fewest_blocked_trades_scenario,
        "best_target_fit_scenario": best_target_fit_scenario,
    }
    run_by_id = {run.scenario.id: run for run in scenario_runs}
    recommended_run = run_by_id[recommended_scenario_id]

    scenario_rows: list[dict[str, Any]] = []
    for metric in metrics:
        scenario_run = run_by_id[metric.scenario_id]
        components = score_components[metric.scenario_id]
        scenario_rows.append(
            {
                **metric.model_dump(mode="json"),
                "score": scores[metric.scenario_id],
                "base_score": components["_base_score"],
                "tie_break_penalty": components["_tie_break_penalty"],
                "sequence_penalty": components["_sequence_penalty"],
                "score_components": {
                    key: value
                    for key, value in components.items()
                    if not key.startswith("_")
                },
                "positioning": classify_scenario_positioning(
                    metric,
                    recommended_scenario_id=recommended_scenario_id,
                    labels=labels,
                ),
                "tradeoff_explanation": explain_scenario_tradeoff(
                    recommended_run=recommended_run,
                    candidate_run=scenario_run,
                ),
                "report_labels": scenario_run.config.constraints.report_labels.model_dump(mode="json"),
                "applied_overrides": scenario_run.scenario.overrides.model_dump(
                    mode="json",
                    exclude_none=True,
                ),
            }
        )

    top_tickers_across_scenarios = top_changed_tickers(
        scenario_runs,
        reference_scenario_id=recommended_scenario_id,
        candidate_scenario_id=lowest_cost_scenario,
    )
    major_tradeoff_tickers: dict[str, list[str]] = {}
    for scenario_id in labels.values():
        major_tradeoff_tickers[scenario_id] = top_changed_tickers(
            scenario_runs,
            reference_scenario_id=recommended_scenario_id,
            candidate_scenario_id=scenario_id,
        )

    second_best_scenario_id = ranking[1]["scenario_id"] if len(ranking) > 1 else recommended_scenario_id
    recommended_components = score_components[recommended_scenario_id]
    second_best_components = score_components[second_best_scenario_id]
    component_deltas = {
        metric_name: float(second_best_components[metric_name] - recommended_components[metric_name])
        for metric_name in SCENARIO_SCORE_WEIGHTS
    }

    return {
        "manifest": {
            "name": manifest.name,
            "description": manifest.description,
            "source_manifest": file_metadata(manifest_path),
            "base_inputs": {name: file_metadata(path) for name, path in base_input_paths.items()},
            "import_profile": file_metadata(import_profile_path) if import_profile_path is not None else None,
        },
        "scenarios": scenario_rows,
        "ranking": ranking,
        "labels": labels,
        "scoring_rule": {
            "weights": SCENARIO_SCORE_WEIGHTS,
            "tie_break": {
                "scale": SCENARIO_TIEBREAK_SCALE,
                "weights": SCENARIO_TIEBREAK_WEIGHTS,
                "rule": (
                    "If weighted scores are near-tied, lower blocked trades, then lower cost, "
                    "then lower turnover wins; if still tied, manifest scenario order is preferred."
                ),
                "sequence_penalty_eps": SCENARIO_SEQUENCE_TIEBREAK_EPS,
            },
            "note": "Lower score is better under the current workflow scoring rule.",
        },
        "recommendation_diagnostics": {
            "recommended_scenario": recommended_scenario_id,
            "second_best_scenario": second_best_scenario_id,
            "score_gap_to_second": float(scores[second_best_scenario_id] - scores[recommended_scenario_id]),
            "component_deltas_vs_second": component_deltas,
        },
        "cross_scenario_explanation": {
            "largest_varying_tickers_vs_recommended": top_tickers_across_scenarios,
            "major_tradeoff_tickers_by_label": major_tradeoff_tickers,
            "scenario_with_most_warnings": max(metrics, key=lambda item: item.warning_finding_count).scenario_id,
            "scenario_with_most_blocking_findings": max(metrics, key=lambda item: item.blocking_finding_count).scenario_id,
            "scenario_with_most_regulatory_findings": max(
                metrics,
                key=lambda item: item.regulatory_finding_count,
            ).scenario_id,
        },
    }


def run_scenario_suite(
    manifest_path: str | Path,
    *,
    import_profile_path: str | Path | None = None,
) -> ScenarioSuiteResult:
    """Run the full scenario suite for one shared snapshot."""

    manifest = load_scenario_manifest(manifest_path)
    base_input_paths = resolve_base_input_paths(manifest_path, manifest)
    resolved_import_profile_path = Path(import_profile_path).resolve() if import_profile_path is not None else None
    import_profile = load_import_profile(resolved_import_profile_path) if resolved_import_profile_path is not None else None
    base_universe, portfolio_state = _load_base_snapshot(base_input_paths, import_profile=import_profile)
    scenario_runs: list[ScenarioRun] = []

    for scenario in manifest.scenarios:
        app_config, constraints_path, execution_profile_path = build_app_config_for_scenario(
            base_input_paths=base_input_paths,
            portfolio_state=portfolio_state.model_copy(deep=True),
            scenario=scenario,
            manifest_path=manifest_path,
        )
        universe = base_universe.copy(deep=True)
        input_findings = collect_data_quality_findings(universe, app_config)
        rebalance_run = run_rebalance(universe, app_config, input_findings=input_findings)
        summary = build_summary(
            rebalance_run.universe,
            rebalance_run.basket,
            rebalance_run.findings,
            app_config,
            cash_before=rebalance_run.cash_before,
            cash_after=rebalance_run.cash_after,
            pre_trade_nav=rebalance_run.pre_trade_nav,
            post_trade_quantities=rebalance_run.post_trade_quantities,
            risk_context=rebalance_run.risk_context,
        )
        scenario_runs.append(
            ScenarioRun(
                scenario=scenario,
                constraints_path=constraints_path,
                execution_profile_path=execution_profile_path,
                config=app_config,
                rebalance_run=rebalance_run,
                summary=summary,
                score=0.0,
            )
        )

    comparison_payload = build_scenario_comparison_payload(
        manifest=manifest,
        manifest_path=manifest_path,
        base_input_paths=base_input_paths,
        scenario_runs=scenario_runs,
        import_profile_path=resolved_import_profile_path,
    )
    scores = {row["scenario_id"]: row["score"] for row in comparison_payload["ranking"]}
    for scenario_run in scenario_runs:
        scenario_run.score = scores[scenario_run.scenario.id]

    from portfolio_os.explain.decision_pack import (  # local import to avoid circular dependency
        render_decision_pack_markdown,
        render_scenario_comparison_markdown,
    )

    scenario_comparison_markdown = render_scenario_comparison_markdown(comparison_payload)
    decision_pack_markdown = render_decision_pack_markdown(comparison_payload, scenario_runs)
    return ScenarioSuiteResult(
        manifest=manifest,
        manifest_path=Path(manifest_path),
        base_input_paths=base_input_paths,
        import_profile_path=resolved_import_profile_path,
        scenario_runs=scenario_runs,
        scenario_comparison_payload=comparison_payload,
        scenario_comparison_markdown=scenario_comparison_markdown,
        decision_pack_markdown=decision_pack_markdown,
    )
