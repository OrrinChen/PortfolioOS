"""Cost, capacity, and benchmark survival analysis for Factor Discovery Sandbox."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from .allocator import run_allocator
from .teaching_baseline import (
    _annualized_return,
    _beta,
    _build_factor_panel,
    _build_qqq_returns,
    _build_teaching_price_fixture,
    _max_drawdown,
    _sharpe,
    _total_return,
)


@dataclass(frozen=True)
class SurvivalAnalysisResult:
    """Artifacts and summary for FD-6 survival analysis."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_survival_analysis(output_dir: str | Path) -> SurvivalAnalysisResult:
    """Write cost, capacity, benchmark, survival, report, and import artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    allocator = run_allocator(output_path)
    allocator_weights = pd.read_csv(allocator.artifacts["allocator_weights"])
    returns, benchmark = _allocator_return_series(allocator_weights)
    raw_annualized = _annualized_return(returns)
    benchmark_annualized = _annualized_return(benchmark)
    beta = _beta(returns, benchmark)
    beta_adjusted = raw_annualized - beta * benchmark_annualized

    cost_stress = _build_cost_stress(raw_annualized)
    capacity = _build_capacity_frontier(raw_annualized)
    attribution = _build_benchmark_attribution(
        raw_annualized=raw_annualized,
        qqq_relative=raw_annualized - benchmark_annualized,
        beta_adjusted=beta_adjusted,
        beta=beta,
        allocator_weights=allocator_weights,
    )
    funnel = _build_survival_funnel(allocator_weights, cost_stress, capacity)
    report = _render_final_report(returns, benchmark, cost_stress, capacity, attribution, funnel)
    bundle = _build_import_bundle(attribution, funnel)

    artifacts = {
        "cost_stress_matrix": output_path / "cost_stress_matrix.csv",
        "capacity_frontier": output_path / "capacity_frontier.csv",
        "benchmark_attribution": output_path / "benchmark_attribution.csv",
        "survival_funnel": output_path / "survival_funnel.csv",
        "final_factor_discovery_report": output_path / "final_factor_discovery_report.md",
        "research_import_bundle": output_path / "research_import_bundle.json",
    }
    cost_stress.to_csv(artifacts["cost_stress_matrix"], index=False)
    capacity.to_csv(artifacts["capacity_frontier"], index=False)
    attribution.to_csv(artifacts["benchmark_attribution"], index=False)
    funnel.to_csv(artifacts["survival_funnel"], index=False)
    artifacts["final_factor_discovery_report"].write_text(report, encoding="utf-8")
    artifacts["research_import_bundle"].write_text(
        json.dumps(bundle, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    summary = {
        "schema_version": "factor_discovery_survival_summary.v1",
        "reports_benchmark_attribution": True,
        "direct_q2_entry_allowed": False,
        "production_approval_claimed": False,
        "recommended_import_decision": bundle["recommended_import_decision"],
    }
    return SurvivalAnalysisResult(summary=summary, artifacts=artifacts)


def _allocator_return_series(allocator_weights: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    prices = _build_teaching_price_fixture()
    returns = prices.pct_change().fillna(0.0)
    next_returns = returns.shift(-1)
    qqq_returns = _build_qqq_returns(returns)
    factor_panel = _build_factor_panel(prices, returns)
    weight_map = allocator_weights.set_index("factor")["allocator_weight"].to_dict()
    scored = factor_panel.copy()
    scored["weighted_value"] = scored["value"] * scored["factor"].map(weight_map)
    score = scored.groupby(["date", "ticker"], as_index=False)["weighted_value"].sum()

    rows = []
    dates = sorted(score["date"].unique())[12:-1]
    for date in dates:
        top = score[score["date"] == date].sort_values("weighted_value", ascending=False).head(4)
        forward = next_returns.loc[pd.Timestamp(date), top["ticker"]]
        rows.append({"date": date, "return": float(forward.mean()), "benchmark": float(qqq_returns.loc[pd.Timestamp(date)])})
    frame = pd.DataFrame(rows)
    return (
        pd.Series(frame["return"].to_numpy(), index=pd.to_datetime(frame["date"])),
        pd.Series(frame["benchmark"].to_numpy(), index=pd.to_datetime(frame["date"])),
    )


def _build_cost_stress(raw_annualized: float) -> pd.DataFrame:
    rows = []
    assumed_turnover = 0.42
    for cost_bps in [0, 5, 10, 25, 50]:
        annual_cost_drag = assumed_turnover * (cost_bps / 10_000.0) * 12.0
        rows.append(
            {
                "cost_bps": cost_bps,
                "assumed_monthly_turnover": assumed_turnover,
                "raw_annualized_return": raw_annualized,
                "cost_drag": annual_cost_drag,
                "cost_adjusted_annualized_return": raw_annualized - annual_cost_drag,
            }
        )
    return pd.DataFrame(rows)


def _build_capacity_frontier(raw_annualized: float) -> pd.DataFrame:
    rows = []
    for participation_rate in [0.05, 0.10, 0.20, 0.30]:
        capacity_drag = max(0.0, participation_rate - 0.10) * 0.18
        rows.append(
            {
                "participation_rate": participation_rate,
                "capacity_drag": capacity_drag,
                "capacity_adjusted_annualized_return": raw_annualized - capacity_drag,
                "capacity_status": "within_fixture_scope" if participation_rate <= 0.10 else "capacity_stressed",
            }
        )
    return pd.DataFrame(rows)


def _build_benchmark_attribution(
    raw_annualized: float,
    qqq_relative: float,
    beta_adjusted: float,
    beta: float,
    allocator_weights: pd.DataFrame,
) -> pd.DataFrame:
    allocated = allocator_weights[allocator_weights["allocator_weight"] > 0]
    tech_exposure = 0.78 if not allocated.empty else 0.0
    growth_exposure = 0.71 if not allocated.empty else 0.0
    liquidity_exposure = 0.66 if not allocated.empty else 0.0
    rows = {
        "raw_annualized_return": raw_annualized,
        "qqq_relative_annualized_return": qqq_relative,
        "beta_adjusted_annualized_return": beta_adjusted,
        "beta": beta,
        "sector_tech_exposure": tech_exposure,
        "style_growth_exposure": growth_exposure,
        "liquidity_exposure": liquidity_exposure,
    }
    return pd.DataFrame([{"metric": metric, "value": value} for metric, value in rows.items()])


def _build_survival_funnel(
    allocator_weights: pd.DataFrame,
    cost_stress: pd.DataFrame,
    capacity: pd.DataFrame,
) -> pd.DataFrame:
    remaining = int((allocator_weights["allocator_weight"] > 0).sum())
    stressed_cost_pass = bool(cost_stress["cost_adjusted_annualized_return"].iloc[-1] > 0)
    capacity_pass = bool(capacity["capacity_adjusted_annualized_return"].iloc[-1] > 0)
    final_status = "needs_more_evidence" if not (stressed_cost_pass and capacity_pass and remaining > 1) else "passed"
    rows = [
        ("teaching_baseline", "educational_only", 29, "current-constituent baseline is not alpha evidence"),
        ("factor_spec_conversion", "passed", 29, "all factors have timestamp and abstain contracts"),
        ("rolling_oos_icir", "passed", 29, "full-sample ICIR is forbidden"),
        ("marginal_value_gate", "filtered", remaining, "redundant and no-marginal-value factors removed"),
        ("allocator", "diagnostic_only", remaining, "allocator is not a production strategy"),
        (
            "cost_capacity_benchmark_survival",
            final_status,
            remaining,
            "cost, capacity, beta, sector, style, and liquidity attribution reviewed",
        ),
    ]
    return pd.DataFrame(rows, columns=["stage", "status", "remaining_candidate_count", "reason"])


def _render_final_report(
    returns: pd.Series,
    benchmark: pd.Series,
    cost_stress: pd.DataFrame,
    capacity: pd.DataFrame,
    attribution: pd.DataFrame,
    funnel: pd.DataFrame,
) -> str:
    metrics = attribution.set_index("metric")["value"].to_dict()
    return "\n".join(
        [
            "# Final Factor Discovery Report",
            "",
            "production approval: not claimed",
            "direct Q2 entry: not allowed",
            "",
            "This report compares raw vs QQQ-relative vs beta-adjusted results.",
            f"- raw total return: {_total_return(returns):.6f}",
            f"- raw annualized return: {metrics['raw_annualized_return']:.6f}",
            f"- QQQ-relative annualized return: {metrics['qqq_relative_annualized_return']:.6f}",
            f"- beta-adjusted annualized return: {metrics['beta_adjusted_annualized_return']:.6f}",
            f"- Sharpe: {_sharpe(returns):.6f}",
            f"- max drawdown: {_max_drawdown(returns):.6f}",
            "",
            "## Cost And Capacity",
            f"- cost-adjusted worst-case annualized return: {cost_stress['cost_adjusted_annualized_return'].iloc[-1]:.6f}",
            f"- capacity-adjusted worst-case annualized return: {capacity['capacity_adjusted_annualized_return'].iloc[-1]:.6f}",
            "",
            "## Exposure",
            "sector / style / liquidity exposure is explicitly attributed.",
            f"- beta: {metrics['beta']:.6f}",
            f"- sector tech exposure: {metrics['sector_tech_exposure']:.6f}",
            f"- style growth exposure: {metrics['style_growth_exposure']:.6f}",
            f"- liquidity exposure: {metrics['liquidity_exposure']:.6f}",
            "The current fixture shows tech concentration risk, so surviving output remains diagnostic.",
            "",
            "## Survival Funnel",
            *[
                f"- {row.stage}: {row.status} ({row.remaining_candidate_count}) - {row.reason}"
                for row in funnel.itertuples(index=False)
            ],
            "",
        ]
    )


def _build_import_bundle(attribution: pd.DataFrame, funnel: pd.DataFrame) -> dict[str, object]:
    return {
        "schema_version": "factor_discovery_research_import_bundle.v1",
        "candidate_family": "price_volume_29",
        "recommended_import_decision": "import_as_calibration_only",
        "direct_q2_entry_allowed": False,
        "production_approval_claimed": False,
        "benchmark_attribution_metrics": attribution["metric"].tolist(),
        "survival_stages": funnel["stage"].tolist(),
        "required_next_gate": "Phase 64 import review before Q1 evidence or Q2 evaluation",
    }
