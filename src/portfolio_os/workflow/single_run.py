"""Public single-run workflow service shared by CLI and backtests."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from portfolio_os.compliance.posttrade import run_posttrade_checks
from portfolio_os.compliance.pretrade import collect_data_quality_findings
from portfolio_os.data.import_profiles import load_import_profile
from portfolio_os.data.market import load_market_snapshot, market_to_frame
from portfolio_os.data.portfolio import (
    build_portfolio_frame,
    load_holdings,
    load_portfolio_state,
    load_target_weights,
)
from portfolio_os.data.reference import load_reference_snapshot, reference_to_frame
from portfolio_os.data.universe import build_universe_frame
from portfolio_os.explain.audit import build_audit_payload
from portfolio_os.explain.summary import build_summary, render_summary_markdown
from portfolio_os.optimizer.rebalancer import RebalanceRun, run_rebalance
from portfolio_os.simulation.benchmarks import (
    build_benchmark_comparison_payload,
    render_benchmark_comparison_markdown,
    run_benchmark_suite,
)
from portfolio_os.utils.config import AppConfig, load_app_config


@dataclass
class SingleRunContext:
    """Validated input state for one rebalance run."""

    app_config: AppConfig
    universe: pd.DataFrame
    input_paths: dict[str, str]
    constraints_path: Path


@dataclass
class SingleRunWorkflowResult:
    """Materialized single-run workflow payload before file export."""

    app_config: AppConfig
    universe: pd.DataFrame
    rebalance_run: RebalanceRun
    summary: dict[str, Any]
    summary_markdown: str
    audit_payload: dict[str, Any]
    export_readiness: dict[str, Any]
    benchmark_summary: dict[str, Any] | None
    benchmark_payload: dict[str, Any] | None
    benchmark_markdown: str | None
    input_paths: dict[str, str]


def _input_path_strings(**paths: Path | None) -> dict[str, str]:
    """Convert Path objects into a plain string mapping."""

    return {name: str(path) for name, path in paths.items() if path is not None}


def load_single_run_context(
    *,
    holdings: Path,
    target: Path,
    market: Path,
    reference: Path,
    portfolio_state: Path,
    constraints: Path,
    config: Path,
    execution_profile: Path,
    import_profile: Path | None = None,
) -> SingleRunContext:
    """Load validated inputs and build the working universe for one run."""

    import_profile_payload = load_import_profile(import_profile) if import_profile is not None else None
    holdings_data = load_holdings(holdings, import_profile=import_profile_payload)
    target_data = load_target_weights(target, import_profile=import_profile_payload)
    state = load_portfolio_state(portfolio_state)
    app_config = load_app_config(
        default_path=config,
        constraints_path=constraints,
        execution_path=execution_profile,
        portfolio_state=state,
    )

    portfolio_frame = build_portfolio_frame(holdings_data, target_data)
    required_tickers = portfolio_frame["ticker"].tolist()
    market_frame = market_to_frame(
        load_market_snapshot(market, required_tickers, import_profile=import_profile_payload)
    )
    reference_frame = reference_to_frame(
        load_reference_snapshot(reference, required_tickers, import_profile=import_profile_payload)
    )
    universe = build_universe_frame(portfolio_frame, market_frame, reference_frame, state)
    return SingleRunContext(
        app_config=app_config,
        universe=universe,
        input_paths=_input_path_strings(
            holdings=holdings,
            target=target,
            market=market,
            reference=reference,
            portfolio_state=portfolio_state,
            config=config,
            constraints=constraints,
            execution_profile=execution_profile,
            import_profile=import_profile,
        ),
        constraints_path=constraints,
    )


def run_single_rebalance(
    *,
    holdings: Path,
    target: Path,
    market: Path,
    reference: Path,
    portfolio_state: Path,
    constraints: Path,
    config: Path,
    execution_profile: Path,
    import_profile: Path | None = None,
    skip_benchmarks: bool = False,
    run_id: str,
    created_at: str,
) -> SingleRunWorkflowResult:
    """Run the full single-period rebalance workflow without writing files."""

    context = load_single_run_context(
        holdings=holdings,
        target=target,
        market=market,
        reference=reference,
        portfolio_state=portfolio_state,
        constraints=constraints,
        config=config,
        execution_profile=execution_profile,
        import_profile=import_profile,
    )

    benchmark_summary = None
    benchmark_payload = None
    benchmark_markdown = None
    input_data_quality_findings = collect_data_quality_findings(context.universe, context.app_config)
    if skip_benchmarks:
        rebalance_run = run_rebalance(
            context.universe,
            context.app_config,
            input_findings=input_data_quality_findings,
        )
    else:
        benchmark_suite = run_benchmark_suite(context.universe, context.app_config)
        rebalance_run = benchmark_suite.runs["portfolio_os_rebalance"]
        benchmark_summary = benchmark_suite.comparison.comparison_summary
        benchmark_payload = build_benchmark_comparison_payload(
            input_paths=context.input_paths,
            suite_result=benchmark_suite,
        )
        benchmark_markdown = render_benchmark_comparison_markdown(benchmark_suite.comparison)

    summary = build_summary(
        rebalance_run.universe,
        rebalance_run.basket,
        rebalance_run.findings,
        context.app_config,
        cash_before=rebalance_run.cash_before,
        cash_after=rebalance_run.cash_after,
        pre_trade_nav=rebalance_run.pre_trade_nav,
        post_trade_quantities=rebalance_run.post_trade_quantities,
        benchmark_summary=benchmark_summary,
        risk_context=rebalance_run.risk_context,
    )
    summary_markdown = render_summary_markdown(summary, rebalance_run.findings)
    export_readiness = run_posttrade_checks(rebalance_run.orders, rebalance_run.findings)
    constraint_snapshot = context.app_config.build_constraint_snapshot(context.constraints_path)
    audit_payload = build_audit_payload(
        input_paths=context.input_paths,
        config=context.app_config,
        constraint_snapshot=constraint_snapshot,
        findings=rebalance_run.findings,
        basket=rebalance_run.basket,
        summary=summary,
        run_id=run_id,
        created_at=created_at,
        export_readiness=export_readiness,
        optimization_metadata={
            "solver_used": rebalance_run.optimization_result.solver_used,
            "solver_fallback_used": rebalance_run.optimization_result.solver_fallback_used,
            "constraint_residual_max": rebalance_run.optimization_result.constraint_residual_max,
            "solver_status": rebalance_run.optimization_result.status,
            "objective_decomposition": rebalance_run.optimization_result.objective_decomposition,
        },
    )

    return SingleRunWorkflowResult(
        app_config=context.app_config,
        universe=context.universe,
        rebalance_run=rebalance_run,
        summary=summary,
        summary_markdown=summary_markdown,
        audit_payload=audit_payload,
        export_readiness=export_readiness,
        benchmark_summary=benchmark_summary,
        benchmark_payload=benchmark_payload,
        benchmark_markdown=benchmark_markdown,
        input_paths=context.input_paths,
    )
