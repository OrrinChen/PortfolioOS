"""Typer CLI entrypoint for PortfolioOS."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pandas as pd
import typer
import yaml

from portfolio_os.alpha.acceptance import run_alpha_acceptance_gate
from portfolio_os.alpha.research import run_alpha_research
from portfolio_os.backtest.engine import run_backtest
from portfolio_os.backtest.sweep import run_backtest_cost_sweep, run_backtest_risk_sweep
from portfolio_os.data.builders.market_builder import (
    build_market_frame,
    build_market_manifest,
    load_tickers_file,
    write_market_csv,
)
from portfolio_os.data.builders.reference_builder import (
    build_reference_frame,
    build_reference_manifest,
    write_reference_csv,
)
from portfolio_os.data.builders.common import builder_manifest_path
from portfolio_os.data.builders.common import classify_builder_error, get_provider_report
from portfolio_os.data.builders.snapshot_builder import build_snapshot_bundle
from portfolio_os.data.builders.target_builder import (
    build_target_frame,
    build_target_manifest,
    target_manifest_path,
    write_target_csv,
)
from portfolio_os.data.import_profiles import load_import_profile
from portfolio_os.data.market import load_market_snapshot, market_to_frame
from portfolio_os.data.portfolio import build_portfolio_frame, load_holdings, load_portfolio_state, load_target_weights
from portfolio_os.data.providers import get_data_provider
from portfolio_os.data.reference import load_reference_snapshot, reference_to_frame
from portfolio_os.data.universe import build_universe_frame
from portfolio_os.domain.errors import PortfolioOSError
from portfolio_os.compliance.posttrade import run_posttrade_checks
from portfolio_os.compliance.pretrade import collect_data_quality_findings
from portfolio_os.explain.audit import build_audit_payload
from portfolio_os.explain.handoff import (
    load_optional_json,
    render_approval_handoff_checklist,
    render_execution_handoff_checklist,
)
from portfolio_os.explain.summary import build_summary, render_summary_markdown
from portfolio_os.execution.adapters.csv_export import export_basket_csv, export_basket_oms_csv
from portfolio_os.execution.reporting import (
    build_execution_child_orders_frame,
    build_execution_fills_frame,
    build_execution_report_payload,
    render_execution_report_markdown,
)
from portfolio_os.execution.simulator import run_execution_simulation
from portfolio_os.optimizer.rebalancer import run_rebalance
from portfolio_os.simulation.benchmarks import (
    build_benchmark_comparison_payload,
    render_benchmark_comparison_markdown,
    run_benchmark_suite,
)
from portfolio_os.simulation.replay import run_replay_suite
from portfolio_os.simulation.scenarios import run_scenario_suite
from portfolio_os.storage.runs import (
    prepare_approval_artifacts,
    prepare_execution_artifacts,
    prepare_replay_artifacts,
    prepare_run_artifacts,
    prepare_scenario_artifacts,
)
from portfolio_os.storage.snapshots import file_metadata, write_json, write_text
from portfolio_os.utils.config import load_app_config
from portfolio_os.utils.logging import configure_logging
from portfolio_os.workflow.approval import (
    build_approval_request_template_payload,
    build_approval_record_payload,
    build_approval_summary_markdown,
    evaluate_approval_request,
    freeze_selected_scenario,
)
from portfolio_os.workflow.single_run import run_single_rebalance

app = typer.Typer(add_completion=False, help="PortfolioOS compliance-aware rebalance CLI.")
alpha_research_app = typer.Typer(add_completion=False, help="PortfolioOS alpha research CLI.")
alpha_acceptance_app = typer.Typer(add_completion=False, help="PortfolioOS alpha acceptance-gate CLI.")
backtest_app = typer.Typer(add_completion=False, help="PortfolioOS historical backtest CLI.")
backtest_sweep_app = typer.Typer(add_completion=False, help="PortfolioOS backtest parameter sweep CLI.")
risk_sweep_app = typer.Typer(add_completion=False, help="PortfolioOS risk aversion parameter sweep CLI.")
replay_app = typer.Typer(add_completion=False, help="PortfolioOS static replay CLI.")
scenario_app = typer.Typer(add_completion=False, help="PortfolioOS scenario analysis CLI.")
approval_app = typer.Typer(add_completion=False, help="PortfolioOS approval and freeze CLI.")
execution_app = typer.Typer(add_completion=False, help="PortfolioOS execution simulation CLI.")
build_market_app = typer.Typer(add_completion=False, help="PortfolioOS market-feed builder CLI.")
build_reference_app = typer.Typer(add_completion=False, help="PortfolioOS reference-feed builder CLI.")
build_target_app = typer.Typer(add_completion=False, help="PortfolioOS target-from-index builder CLI.")
build_snapshot_app = typer.Typer(add_completion=False, help="PortfolioOS snapshot-bundle builder CLI.")


def _input_path_strings(**paths: Path) -> dict[str, str]:
    """Convert Path objects into a plain string mapping."""

    return {name: str(path) for name, path in paths.items() if path is not None}


def _builder_error_hint(provider, feed_name: str) -> str:
    """Return a concise user-facing hint for builder failures."""

    report = get_provider_report(provider, feed_name)
    alternative = report.get("recommended_alternative_path")
    if alternative == "provide_target_csv_and_continue":
        return "You can continue with client-provided target.csv."
    if alternative == "run_market_and_reference_builders_only":
        return "Market and reference feeds can still be built and used."
    if alternative == "provide_reference_csv_and_continue":
        return "You can continue with client-provided reference.csv."
    return ""


def _merge_error_and_hint(error_text: str, hint: str) -> str:
    """Append a hint only when it is not already present in the error text."""

    if not hint:
        return error_text
    if hint in error_text:
        return error_text
    return f"{error_text} {hint}"


def _load_single_run_context(
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
):
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
    return app_config, universe


@backtest_app.command()
def main(
    manifest: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    output_dir: Path = typer.Option(...),
) -> None:
    """Run the minimal PortfolioOS historical backtest loop."""

    logger = configure_logging()
    try:
        result = run_backtest(manifest)
        output_dir.mkdir(parents=True, exist_ok=True)
        backtest_results_path = output_dir / "backtest_results.json"
        nav_series_path = output_dir / "nav_series.csv"
        period_attribution_path = output_dir / "period_attribution.csv"
        backtest_report_path = output_dir / "backtest_report.md"
        alpha_panel_path: Path | None = None
        if result.alpha_panel is not None:
            alpha_panel_path = output_dir / "alpha_panel.csv"
            result.alpha_panel.to_csv(alpha_panel_path, index=False)
        write_json(backtest_results_path, result.to_payload(alpha_panel_path=alpha_panel_path))
        result.nav_series.to_csv(nav_series_path, index=False)
        result.period_attribution.to_csv(period_attribution_path, index=False)
        write_text(backtest_report_path, result.report_markdown)
        typer.echo(f"backtest_results.json: {backtest_results_path}")
        typer.echo(f"nav_series.csv: {nav_series_path}")
        typer.echo(f"period_attribution.csv: {period_attribution_path}")
        typer.echo(f"backtest_report.md: {backtest_report_path}")
        if alpha_panel_path is not None:
            typer.echo(f"alpha_panel.csv: {alpha_panel_path}")
    except PortfolioOSError as exc:
        logger.error("%s", exc)
        raise typer.Exit(code=1) from exc


@alpha_research_app.command()
def main(
    returns_file: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    output_dir: Path = typer.Option(...),
    reversal_lookback_days: int = typer.Option(21),
    momentum_lookback_days: int = typer.Option(126),
    momentum_skip_days: int = typer.Option(21),
    forward_horizon_days: int = typer.Option(5),
    reversal_weight: float = typer.Option(0.5),
    momentum_weight: float = typer.Option(0.5),
    min_assets_per_date: int = typer.Option(10),
    quantiles: int = typer.Option(5),
) -> None:
    """Run the deterministic baseline alpha research workflow."""

    logger = configure_logging()
    try:
        result = run_alpha_research(
            returns_file=returns_file,
            output_dir=output_dir,
            reversal_lookback_days=reversal_lookback_days,
            momentum_lookback_days=momentum_lookback_days,
            momentum_skip_days=momentum_skip_days,
            forward_horizon_days=forward_horizon_days,
            reversal_weight=reversal_weight,
            momentum_weight=momentum_weight,
            min_assets_per_date=min_assets_per_date,
            quantiles=quantiles,
        )
        typer.echo(f"alpha_signal_panel.csv: {result.output_dir / 'alpha_signal_panel.csv'}")
        typer.echo(f"alpha_ic_by_date.csv: {result.output_dir / 'alpha_ic_by_date.csv'}")
        typer.echo(f"alpha_signal_summary.csv: {result.output_dir / 'alpha_signal_summary.csv'}")
        typer.echo(f"alpha_research_summary.json: {result.output_dir / 'alpha_research_summary.json'}")
        typer.echo(f"alpha_research_report.md: {result.output_dir / 'alpha_research_report.md'}")
    except PortfolioOSError as exc:
        logger.error("%s", exc)
        raise typer.Exit(code=1) from exc


@alpha_acceptance_app.command()
def main(
    returns_file: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    output_dir: Path = typer.Option(...),
    max_rounds: int = typer.Option(3),
) -> None:
    """Run the Phase 1 alpha acceptance gate on one frozen returns snapshot."""

    logger = configure_logging()
    try:
        result = run_alpha_acceptance_gate(
            returns_file=returns_file,
            output_dir=output_dir,
            max_rounds=max_rounds,
        )
        typer.echo(f"alpha_sweep_summary.csv: {result.output_dir / 'alpha_sweep_summary.csv'}")
        typer.echo(f"alpha_sweep_manifest.json: {result.output_dir / 'alpha_sweep_manifest.json'}")
        typer.echo(f"alpha_acceptance_decision.json: {result.output_dir / 'alpha_acceptance_decision.json'}")
        typer.echo(f"alpha_acceptance_note.md: {result.output_dir / 'alpha_acceptance_note.md'}")
    except PortfolioOSError as exc:
        logger.error("%s", exc)
        raise typer.Exit(code=1) from exc


@backtest_sweep_app.command()
def main(
    manifest: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    output_dir: Path = typer.Option(...),
    cost_bundle_multiplier: list[float] = typer.Option(
        [0.1, 0.3, 0.5, 1.0, 2.0],
        help="Repeatable multiplier that scales transaction_cost, transaction_fee, turnover_penalty, and slippage_penalty together.",
    ),
) -> None:
    """Run a deterministic cost bundle parameter sweep on top of one backtest manifest."""

    logger = configure_logging()
    try:
        result = run_backtest_cost_sweep(
            manifest_path=manifest,
            output_dir=output_dir,
            cost_bundle_multipliers=list(cost_bundle_multiplier),
        )
        typer.echo(f"sweep_summary.csv: {result.output_dir / 'sweep_summary.csv'}")
        typer.echo(f"efficient_frontier_report.md: {result.output_dir / 'efficient_frontier_report.md'}")
        typer.echo(f"backtest_sweep_manifest.json: {result.output_dir / 'backtest_sweep_manifest.json'}")
    except PortfolioOSError as exc:
        logger.error("%s", exc)
        raise typer.Exit(code=1) from exc


@risk_sweep_app.command()
def main(
    manifest: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    output_dir: Path = typer.Option(...),
    risk_aversion_multiplier: list[float] = typer.Option(
        [1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0],
        help="Repeatable multiplier that scales risk_term objective weight.",
    ),
) -> None:
    """Run a deterministic risk aversion parameter sweep on top of one backtest manifest."""

    logger = configure_logging()
    try:
        result = run_backtest_risk_sweep(
            manifest_path=manifest,
            output_dir=output_dir,
            risk_aversion_multipliers=list(risk_aversion_multiplier),
        )
        typer.echo(f"risk_sweep_summary.csv: {result.output_dir / 'risk_sweep_summary.csv'}")
        typer.echo(f"risk_aversion_frontier_report.md: {result.output_dir / 'risk_aversion_frontier_report.md'}")
        typer.echo(f"risk_sweep_manifest.json: {result.output_dir / 'risk_sweep_manifest.json'}")
    except PortfolioOSError as exc:
        logger.error("%s", exc)
        raise typer.Exit(code=1) from exc


@build_market_app.command()
def main(
    tickers_file: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    as_of_date: str = typer.Option(...),
    provider: str = typer.Option("mock"),
    provider_token: str | None = typer.Option(
        None,
        help="Provider token. For tushare this overrides the TUSHARE_TOKEN environment variable.",
    ),
    output: Path = typer.Option(...),
) -> None:
    """Build a standard `market.csv` from a ticker list and provider."""

    logger = configure_logging()
    provider_instance = None
    manifest_path = builder_manifest_path(output)
    try:
        provider_instance = get_data_provider(provider, provider_token=provider_token)
        tickers = load_tickers_file(tickers_file)
        frame = build_market_frame(
            provider=provider_instance,
            tickers=tickers,
            as_of_date=as_of_date,
        )
        write_market_csv(frame, output)
        write_json(
            manifest_path,
            build_market_manifest(
                provider=provider_instance,
                as_of_date=as_of_date,
                tickers_file=tickers_file,
                output_path=output,
                tickers=tickers,
            ),
        )
        typer.echo(f"market.csv: {output}")
        typer.echo(f"market_manifest.json: {manifest_path}")
    except PortfolioOSError as exc:
        if provider_instance is not None:
            tickers = load_tickers_file(tickers_file)
            write_json(
                manifest_path,
                build_market_manifest(
                    provider=provider_instance,
                    as_of_date=as_of_date,
                    tickers_file=tickers_file,
                    output_path=output,
                    tickers=tickers,
                    build_status=classify_builder_error(exc),
                    error_message=str(exc),
                ),
            )
            hint = _builder_error_hint(provider_instance, "market")
            logger.error("%s", _merge_error_and_hint(str(exc), hint))
        else:
            logger.error("%s", exc)
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc


@build_reference_app.command()
def main(
    tickers_file: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    as_of_date: str = typer.Option(...),
    provider: str = typer.Option("mock"),
    provider_token: str | None = typer.Option(
        None,
        help="Provider token. For tushare this overrides the TUSHARE_TOKEN environment variable.",
    ),
    output: Path = typer.Option(...),
    overlay: Path | None = typer.Option(
        None,
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Optional overlay file for blacklist and manager aggregate fields.",
    ),
) -> None:
    """Build a standard `reference.csv` from a ticker list and provider."""

    logger = configure_logging()
    provider_instance = None
    manifest_path = builder_manifest_path(output)
    try:
        provider_instance = get_data_provider(provider, provider_token=provider_token)
        tickers = load_tickers_file(tickers_file)
        frame = build_reference_frame(
            provider=provider_instance,
            tickers=tickers,
            as_of_date=as_of_date,
            overlay_path=overlay,
        )
        write_reference_csv(frame, output)
        write_json(
            manifest_path,
            build_reference_manifest(
                provider=provider_instance,
                as_of_date=as_of_date,
                tickers_file=tickers_file,
                overlay_path=overlay,
                output_path=output,
                frame=frame,
            ),
        )
        typer.echo(f"reference.csv: {output}")
        typer.echo(f"reference_manifest.json: {manifest_path}")
    except PortfolioOSError as exc:
        if provider_instance is not None:
            tickers = load_tickers_file(tickers_file)
            write_json(
                manifest_path,
                build_reference_manifest(
                    provider=provider_instance,
                    as_of_date=as_of_date,
                    tickers_file=tickers_file,
                    overlay_path=overlay,
                    output_path=output,
                    frame=pd.DataFrame(columns=["ticker"]),
                    build_status=classify_builder_error(exc),
                    error_message=str(exc),
                ),
            )
            hint = _builder_error_hint(provider_instance, "reference")
            logger.error("%s", _merge_error_and_hint(str(exc), hint))
        else:
            logger.error("%s", exc)
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc


@build_target_app.command()
def main(
    index_code: str = typer.Option(...),
    as_of_date: str = typer.Option(...),
    provider: str = typer.Option("mock"),
    provider_token: str | None = typer.Option(
        None,
        help="Provider token. For tushare this overrides the TUSHARE_TOKEN environment variable.",
    ),
    output: Path = typer.Option(...),
) -> None:
    """Build a standard `target.csv` from provider index weights."""

    logger = configure_logging()
    provider_instance = None
    try:
        provider_instance = get_data_provider(provider, provider_token=provider_token)
        frame, manifest = build_target_frame(
            provider=provider_instance,
            index_code=index_code,
            as_of_date=as_of_date,
        )
        write_target_csv(frame, output)
        manifest_path = target_manifest_path(output)
        write_json(
            manifest_path,
            build_target_manifest(
                provider=provider_instance,
                as_of_date=as_of_date,
                index_code=index_code,
                output_path=output,
                details=manifest,
                frame=frame,
            ),
        )
        typer.echo(f"target.csv: {output}")
        typer.echo(f"target_manifest.json: {manifest_path}")
    except PortfolioOSError as exc:
        manifest_path = target_manifest_path(output)
        if provider_instance is not None:
            write_json(
                manifest_path,
                build_target_manifest(
                    provider=provider_instance,
                    as_of_date=as_of_date,
                    index_code=index_code,
                    output_path=output,
                    details={
                        "index_code": index_code,
                        "input_weight_sum": None,
                        "output_weight_sum": None,
                        "normalized": False,
                        "normalization_tolerance": 0.02,
                    },
                    frame=pd.DataFrame(columns=["ticker", "target_weight"]),
                    build_status=classify_builder_error(exc),
                    error_message=str(exc),
                ),
            )
            hint = _builder_error_hint(provider_instance, "target")
            logger.error("%s", _merge_error_and_hint(str(exc), hint))
            typer.echo(f"target_manifest.json: {manifest_path}")
        else:
            logger.error("%s", exc)
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc


@build_snapshot_app.command()
def main(
    tickers_file: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    index_code: str = typer.Option(...),
    as_of_date: str = typer.Option(...),
    provider: str = typer.Option("mock"),
    provider_token: str | None = typer.Option(
        None,
        help="Provider token. For tushare this overrides the TUSHARE_TOKEN environment variable.",
    ),
    reference_overlay: Path | None = typer.Option(
        None,
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Optional local overlay file for reference blacklist and manager aggregate fields.",
    ),
    allow_partial_build: bool = typer.Option(
        False,
        help="Keep successful partial snapshot outputs even if one child step fails.",
    ),
    output_dir: Path = typer.Option(...),
) -> None:
    """Build a full static snapshot bundle for PortfolioOS."""

    logger = configure_logging()
    try:
        provider_instance = get_data_provider(provider, provider_token=provider_token)
        bundle = build_snapshot_bundle(
            provider=provider_instance,
            tickers_file=tickers_file,
            index_code=index_code,
            as_of_date=as_of_date,
            output_dir=output_dir,
            reference_overlay=reference_overlay,
            allow_partial_build=allow_partial_build,
        )
        if bundle["market_path"] is not None:
            typer.echo(f"market.csv: {bundle['market_path']}")
        if bundle["reference_path"] is not None:
            typer.echo(f"reference.csv: {bundle['reference_path']}")
        if bundle["target_path"] is not None:
            typer.echo(f"target.csv: {bundle['target_path']}")
        typer.echo(f"snapshot_manifest.json: {bundle['snapshot_manifest_path']}")
    except PortfolioOSError as exc:
        logger.error("%s", exc)
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc


@app.command()
def main(
    holdings: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    target: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    market: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    reference: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    portfolio_state: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    constraints: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    output_dir: Path = typer.Option(...),
    config: Path = typer.Option(Path("config/default.yaml"), exists=True, file_okay=True, dir_okay=False),
    execution_profile: Path = typer.Option(
        Path("config/execution/conservative.yaml"),
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
    import_profile: Path | None = typer.Option(
        None,
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Optional declarative import profile for mapped pilot input files.",
    ),
    skip_benchmarks: bool = typer.Option(
        False,
        help="Skip benchmark comparison output generation.",
    ),
) -> None:
    """Run the full PortfolioOS MVP pipeline."""

    logger = configure_logging()
    try:
        run_artifacts = prepare_run_artifacts(output_dir)
        workflow_result = run_single_rebalance(
            holdings=holdings,
            target=target,
            market=market,
            reference=reference,
            portfolio_state=portfolio_state,
            constraints=constraints,
            config=config,
            execution_profile=execution_profile,
            import_profile=import_profile,
            skip_benchmarks=skip_benchmarks,
            run_id=run_artifacts.run_id,
            created_at=run_artifacts.created_at,
        )
        export_basket_csv(workflow_result.rebalance_run.basket, run_artifacts.orders_path)
        export_basket_oms_csv(
            basket=workflow_result.rebalance_run.basket,
            findings=workflow_result.rebalance_run.findings,
            config=workflow_result.app_config,
            basket_id=run_artifacts.run_id,
            path=run_artifacts.orders_oms_path,
        )
        write_json(run_artifacts.audit_path, workflow_result.audit_payload)
        write_text(run_artifacts.summary_path, workflow_result.summary_markdown)
        if workflow_result.benchmark_payload is not None and workflow_result.benchmark_markdown is not None:
            write_json(run_artifacts.benchmark_json_path, workflow_result.benchmark_payload)
            write_text(run_artifacts.benchmark_markdown_path, workflow_result.benchmark_markdown)
        write_json(
            run_artifacts.manifest_path,
            {
                "run_id": run_artifacts.run_id,
                "created_at": run_artifacts.created_at,
                "orders_path": run_artifacts.orders_path,
                "orders_oms_path": run_artifacts.orders_oms_path,
                "audit_path": run_artifacts.audit_path,
                "summary_path": run_artifacts.summary_path,
                "benchmark_json_path": (
                    run_artifacts.benchmark_json_path if workflow_result.benchmark_payload is not None else None
                ),
                "benchmark_markdown_path": (
                    run_artifacts.benchmark_markdown_path if workflow_result.benchmark_markdown is not None else None
                ),
                "import_profile": str(import_profile) if import_profile is not None else None,
                "benchmarks_generated": workflow_result.benchmark_payload is not None,
            },
        )

        typer.echo(f"Run completed: {run_artifacts.run_id}")
        typer.echo(f"orders.csv: {run_artifacts.orders_path}")
        typer.echo(f"orders_oms.csv: {run_artifacts.orders_oms_path}")
        typer.echo(f"audit.json: {run_artifacts.audit_path}")
        typer.echo(f"summary.md: {run_artifacts.summary_path}")
        if workflow_result.benchmark_payload is not None and workflow_result.benchmark_markdown is not None:
            typer.echo(f"benchmark_comparison.json: {run_artifacts.benchmark_json_path}")
            typer.echo(f"benchmark_comparison.md: {run_artifacts.benchmark_markdown_path}")
    except PortfolioOSError as exc:
        logger.error("%s", exc)
        raise typer.Exit(code=1) from exc


@execution_app.command()
def main(
    request: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    output_dir: Path = typer.Option(...),
    calibration_profile: Path | None = typer.Option(
        None,
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Optional execution calibration profile used when resolving the intraday curve and defaults.",
    ),
) -> None:
    """Run a lightweight intraday execution simulation for a frozen basket."""

    logger = configure_logging()
    try:
        execution_artifacts = prepare_execution_artifacts(output_dir)
        simulation_result = run_execution_simulation(
            request,
            run_id=execution_artifacts.run_id,
            created_at=execution_artifacts.created_at,
            calibration_profile_path=calibration_profile,
        )
        strict_stress_profile_path = Path("config/calibration_profiles/low_liquidity_stress_strict.yaml").resolve()
        default_stress_profile_path = Path("config/calibration_profiles/low_liquidity_stress.yaml").resolve()
        stress_profile_path = (
            strict_stress_profile_path
            if strict_stress_profile_path.exists()
            else default_stress_profile_path
        )
        stress_result = None
        if stress_profile_path.exists():
            baseline_profile_path = Path(
                simulation_result.resolved_calibration["selected_profile"]["path"]
            ).resolve()
            if baseline_profile_path != stress_profile_path:
                stress_result = run_execution_simulation(
                    request,
                    run_id=f"{execution_artifacts.run_id}_stress",
                    created_at=execution_artifacts.created_at,
                    calibration_profile_path=stress_profile_path,
                )

        execution_report_payload = build_execution_report_payload(
            simulation_result,
            stress_result=stress_result,
        )
        execution_report_markdown = render_execution_report_markdown(
            simulation_result,
            stress_result=stress_result,
        )
        execution_fills_frame = build_execution_fills_frame(simulation_result)
        execution_child_orders_frame = build_execution_child_orders_frame(simulation_result)
        execution_handoff_checklist = render_execution_handoff_checklist(
            simulation_result,
            approval_record=load_optional_json(simulation_result.request_metadata.get("approval_record")),
            freeze_manifest=load_optional_json(
                Path(simulation_result.request_metadata["artifact_dir"]) / "freeze_manifest.json"
            ),
            audit_payload=load_optional_json(simulation_result.request_metadata.get("audit")),
        )

        write_json(execution_artifacts.execution_report_json_path, execution_report_payload)
        write_text(execution_artifacts.execution_report_markdown_path, execution_report_markdown)
        execution_fills_frame.to_csv(execution_artifacts.execution_fills_path, index=False)
        execution_child_orders_frame.to_csv(execution_artifacts.execution_child_orders_path, index=False)
        write_text(execution_artifacts.handoff_checklist_path, execution_handoff_checklist)
        write_json(
            execution_artifacts.manifest_path,
            {
                "run_id": execution_artifacts.run_id,
                "created_at": execution_artifacts.created_at,
                "request": str(request),
                "calibration_profile": str(calibration_profile) if calibration_profile is not None else None,
                "stress_calibration_profile": str(stress_profile_path) if stress_result is not None else None,
                "stress_test_generated": stress_result is not None,
                "execution_report_json_path": execution_artifacts.execution_report_json_path,
                "execution_report_markdown_path": execution_artifacts.execution_report_markdown_path,
                "execution_fills_path": execution_artifacts.execution_fills_path,
                "execution_child_orders_path": execution_artifacts.execution_child_orders_path,
                "handoff_checklist_path": execution_artifacts.handoff_checklist_path,
            },
        )

        typer.echo(f"Execution simulation completed: {execution_artifacts.run_id}")
        typer.echo(f"execution_report.json: {execution_artifacts.execution_report_json_path}")
        typer.echo(f"execution_report.md: {execution_artifacts.execution_report_markdown_path}")
        typer.echo(f"execution_fills.csv: {execution_artifacts.execution_fills_path}")
        typer.echo(f"execution_child_orders.csv: {execution_artifacts.execution_child_orders_path}")
        typer.echo(f"handoff_checklist.md: {execution_artifacts.handoff_checklist_path}")
    except PortfolioOSError as exc:
        logger.error("%s", exc)
        raise typer.Exit(code=1) from exc


@replay_app.command()
def main(
    manifest: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    constraints: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    output_dir: Path = typer.Option(...),
    config: Path = typer.Option(Path("config/default.yaml"), exists=True, file_okay=True, dir_okay=False),
    execution_profile: Path = typer.Option(
        Path("config/execution/conservative.yaml"),
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
    import_profile: Path | None = typer.Option(
        None,
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Optional declarative import profile for mapped replay sample inputs.",
    ),
) -> None:
    """Run a batch replay suite of static snapshots."""

    logger = configure_logging()
    try:
        replay_suite = run_replay_suite(
            manifest_path=manifest,
            constraints_path=constraints,
            config_path=config,
            execution_profile_path=execution_profile,
            import_profile_path=import_profile,
        )
        replay_artifacts = prepare_replay_artifacts(output_dir)
        sample_results_root = Path(replay_artifacts.sample_results_dir)
        suite_payload = deepcopy(replay_suite.suite_results_payload)

        for sample in suite_payload["samples"]:
            sample_name = sample["sample_name"]
            sample_output_dir = sample_results_root / sample_name
            sample_output_dir.mkdir(parents=True, exist_ok=True)
            sample_run = next(
                item for item in replay_suite.sample_runs if item.sample_name == sample_name
            )
            sample_json_path = sample_output_dir / "benchmark_comparison.json"
            sample_markdown_path = sample_output_dir / "benchmark_comparison.md"
            write_json(sample_json_path, sample_run.comparison_payload)
            write_text(sample_markdown_path, sample_run.comparison_markdown)
            sample["output_files"] = {
                "benchmark_comparison_json": str(sample_json_path),
                "benchmark_comparison_markdown": str(sample_markdown_path),
            }

        write_json(replay_artifacts.suite_results_path, suite_payload)
        write_text(replay_artifacts.suite_summary_path, replay_suite.suite_summary_markdown)
        write_json(
            replay_artifacts.manifest_path,
            {
                "run_id": replay_artifacts.run_id,
                "created_at": replay_artifacts.created_at,
                "suite_results_path": replay_artifacts.suite_results_path,
                "suite_summary_path": replay_artifacts.suite_summary_path,
                "sample_results_dir": replay_artifacts.sample_results_dir,
                "source_manifest": str(manifest),
                "import_profile": str(import_profile) if import_profile is not None else None,
            },
        )

        typer.echo(f"Replay completed: {replay_artifacts.run_id}")
        typer.echo(f"suite_results.json: {replay_artifacts.suite_results_path}")
        typer.echo(f"suite_summary.md: {replay_artifacts.suite_summary_path}")
        typer.echo(f"sample_results: {replay_artifacts.sample_results_dir}")
    except PortfolioOSError as exc:
        logger.error("%s", exc)
        raise typer.Exit(code=1) from exc

@scenario_app.command()
def main(
    manifest: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    output_dir: Path = typer.Option(...),
    import_profile: Path | None = typer.Option(
        None,
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Optional declarative import profile for mapped scenario base inputs.",
    ),
    write_per_scenario_artifacts: bool = typer.Option(
        True,
        help="Write per-scenario orders, OMS export, audit, and summary files.",
    ),
) -> None:
    """Run a what-if scenario suite on one shared snapshot."""

    logger = configure_logging()
    try:
        scenario_suite = run_scenario_suite(manifest, import_profile_path=import_profile)
        scenario_artifacts = prepare_scenario_artifacts(output_dir)
        scenario_results_root = Path(scenario_artifacts.scenario_results_dir)

        if write_per_scenario_artifacts:
            for scenario_run in scenario_suite.scenario_runs:
                scenario_output_dir = scenario_results_root / scenario_run.scenario.id
                scenario_output_dir.mkdir(parents=True, exist_ok=True)
                summary_markdown = render_summary_markdown(
                    scenario_run.summary,
                    scenario_run.rebalance_run.findings,
                )
                export_readiness = run_posttrade_checks(
                    scenario_run.rebalance_run.orders,
                    scenario_run.rebalance_run.findings,
                )
                constraint_snapshot = scenario_run.config.build_constraint_snapshot(
                    scenario_run.scenario.constraints
                )
                audit_payload = build_audit_payload(
                    input_paths={
                        **{name: str(path) for name, path in scenario_suite.base_input_paths.items()},
                        "constraints": str(scenario_run.constraints_path),
                        "execution_profile": str(scenario_run.execution_profile_path),
                        **({"import_profile": str(import_profile)} if import_profile is not None else {}),
                    },
                    config=scenario_run.config,
                    constraint_snapshot=constraint_snapshot,
                    findings=scenario_run.rebalance_run.findings,
                    basket=scenario_run.rebalance_run.basket,
                    summary=scenario_run.summary,
                    run_id=scenario_artifacts.run_id,
                    created_at=scenario_artifacts.created_at,
                    export_readiness=export_readiness,
                    optimization_metadata={
                        "solver_used": scenario_run.rebalance_run.optimization_result.solver_used,
                        "solver_fallback_used": scenario_run.rebalance_run.optimization_result.solver_fallback_used,
                        "constraint_residual_max": scenario_run.rebalance_run.optimization_result.constraint_residual_max,
                        "solver_status": scenario_run.rebalance_run.optimization_result.status,
                        "objective_decomposition": scenario_run.rebalance_run.optimization_result.objective_decomposition,
                    },
                )
                export_basket_csv(
                    scenario_run.rebalance_run.basket,
                    scenario_output_dir / "orders.csv",
                )
                export_basket_oms_csv(
                    basket=scenario_run.rebalance_run.basket,
                    findings=scenario_run.rebalance_run.findings,
                    config=scenario_run.config,
                    basket_id=f"{scenario_artifacts.run_id}_{scenario_run.scenario.id}",
                    path=scenario_output_dir / "orders_oms.csv",
                )
                write_json(scenario_output_dir / "audit.json", audit_payload)
                write_text(scenario_output_dir / "summary.md", summary_markdown)

        write_json(
            scenario_artifacts.scenario_comparison_json_path,
            scenario_suite.scenario_comparison_payload,
        )
        write_text(
            scenario_artifacts.scenario_comparison_markdown_path,
            scenario_suite.scenario_comparison_markdown,
        )
        write_text(
            scenario_artifacts.decision_pack_path,
            scenario_suite.decision_pack_markdown,
        )
        write_json(
            scenario_artifacts.manifest_path,
            {
                "run_id": scenario_artifacts.run_id,
                "created_at": scenario_artifacts.created_at,
                "source_manifest": str(manifest),
                "scenario_comparison_json_path": scenario_artifacts.scenario_comparison_json_path,
                "scenario_comparison_markdown_path": scenario_artifacts.scenario_comparison_markdown_path,
                "decision_pack_path": scenario_artifacts.decision_pack_path,
                "scenario_results_dir": scenario_artifacts.scenario_results_dir,
                "import_profile": str(import_profile) if import_profile is not None else None,
                "write_per_scenario_artifacts": write_per_scenario_artifacts,
            },
        )

        typer.echo(f"Scenario analysis completed: {scenario_artifacts.run_id}")
        typer.echo(f"scenario_comparison.json: {scenario_artifacts.scenario_comparison_json_path}")
        typer.echo(f"scenario_comparison.md: {scenario_artifacts.scenario_comparison_markdown_path}")
        typer.echo(f"decision_pack.md: {scenario_artifacts.decision_pack_path}")
        if write_per_scenario_artifacts:
            typer.echo(f"scenario_results: {scenario_artifacts.scenario_results_dir}")
    except PortfolioOSError as exc:
        logger.error("%s", exc)
        raise typer.Exit(code=1) from exc


def _run_approval_workflow(
    *,
    request: Path,
    output_dir: Path,
) -> None:
    """Run the approval workflow and materialize freeze artifacts."""

    logger = configure_logging()
    try:
        evaluation = evaluate_approval_request(request)
        approval_artifacts = prepare_approval_artifacts(output_dir)
        approval_record = build_approval_record_payload(
            evaluation,
            created_at=approval_artifacts.created_at,
        )
        approval_summary = build_approval_summary_markdown(
            evaluation,
            approval_record,
        )
        approval_handoff_checklist = render_approval_handoff_checklist(
            evaluation,
            approval_record,
        )
        can_freeze = evaluation.approval_status in {"approved", "approved_with_override"}
        if can_freeze:
            freeze_manifest = freeze_selected_scenario(
                evaluation,
                output_dir=output_dir,
                created_at=approval_artifacts.created_at,
            )
        else:
            freeze_manifest = {
                "created_at": approval_artifacts.created_at,
                "approval_status": evaluation.approval_status,
                "selected_scenario": evaluation.selected_scenario_id,
                "override_used": bool(approval_record.get("override_used", False)),
                "override": approval_record.get("override", {}),
                "source_artifacts": {
                    key: file_metadata(path_obj)
                    for key, path_obj in evaluation.selected_artifact_paths.items()
                },
                "final_artifacts": {},
            }

        write_json(approval_artifacts.approval_record_path, approval_record)
        write_text(approval_artifacts.approval_summary_path, approval_summary)
        write_text(approval_artifacts.handoff_checklist_path, approval_handoff_checklist)
        write_json(approval_artifacts.freeze_manifest_path, freeze_manifest)
        write_json(
            approval_artifacts.manifest_path,
            {
                "run_id": approval_artifacts.run_id,
                "created_at": approval_artifacts.created_at,
                "request": str(request),
                "approval_record_path": approval_artifacts.approval_record_path,
                "approval_summary_path": approval_artifacts.approval_summary_path,
                "freeze_manifest_path": approval_artifacts.freeze_manifest_path,
                "handoff_checklist_path": approval_artifacts.handoff_checklist_path,
                "approval_status": evaluation.approval_status,
            },
        )

        typer.echo(f"Approval workflow completed: {approval_artifacts.run_id}")
        typer.echo(f"approval_record.json: {approval_artifacts.approval_record_path}")
        typer.echo(f"approval_summary.md: {approval_artifacts.approval_summary_path}")
        typer.echo(f"freeze_manifest.json: {approval_artifacts.freeze_manifest_path}")
        typer.echo(f"handoff_checklist.md: {approval_artifacts.handoff_checklist_path}")
        if can_freeze:
            typer.echo(f"final_orders.csv: {approval_artifacts.final_orders_path}")
            typer.echo(f"final_orders_oms.csv: {approval_artifacts.final_orders_oms_path}")
            typer.echo(f"final_audit.json: {approval_artifacts.final_audit_path}")
            typer.echo(f"final_summary.md: {approval_artifacts.final_summary_path}")
        else:
            raise typer.Exit(code=1)
    except PortfolioOSError as exc:
        logger.error("%s", exc)
        raise typer.Exit(code=1) from exc


@approval_app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    request: Path | None = typer.Option(None, exists=True, file_okay=True, dir_okay=False),
    output_dir: Path | None = typer.Option(None),
) -> None:
    """Approve and freeze a final execution package from scenario analysis."""

    if ctx.invoked_subcommand is not None:
        return
    if request is None:
        raise typer.BadParameter("Missing required option.", param_hint="--request")
    if output_dir is None:
        raise typer.BadParameter("Missing required option.", param_hint="--output-dir")
    _run_approval_workflow(
        request=request,
        output_dir=output_dir,
    )


@approval_app.command("template")
def template(
    scenario_output_dir: Path = typer.Option(..., exists=True, file_okay=False, dir_okay=True),
    output: Path = typer.Option(...),
    selected_scenario: str | None = typer.Option(
        None,
        help="Optional explicit scenario_id. Defaults to recommended scenario from scenario_comparison.json.",
    ),
) -> None:
    """Generate a draft approval request YAML from scenario outputs."""

    logger = configure_logging()
    try:
        payload = build_approval_request_template_payload(
            scenario_output_dir=scenario_output_dir,
            selected_scenario=selected_scenario,
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            yaml.safe_dump(payload, sort_keys=False, allow_unicode=False),
            encoding="utf-8",
        )
        typer.echo(f"approval_request_template.yaml: {output}")
    except PortfolioOSError as exc:
        logger.error("%s", exc)
        raise typer.Exit(code=1) from exc


if __name__ == "__main__":
    app()
