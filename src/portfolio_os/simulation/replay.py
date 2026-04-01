"""Batch replay helpers for static snapshot suites."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from portfolio_os.compliance.findings import summarize_findings
from portfolio_os.data.import_profiles import ImportProfile, load_import_profile
from portfolio_os.data.loaders import read_yaml
from portfolio_os.data.market import load_market_snapshot, market_to_frame
from portfolio_os.data.portfolio import build_portfolio_frame, load_holdings, load_portfolio_state, load_target_weights
from portfolio_os.data.reference import load_reference_snapshot, reference_to_frame
from portfolio_os.data.universe import build_universe_frame
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.explain.trade_reason import reason_label_from_finding, summarize_reason_counts
from portfolio_os.simulation.benchmarks import (
    BenchmarkSuiteResult,
    build_benchmark_comparison_payload,
    render_benchmark_comparison_markdown,
    run_benchmark_suite,
)
from portfolio_os.storage.snapshots import file_metadata
from portfolio_os.utils.config import AppConfig, load_app_config


class ReplayManifest(BaseModel):
    """Replay-suite manifest."""

    name: str
    description: str | None = None
    samples: list[str] = Field(default_factory=list)


@dataclass
class ReplaySampleRun:
    """One sample's benchmark comparison payload and supporting objects."""

    sample_name: str
    input_paths: dict[str, Path]
    config: AppConfig
    universe: pd.DataFrame
    benchmark_suite: BenchmarkSuiteResult
    comparison_payload: dict[str, Any]
    comparison_markdown: str
    differences: dict[str, dict[str, float]]


@dataclass
class ReplaySuiteRun:
    """Replay-suite result for multiple static samples."""

    manifest: ReplayManifest
    manifest_path: Path
    sample_runs: list[ReplaySampleRun]
    import_profile_path: Path | None
    aggregate_summary: dict[str, Any]
    suite_results_payload: dict[str, Any]
    suite_summary_markdown: str


def load_replay_manifest(path: str | Path) -> ReplayManifest:
    """Load and validate a replay-suite manifest."""

    payload = read_yaml(path)
    manifest = ReplayManifest.model_validate(payload)
    if not manifest.samples:
        raise InputValidationError("Replay manifest must include at least one sample.")
    return manifest


def resolve_sample_input_paths(manifest_path: str | Path, sample_name: str) -> dict[str, Path]:
    """Resolve the required files for one replay sample."""

    manifest_dir = Path(manifest_path).resolve().parent
    sample_dir = manifest_dir / sample_name
    expected_files = {
        "holdings": sample_dir / "holdings.csv",
        "target": sample_dir / "target.csv",
        "market": sample_dir / "market.csv",
        "reference": sample_dir / "reference.csv",
        "portfolio_state": sample_dir / "portfolio_state.yaml",
    }
    missing = [name for name, path in expected_files.items() if not path.exists()]
    if missing:
        raise InputValidationError(
            f"Replay sample {sample_name} is missing required file(s): {', '.join(missing)}"
        )
    return expected_files


def load_sample_universe(
    *,
    input_paths: dict[str, Path],
    constraints_path: str | Path,
    config_path: str | Path,
    execution_profile_path: str | Path,
    import_profile: ImportProfile | None = None,
) -> tuple[AppConfig, pd.DataFrame]:
    """Load one static replay sample into the validated working universe."""

    holdings = load_holdings(input_paths["holdings"], import_profile=import_profile)
    targets = load_target_weights(input_paths["target"], import_profile=import_profile)
    portfolio_state = load_portfolio_state(input_paths["portfolio_state"])
    app_config = load_app_config(
        default_path=config_path,
        constraints_path=constraints_path,
        execution_path=execution_profile_path,
        portfolio_state=portfolio_state,
    )
    portfolio_frame = build_portfolio_frame(holdings, targets)
    required_tickers = portfolio_frame["ticker"].tolist()
    market_frame = market_to_frame(
        load_market_snapshot(input_paths["market"], required_tickers, import_profile=import_profile)
    )
    reference_frame = reference_to_frame(
        load_reference_snapshot(input_paths["reference"], required_tickers, import_profile=import_profile)
    )
    universe = build_universe_frame(portfolio_frame, market_frame, reference_frame, portfolio_state)
    return app_config, universe


def build_sample_differences(benchmark_suite: BenchmarkSuiteResult) -> dict[str, dict[str, float]]:
    """Build per-sample PortfolioOS versus baseline differences."""

    metrics_by_name = {
        metric.strategy_name: metric for metric in benchmark_suite.comparison.strategies
    }
    portfolio_os_metrics = metrics_by_name["portfolio_os_rebalance"]
    result: dict[str, dict[str, float]] = {}
    for baseline_name, key in (
        ("naive_target_rebalance", "portfolio_os_vs_naive"),
        ("cost_unaware_rebalance", "portfolio_os_vs_cost_unaware"),
    ):
        baseline_metrics = metrics_by_name[baseline_name]
        result[key] = {
            "cost_savings": float(
                baseline_metrics.estimated_total_cost - portfolio_os_metrics.estimated_total_cost
            ),
            "turnover_reduction": float(
                baseline_metrics.turnover - portfolio_os_metrics.turnover
            ),
            "blocked_trade_reduction": float(
                baseline_metrics.blocked_trade_count - portfolio_os_metrics.blocked_trade_count
            ),
            "finding_count_difference": float(
                baseline_metrics.compliance_finding_count
                - portfolio_os_metrics.compliance_finding_count
            ),
            "target_deviation_delta": float(
                portfolio_os_metrics.target_deviation_after
                - baseline_metrics.target_deviation_after
            ),
        }
    return result


def run_sample_benchmark(
    *,
    manifest_path: str | Path,
    sample_name: str,
    constraints_path: str | Path,
    config_path: str | Path,
    execution_profile_path: str | Path,
    import_profile: ImportProfile | None = None,
    import_profile_path: str | Path | None = None,
) -> ReplaySampleRun:
    """Run the benchmark suite for one replay sample."""

    input_paths = resolve_sample_input_paths(manifest_path, sample_name)
    app_config, universe = load_sample_universe(
        input_paths=input_paths,
        constraints_path=constraints_path,
        config_path=config_path,
        execution_profile_path=execution_profile_path,
        import_profile=import_profile,
    )
    benchmark_suite = run_benchmark_suite(universe, app_config)
    comparison_payload = build_benchmark_comparison_payload(
        input_paths={
            **{name: str(path) for name, path in input_paths.items()},
            **({"import_profile": str(import_profile_path)} if import_profile_path is not None else {}),
        },
        suite_result=benchmark_suite,
    )
    comparison_markdown = render_benchmark_comparison_markdown(benchmark_suite.comparison)
    differences = build_sample_differences(benchmark_suite)
    return ReplaySampleRun(
        sample_name=sample_name,
        input_paths=input_paths,
        config=app_config,
        universe=universe,
        benchmark_suite=benchmark_suite,
        comparison_payload=comparison_payload,
        comparison_markdown=comparison_markdown,
        differences=differences,
    )


def summarize_distribution(
    values: list[float],
    *,
    include_min_max: bool = False,
    include_positive_rate: bool = False,
    include_quartiles: bool = False,
) -> dict[str, float]:
    """Summarize a distribution with simple descriptive statistics."""

    array = np.asarray(values, dtype=float)
    if array.size == 0:
        summary = {"mean": 0.0, "median": 0.0}
        if include_min_max:
            summary["min"] = 0.0
            summary["max"] = 0.0
        if include_positive_rate:
            summary["positive_rate"] = 0.0
        if include_quartiles:
            summary["p25"] = 0.0
            summary["p75"] = 0.0
        return summary

    summary = {
        "mean": float(np.mean(array)),
        "median": float(np.median(array)),
    }
    if include_min_max:
        summary["min"] = float(np.min(array))
        summary["max"] = float(np.max(array))
    if include_positive_rate:
        summary["positive_rate"] = float(np.mean(array > 0))
    if include_quartiles:
        summary["p25"] = float(np.percentile(array, 25))
        summary["p75"] = float(np.percentile(array, 75))
    return summary


def explain_sample_difference(sample_run: ReplaySampleRun, comparison_key: str) -> str:
    """Return a short client-facing explanation for one sample highlight."""

    diff = sample_run.differences[comparison_key]
    if diff["blocked_trade_reduction"] > 0:
        return "benefit was helped by fewer blocked tickets in the PortfolioOS basket"
    if diff["turnover_reduction"] > 0:
        return "benefit was mainly driven by lower turnover and fewer high-friction trades"
    if diff["cost_savings"] > 0:
        return "benefit came from the cost-aware objective even with a similar trade count"
    return "benefit was limited because the baseline basket was already relatively close to the feasible set"


def summarize_portfolio_os_finding_patterns(sample_runs: list[ReplaySampleRun]) -> dict[str, Any]:
    """Aggregate finding and blocked-reason patterns across PortfolioOS runs."""

    blocking_category_counter: dict[str, int] = {}
    warning_category_counter: dict[str, int] = {}
    blocked_reason_counter: dict[str, int] = {}
    manager_warning_samples: list[str] = []

    for sample_run in sample_runs:
        portfolio_os_run = sample_run.benchmark_suite.runs["portfolio_os_rebalance"]
        for finding in portfolio_os_run.findings:
            if finding.blocking:
                blocking_category_counter[finding.category.value] = (
                    blocking_category_counter.get(finding.category.value, 0) + 1
                )
            if finding.severity.value == "WARNING":
                warning_category_counter[finding.category.value] = (
                    warning_category_counter.get(finding.category.value, 0) + 1
                )
            if finding.code in {"trade_blocked", "no_order_due_to_constraint"}:
                reason_label = reason_label_from_finding(finding)
                if reason_label is not None:
                    human_reason = summarize_reason_counts([finding], blocked_only=True)
                    for key, count in human_reason.items():
                        blocked_reason_counter[key] = blocked_reason_counter.get(key, 0) + count
            if finding.code == "manager_aggregate_limit":
                manager_warning_samples.append(sample_run.sample_name)

    def _top(counter: dict[str, int]) -> dict[str, Any]:
        if not counter:
            return {"label": "none", "count": 0}
        label = max(counter, key=counter.get)
        return {"label": label, "count": int(counter[label])}

    return {
        "most_common_blocking_category": _top(blocking_category_counter),
        "most_common_warning_category": _top(warning_category_counter),
        "most_common_blocked_reason": _top(blocked_reason_counter),
        "manager_aggregate_warning_samples": sorted(manager_warning_samples),
    }


def summarize_strategy_across_samples(
    sample_runs: list[ReplaySampleRun],
) -> dict[str, dict[str, Any]]:
    """Aggregate high-level metrics by strategy across replay samples."""

    strategies = {
        "naive_target_rebalance": [],
        "cost_unaware_rebalance": [],
        "portfolio_os_rebalance": [],
    }
    for sample_run in sample_runs:
        for metric in sample_run.benchmark_suite.comparison.strategies:
            strategies[metric.strategy_name].append(metric)

    summary: dict[str, dict[str, Any]] = {}
    for strategy_name, metrics in strategies.items():
        summary[strategy_name] = {
            "sample_count": len(metrics),
            "estimated_total_cost": summarize_distribution(
                [metric.estimated_total_cost for metric in metrics],
                include_min_max=True,
            ),
            "turnover": summarize_distribution([metric.turnover for metric in metrics], include_quartiles=True),
            "blocked_trade_count": summarize_distribution(
                [float(metric.blocked_trade_count) for metric in metrics]
            ),
            "compliance_finding_count": summarize_distribution(
                [float(metric.compliance_finding_count) for metric in metrics]
            ),
            "target_deviation_after": summarize_distribution(
                [metric.target_deviation_after for metric in metrics]
            ),
        }
    return summary


def summarize_comparison_differences(
    sample_runs: list[ReplaySampleRun],
    comparison_key: str,
) -> dict[str, Any]:
    """Aggregate PortfolioOS versus baseline differences across samples."""

    cost_savings = [sample_run.differences[comparison_key]["cost_savings"] for sample_run in sample_runs]
    turnover_reduction = [
        sample_run.differences[comparison_key]["turnover_reduction"] for sample_run in sample_runs
    ]
    blocked_trade_reduction = [
        sample_run.differences[comparison_key]["blocked_trade_reduction"] for sample_run in sample_runs
    ]
    finding_count_difference = [
        sample_run.differences[comparison_key]["finding_count_difference"] for sample_run in sample_runs
    ]
    target_deviation_delta = [
        sample_run.differences[comparison_key]["target_deviation_delta"] for sample_run in sample_runs
    ]
    return {
        "sample_count": len(sample_runs),
        "cost_savings": summarize_distribution(
            cost_savings,
            include_min_max=True,
            include_positive_rate=True,
            include_quartiles=True,
        ),
        "turnover_reduction": summarize_distribution(turnover_reduction, include_quartiles=True),
        "blocked_trade_reduction": summarize_distribution(blocked_trade_reduction),
        "finding_count_difference": summarize_distribution(finding_count_difference),
        "target_deviation_delta": summarize_distribution(target_deviation_delta),
    }


def build_replay_aggregate_summary(
    manifest: ReplayManifest,
    sample_runs: list[ReplaySampleRun],
) -> dict[str, Any]:
    """Build the aggregate replay-suite summary."""

    comparison_summaries = {
        "portfolio_os_vs_naive": summarize_comparison_differences(sample_runs, "portfolio_os_vs_naive"),
        "portfolio_os_vs_cost_unaware": summarize_comparison_differences(
            sample_runs, "portfolio_os_vs_cost_unaware"
        ),
    }
    strategy_summary = summarize_strategy_across_samples(sample_runs)
    best_sample = max(
        sample_runs,
        key=lambda sample_run: sample_run.differences["portfolio_os_vs_naive"]["cost_savings"],
    )
    worst_sample = min(
        sample_runs,
        key=lambda sample_run: sample_run.differences["portfolio_os_vs_naive"]["cost_savings"],
    )
    median_cost_saving = comparison_summaries["portfolio_os_vs_naive"]["cost_savings"]["median"]
    median_turnover_reduction = comparison_summaries["portfolio_os_vs_naive"]["turnover_reduction"]["median"]
    conclusion = (
        f"Across {len(sample_runs)} static replay samples, PortfolioOS reduced estimated trading cost by median "
        f"{median_cost_saving:.2f} and turnover by median {median_turnover_reduction:.4f} "
        "versus naive rebalance."
    )
    finding_patterns = summarize_portfolio_os_finding_patterns(sample_runs)
    best_blocked_trade_reduction_sample = max(
        sample_runs,
        key=lambda sample_run: sample_run.differences["portfolio_os_vs_naive"]["blocked_trade_reduction"],
    )
    return {
        "suite_name": manifest.name,
        "suite_description": manifest.description,
        "sample_count": len(sample_runs),
        "strategy_summary": strategy_summary,
        "comparisons": comparison_summaries,
        "finding_patterns": finding_patterns,
        "best_sample_vs_naive": {
            "sample_name": best_sample.sample_name,
            "cost_savings": float(best_sample.differences["portfolio_os_vs_naive"]["cost_savings"]),
            "explanation": explain_sample_difference(best_sample, "portfolio_os_vs_naive"),
        },
        "worst_sample_vs_naive": {
            "sample_name": worst_sample.sample_name,
            "cost_savings": float(worst_sample.differences["portfolio_os_vs_naive"]["cost_savings"]),
            "explanation": explain_sample_difference(worst_sample, "portfolio_os_vs_naive"),
        },
        "best_blocked_trade_reduction_sample": {
            "sample_name": best_blocked_trade_reduction_sample.sample_name,
            "blocked_trade_reduction": float(
                best_blocked_trade_reduction_sample.differences["portfolio_os_vs_naive"]["blocked_trade_reduction"]
            ),
            "explanation": explain_sample_difference(best_blocked_trade_reduction_sample, "portfolio_os_vs_naive"),
        },
        "conclusion": conclusion,
    }


def build_suite_results_payload(
    *,
    manifest: ReplayManifest,
    manifest_path: str | Path,
    sample_runs: list[ReplaySampleRun],
    aggregate_summary: dict[str, Any],
    import_profile_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build the suite JSON payload."""

    samples: list[dict[str, Any]] = []
    for sample_run in sample_runs:
        portfolio_os_run = sample_run.benchmark_suite.runs["portfolio_os_rebalance"]
        samples.append(
            {
                "sample_name": sample_run.sample_name,
                "inputs": {
                    name: file_metadata(path) for name, path in sample_run.input_paths.items()
                },
                "strategies": [
                    metric.model_dump(mode="json")
                    for metric in sample_run.benchmark_suite.comparison.strategies
                ],
                "comparison_summary": sample_run.benchmark_suite.comparison.comparison_summary,
                "differences": sample_run.differences,
                "portfolio_os_finding_summary": summarize_findings(portfolio_os_run.findings),
                "portfolio_os_blocked_reason_counts": summarize_reason_counts(
                    portfolio_os_run.findings,
                    blocked_only=True,
                ),
                "portfolio_os_repair_reason_counts": summarize_reason_counts(
                    portfolio_os_run.findings,
                    repaired_only=True,
                ),
            }
        )
    return {
        "suite": {
            "name": manifest.name,
            "description": manifest.description,
            "manifest": file_metadata(manifest_path),
            "import_profile": file_metadata(import_profile_path) if import_profile_path is not None else None,
            "sample_count": len(sample_runs),
        },
        "samples": samples,
        "aggregate_summary": aggregate_summary,
    }


def render_suite_summary_markdown(payload: dict[str, Any]) -> str:
    """Render a demo-friendly replay suite summary."""

    suite = payload["suite"]
    aggregate_summary = payload["aggregate_summary"]
    naive_summary = aggregate_summary["comparisons"]["portfolio_os_vs_naive"]
    cost_unaware_summary = aggregate_summary["comparisons"]["portfolio_os_vs_cost_unaware"]
    strategy_summary = aggregate_summary["strategy_summary"]
    best_sample = aggregate_summary["best_sample_vs_naive"]
    worst_sample = aggregate_summary["worst_sample_vs_naive"]
    best_blocked_sample = aggregate_summary["best_blocked_trade_reduction_sample"]
    finding_patterns = aggregate_summary["finding_patterns"]
    lines = [
        "# Replay Suite Summary",
        "",
        "> Auxiliary decision-support tool only. Not investment advice.",
        "",
        "## Suite",
        f"- name: {suite['name']}",
        f"- sample_count: {suite['sample_count']}",
        f"- description: {suite.get('description') or 'N/A'}",
        "",
        "## Strategy Overview",
        "",
        "| Strategy | Mean Cost | Median Cost | Mean Turnover | Median Turnover | Mean Blocked Trades | Median Findings |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for strategy_name in (
        "naive_target_rebalance",
        "cost_unaware_rebalance",
        "portfolio_os_rebalance",
    ):
        strategy = strategy_summary[strategy_name]
        lines.append(
            f"| {strategy_name} | {strategy['estimated_total_cost']['mean']:.2f} | "
            f"{strategy['estimated_total_cost']['median']:.2f} | {strategy['turnover']['mean']:.4f} | "
            f"{strategy['turnover']['median']:.4f} | {strategy['blocked_trade_count']['mean']:.2f} | "
            f"{strategy['compliance_finding_count']['median']:.2f} |"
        )

    lines.extend(
        [
            "",
            "## PortfolioOS vs Naive",
            f"- median_cost_savings: {naive_summary['cost_savings']['median']:.2f}",
            f"- mean_cost_savings: {naive_summary['cost_savings']['mean']:.2f}",
            f"- p25_cost_savings: {naive_summary['cost_savings']['p25']:.2f}",
            f"- p75_cost_savings: {naive_summary['cost_savings']['p75']:.2f}",
            f"- median_turnover_reduction: {naive_summary['turnover_reduction']['median']:.4f}",
            f"- p25_turnover_reduction: {naive_summary['turnover_reduction']['p25']:.4f}",
            f"- p75_turnover_reduction: {naive_summary['turnover_reduction']['p75']:.4f}",
            f"- median_blocked_trade_reduction: {naive_summary['blocked_trade_reduction']['median']:.2f}",
            f"- median_finding_count_difference: {naive_summary['finding_count_difference']['median']:.2f}",
            f"- positive_cost_savings_rate: {naive_summary['cost_savings']['positive_rate']:.2%}",
            "",
            "## PortfolioOS vs Cost-Unaware",
            f"- median_cost_savings: {cost_unaware_summary['cost_savings']['median']:.2f}",
            f"- mean_cost_savings: {cost_unaware_summary['cost_savings']['mean']:.2f}",
            f"- p25_cost_savings: {cost_unaware_summary['cost_savings']['p25']:.2f}",
            f"- p75_cost_savings: {cost_unaware_summary['cost_savings']['p75']:.2f}",
            f"- median_turnover_reduction: {cost_unaware_summary['turnover_reduction']['median']:.4f}",
            f"- p25_turnover_reduction: {cost_unaware_summary['turnover_reduction']['p25']:.4f}",
            f"- p75_turnover_reduction: {cost_unaware_summary['turnover_reduction']['p75']:.4f}",
            f"- median_blocked_trade_reduction: {cost_unaware_summary['blocked_trade_reduction']['median']:.2f}",
            f"- median_finding_count_difference: {cost_unaware_summary['finding_count_difference']['median']:.2f}",
            "",
            "## Findings And Constraints",
            f"- most_common_blocking_category: {finding_patterns['most_common_blocking_category']['label']}",
            f"- most_common_warning_category: {finding_patterns['most_common_warning_category']['label']}",
            f"- most_common_blocked_reason: {finding_patterns['most_common_blocked_reason']['label']}",
            f"- manager_aggregate_warning_samples: {', '.join(finding_patterns['manager_aggregate_warning_samples']) or 'none'}",
            "",
            "## Best And Worst Samples",
            f"- best_sample_vs_naive: {best_sample['sample_name']} ({best_sample['cost_savings']:.2f}) - {best_sample['explanation']}",
            f"- worst_sample_vs_naive: {worst_sample['sample_name']} ({worst_sample['cost_savings']:.2f}) - {worst_sample['explanation']}",
            f"- best_blocked_trade_reduction_sample: {best_blocked_sample['sample_name']} ({best_blocked_sample['blocked_trade_reduction']:.2f}) - {best_blocked_sample['explanation']}",
            "",
            "## Conclusion",
            f"{aggregate_summary['conclusion']} This gives a client-ready evidence pack that combines cost, execution friction, and structured control visibility across the suite.",
            "",
        ]
    )
    return "\n".join(lines)


def run_replay_suite(
    *,
    manifest_path: str | Path,
    constraints_path: str | Path,
    config_path: str | Path,
    execution_profile_path: str | Path,
    import_profile_path: str | Path | None = None,
) -> ReplaySuiteRun:
    """Run the full static replay suite defined by a manifest."""

    manifest = load_replay_manifest(manifest_path)
    resolved_import_profile_path = Path(import_profile_path).resolve() if import_profile_path is not None else None
    import_profile = load_import_profile(resolved_import_profile_path) if resolved_import_profile_path is not None else None
    sample_runs = [
        run_sample_benchmark(
            manifest_path=manifest_path,
            sample_name=sample_name,
            constraints_path=constraints_path,
            config_path=config_path,
            execution_profile_path=execution_profile_path,
            import_profile=import_profile,
            import_profile_path=resolved_import_profile_path,
        )
        for sample_name in manifest.samples
    ]
    aggregate_summary = build_replay_aggregate_summary(manifest, sample_runs)
    suite_results_payload = build_suite_results_payload(
        manifest=manifest,
        manifest_path=manifest_path,
        sample_runs=sample_runs,
        aggregate_summary=aggregate_summary,
        import_profile_path=resolved_import_profile_path,
    )
    suite_summary_markdown = render_suite_summary_markdown(suite_results_payload)
    return ReplaySuiteRun(
        manifest=manifest,
        manifest_path=Path(manifest_path),
        sample_runs=sample_runs,
        import_profile_path=resolved_import_profile_path,
        aggregate_summary=aggregate_summary,
        suite_results_payload=suite_results_payload,
        suite_summary_markdown=suite_summary_markdown,
    )
