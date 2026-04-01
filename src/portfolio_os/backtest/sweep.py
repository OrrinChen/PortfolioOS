"""Cost bundle parameter sweep built on top of the historical backtest."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from portfolio_os.backtest.engine import BacktestResult, run_backtest
from portfolio_os.backtest.manifest import load_backtest_manifest
from portfolio_os.backtest.report import render_cost_sweep_report, render_risk_sweep_report
from portfolio_os.storage.snapshots import write_json, write_text
from portfolio_os.utils.config import load_yaml_file


_COST_WEIGHT_KEYS = (
    "transaction_cost",
    "transaction_fee",
    "turnover_penalty",
    "slippage_penalty",
)


@dataclass
class CostSweepRunResult:
    """One scaled backtest run inside the sweep."""

    cost_bundle_multiplier: float
    run_dir: Path
    scaled_config_path: Path
    derived_manifest_path: Path
    scaled_objective_weights: dict[str, float]
    backtest_result: BacktestResult


@dataclass
class BacktestSweepResult:
    """Top-level sweep result with all archived runs."""

    base_manifest_path: Path
    output_dir: Path
    run_results: list[CostSweepRunResult]
    summary_frame: pd.DataFrame
    report_markdown: str


def _format_multiplier_label(multiplier: float) -> str:
    """Create a filesystem-safe stable label."""

    return f"{multiplier:.3f}".rstrip("0").rstrip(".").replace("-", "neg_").replace(".", "p")


def _write_backtest_artifacts(result: BacktestResult, output_dir: Path) -> None:
    """Write the standard backtest artifact set to one directory."""

    output_dir.mkdir(parents=True, exist_ok=True)
    write_json(output_dir / "backtest_results.json", result.to_payload())
    result.nav_series.to_csv(output_dir / "nav_series.csv", index=False)
    result.period_attribution.to_csv(output_dir / "period_attribution.csv", index=False)
    write_text(output_dir / "backtest_report.md", result.report_markdown)


def _scaled_config_payload(
    base_config_payload: dict[str, Any],
    *,
    base_config_path: Path,
    multiplier: float,
) -> tuple[dict[str, Any], dict[str, float]]:
    """Scale all cost-side objective weights together."""

    payload = {
        key: value.copy() if isinstance(value, dict) else value
        for key, value in base_config_payload.items()
    }
    risk_model_payload = dict(payload.get("risk_model", {}))
    for key in ("returns_path", "factor_exposure_path"):
        raw_path = risk_model_payload.get(key)
        if not raw_path:
            continue
        candidate = Path(str(raw_path))
        if not candidate.is_absolute():
            risk_model_payload[key] = str((base_config_path.parent / candidate).resolve())
    if risk_model_payload:
        payload["risk_model"] = risk_model_payload
    objective_weights = dict(payload.get("objective_weights", {}))
    scaled_values: dict[str, float] = {}
    for key in _COST_WEIGHT_KEYS:
        base_value = float(objective_weights.get(key, 0.0) or 0.0)
        scaled_value = base_value * float(multiplier)
        objective_weights[key] = scaled_value
        scaled_values[key] = scaled_value
    payload["objective_weights"] = objective_weights
    return payload, scaled_values


def _build_summary_frame(run_results: list[CostSweepRunResult]) -> pd.DataFrame:
    """Flatten sweep results into one comparable summary table."""

    rows: list[dict[str, Any]] = []
    for run in run_results:
        optimizer = run.backtest_result.summary["strategies"]["optimizer"]
        comparison = run.backtest_result.summary.get("comparison", {})
        rows.append(
            {
                "cost_bundle_multiplier": run.cost_bundle_multiplier,
                "run_dir": str(run.run_dir),
                "ending_nav": float(optimizer["ending_nav"]),
                "total_return": float(optimizer["total_return"]),
                "annualized_return": float(optimizer["annualized_return"]),
                "sharpe": float(optimizer["sharpe"]),
                "max_drawdown": float(optimizer["max_drawdown"]),
                "total_turnover": float(optimizer["total_turnover"]),
                "total_transaction_cost": float(optimizer["total_transaction_cost"]),
                "optimizer_vs_naive_ending_nav_delta": float(
                    comparison.get("optimizer_vs_naive_ending_nav_delta", 0.0)
                ),
            }
        )
    return pd.DataFrame(rows).sort_values("cost_bundle_multiplier").reset_index(drop=True)


def run_backtest_cost_sweep(
    *,
    manifest_path: str | Path,
    output_dir: str | Path,
    cost_bundle_multipliers: list[float],
) -> BacktestSweepResult:
    """Run the backtest once per cost bundle multiplier and archive all results."""

    base_manifest = load_backtest_manifest(manifest_path)
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    runs_root = output_root / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)

    base_manifest_payload = yaml.safe_load(base_manifest.manifest_path.read_text(encoding="utf-8"))
    base_config_payload = load_yaml_file(base_manifest.config)
    run_results: list[CostSweepRunResult] = []

    for multiplier in sorted(cost_bundle_multipliers):
        label = _format_multiplier_label(multiplier)
        run_dir = runs_root / f"cost_bundle_{label}x"
        run_dir.mkdir(parents=True, exist_ok=True)
        scaled_config_payload, scaled_weights = _scaled_config_payload(
            base_config_payload,
            base_config_path=base_manifest.config,
            multiplier=multiplier,
        )
        scaled_config_path = run_dir / "config_scaled.yaml"
        scaled_config_path.write_text(
            yaml.safe_dump(scaled_config_payload, sort_keys=False, allow_unicode=False),
            encoding="utf-8",
        )
        derived_manifest_payload = dict(base_manifest_payload)
        derived_manifest_payload["config"] = str(scaled_config_path)
        derived_manifest_path = run_dir / "manifest_scaled.yaml"
        derived_manifest_path.write_text(
            yaml.safe_dump(derived_manifest_payload, sort_keys=False, allow_unicode=False),
            encoding="utf-8",
        )
        backtest_result = run_backtest(derived_manifest_path)
        _write_backtest_artifacts(backtest_result, run_dir)
        run_results.append(
            CostSweepRunResult(
                cost_bundle_multiplier=float(multiplier),
                run_dir=run_dir,
                scaled_config_path=scaled_config_path,
                derived_manifest_path=derived_manifest_path,
                scaled_objective_weights=scaled_weights,
                backtest_result=backtest_result,
            )
        )

    summary_frame = _build_summary_frame(run_results)
    report_markdown = render_cost_sweep_report(summary_frame, base_manifest_path=base_manifest.manifest_path)
    summary_frame.to_csv(output_root / "sweep_summary.csv", index=False)
    write_text(output_root / "efficient_frontier_report.md", report_markdown)
    write_json(
        output_root / "backtest_sweep_manifest.json",
        {
            "base_manifest_path": str(base_manifest.manifest_path),
            "output_dir": str(output_root),
            "cost_bundle_multipliers": [float(item) for item in sorted(cost_bundle_multipliers)],
            "run_dirs": [str(run.run_dir) for run in run_results],
        },
    )
    return BacktestSweepResult(
        base_manifest_path=base_manifest.manifest_path,
        output_dir=output_root,
        run_results=run_results,
        summary_frame=summary_frame,
        report_markdown=report_markdown,
    )


_RISK_WEIGHT_KEYS = ("risk_term",)


@dataclass
class RiskSweepRunResult:
    """One scaled backtest run inside the risk aversion sweep."""

    risk_aversion_multiplier: float
    run_dir: Path
    scaled_config_path: Path
    derived_manifest_path: Path
    scaled_objective_weights: dict[str, float]
    backtest_result: BacktestResult


@dataclass
class RiskAversionSweepResult:
    """Top-level risk aversion sweep result with all archived runs."""

    base_manifest_path: Path
    output_dir: Path
    run_results: list[RiskSweepRunResult]
    summary_frame: pd.DataFrame
    report_markdown: str


def _risk_scaled_config_payload(
    base_config_payload: dict[str, Any],
    *,
    base_config_path: Path,
    multiplier: float,
) -> tuple[dict[str, Any], dict[str, float]]:
    """Scale only the risk-term objective weight while preserving other weights."""

    payload = deepcopy(base_config_payload)
    risk_model_payload = dict(payload.get("risk_model", {}))
    for key in ("returns_path", "factor_exposure_path"):
        raw_path = risk_model_payload.get(key)
        if not raw_path:
            continue
        candidate = Path(str(raw_path))
        if not candidate.is_absolute():
            risk_model_payload[key] = str((base_config_path.parent / candidate).resolve())
    if risk_model_payload:
        payload["risk_model"] = risk_model_payload
    objective_weights = dict(payload.get("objective_weights", {}))
    scaled_values: dict[str, float] = {}
    for key in _RISK_WEIGHT_KEYS:
        raw_base_value = objective_weights.get(key, 1.0)
        base_value = 1.0 if raw_base_value is None else float(raw_base_value)
        scaled_value = base_value * float(multiplier)
        objective_weights[key] = scaled_value
        scaled_values[key] = scaled_value
    payload["objective_weights"] = objective_weights
    return payload, scaled_values


def _compute_annualized_volatility(backtest_result: BacktestResult) -> float:
    """Compute annualized optimizer NAV volatility from daily returns."""

    optimizer_nav = (
        backtest_result.nav_series.loc[backtest_result.nav_series["strategy"] == "optimizer", ["date", "nav"]]
        .copy()
        .sort_values("date")
    )
    if optimizer_nav.empty:
        return 0.0
    daily_returns = optimizer_nav["nav"].pct_change().dropna()
    if daily_returns.empty:
        return 0.0
    volatility = float(daily_returns.std(ddof=1))
    if pd.isna(volatility):
        return 0.0
    return float(volatility * (252.0 ** 0.5))


def _build_risk_sweep_summary_frame(run_results: list[RiskSweepRunResult]) -> pd.DataFrame:
    """Flatten risk sweep runs into one comparable summary table."""

    rows: list[dict[str, Any]] = []
    for run in run_results:
        optimizer = run.backtest_result.summary["strategies"]["optimizer"]
        comparison = run.backtest_result.summary.get("comparison", {})
        rows.append(
            {
                "risk_aversion_multiplier": run.risk_aversion_multiplier,
                "risk_term_weight": float(run.scaled_objective_weights["risk_term"]),
                "run_dir": str(run.run_dir),
                "ending_nav": float(optimizer["ending_nav"]),
                "total_return": float(optimizer["total_return"]),
                "annualized_return": float(optimizer["annualized_return"]),
                "annualized_volatility": _compute_annualized_volatility(run.backtest_result),
                "sharpe": float(optimizer["sharpe"]),
                "max_drawdown": float(optimizer["max_drawdown"]),
                "total_turnover": float(optimizer["total_turnover"]),
                "total_transaction_cost": float(optimizer["total_transaction_cost"]),
                "optimizer_vs_naive_ending_nav_delta": float(
                    comparison.get("optimizer_vs_naive_ending_nav_delta", 0.0)
                ),
            }
        )
    return pd.DataFrame(rows).sort_values("risk_aversion_multiplier").reset_index(drop=True)


def run_backtest_risk_sweep(
    *,
    manifest_path: str | Path,
    output_dir: str | Path,
    risk_aversion_multipliers: list[float],
) -> RiskAversionSweepResult:
    """Run the backtest once per risk aversion multiplier and archive all results."""

    base_manifest = load_backtest_manifest(manifest_path)
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    runs_root = output_root / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)

    base_manifest_payload = yaml.safe_load(base_manifest.manifest_path.read_text(encoding="utf-8"))
    base_config_payload = load_yaml_file(base_manifest.config)
    run_results: list[RiskSweepRunResult] = []

    for multiplier in sorted(risk_aversion_multipliers):
        label = _format_multiplier_label(multiplier)
        run_dir = runs_root / f"risk_aversion_{label}x"
        run_dir.mkdir(parents=True, exist_ok=True)
        scaled_config_payload, scaled_weights = _risk_scaled_config_payload(
            base_config_payload,
            base_config_path=base_manifest.config,
            multiplier=multiplier,
        )
        scaled_config_path = run_dir / "config_scaled.yaml"
        scaled_config_path.write_text(
            yaml.safe_dump(scaled_config_payload, sort_keys=False, allow_unicode=False),
            encoding="utf-8",
        )
        derived_manifest_payload = dict(base_manifest_payload)
        derived_manifest_payload["config"] = str(scaled_config_path)
        derived_manifest_path = run_dir / "manifest_scaled.yaml"
        derived_manifest_path.write_text(
            yaml.safe_dump(derived_manifest_payload, sort_keys=False, allow_unicode=False),
            encoding="utf-8",
        )
        backtest_result = run_backtest(derived_manifest_path)
        _write_backtest_artifacts(backtest_result, run_dir)
        run_results.append(
            RiskSweepRunResult(
                risk_aversion_multiplier=float(multiplier),
                run_dir=run_dir,
                scaled_config_path=scaled_config_path,
                derived_manifest_path=derived_manifest_path,
                scaled_objective_weights=scaled_weights,
                backtest_result=backtest_result,
            )
        )

    summary_frame = _build_risk_sweep_summary_frame(run_results)
    report_markdown = render_risk_sweep_report(summary_frame, base_manifest_path=base_manifest.manifest_path)
    summary_frame.to_csv(output_root / "risk_sweep_summary.csv", index=False)
    write_text(output_root / "risk_aversion_frontier_report.md", report_markdown)
    write_json(
        output_root / "risk_sweep_manifest.json",
        {
            "base_manifest_path": str(base_manifest.manifest_path),
            "output_dir": str(output_root),
            "risk_aversion_multipliers": [float(item) for item in sorted(risk_aversion_multipliers)],
            "run_dirs": [str(run.run_dir) for run in run_results],
        },
    )
    return RiskAversionSweepResult(
        base_manifest_path=base_manifest.manifest_path,
        output_dir=output_root,
        run_results=run_results,
        summary_frame=summary_frame,
        report_markdown=report_markdown,
    )
