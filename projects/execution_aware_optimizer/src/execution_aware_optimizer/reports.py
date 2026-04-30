"""Markdown report builder for the Execution-Aware Portfolio Optimizer."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from execution_aware_optimizer.alpha_input import AlphaInputReport
from execution_aware_optimizer.cost_sensitivity import CostSensitivityResultRow
from execution_aware_optimizer.diagnostics import ConstraintDiagnostics
from execution_aware_optimizer.experiment_config import ExperimentConfig
from execution_aware_optimizer.ladder import LadderResultRow


def _fmt_optional(value: object) -> str:
    """Format optional report values without inventing data."""

    if value is None:
        return "Not available"
    return str(value)


def _fmt_float(value: float | None) -> str:
    """Format report floats with stable precision."""

    if value is None:
        return "Not available"
    return f"{value:.6f}"


def _mean(values: Iterable[float | None]) -> float | None:
    """Return the mean of available values, or None when no values exist."""

    available = [value for value in values if value is not None]
    if not available:
        return None
    return sum(available) / len(available)


def _cost_drag(row: LadderResultRow) -> float | None:
    """Return cost drag from row attribution without fabricating missing values."""

    if row.gross_return is not None and row.net_return is not None:
        return row.gross_return - row.net_return
    return row.estimated_transaction_cost


def _group_rows_by_layer(rows: Iterable[LadderResultRow]) -> dict[str, list[LadderResultRow]]:
    grouped: dict[str, list[LadderResultRow]] = {}
    for row in rows:
        grouped.setdefault(row.layer_name, []).append(row)
    return grouped


def _render_ladder_table(rows: Iterable[LadderResultRow]) -> list[str]:
    """Render a compact ladder table."""

    lines = [
        "| layer | date | gross_return | net_return | turnover | cost | infeasibility_reason |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            "| {layer} | {date} | {gross} | {net} | {turnover} | {cost} | {reason} |".format(
                layer=row.layer_name,
                date=_fmt_optional(row.date.isoformat() if row.date else None),
                gross=_fmt_optional(row.gross_return),
                net=_fmt_optional(row.net_return),
                turnover=_fmt_optional(row.turnover),
                cost=_fmt_optional(row.estimated_transaction_cost),
                reason=row.infeasibility_reason or "",
            )
        )
    return lines


def _render_gross_net_summary_table(rows: Iterable[LadderResultRow]) -> list[str]:
    """Render layer-level gross/net summary statistics from observed rows."""

    lines = [
        "| layer | observations | mean_gross_return | mean_net_return | mean_cost_drag | mean_turnover | unavailable_rows |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for layer_name, layer_rows in _group_rows_by_layer(rows).items():
        observed_rows = [
            row
            for row in layer_rows
            if any(
                value is not None
                for value in (
                    row.gross_return,
                    row.net_return,
                    row.turnover,
                    row.estimated_transaction_cost,
                    row.realized_transaction_cost,
                )
            )
        ]
        unavailable_count = sum(1 for row in layer_rows if row.infeasibility_reason)
        lines.append(
            "| {layer} | {observations} | {gross} | {net} | {drag} | {turnover} | {unavailable} |".format(
                layer=layer_name,
                observations=len(observed_rows),
                gross=_fmt_float(_mean(row.gross_return for row in observed_rows)),
                net=_fmt_float(_mean(row.net_return for row in observed_rows)),
                drag=_fmt_float(_mean(_cost_drag(row) for row in observed_rows)),
                turnover=_fmt_float(_mean(row.turnover for row in observed_rows)),
                unavailable=unavailable_count,
            )
        )
    return lines


def _render_alpha_decay_table(rows: Iterable[LadderResultRow]) -> list[str]:
    """Render net-return decay versus the raw-alpha layer when available."""

    grouped = _group_rows_by_layer(rows)
    raw_rows = grouped.get("raw_top_alpha_equal_weight", [])
    raw_mean_net = _mean(row.net_return for row in raw_rows)
    if raw_mean_net is None:
        return ["Alpha decay cannot be summarized until the raw layer has net return observations."]

    lines = [
        "| layer | mean_net_return | net_decay_vs_raw | mean_cost_drag |",
        "|---|---:|---:|---:|",
    ]
    for layer_name, layer_rows in grouped.items():
        layer_mean_net = _mean(row.net_return for row in layer_rows)
        if layer_mean_net is None:
            lines.append(f"| {layer_name} | Not available | Not available | Not available |")
            continue
        lines.append(
            "| {layer} | {net} | {decay} | {drag} |".format(
                layer=layer_name,
                net=_fmt_float(layer_mean_net),
                decay=_fmt_float(raw_mean_net - layer_mean_net),
                drag=_fmt_float(_mean(_cost_drag(row) for row in layer_rows)),
            )
        )
    return lines


def _render_cost_sensitivity_summary_table(rows: Iterable[CostSensitivityResultRow]) -> list[str]:
    """Render cost-sensitivity results grouped by cost assumption and layer."""

    grouped: dict[tuple[int, str], list[CostSensitivityResultRow]] = {}
    for row in rows:
        grouped.setdefault((row.cost_bps, row.layer_name), []).append(row)

    lines = [
        "| cost_bps | layer | observations | mean_gross_return | mean_net_return | mean_cost_drag | mean_turnover | unavailable_rows |",
        "|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for (cost_bps, layer_name), layer_rows in sorted(grouped.items()):
        observed_rows = [
            row
            for row in layer_rows
            if any(
                value is not None
                for value in (
                    row.gross_return,
                    row.net_return,
                    row.turnover,
                    row.estimated_transaction_cost,
                    row.realized_transaction_cost,
                )
            )
        ]
        unavailable_count = sum(1 for row in layer_rows if row.infeasibility_reason)
        lines.append(
            "| {cost_bps} | {layer} | {observations} | {gross} | {net} | {drag} | {turnover} | {unavailable} |".format(
                cost_bps=cost_bps,
                layer=layer_name,
                observations=len(observed_rows),
                gross=_fmt_float(_mean(row.gross_return for row in observed_rows)),
                net=_fmt_float(_mean(row.net_return for row in observed_rows)),
                drag=_fmt_float(_mean(_cost_drag(row) for row in observed_rows)),
                turnover=_fmt_float(_mean(row.turnover for row in observed_rows)),
                unavailable=unavailable_count,
            )
        )
    return lines


def render_execution_aware_optimizer_report(
    *,
    config: ExperimentConfig,
    alpha_report: AlphaInputReport | None,
    ladder_rows: list[LadderResultRow],
    diagnostics: ConstraintDiagnostics,
    cost_sensitivity_rows: list[CostSensitivityResultRow] | None = None,
) -> str:
    """Render the required project report sections."""

    cost_sensitivity_rows = cost_sensitivity_rows or []
    lines = [
        "# Execution-Aware Portfolio Optimizer Report",
        "",
        "## 1. Research question",
        "",
        "Can a raw alpha signal survive risk, sector, position, turnover, liquidity, and transaction-cost constraints?",
        "",
        "## 2. Alpha input",
        "",
        (
            "No alpha input was loaded. The project can accept CSV/parquet files with "
            "`date`, `symbol`, and `alpha_score` columns."
            if alpha_report is None
            else "\n".join(f"- {key}: `{value}`" for key, value in alpha_report.as_dict().items())
        ),
        "",
        "## 3. Portfolio construction ladder",
        "",
        *_render_ladder_table(ladder_rows),
        "",
        "## 4. Cost model",
        "",
        f"- Transaction-cost objective mode: `{config.portfolioos.transaction_cost_objective_mode}`",
        "- Cost sensitivity levels: " + ", ".join(f"{value} bps" for value in config.cost_sensitivity_bps),
        "",
        "## 5. Constraint diagnostics",
        "",
        f"- Binding constraints: `{diagnostics.binding_constraints}`",
        f"- Rejected symbols: `{diagnostics.rejected_symbols}`",
        f"- Infeasible rebalance dates: `{diagnostics.infeasible_rebalance_dates}`",
        f"- TODOs: `{diagnostics.todos}`",
        "",
        "## 6. Gross vs net performance",
        "",
        "Gross and net rows are reported only when the underlying PortfolioOS adapter returns period attribution.",
        "",
        *_render_gross_net_summary_table(ladder_rows),
        "",
        "## 7. Alpha decay under constraints",
        "",
        "Alpha decay is not fabricated. Missing layers remain marked with `infeasibility_reason` until PortfolioOS exposes the required adapter hooks.",
        "",
        *_render_alpha_decay_table(ladder_rows),
        "",
        "## 8. Cost sensitivity",
        "",
    ]
    if cost_sensitivity_rows:
        lines.append(
            "Cost-sensitivity rows are summarized only from supplied CSV results. "
            "Unavailable rows remain unavailable until an explicit PortfolioOS execution path produces attribution."
        )
        lines.extend(["", *_render_cost_sensitivity_summary_table(cost_sensitivity_rows)])
    else:
        lines.append("Cost-sensitivity output is not available yet.")
    lines.extend(
        [
            "",
            "## 9. Infeasibility / failure cases",
            "",
            "Rows with `infeasibility_reason` are intentional audit records, not failed backtest numbers.",
            "",
            "## 10. Reproducibility instructions",
            "",
            "Run the project scripts from the repository root with Poetry and explicit configs, for example:",
            "",
            "```bash",
            "PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_alpha_decay_ladder.py --config projects/execution_aware_optimizer/configs/alpha_decay_ladder.yaml",
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def write_execution_aware_optimizer_report(
    path: str | Path,
    *,
    config: ExperimentConfig,
    alpha_report: AlphaInputReport | None,
    ladder_rows: list[LadderResultRow],
    diagnostics: ConstraintDiagnostics,
    cost_sensitivity_rows: list[CostSensitivityResultRow] | None = None,
) -> Path:
    """Write the markdown report and return its path."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_execution_aware_optimizer_report(
            config=config,
            alpha_report=alpha_report,
            ladder_rows=ladder_rows,
            diagnostics=diagnostics,
            cost_sensitivity_rows=cost_sensitivity_rows,
        ),
        encoding="utf-8",
    )
    return output_path
