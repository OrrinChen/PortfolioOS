"""Markdown report builder for the Execution-Aware Portfolio Optimizer."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from execution_aware_optimizer.alpha_input import AlphaInputReport
from execution_aware_optimizer.diagnostics import ConstraintDiagnostics
from execution_aware_optimizer.experiment_config import ExperimentConfig
from execution_aware_optimizer.ladder import LadderResultRow


def _fmt_optional(value: object) -> str:
    """Format optional report values without inventing data."""

    if value is None:
        return "Not available"
    return str(value)


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


def render_execution_aware_optimizer_report(
    *,
    config: ExperimentConfig,
    alpha_report: AlphaInputReport | None,
    ladder_rows: list[LadderResultRow],
    diagnostics: ConstraintDiagnostics,
    cost_sensitivity_rows: list[LadderResultRow] | None = None,
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
        "## 7. Alpha decay under constraints",
        "",
        "Alpha decay is not fabricated. Missing layers remain marked with `infeasibility_reason` until PortfolioOS exposes the required adapter hooks.",
        "",
        "## 8. Cost sensitivity",
        "",
    ]
    if cost_sensitivity_rows:
        lines.extend(_render_ladder_table(cost_sensitivity_rows))
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
    cost_sensitivity_rows: list[LadderResultRow] | None = None,
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
