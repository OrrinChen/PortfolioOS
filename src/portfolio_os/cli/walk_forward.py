"""PortfolioOS walk-forward portfolio quant CLI."""

from __future__ import annotations

from pathlib import Path
import shutil
import typer

from portfolio_os.backtest.walk_forward import run_walk_forward


app = typer.Typer(add_completion=False, help="PortfolioOS historical walk-forward portfolio CLI.")


@app.command()
def main(
    manifest: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    output_dir: Path = typer.Option(...),
    frequency: str = typer.Option("monthly"),
) -> None:
    """Run weekly/monthly portfolio walk-forward evaluation."""

    if frequency not in {"monthly", "weekly"}:
        raise typer.BadParameter("frequency must be monthly or weekly")
    result = run_walk_forward(
        manifest_path=manifest,
        output_dir=output_dir,
        frequency=frequency,  # type: ignore[arg-type]
    )
    # Compatibility aliases for the Portfolio Quant v1 smoke target.
    alias_pairs = {
        "walk_forward_summary.json": "portfolio_quant_summary.json",
        "walk_forward_nav_curve.csv": "portfolio_quant_nav_curve.csv",
        "walk_forward_drawdown_curve.csv": "portfolio_quant_drawdown_curve.csv",
        "walk_forward_turnover_distribution.csv": "portfolio_quant_turnover_distribution.csv",
        "walk_forward_cost_attribution.csv": "portfolio_quant_cost_attribution.csv",
        "walk_forward_multi_snapshot_replay.csv": "portfolio_quant_multi_snapshot_replay.csv",
        "walk_forward_strategy_comparison.csv": "portfolio_quant_strategy_comparison.csv",
        "walk_forward_policy_breaches.csv": "portfolio_quant_policy_breaches.csv",
        "walk_forward_report.md": "portfolio_quant_report.md",
    }
    for source_name, alias_name in alias_pairs.items():
        shutil.copyfile(result.output_dir / source_name, result.output_dir / alias_name)
    typer.echo(f"walk_forward_summary.json: {result.output_dir / 'walk_forward_summary.json'}")
    typer.echo(f"walk_forward_report.md: {result.output_dir / 'walk_forward_report.md'}")


if __name__ == "__main__":
    app()
