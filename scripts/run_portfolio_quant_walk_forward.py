from __future__ import annotations

from pathlib import Path

import typer

from portfolio_os.cli.walk_forward import main as walk_forward_main


app = typer.Typer(add_completion=False)


@app.command()
def main(
    manifest: Path = typer.Option(
        Path("data/backtest_samples/manifest_us_expanded.yaml"),
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
    output_dir: Path = typer.Option(Path("outputs/portfolio_quant_walk_forward")),
) -> None:
    """Run the local portfolio quant walk-forward smoke path."""

    walk_forward_main(
        manifest=manifest,
        output_dir=output_dir,
        frequency="monthly",
    )


if __name__ == "__main__":
    app()
