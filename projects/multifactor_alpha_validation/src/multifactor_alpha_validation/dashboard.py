from __future__ import annotations

from html import escape
from pathlib import Path

from multifactor_alpha_validation.allocator import AllocatorResult
from multifactor_alpha_validation.cost_capacity import SurvivalResult
from multifactor_alpha_validation.registry import FactorRegistryResult


def render_factor_dashboard(
    registry: FactorRegistryResult,
    survival: SurvivalResult,
    allocator: AllocatorResult,
) -> str:
    registry_rows = "".join(
        f"<tr><td>{escape(str(row.factor_id))}</td><td>{escape(str(row.final_status))}</td><td>{escape(str(row.stop_layer))}</td></tr>"
        for row in registry.decision_table.itertuples(index=False)
    )
    weight_rows = "".join(
        f"<tr><td>{escape(str(row.factor_id))}</td><td>{row.weight:.6f}</td><td>{escape(str(row.zero_weight_reason))}</td></tr>"
        for row in allocator.factor_weights.itertuples(index=False)
    )
    funnel_rows = "".join(
        f"<tr><td>{escape(str(row.layer))}</td><td>{int(row.factor_count)}</td><td>{escape(str(row.status))}</td></tr>"
        for row in survival.survival_funnel.itertuples(index=False)
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Multi-Factor Alpha Validation Dashboard</title>
</head>
<body>
  <h1>Multi-Factor Alpha Validation Engine</h1>
  <section><h2>Project Summary</h2><p>PIT-safe, redundancy-aware, cost-aware factor validation.</p></section>
  <section><h2>Survival Funnel</h2><table>{funnel_rows}</table></section>
  <section><h2>Factor Registry</h2><table>{registry_rows}</table></section>
  <section><h2>Allocator Weights</h2><table>{weight_rows}</table></section>
  <section><h2>Zero-Weight Attribution</h2><p>Every zero weight has an explicit reason.</p></section>
  <section><h2>Cost Stress</h2><p>{len(survival.cost_stress_matrix)} cost stress rows.</p></section>
  <section><h2>Capacity Frontier</h2><p>{len(survival.capacity_frontier)} capacity frontier rows.</p></section>
  <section><h2>Benchmark Attribution</h2><p>Raw, relative, beta-adjusted, sector/style-adjusted readouts are separate.</p></section>
  <section><h2>Non-Claims</h2><p>No production approval. No live trading. No direct Q2 entry.</p></section>
</body>
</html>
"""


def write_factor_dashboard(html: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "dashboard.html"
    path.write_text(html, encoding="utf-8")
    return path
