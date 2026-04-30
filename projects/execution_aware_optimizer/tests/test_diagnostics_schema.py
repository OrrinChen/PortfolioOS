from __future__ import annotations

import pytest

from execution_aware_optimizer.diagnostics import build_constraint_diagnostics
from execution_aware_optimizer.ladder import LadderResultRow


def test_diagnostics_schema_serializes_available_metrics_without_dual_values() -> None:
    rows = [
        LadderResultRow(
            layer_name="turnover_constrained",
            date="2026-01-31",
            gross_return=0.02,
            net_return=0.017,
            turnover=0.3,
            estimated_transaction_cost=0.003,
            realized_transaction_cost=None,
            num_positions=12,
            max_position_weight=0.11,
            cash_weight=0.02,
            rejected_symbols=["XYZ"],
            binding_constraints=["max_turnover"],
        ),
        LadderResultRow(
            layer_name="liquidity_constrained",
            date="2026-02-28",
            infeasibility_reason="participation limit infeasible",
        ),
    ]

    diagnostics = build_constraint_diagnostics(
        rows,
        turnover_budget=0.45,
        liquidity_budget=0.10,
    )
    payload = diagnostics.model_dump(mode="json")

    assert payload["binding_constraints"] == ["max_turnover"]
    assert payload["rejected_symbols"] == ["XYZ"]
    assert payload["infeasible_rebalance_dates"] == ["2026-02-28"]
    assert payload["cost_drag"] == pytest.approx(0.003)
    assert payload["turnover_budget_usage"] == pytest.approx(0.3 / 0.45)
    assert payload["liquidity_budget_usage"] is None
    assert "dual values" in payload["todos"][0]
