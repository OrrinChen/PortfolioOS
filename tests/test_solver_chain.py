from __future__ import annotations

import numpy as np

from portfolio_os.domain.errors import OptimizationError
from portfolio_os.optimizer import solver as solver_module


def test_solver_chain_falls_back_to_scs_when_primary_fails(sample_context: dict, monkeypatch) -> None:
    universe = sample_context["universe"]
    config = sample_context["config"].model_copy(deep=True)
    config.solver.name = "CLARABEL"

    monkeypatch.setattr(
        solver_module,
        "_solver_chain",
        lambda _config: [("CLARABEL", {}), ("SCS", {"max_iters": 10, "eps": 1e-4})],
    )
    calls: list[str] = []

    def _fake_solve_once(*, problem, constraints, trades, solver_name, solver_kwargs):
        _ = problem
        _ = constraints
        _ = trades
        _ = solver_kwargs
        calls.append(solver_name)
        if solver_name == "CLARABEL":
            raise OptimizationError("status=infeasible")
        return solver_module._SolveAttempt(
            solver_name=solver_name,
            solver_kwargs={},
            status="optimal",
            objective_value=0.0,
            trade_values=np.zeros(len(universe), dtype=float),
            constraint_residual_max=0.0,
        )

    monkeypatch.setattr(solver_module, "_solve_once", _fake_solve_once)

    result = solver_module.solve_rebalance_problem(universe, config)
    assert calls == ["CLARABEL", "SCS"]
    assert result.solver_used == "SCS"
    assert result.solver_fallback_used is True
    assert result.constraint_residual_max == 0.0
