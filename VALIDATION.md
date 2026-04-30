# VALIDATION.md

Use this file to choose the smallest validation set that proves the touched work.

## Always Run

```bash
git status --short
git diff --check
```

## Q1 Project

Run when touching `projects/agentic_alpha_triage`:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run pytest projects/agentic_alpha_triage/tests -q
```

## Q2 Project

Run when touching `projects/execution_aware_optimizer`:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests -q
```

Smoke scripts, default non-execution mode:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_alpha_decay_ladder.py --config projects/execution_aware_optimizer/configs/alpha_decay_ladder.yaml --output projects/execution_aware_optimizer/reports/alpha_decay_ladder_results.csv --report projects/execution_aware_optimizer/reports/execution_aware_optimizer_report.md
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_cost_sensitivity.py --config projects/execution_aware_optimizer/configs/cost_sensitivity.yaml --output projects/execution_aware_optimizer/reports/cost_sensitivity_results.csv
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_constraint_diagnostics.py --config projects/execution_aware_optimizer/configs/alpha_decay_ladder.yaml --output projects/execution_aware_optimizer/reports/constraint_diagnostics.json
```

## PortfolioOS Core

Run a focused subset when touching optimizer, cost, execution, backtest, replay, scenario, or alpha bridge code:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_objective_integration_mode.py tests/test_solver_objective_decomposition.py tests/test_optimizer.py tests/test_cost.py tests/test_execution_simulation.py tests/test_backtest.py tests/test_alpha_backtest_bridge.py tests/test_event_targets.py tests/test_replay.py tests/test_scenarios.py -q
```

Run the full suite before a broad platform change or release-style handoff:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest -q
```

## Docs-Only Changes

For docs-only changes, run:

```bash
git diff --check
```

If the docs update changes workflow instructions, also run the relevant project tests for the affected project area.

## External Services

Do not run live Alpaca, FMP, WRDS, Tushare, or other paid/external-service workflows unless the user explicitly asks and required credentials are present in the environment.
