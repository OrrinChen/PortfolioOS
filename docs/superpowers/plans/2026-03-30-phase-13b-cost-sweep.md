# Phase 13b Cost Sweep Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a deterministic backtest sweep tool that scales the cost-side objective weights together, runs the existing backtest for each multiplier, and exports a compact frontier summary.

**Architecture:** Keep the current backtest engine unchanged at its core and add a new sweep layer that derives scaled config files, runs `run_backtest(...)` for each multiplier, archives per-run artifacts, and renders one top-level CSV/Markdown summary. Preserve the existing `portfolio-os-backtest` CLI and add a separate sweep CLI so the current single-run interface does not change.

**Tech Stack:** Python 3.11, pandas, Typer, PyYAML, existing PortfolioOS backtest and config helpers.

---

### Task 1: Lock the sweep contract with failing tests

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\tests\test_backtest.py`

- [ ] Add a failing engine-level sweep test that verifies cost bundle scaling across `transaction_cost`, `transaction_fee`, `turnover_penalty`, and `slippage_penalty`.
- [ ] Add a failing CLI test that verifies `sweep_summary.csv` and `efficient_frontier_report.md` are written.
- [ ] Run `python -m pytest tests\test_backtest.py -q` and confirm the new assertions fail for the expected missing sweep functionality.

### Task 2: Implement the sweep layer

**Files:**
- Create: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\backtest\sweep.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\backtest\report.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\backtest\__init__.py`

- [ ] Add a `run_backtest_cost_sweep(...)` workflow that:
  - loads one base manifest
  - writes one scaled config file per multiplier
  - runs the existing backtest once per multiplier
  - archives per-run artifacts under the sweep output directory
- [ ] Add summary CSV and markdown frontier rendering.
- [ ] Re-run `python -m pytest tests\test_backtest.py -q` and confirm green.

### Task 3: Wire the sweep CLI and verify end-to-end

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\api\cli.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\pyproject.toml`
- Modify: `C:\Users\14574\Quant\PortfolioOS\TASK_MEMORY.md`

- [ ] Add a separate `portfolio-os-backtest-sweep` CLI entry.
- [ ] Run `python -m pytest tests\test_backtest.py tests\test_single_run_workflow.py -q`.
- [ ] Run `python -m pytest -q`.
- [ ] Run one real sweep against `data/backtest_samples/manifest_us_expanded.yaml`.
- [ ] Update `TASK_MEMORY.md` with artifacts and latest verification evidence.
