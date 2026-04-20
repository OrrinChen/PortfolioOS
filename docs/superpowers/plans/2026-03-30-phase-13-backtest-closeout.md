# Phase 13 Backtest Closeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish Phase 13 by adding period attribution, summary stats, and markdown reporting to the existing monthly backtest CLI without widening scope.

**Architecture:** Keep the current monthly engine intact and layer attribution/reporting on top of the existing rebalance loop. Compute period-level PnL decomposition from the already-known rebalance decisions and T+1 fill assumptions, then render a small JSON/CSV/Markdown artifact set from the same `BacktestResult`.

**Tech Stack:** Python 3.11, pandas, Typer, existing PortfolioOS config and optimizer workflow.

---

### Task 1: Extend the tests to lock the closeout contract

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\tests\test_backtest.py`

- [ ] Add failing assertions for summary stats, comparison fields, and period attribution columns.
- [ ] Add a failing CLI assertion for `period_attribution.csv` and `backtest_report.md`.
- [ ] Run `python -m pytest tests\test_backtest.py -q` and confirm the new assertions fail for the expected missing fields/files.

### Task 2: Add attribution and reporting modules

**Files:**
- Create: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\backtest\attribution.py`
- Create: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\backtest\report.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\backtest\engine.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\backtest\__init__.py`

- [ ] Implement period attribution rows with `holding_pnl`, `active_trading_pnl`, `trading_cost_pnl`, `period_pnl`, `turnover`, and period dates.
- [ ] Implement summary-stat helpers for annualized return, Sharpe, max drawdown, and optimizer-vs-naive deltas.
- [ ] Render `backtest_report.md` from the finished result object.
- [ ] Re-run `python -m pytest tests\test_backtest.py -q` and confirm green.

### Task 3: Wire CLI artifacts and verify full closeout

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\api\cli.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\TASK_MEMORY.md`

- [ ] Extend `portfolio-os-backtest` to write `period_attribution.csv` and `backtest_report.md`.
- [ ] Run `python -m pytest tests\test_backtest.py tests\test_single_run_workflow.py -q`.
- [ ] Run `python -m pytest -q`.
- [ ] Run a real smoke call against `data/backtest_samples/manifest_us_expanded.yaml`.
- [ ] Update `TASK_MEMORY.md` with the finished Phase 13 status and latest verification evidence.
