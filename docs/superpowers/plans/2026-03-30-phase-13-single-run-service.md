# Phase 13 Single-Run Service Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract a public single-run workflow service that preserves current CLI behavior and data-quality gates, so Phase 13 backtests can reuse the engine through library calls.

**Architecture:** Move the single-run load and orchestration logic out of `src/portfolio_os/api/cli.py` into a new `src/portfolio_os/workflow/single_run.py` module. Keep CLI as a thin shell that parses arguments, calls the workflow service, and writes artifacts with the existing schema.

**Tech Stack:** Python 3.11, pandas, Typer, pytest, existing PortfolioOS workflow/config/audit modules.

---

### Task 1: Add failing tests for the public single-run workflow and CLI delegation

**Files:**
- Create: `C:\Users\14574\Quant\PortfolioOS\tests\test_single_run_workflow.py`

- [ ] Add a workflow test that imports `run_single_rebalance` and verifies it preserves data-quality findings in the audit payload.
- [ ] Add a CLI test that monkeypatches the workflow service and verifies the main CLI delegates to it.
- [ ] Run the new test file and confirm it fails before implementation.

### Task 2: Extract the public workflow service

**Files:**
- Create: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\workflow\single_run.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\workflow\__init__.py`

- [ ] Introduce a public context/result model for single-run workflow orchestration.
- [ ] Move the existing single-run context loader out of `api/cli.py` without changing its behavior.
- [ ] Add a public `run_single_rebalance(...)` function that preserves data-quality checks, import-profile mapping, benchmark generation, summary construction, and audit payload generation.

### Task 3: Thin the CLI shell without changing outputs

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\api\cli.py`

- [ ] Replace direct single-run orchestration in the CLI with a call to `run_single_rebalance(...)`.
- [ ] Keep artifact filenames, manifest schema, benchmark handling, and console output unchanged.

### Task 4: Verify and update handoff memory

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\TASK_MEMORY.md`

- [ ] Run `pytest -q` and confirm the full suite remains green.
- [ ] Update `TASK_MEMORY.md` with the new public workflow entrypoint and verification result.
