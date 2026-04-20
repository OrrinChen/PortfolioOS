# Phase 1 US Alpha Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first deterministic alpha research baseline to PortfolioOS so the project can produce signal diagnostics from a normalized returns history.

**Architecture:** Introduce a small `portfolio_os.alpha` package plus a standalone CLI. The first slice is intentionally research-only: it transforms `returns_long.csv` into baseline reversal and momentum signals, forward-return labels, IC metrics, and artifact files without yet changing portfolio construction.

**Tech Stack:** pandas, numpy, Typer, pytest, existing storage helpers.

---

### Task 1: Add Alpha Research Tests

**Files:**
- Create: `tests/test_alpha_research.py`

- [ ] **Step 1: Write the failing tests**
- [ ] **Step 2: Run the focused alpha tests and verify they fail for missing-module reasons**

### Task 2: Add Alpha Research Core

**Files:**
- Create: `src/portfolio_os/alpha/research.py`
- Create: `src/portfolio_os/alpha/report.py`
- Create: `src/portfolio_os/alpha/__init__.py`

- [ ] **Step 1: Implement returns loading and panel construction**
- [ ] **Step 2: Implement reversal and momentum signal generation**
- [ ] **Step 3: Implement forward-return labeling and evaluation metrics**
- [ ] **Step 4: Implement markdown reporting**
- [ ] **Step 5: Re-run focused tests**

### Task 3: Add CLI And Entry Point

**Files:**
- Modify: `src/portfolio_os/api/cli.py`
- Modify: `pyproject.toml`

- [ ] **Step 1: Add `alpha_research_app`**
- [ ] **Step 2: Add CLI artifact writing**
- [ ] **Step 3: Add poetry entry point**
- [ ] **Step 4: Re-run focused tests**

### Task 4: Record The Platform Direction

**Files:**
- Create: `docs/platform_ml_rl_roadmap.md`
- Create: `docs/superpowers/specs/2026-04-01-phase-1-us-alpha-core-design.md`
- Modify: `TASK_MEMORY.md`

- [ ] **Step 1: Write the platform roadmap**
- [ ] **Step 2: Record the Phase 1 design**
- [ ] **Step 3: Update task memory after implementation and verification**

### Task 5: Verify End-To-End

**Files:**
- Create: `outputs/...` during manual run only

- [ ] **Step 1: Run focused alpha research tests**

Run:

```bash
python -m pytest tests/test_alpha_research.py -q
```

Expected: all new alpha research tests pass.

- [ ] **Step 2: Run a broader regression slice**

Run:

```bash
python -m pytest tests/test_alpha_research.py tests/test_backtest.py -q
```

Expected: alpha research tests plus backtest tests pass together.

- [ ] **Step 3: Run one manual alpha research smoke command**

Run:

```bash
python -c "import sys; sys.path.insert(0, 'src'); from portfolio_os.api.cli import alpha_research_app; sys.argv = ['portfolio-os-alpha-research', '--returns-file', 'data/risk_inputs_us_expanded/returns_long.csv', '--output-dir', 'outputs/alpha_research_us_expanded_smoke']; alpha_research_app()"
```

Expected:

- `alpha_signal_panel.csv`
- `alpha_ic_by_date.csv`
- `alpha_research_summary.json`
- `alpha_research_report.md`
