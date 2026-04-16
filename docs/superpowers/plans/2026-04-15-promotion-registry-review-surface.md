# Promotion Registry Review Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reviewer-facing CLI that scans promotion bundles, validates them through the existing contract loader, and emits a registry CSV/JSON/Markdown summary.

**Architecture:** Reuse the existing promotion-contract validator as the single validation source of truth. Add one workflow module for recursive discovery plus registry rendering, then expose it through one narrow CLI command and cover it with unit and CLI tests.

**Tech Stack:** Python 3.11, Typer CLI, pandas, pytest

---

### Task 1: Add workflow-level registry tests

**Files:**
- Create: `C:\Users\14574\Quant\PortfolioOS\tests\test_promotion_registry.py`

- [ ] **Step 1: Write the failing workflow tests**

```python
def test_run_promotion_registry_builds_outputs_for_multiple_bundles(tmp_path: Path) -> None:
    ...

def test_run_promotion_registry_rejects_empty_scan_root(tmp_path: Path) -> None:
    ...
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
python -m pytest tests\test_promotion_registry.py -q
```

Expected:

- `FAIL` because `portfolio_os.workflow.promotion_registry` does not exist yet

- [ ] **Step 3: Write the minimal workflow implementation**

Create a workflow module that:

- scans recursively for `promotion_bundle.json`
- validates each bundle with `load_promotion_contract`
- writes registry CSV, JSON manifest, and Markdown summary

- [ ] **Step 4: Run tests to verify they pass**

Run:

```powershell
python -m pytest tests\test_promotion_registry.py -q
```

Expected:

- `PASS`

### Task 2: Add CLI entrypoint coverage

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\api\cli.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\tests\test_e2e_cli.py`

- [ ] **Step 1: Write the failing CLI test**

```python
def test_promotion_registry_cli_produces_expected_outputs(tmp_path) -> None:
    ...
```

- [ ] **Step 2: Run the CLI test to verify it fails**

Run:

```powershell
python -m pytest tests\test_e2e_cli.py -q -k promotion_registry
```

Expected:

- `FAIL` because the CLI command does not exist yet

- [ ] **Step 3: Implement the CLI command**

Add a top-level command that accepts:

- `--input-root`
- `--output-dir`

and echoes:

- `promotion_registry.csv`
- `promotion_registry_manifest.json`
- `promotion_registry_summary.md`

- [ ] **Step 4: Run the CLI test to verify it passes**

Run:

```powershell
python -m pytest tests\test_e2e_cli.py -q -k promotion_registry
```

Expected:

- `PASS`

### Task 3: Verify the whole slice

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\docs\research_promotion_contract.md`

- [ ] **Step 1: Update docs to mention the new registry surface**

Add one short section documenting that the contract now has a reviewer-facing registry/export path.

- [ ] **Step 2: Run focused verification**

Run:

```powershell
python -m pytest tests\test_alpha_promotion_contract.py tests\test_promotion_registry.py tests\test_e2e_cli.py -q
```

Expected:

- all targeted tests pass

- [ ] **Step 3: Run syntax verification for touched modules**

Run:

```powershell
python -m py_compile src\portfolio_os\alpha\promotion_contract.py src\portfolio_os\workflow\promotion_registry.py src\portfolio_os\api\cli.py
```

Expected:

- no output

- [ ] **Step 4: Prepare commit**

Stage only the new registry files and related docs/tests for this slice.
