# Phase 13 Backtest Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a minimal monthly historical backtest loop that reuses the PortfolioOS optimizer through library calls, compares against naive and buy-and-hold baselines, and exports daily NAV plus summary JSON.

**Architecture:** Introduce a new `portfolio_os.backtest` package with a manifest model, deterministic schedule and price reconstruction helpers, and an engine that loops through rebalance dates using the shared single-run workflow service. Keep the first version narrow: monthly rebalance only, static target weights, static market/reference snapshots, in-memory T+1 close execution with commission plus fixed-spread assumptions, and minimal CLI/report outputs.

**Tech Stack:** Python 3.11, pandas, pydantic, Typer, existing PortfolioOS workflow/config/benchmark utilities.

---

### Task 1: Add red tests for manifest parsing and minimal backtest loop

**Files:**
- Create: `C:\Users\14574\Quant\PortfolioOS\tests\test_backtest.py`

- [ ] **Step 1: Write a failing manifest parse test**

```python
def test_load_backtest_manifest_reads_monthly_us_expanded_sample(project_root: Path) -> None:
    from portfolio_os.backtest.manifest import load_backtest_manifest

    manifest = load_backtest_manifest(
        project_root / "data" / "backtest_samples" / "manifest_us_expanded.yaml"
    )

    assert manifest.name == "us_expanded_monthly"
    assert manifest.rebalance.frequency == "monthly"
    assert manifest.market_snapshot
```

- [ ] **Step 2: Write a failing engine smoke test**

```python
def test_run_backtest_produces_optimizer_naive_and_buy_hold_nav(sample_backtest_manifest: Path) -> None:
    from portfolio_os.backtest.engine import run_backtest

    result = run_backtest(sample_backtest_manifest)

    assert {"optimizer", "naive_pro_rata", "buy_and_hold"} <= set(result.nav_series["strategy"])
    assert result.summary["rebalance_count"] > 0
```

- [ ] **Step 3: Write a failing CLI smoke test**

```python
def test_backtest_cli_writes_json_and_nav_series(project_root: Path, tmp_path: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(
        backtest_app,
        ["--manifest", str(project_root / "data" / "backtest_samples" / "manifest_us_expanded.yaml"), "--output-dir", str(tmp_path)],
    )
    assert result.exit_code == 0
    assert (tmp_path / "backtest_results.json").exists()
    assert (tmp_path / "nav_series.csv").exists()
```

- [ ] **Step 4: Run the new tests to confirm RED**

Run: `pytest tests\test_backtest.py -q`
Expected: FAIL with missing `portfolio_os.backtest` module / missing CLI wiring.

### Task 2: Add manifest schema and frozen sample inputs

**Files:**
- Create: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\backtest\__init__.py`
- Create: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\backtest\manifest.py`
- Create: `C:\Users\14574\Quant\PortfolioOS\data\backtest_samples\manifest_us_expanded.yaml`

- [ ] **Step 1: Implement manifest models**

```python
class BacktestRebalanceConfig(BaseModel):
    frequency: Literal["monthly"]


class BacktestManifest(BaseModel):
    name: str
    description: str | None = None
    returns_file: str
    market_snapshot: str
    initial_holdings: str
    target_weights: str
    reference: str
    portfolio_state: str
    config: str
    constraints: str
    execution_profile: str
    baselines: list[str] = Field(default_factory=lambda: ["naive_pro_rata", "buy_and_hold"])
    rebalance: BacktestRebalanceConfig
```

- [ ] **Step 2: Add a loader that resolves paths relative to the manifest**

```python
def load_backtest_manifest(path: str | Path) -> LoadedBacktestManifest:
    ...
```

- [ ] **Step 3: Add the first frozen sample manifest**

```yaml
name: us_expanded_monthly
description: "Expanded US monthly backtest smoke sample"
returns_file: data/risk_inputs_us_expanded/returns_long.csv
market_snapshot: data/universe/us_universe_market_2026-03-27.csv
initial_holdings: data/samples/us/sample_us_04/holdings.csv
target_weights: data/samples/us/sample_us_04/target.csv
reference: data/samples/us/sample_us_04/reference.csv
portfolio_state: data/samples/us/sample_us_04/portfolio_state.yaml
config: config/us_expanded.yaml
constraints: config/constraints/us_public_fund.yaml
execution_profile: config/execution/conservative.yaml
rebalance:
  frequency: monthly
baselines:
  - naive_pro_rata
  - buy_and_hold
```

- [ ] **Step 4: Run the manifest parse test to confirm GREEN**

Run: `pytest tests\test_backtest.py -q -k manifest`
Expected: PASS

### Task 3: Implement baseline helpers and the monthly engine

**Files:**
- Create: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\backtest\baseline.py`
- Create: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\backtest\engine.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\utils\config.py`

- [ ] **Step 1: Extend execution-profile schema with an optional fixed spread assumption**

```python
class ExecutionProfile(BaseModel):
    urgency: str = "low"
    slice_ratio: float = 0.25
    max_child_orders: int = 4
    backtest_fixed_half_spread_bps: float = 5.0
```

- [ ] **Step 2: Add baseline order construction helpers**

```python
def build_naive_rebalance_orders(universe: pd.DataFrame, config: AppConfig) -> list[Order]:
    ...

def apply_no_trade_baseline(holdings: pd.DataFrame) -> pd.DataFrame:
    return holdings.copy()
```

- [ ] **Step 3: Add price reconstruction and rebalance schedule helpers**

```python
def reconstruct_price_panel(returns_long: pd.DataFrame, base_price: float = 1.0) -> pd.DataFrame:
    ...

def build_monthly_rebalance_schedule(price_panel: pd.DataFrame) -> list[pd.Timestamp]:
    ...
```

- [ ] **Step 4: Add the main backtest loop**

```python
def run_backtest(manifest_path: str | Path) -> BacktestResult:
    ...
```

- [ ] **Step 5: Run engine tests to confirm GREEN**

Run: `pytest tests\test_backtest.py -q -k "engine or produces_optimizer"`
Expected: PASS

### Task 4: Wire the minimal CLI and export artifacts

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\api\cli.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\pyproject.toml`

- [ ] **Step 1: Add a `backtest_app` Typer entry**

```python
backtest_app = typer.Typer(add_completion=False, help="PortfolioOS historical backtest CLI.")
```

- [ ] **Step 2: Add the CLI command**

```python
@backtest_app.command()
def main(manifest: Path = typer.Option(...), output_dir: Path = typer.Option(...)) -> None:
    ...
```

- [ ] **Step 3: Add the script entry in Poetry**

```toml
portfolio-os-backtest = "portfolio_os.api.cli:backtest_app"
```

- [ ] **Step 4: Run the CLI smoke test**

Run: `pytest tests\test_backtest.py -q -k cli`
Expected: PASS

### Task 5: Full verification and memory update

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\TASK_MEMORY.md`

- [ ] **Step 1: Run the focused backtest tests**

Run: `pytest tests\test_backtest.py tests\test_single_run_workflow.py -q`
Expected: PASS

- [ ] **Step 2: Run the full suite**

Run: `pytest -q`
Expected: PASS with no new failures

- [ ] **Step 3: Update task memory**

```markdown
- Phase 13 minimal backtest engine is now in place with monthly schedule support
- public CLI entry `portfolio-os-backtest` now writes `backtest_results.json` and `nav_series.csv`
```
