# Phase 1.5 Alpha Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrate the accepted Phase 1 alpha recipe into the backtest and optimizer so PortfolioOS can run a walk-forward optimizer-plus-alpha experiment against naive and alpha-only baselines.

**Architecture:** Add an optional alpha term to the optimizer objective, implement a manifest-driven walk-forward alpha bridge that computes expected returns and alpha-only target weights at each rebalance, and extend the backtest/reporting stack to compare optimizer, naive, and alpha-only strategies without changing existing default configs or legacy manifests.

**Tech Stack:** pandas, numpy, statistics.NormalDist, Typer-free library integration through existing backtest entrypoints, pytest.

---

### Task 1: Add Schema And Objective Tests First

**Files:**
- Modify: `tests/test_objective_integration_mode.py`
- Modify: `tests/test_solver_objective_decomposition.py`
- Modify: `tests/test_backtest.py`
- Modify: `src/portfolio_os/utils/config.py`
- Modify: `src/portfolio_os/backtest/manifest.py`

- [ ] **Step 1: Write a failing objective test for `alpha_weight = 0.0` compatibility**

```python
def test_alpha_weight_zero_keeps_objective_value_unchanged(sample_context: dict) -> None:
    universe = sample_context["universe"].copy()
    universe["expected_return"] = np.array([0.10, -0.05, 0.03, 0.01], dtype=float)[: len(universe)]
    config = sample_context["config"].model_copy(deep=True)
    config.objective_weights.alpha_weight = 0.0
    trades = cp.Variable(len(universe))
    trades.value = np.array([(-1.0) ** i * 0.5 for i in range(len(universe))], dtype=float)
    objective, components = build_objective(
        trades,
        universe,
        config,
        _pre_trade_nav(universe, config),
        risk_context=None,
    )
    legacy_total = (
        float(config.objective_weights.target_deviation or 0.0) * float(components["target_deviation"].value)
        + float(config.objective_weights.transaction_fee or 0.0) * float(components["transaction_fee"].value)
        + float(config.objective_weights.turnover_penalty or 0.0) * float(components["turnover_penalty"].value)
        + float(config.objective_weights.slippage_penalty or 0.0) * float(components["slippage_penalty"].value)
    )
    assert float(objective.value) == pytest.approx(legacy_total, abs=1e-10)
```

- [ ] **Step 2: Write a failing objective test for nonzero alpha contribution**

```python
def test_alpha_weight_adds_negative_alpha_reward_term(sample_context: dict) -> None:
    universe = sample_context["universe"].copy()
    universe["expected_return"] = np.array([0.20, 0.05, -0.02, -0.10], dtype=float)[: len(universe)]
    config = sample_context["config"].model_copy(deep=True)
    config.objective_weights.alpha_weight = 2.5
    trades = cp.Variable(len(universe))
    trades.value = np.zeros(len(universe), dtype=float)
    objective, components = build_objective(
        trades,
        universe,
        config,
        _pre_trade_nav(universe, config),
        risk_context=None,
    )
    alpha_reward = float(components["alpha_reward"].value)
    assert "alpha_reward" in components
    assert alpha_reward != 0.0
    assert float(objective.value) == pytest.approx(
        float(config.objective_weights.target_deviation or 0.0) * float(components["target_deviation"].value)
        + float(config.objective_weights.transaction_fee or 0.0) * float(components["transaction_fee"].value)
        + float(config.objective_weights.turnover_penalty or 0.0) * float(components["turnover_penalty"].value)
        + float(config.objective_weights.slippage_penalty or 0.0) * float(components["slippage_penalty"].value)
        - float(config.objective_weights.alpha_weight) * alpha_reward,
        abs=1e-10,
    )
```

- [ ] **Step 3: Write a failing decomposition test for alpha components**

```python
def test_augment_mode_objective_decomposition_includes_alpha_components(sample_context: dict, monkeypatch) -> None:
    config = sample_context["config"].model_copy(deep=True)
    config.objective_weights.alpha_weight = 3.0
    fake_components = {
        "target_deviation": _ConstantExpr(1.0),
        "transaction_fee": _ConstantExpr(2.0),
        "turnover_penalty": _ConstantExpr(3.0),
        "slippage_penalty": _ConstantExpr(4.0),
        "alpha_reward": _ConstantExpr(5.0),
    }
    monkeypatch.setattr(solver_module, "build_objective", lambda *args, **kwargs: (0.0, fake_components))
    result = solve_rebalance_problem(sample_context["universe"], config)
    components = result.objective_decomposition["components"]
    assert "alpha_reward" in components
    assert components["alpha_reward"]["weight"] == 3.0
    assert components["alpha_reward"]["weighted_value"] == pytest.approx(-15.0)
```

- [ ] **Step 4: Write a failing manifest/schema test for optional alpha block**

```python
def test_load_backtest_manifest_reads_optional_alpha_model_block(tmp_path: Path) -> None:
    manifest_path = _write_backtest_fixture(tmp_path)
    manifest_payload = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    manifest_payload["alpha_model"] = {
        "enabled": True,
        "recipe_name": "alt_momentum_4_1",
        "quantiles": 5,
        "forward_horizon_days": 5,
        "min_evaluation_dates": 20,
        "zscore_winsor_limit": 3.0,
        "t_stat_full_confidence": 3.0,
        "max_abs_expected_return": 0.30,
        "write_alpha_panel": True,
        "add_alpha_only_baseline": True,
    }
    manifest_path.write_text(yaml.safe_dump(manifest_payload, sort_keys=False, allow_unicode=False), encoding="utf-8")
    manifest = load_backtest_manifest(manifest_path)
    assert manifest.alpha_model is not None
    assert manifest.alpha_model.enabled is True
    assert manifest.alpha_model.recipe_name == "alt_momentum_4_1"
```

- [ ] **Step 5: Run the failing subset**

Run: `python -m pytest tests/test_objective_integration_mode.py tests/test_solver_objective_decomposition.py tests/test_backtest.py -k "alpha"`  
Expected: FAIL because alpha objective weights and manifest alpha model are not implemented yet

- [ ] **Step 6: Implement the minimal schema and objective support**

```python
class ObjectiveWeights(BaseModel):
    risk_term: float = 1.0
    tracking_error: float = 1.0
    transaction_cost: float = 1.0
    alpha_weight: float = 0.0
    target_deviation: float | None = None
    transaction_fee: float | None = None
    turnover_penalty: float | None = None
    slippage_penalty: float | None = None
```

```python
expected_return = universe["expected_return"].to_numpy(dtype=float) if "expected_return" in universe.columns else None
alpha_reward = cp.sum(cp.multiply(expected_return, post_trade_weights)) if expected_return is not None else cp.Constant(0.0)
legacy_objective = legacy_objective - float(config.objective_weights.alpha_weight or 0.0) * alpha_reward
```

- [ ] **Step 7: Re-run the focused subset**

Run: `python -m pytest tests/test_objective_integration_mode.py tests/test_solver_objective_decomposition.py tests/test_backtest.py -k "alpha"`  
Expected: PASS for the new alpha schema/objective assertions

- [ ] **Step 8: Commit**

```bash
git add src/portfolio_os/utils/config.py src/portfolio_os/optimizer/objective.py src/portfolio_os/optimizer/solver.py src/portfolio_os/backtest/manifest.py tests/test_objective_integration_mode.py tests/test_solver_objective_decomposition.py tests/test_backtest.py
git commit -m "feat: add phase 1.5 alpha objective schema"
```

### Task 2: Build The Walk-Forward Alpha Bridge

**Files:**
- Create: `src/portfolio_os/alpha/backtest_bridge.py`
- Modify: `src/portfolio_os/alpha/__init__.py`
- Modify: `tests/test_alpha_research.py`
- Modify: `tests/test_backtest.py`

- [ ] **Step 1: Write a failing alpha-bridge test for top-quintile equal weights**

```python
def test_build_alpha_only_target_weights_selects_top_quintile_equal_weight() -> None:
    frame = pd.DataFrame(
        {
            "ticker": ["A", "B", "C", "D", "E"],
            "alpha_score": [5.0, 4.0, 3.0, 2.0, 1.0],
        }
    )
    weights = build_alpha_only_target_weights(frame, quantiles=5)
    assert weights == {"A": 1.0, "B": 0.0, "C": 0.0, "D": 0.0, "E": 0.0}
```

- [ ] **Step 2: Write a failing alpha-bridge test for walk-forward non-lookahead**

```python
def test_build_alpha_snapshot_uses_only_history_up_to_rebalance_date(tmp_path: Path) -> None:
    manifest_path = _write_backtest_fixture(tmp_path)
    returns_path = manifest_path.parent / "returns_long.csv"
    snapshot = build_alpha_snapshot_for_rebalance(
        returns_file=returns_path,
        rebalance_date="2026-02-27",
        quantiles=5,
    )
    assert pd.to_datetime(snapshot.signal_frame["date"]).max() <= pd.Timestamp("2026-02-27")
    assert snapshot.current_cross_section["date"].nunique() == 1
```

- [ ] **Step 3: Write a failing alpha-bridge test for expected-return columns**

```python
def test_build_alpha_snapshot_outputs_expected_return_and_zscore_columns(tmp_path: Path) -> None:
    manifest_path = _write_backtest_fixture(tmp_path)
    returns_path = manifest_path.parent / "returns_long.csv"
    snapshot = build_alpha_snapshot_for_rebalance(
        returns_file=returns_path,
        rebalance_date="2026-03-30",
        quantiles=5,
    )
    assert {"ticker", "alpha_score", "alpha_rank_pct", "alpha_zscore", "expected_return", "quantile"} <= set(snapshot.current_cross_section.columns)
```

- [ ] **Step 4: Run the failing alpha subset**

Run: `python -m pytest tests/test_backtest.py -k "alpha_only or alpha_snapshot"`  
Expected: FAIL with missing bridge module/functions

- [ ] **Step 5: Implement the bridge module**

```python
@dataclass
class AlphaRebalanceSnapshot:
    rebalance_date: pd.Timestamp
    signal_frame: pd.DataFrame
    ic_frame: pd.DataFrame
    current_cross_section: pd.DataFrame
    alpha_only_target_weights: dict[str, float]
```

```python
def build_alpha_snapshot_for_rebalance(...):
    returns_panel = load_alpha_returns_panel(returns_file)
    returns_panel = returns_panel.loc[returns_panel.index <= rebalance_date]
    signal_frame = build_alpha_research_frame(...)
    ic_frame = build_alpha_ic_frame(...)
    current_cross_section = ...
    current_cross_section["alpha_rank_pct"] = ...
    current_cross_section["alpha_zscore"] = ...
    current_cross_section["expected_return"] = ...
    current_cross_section["quantile"] = ...
    return AlphaRebalanceSnapshot(...)
```

- [ ] **Step 6: Re-run the bridge subset**

Run: `python -m pytest tests/test_backtest.py -k "alpha_only or alpha_snapshot"`  
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/portfolio_os/alpha/backtest_bridge.py src/portfolio_os/alpha/__init__.py tests/test_backtest.py
git commit -m "feat: add walk-forward alpha bridge"
```

### Task 3: Integrate Alpha Into The Backtest Engine

**Files:**
- Modify: `src/portfolio_os/backtest/engine.py`
- Modify: `src/portfolio_os/backtest/baseline.py`
- Modify: `src/portfolio_os/backtest/attribution.py`
- Modify: `tests/test_backtest.py`

- [ ] **Step 1: Write a failing backtest test for the alpha-only baseline**

```python
def test_run_backtest_alpha_manifest_adds_alpha_only_strategy(tmp_path: Path) -> None:
    manifest_path = _write_alpha_enabled_backtest_fixture(tmp_path)
    result = run_backtest(manifest_path)
    assert {"optimizer", "naive_pro_rata", "buy_and_hold", "alpha_only_top_quintile"} <= set(result.nav_series["strategy"])
```

- [ ] **Step 2: Write a failing backtest test for the alpha panel artifact**

```python
def test_run_backtest_alpha_manifest_writes_alpha_panel_artifact(tmp_path: Path) -> None:
    manifest_path = _write_alpha_enabled_backtest_fixture(tmp_path)
    output_dir = tmp_path / "alpha_backtest_output"
    result = run_backtest(manifest_path, output_dir=output_dir)
    assert (output_dir / "alpha_panel.csv").exists()
```

- [ ] **Step 3: Write a failing compatibility test for `alpha_weight = 0.0`**

```python
def test_alpha_enabled_manifest_with_zero_alpha_weight_matches_baseline_optimizer(tmp_path: Path) -> None:
    baseline_manifest = _write_backtest_fixture(tmp_path / "baseline")
    alpha_manifest = _write_alpha_enabled_backtest_fixture(tmp_path / "alpha", alpha_weight=0.0)
    baseline_result = run_backtest(baseline_manifest)
    alpha_result = run_backtest(alpha_manifest)
    baseline_optimizer = baseline_result.nav_series.loc[baseline_result.nav_series["strategy"] == "optimizer", "nav"].reset_index(drop=True)
    alpha_optimizer = alpha_result.nav_series.loc[alpha_result.nav_series["strategy"] == "optimizer", "nav"].reset_index(drop=True)
    pd.testing.assert_series_equal(alpha_optimizer, baseline_optimizer, check_names=False)
```

- [ ] **Step 4: Run the failing backtest subset**

Run: `python -m pytest tests/test_backtest.py -k "alpha_manifest or alpha_enabled"`  
Expected: FAIL because the engine does not yet inject expected returns or run alpha-only

- [ ] **Step 5: Implement alpha injection and the alpha-only baseline**

```python
if manifest.alpha_model and manifest.alpha_model.enabled:
    alpha_snapshot = build_alpha_snapshot_for_rebalance(...)
    optimizer_universe = optimizer_universe.merge(
        alpha_snapshot.current_cross_section[["ticker", "expected_return"]],
        on="ticker",
        how="left",
    )
```

```python
def run_alpha_only_top_quintile(universe, config: AppConfig, *, alpha_target_weights: dict[str, float], input_findings=None) -> RebalanceRun:
    alpha_universe = universe.copy()
    alpha_universe["target_weight"] = alpha_universe["ticker"].map(alpha_target_weights).fillna(0.0).astype(float)
    run = naive_target_rebalance(alpha_universe, config, input_findings=input_findings)
    run.strategy_name = "alpha_only_top_quintile"
    return run
```

- [ ] **Step 6: Re-run the backtest subset**

Run: `python -m pytest tests/test_backtest.py -k "alpha_manifest or alpha_enabled"`  
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/portfolio_os/backtest/engine.py src/portfolio_os/backtest/baseline.py src/portfolio_os/backtest/attribution.py tests/test_backtest.py
git commit -m "feat: add alpha-enabled backtest strategies"
```

### Task 4: Extend Reporting And Artifacts

**Files:**
- Modify: `src/portfolio_os/backtest/report.py`
- Modify: `tests/test_backtest.py`

- [ ] **Step 1: Write a failing report test for optimizer-vs-alpha-only comparison**

```python
def test_backtest_report_includes_optimizer_vs_alpha_only_section(tmp_path: Path) -> None:
    manifest_path = _write_alpha_enabled_backtest_fixture(tmp_path)
    result = run_backtest(manifest_path)
    assert "## Optimizer Vs Alpha-Only" in result.report_markdown
    assert "alpha_only_top_quintile" in result.report_markdown
```

- [ ] **Step 2: Write a failing summary test for alpha metadata**

```python
def test_backtest_summary_includes_alpha_metadata_when_enabled(tmp_path: Path) -> None:
    manifest_path = _write_alpha_enabled_backtest_fixture(tmp_path)
    result = run_backtest(manifest_path)
    assert result.summary["alpha"]["enabled"] is True
    assert result.summary["alpha"]["recipe_name"] == "alt_momentum_4_1"
```

- [ ] **Step 3: Run the failing report subset**

Run: `python -m pytest tests/test_backtest.py -k "alpha_metadata or alpha_only_section"`  
Expected: FAIL

- [ ] **Step 4: Implement report and summary updates**

```python
summary["alpha"] = {
    "enabled": True,
    "recipe_name": "alt_momentum_4_1",
    "alpha_panel_path": str(alpha_panel_path),
    "alpha_weight": float(app_config.objective_weights.alpha_weight),
}
```

```python
lines.extend(
    [
        "",
        "## Optimizer Vs Alpha-Only",
        f"- Ending NAV delta: {_fmt_float(comparison['optimizer_vs_alpha_only_ending_nav_delta'])}",
        f"- Sharpe delta: {_fmt_float(comparison['optimizer_vs_alpha_only_sharpe_delta'])}",
    ]
)
```

- [ ] **Step 5: Re-run the report subset**

Run: `python -m pytest tests/test_backtest.py -k "alpha_metadata or alpha_only_section"`  
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/portfolio_os/backtest/report.py src/portfolio_os/backtest/attribution.py tests/test_backtest.py
git commit -m "feat: report alpha-enabled backtest comparisons"
```

### Task 5: Add Phase 1.5 Research Inputs And Run The Real Experiment

**Files:**
- Create: `config/us_expanded_alpha_phase_1_5.yaml`
- Create: `data/backtest_samples/manifest_us_expanded_alpha_phase_1_5.yaml`
- Create: `docs/phase_1_5_alpha_decision_note.md`
- Modify: `TASK_MEMORY.md`

- [ ] **Step 1: Add a dedicated research config with nonzero alpha weight**

```yaml
project:
  name: PortfolioOS
  disclaimer: "Auxiliary decision-support tool only. Not investment advice."
trading:
  market: us
  lot_size: 1
  allow_fractional_shares_in_optimizer: true
fees:
  commission_rate: 0.0003
  transfer_fee_rate: 0.0
  stamp_duty_rate: 0.0
slippage:
  k: 3.498400399110418
  alpha: 0.6
objective_weights:
  risk_term: 1.0
  tracking_error: 1.0
  transaction_cost: 1.0
  alpha_weight: 1.0
  target_deviation: 100000.0
  transaction_fee: 1.0
  turnover_penalty: 0.03
  slippage_penalty: 1.0
```

- [ ] **Step 2: Add a dedicated research manifest with `alpha_model` enabled**

```yaml
name: us_expanded_monthly_alpha_phase_1_5
description: "Expanded US monthly backtest with walk-forward alpha integration"
returns_file: data/risk_inputs_us_expanded/returns_long.csv
market_snapshot: data/universe/us_universe_market_2026-03-27.csv
initial_holdings: data/samples/us/sample_us_04/holdings.csv
target_weights: data/samples/us/sample_us_04/target.csv
reference: data/samples/us/sample_us_04/reference.csv
portfolio_state: data/samples/us/sample_us_04/portfolio_state.yaml
config: config/us_expanded_alpha_phase_1_5.yaml
constraints: config/constraints/us_public_fund.yaml
execution_profile: config/execution/conservative.yaml
baselines:
  - naive_pro_rata
  - buy_and_hold
alpha_model:
  enabled: true
  recipe_name: alt_momentum_4_1
  quantiles: 5
  forward_horizon_days: 5
  min_evaluation_dates: 20
  zscore_winsor_limit: 3.0
  t_stat_full_confidence: 3.0
  max_abs_expected_return: 0.30
  write_alpha_panel: true
  add_alpha_only_baseline: true
```

- [ ] **Step 3: Run focused alpha-enabled regression**

Run: `python -m pytest tests/test_backtest.py tests/test_objective_integration_mode.py tests/test_solver_objective_decomposition.py -q`  
Expected: PASS

- [ ] **Step 4: Run full regression**

Run: `python -m pytest -q`  
Expected: PASS

- [ ] **Step 5: Run the real Phase 1.5 backtest**

Run: `python -m portfolio_os.api.cli backtest --manifest data/backtest_samples/manifest_us_expanded_alpha_phase_1_5.yaml --output-dir outputs/phase1_5_alpha_us_expanded`

Expected outputs:

- `backtest_results.json`
- `nav_series.csv`
- `period_attribution.csv`
- `backtest_report.md`
- `alpha_panel.csv`

- [ ] **Step 6: Write the decision note**

```markdown
# Phase 1.5 Alpha Decision Note

- accepted recipe: `alt_momentum_4_1`
- alpha bridge: walk-forward rank -> z -> empirical annualized expected return
- hard gate: optimizer Sharpe vs naive
- stretch benchmark: optimizer Sharpe vs alpha_only_top_quintile
- final decision:
- promotion status:
```

- [ ] **Step 7: Update task memory**

```markdown
- Phase 1.5 alpha integration is implemented in the backtest stack.
- Dedicated research manifest: `data/backtest_samples/manifest_us_expanded_alpha_phase_1_5.yaml`
- Hard gate result:
- Stretch benchmark result:
- Next recommended action:
```

- [ ] **Step 8: Commit**

```bash
git add config/us_expanded_alpha_phase_1_5.yaml data/backtest_samples/manifest_us_expanded_alpha_phase_1_5.yaml docs/phase_1_5_alpha_decision_note.md TASK_MEMORY.md
git commit -m "docs: record phase 1.5 alpha decision"
```

### Task 6: Final Verification

**Files:**
- No new files

- [ ] **Step 1: Re-run the full suite after all commits**

Run: `python -m pytest -q`  
Expected: PASS

- [ ] **Step 2: Confirm worktree is clean**

Run: `git status --short --branch`  
Expected: clean branch state
