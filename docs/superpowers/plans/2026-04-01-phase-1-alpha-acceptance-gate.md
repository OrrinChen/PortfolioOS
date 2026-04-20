# Phase 1 Alpha Acceptance Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run the Phase 1 alpha acceptance gate so PortfolioOS can automatically evaluate factor recipes on the frozen US snapshot and close Phase 1 with a machine-readable `accepted` or `rejected_but_infrastructure_complete` decision.

**Architecture:** Add a new `portfolio_os.alpha.acceptance` workflow that wraps the existing alpha research engine with recipe definitions, common-date alignment, chronological development/holdout slicing, monthly factor-turnover measurement, dual-gate evaluation, and three-round deterministic recipe expansion. Expose the workflow through a dedicated CLI and human-readable report/note outputs, then run it on the frozen expanded-US snapshot and record the result in project docs and memory.

**Tech Stack:** pandas, numpy, Typer, pytest, existing `portfolio_os.alpha` research helpers, existing snapshot/report helpers.

---

### Task 1: Add Acceptance-Gate Tests First

**Files:**
- Create: `tests/test_alpha_acceptance.py`

- [ ] **Step 1: Write failing tests for the acceptance engine core**

```python
def test_run_alpha_acceptance_gate_accepts_baseline_when_it_clears_absolute_gate(tmp_path: Path) -> None:
    snapshot = _write_acceptance_fixture(tmp_path)
    result = run_alpha_acceptance_gate(
        returns_file=snapshot.returns_file,
        output_dir=tmp_path / "acceptance_output",
        recipe_configs=default_round_one_recipes(),
        max_rounds=1,
    )
    assert result.decision_payload["status"] == "accepted"
    assert result.decision_payload["acceptance_mode"] == "accepted_as_baseline"


def test_run_alpha_acceptance_gate_can_reject_when_no_recipe_clears_gate(tmp_path: Path) -> None:
    snapshot = _write_rejection_fixture(tmp_path)
    result = run_alpha_acceptance_gate(
        returns_file=snapshot.returns_file,
        output_dir=tmp_path / "acceptance_output",
        recipe_configs=default_round_one_recipes(),
        max_rounds=3,
    )
    assert result.decision_payload["status"] == "rejected_but_infrastructure_complete"


def test_run_alpha_acceptance_gate_writes_expected_artifacts(tmp_path: Path) -> None:
    snapshot = _write_acceptance_fixture(tmp_path)
    run_alpha_acceptance_gate(
        returns_file=snapshot.returns_file,
        output_dir=tmp_path / "acceptance_output",
        recipe_configs=default_round_one_recipes(),
        max_rounds=1,
    )
    assert (tmp_path / "acceptance_output" / "alpha_sweep_summary.csv").exists()
    assert (tmp_path / "acceptance_output" / "alpha_sweep_manifest.json").exists()
    assert (tmp_path / "acceptance_output" / "alpha_acceptance_decision.json").exists()
    assert (tmp_path / "acceptance_output" / "alpha_acceptance_note.md").exists()
```

- [ ] **Step 2: Include focused tests for turnover, common-date alignment, and round expansion**

```python
def test_compute_mean_monthly_factor_turnover_returns_zero_for_stable_top_bucket() -> None:
    frame = _build_turnover_fixture()
    turnover = _compute_mean_monthly_factor_turnover(frame, score_column="alpha_score", quantiles=2)
    assert turnover == 0.0


def test_align_recipe_results_uses_common_evaluation_dates_only(tmp_path: Path) -> None:
    recipe_results = _build_alignment_fixture(tmp_path)
    summary = _align_recipe_slice_metrics(recipe_results)
    assert summary["common_evaluation_date_count"] == 3


def test_expand_round_candidates_deduplicates_previous_recipes() -> None:
    expanded = _expand_round_recipes(
        round_number=2,
        parent_recipes=default_round_one_recipes()[:2],
        tested_recipe_names={item.recipe_name for item in default_round_one_recipes()},
    )
    assert len({item.recipe_name for item in expanded}) == len(expanded)
```

- [ ] **Step 3: Add a CLI test**

```python
def test_alpha_acceptance_cli_writes_outputs(tmp_path: Path) -> None:
    snapshot = _write_acceptance_fixture(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        alpha_acceptance_app,
        [
            "--returns-file", str(snapshot.returns_file),
            "--output-dir", str(tmp_path / "cli_output"),
            "--max-rounds", "1",
        ],
    )
    assert result.exit_code == 0, result.output
    assert (tmp_path / "cli_output" / "alpha_acceptance_decision.json").exists()
```

- [ ] **Step 4: Run the focused test file to verify RED**

Run: `python -m pytest tests/test_alpha_acceptance.py -q`

Expected: FAIL with import or missing-symbol errors for the new acceptance workflow.

- [ ] **Step 5: Commit**

```bash
git add tests/test_alpha_acceptance.py
git commit -m "test: add alpha acceptance gate coverage"
```

### Task 2: Implement The Acceptance Engine

**Files:**
- Create: `src/portfolio_os/alpha/acceptance.py`
- Modify: `src/portfolio_os/alpha/research.py`
- Modify: `src/portfolio_os/alpha/__init__.py`

- [ ] **Step 1: Implement recipe and result dataclasses**

```python
@dataclass(frozen=True)
class AlphaRecipeConfig:
    recipe_name: str
    reversal_lookback_days: int
    momentum_lookback_days: int
    momentum_skip_days: int
    forward_horizon_days: int
    reversal_weight: float
    momentum_weight: float
    quantiles: int = 5
    min_assets_per_date: int = 20


@dataclass
class AlphaAcceptanceResult:
    returns_file: Path
    output_dir: Path
    summary_frame: pd.DataFrame
    decision_payload: dict[str, object]
    note_markdown: str


def default_round_one_recipes() -> list[AlphaRecipeConfig]:
    return [
        AlphaRecipeConfig("equal_weight_momentum_6_1", 21, 126, 21, 5, 0.0, 1.0),
        AlphaRecipeConfig("momentum_heavy_10_90", 21, 126, 21, 5, 0.1, 0.9),
        AlphaRecipeConfig("momentum_heavy_25_75", 21, 126, 21, 5, 0.25, 0.75),
        AlphaRecipeConfig("current_50_50", 21, 126, 21, 5, 0.5, 0.5),
        AlphaRecipeConfig("alt_momentum_4_1", 21, 84, 21, 5, 0.0, 1.0),
    ]
```

- [ ] **Step 2: Add reusable evaluation helpers**

```python
def build_alpha_recipe_result(
    returns_panel: pd.DataFrame,
    *,
    recipe: AlphaRecipeConfig,
) -> dict[str, pd.DataFrame]:
    signal_frame = build_alpha_research_frame(
        returns_panel,
        reversal_lookback_days=recipe.reversal_lookback_days,
        momentum_lookback_days=recipe.momentum_lookback_days,
        momentum_skip_days=recipe.momentum_skip_days,
        forward_horizon_days=recipe.forward_horizon_days,
        reversal_weight=recipe.reversal_weight,
        momentum_weight=recipe.momentum_weight,
    )
    ic_frame = build_alpha_ic_frame(
        signal_frame,
        min_assets_per_date=recipe.min_assets_per_date,
        quantiles=recipe.quantiles,
    )
    return {"signal_frame": signal_frame, "ic_frame": ic_frame}
```

- [ ] **Step 3: Implement common-date alignment, chronological slicing, and turnover**

```python
def _intersect_evaluation_dates(recipe_ic_frames: list[pd.DataFrame]) -> list[str]:
    date_sets = [set(frame["date"].tolist()) for frame in recipe_ic_frames if not frame.empty]
    if not date_sets:
        return []
    return sorted(set.intersection(*date_sets))


def _split_common_dates(common_dates: list[str]) -> tuple[list[str], list[str]]:
    split_index = int(len(common_dates) * 0.6)
    return common_dates[:split_index], common_dates[split_index:]


def _compute_mean_monthly_factor_turnover(
    signal_frame: pd.DataFrame,
    *,
    score_column: str,
    quantiles: int,
) -> float:
    monthly_dates = (
        pd.to_datetime(signal_frame["date"])
        .to_series(index=signal_frame.index)
        .groupby(pd.to_datetime(signal_frame["date"]).dt.to_period("M"))
        .max()
        .dt.strftime("%Y-%m-%d")
        .tolist()
    )
    return _average_top_quantile_turnover(signal_frame, score_column=score_column, quantiles=quantiles, monthly_dates=monthly_dates)
```

- [ ] **Step 4: Implement gate evaluation and deterministic round expansion**

```python
def _evaluate_relative_gate(candidate_row: pd.Series, baseline_row: pd.Series) -> bool:
    return (
        float(candidate_row["mean_rank_ic"]) > float(baseline_row["mean_rank_ic"])
        and float(candidate_row["mean_top_bottom_spread"]) > float(baseline_row["mean_top_bottom_spread"])
        and float(candidate_row["positive_rank_ic_ratio"]) >= float(baseline_row["positive_rank_ic_ratio"]) - 0.02
    )


def _evaluate_absolute_gate(summary_row: pd.Series) -> bool:
    return (
        float(summary_row["mean_rank_ic"]) >= 0.01
        and float(summary_row["positive_rank_ic_ratio"]) >= 0.52
        and float(summary_row["mean_top_bottom_spread"]) > 0.0
        and int(summary_row["evaluation_date_count"]) >= 40
        and float(summary_row["mean_monthly_factor_turnover"]) <= 0.8
    )


def _expand_round_recipes(
    *,
    round_number: int,
    parent_recipes: list[AlphaRecipeConfig],
    tested_recipe_names: set[str],
) -> list[AlphaRecipeConfig]:
    candidates: list[AlphaRecipeConfig] = []
    for parent in parent_recipes:
        for lookback in (84, 126, 168) if round_number == 2 else (63, parent.momentum_lookback_days, 189):
            for skip in (10, 21) if round_number == 2 else (5, 10, 21):
                for reversal_weight in (0.0, 0.1, 0.25) if round_number == 2 else (0.0, 0.05, 0.1, 0.25):
                    recipe = AlphaRecipeConfig(
                        recipe_name=f\"mom_{lookback}_{skip}_rev_{int(reversal_weight * 100):02d}\",
                        reversal_lookback_days=21,
                        momentum_lookback_days=lookback,
                        momentum_skip_days=skip,
                        forward_horizon_days=5,
                        reversal_weight=reversal_weight,
                        momentum_weight=1.0 - reversal_weight,
                    )
                    if recipe.recipe_name not in tested_recipe_names:
                        candidates.append(recipe)
    return candidates
```

- [ ] **Step 5: Implement the top-level workflow**

```python
def run_alpha_acceptance_gate(
    *,
    returns_file: str | Path,
    output_dir: str | Path,
    recipe_configs: list[AlphaRecipeConfig] | None = None,
    max_rounds: int = 3,
) -> AlphaAcceptanceResult:
    recipes = list(recipe_configs or default_round_one_recipes())
    returns_panel = load_returns_panel(returns_file)
    round_results = _run_acceptance_rounds(returns_panel=returns_panel, recipes=recipes, max_rounds=max_rounds)
    summary_frame = _build_acceptance_summary_frame(round_results)
    decision_payload = _build_acceptance_decision_payload(round_results, summary_frame)
    note_markdown = render_alpha_acceptance_note(decision_payload, summary_frame=summary_frame)
    _write_acceptance_artifacts(output_dir, summary_frame, decision_payload, note_markdown)
    return AlphaAcceptanceResult(Path(returns_file), Path(output_dir), summary_frame, decision_payload, note_markdown)
```

- [ ] **Step 6: Export the public API**

```python
from portfolio_os.alpha.acceptance import AlphaAcceptanceResult, AlphaRecipeConfig, run_alpha_acceptance_gate
```

- [ ] **Step 7: Run focused tests to verify GREEN**

Run: `python -m pytest tests/test_alpha_acceptance.py -q`

Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add src/portfolio_os/alpha/acceptance.py src/portfolio_os/alpha/research.py src/portfolio_os/alpha/__init__.py tests/test_alpha_acceptance.py
git commit -m "feat: add alpha acceptance engine"
```

### Task 3: Add Reporting And CLI Surface

**Files:**
- Modify: `src/portfolio_os/alpha/report.py`
- Modify: `src/portfolio_os/api/cli.py`
- Modify: `pyproject.toml`

- [ ] **Step 1: Add acceptance report rendering**

```python
def render_alpha_acceptance_note(
    decision_payload: dict[str, object],
    *,
    summary_frame: pd.DataFrame,
) -> str:
    winner = decision_payload.get("accepted_recipe_name")
    status = decision_payload["status"]
    return "\n".join(
        [
            "# Phase 1 Alpha Acceptance Note",
            "",
            f"- Final status: `{status}`",
            f"- Accepted recipe: `{winner}`" if winner else "- Accepted recipe: none",
            f"- Acceptance mode: `{decision_payload.get('acceptance_mode')}`",
            "",
            "## Holdout Summary",
            summary_frame.to_csv(index=False),
        ]
    )
```

- [ ] **Step 2: Add a dedicated CLI app**

```python
alpha_acceptance_app = typer.Typer(add_completion=False, help="PortfolioOS alpha acceptance-gate CLI.")


@alpha_acceptance_app.command()
def main(
    returns_file: Path = typer.Option(..., exists=True, file_okay=True, dir_okay=False),
    output_dir: Path = typer.Option(...),
    max_rounds: int = typer.Option(3),
) -> None:
    result = run_alpha_acceptance_gate(
        returns_file=returns_file,
        output_dir=output_dir,
        max_rounds=max_rounds,
    )
```

- [ ] **Step 3: Add the poetry entry point**

```toml
portfolio-os-alpha-acceptance = "portfolio_os.api.cli:alpha_acceptance_app"
```

- [ ] **Step 4: Re-run focused tests including CLI**

Run: `python -m pytest tests/test_alpha_acceptance.py -q`

Expected: PASS including CLI coverage.

- [ ] **Step 5: Commit**

```bash
git add src/portfolio_os/alpha/report.py src/portfolio_os/api/cli.py pyproject.toml tests/test_alpha_acceptance.py
git commit -m "feat: add alpha acceptance cli and reports"
```

### Task 4: Run The Real Frozen-Snapshot Gate

**Files:**
- Create: `docs/phase_1_alpha_closeout_note.md`
- Modify: `TASK_MEMORY.md`

- [ ] **Step 1: Run the acceptance gate on the frozen expanded-US snapshot**

Run:

```bash
python -c "import sys; sys.path.insert(0, 'src'); from portfolio_os.api.cli import alpha_acceptance_app; sys.argv = ['portfolio-os-alpha-acceptance', '--returns-file', 'data/risk_inputs_us_expanded/returns_long.csv', '--output-dir', 'outputs/phase1_alpha_acceptance_us_expanded']; alpha_acceptance_app()"
```

Expected:

- `outputs/phase1_alpha_acceptance_us_expanded/alpha_sweep_summary.csv`
- `outputs/phase1_alpha_acceptance_us_expanded/alpha_sweep_manifest.json`
- `outputs/phase1_alpha_acceptance_us_expanded/alpha_acceptance_decision.json`
- `outputs/phase1_alpha_acceptance_us_expanded/alpha_acceptance_note.md`

- [ ] **Step 2: Write the project closeout note**

```markdown
# Phase 1 Alpha Closeout Note

- snapshot: `data/risk_inputs_us_expanded/risk_inputs_manifest.json`
- final status: `accepted` or `rejected_but_infrastructure_complete`
- winning or baseline recipe:
- holdout metrics:
- recommended next step:
```

- [ ] **Step 3: Update task memory with the gate outcome**

```markdown
- Phase 1 acceptance gate is now implemented and has been run on the frozen expanded-US snapshot.
- final Phase 1 status:
- winning recipe or rejection reason:
```

- [ ] **Step 4: Commit**

```bash
git add docs/phase_1_alpha_closeout_note.md TASK_MEMORY.md
git commit -m "docs: record phase 1 alpha closeout"
```

### Task 5: Verify End-To-End

**Files:**
- No additional source files required

- [ ] **Step 1: Run focused alpha acceptance tests**

Run: `python -m pytest tests/test_alpha_acceptance.py -q`

Expected: all new acceptance tests pass.

- [ ] **Step 2: Run alpha regression slice**

Run: `python -m pytest tests/test_alpha_acceptance.py tests/test_alpha_research.py -q`

Expected: acceptance tests and prior alpha research tests pass together.

- [ ] **Step 3: Run full regression**

Run: `python -m pytest -q`

Expected: full suite passes from the worktree baseline plus new acceptance coverage.

- [ ] **Step 4: Confirm worktree status is clean except intended commits**

Run: `git status --short --branch`

Expected: clean branch state after final commit.
