# Calibration Residualization Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the next charter-required D3 read by residualizing calibration-family expressions against the frozen baseline and writing residualized evaluation artifacts.

**Architecture:** Extend the existing calibration harness rather than creating a second runner. Keep the new logic expression-only, baseline-anchored, and artifact-driven: build residualized signal frames from the existing calibration expressions, evaluate them on the same forward-return grid, and write one residualized summary artifact plus note/memory updates. Tests stay focused on artifact shape and summary semantics.

**Tech Stack:** Python, pandas, pytest, existing `portfolio_os.alpha.discovery_calibration` helpers, markdown notes

---

### Task 1: Add Failing Tests For Baseline Residualization Outputs

**Files:**
- Modify: `tests/test_alpha_discovery_calibration.py`

- [ ] **Step 1: Write the failing tests**

Add tests that require:

```python
def test_build_baseline_residualized_summary_returns_expression_rows_only(tmp_path: Path) -> None:
    returns_path, reference_path = _write_fixture(tmp_path)
    result = run_us_residual_momentum_calibration_from_files(
        returns_file=returns_path,
        universe_reference_file=reference_path,
        output_dir=tmp_path / "calibration_run",
        random_seed=7,
    )

    residualized = build_baseline_residualized_expression_summary(
        returns_panel=load_alpha_returns_panel(returns_path),
        universe_reference=pd.read_csv(reference_path),
        expression_ids=["RM1_MKT_RESIDUAL", "RM2_SECTOR_RESIDUAL", "RM3_VOL_MANAGED"],
    )

    assert list(residualized["expression_id"]) == ["RM1_MKT_RESIDUAL", "RM2_SECTOR_RESIDUAL", "RM3_VOL_MANAGED"]
    assert {"residualized_mean_rank_ic", "residualized_rank_ic_t", "residualized_mean_top_bottom_spread"}.issubset(residualized.columns)


def test_run_us_residual_momentum_calibration_writes_residualized_summary_artifact(tmp_path: Path) -> None:
    returns_path, reference_path = _write_fixture(tmp_path)
    output_dir = tmp_path / "calibration_run"

    result = run_us_residual_momentum_calibration_from_files(
        returns_file=returns_path,
        universe_reference_file=reference_path,
        output_dir=output_dir,
        random_seed=7,
    )

    assert (output_dir / "residualized_vs_baseline_summary.csv").exists()
    expression_rows = result.summary_frame.loc[result.summary_frame["role"] == "expression"]
    assert "baseline_residualized_rank_ic_t" in expression_rows.columns
    assert expression_rows["baseline_residualized_rank_ic_t"].notna().all()
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
python -m pytest tests/test_alpha_discovery_calibration.py -q
```

Expected:
- FAIL because `build_baseline_residualized_expression_summary` does not exist and the residualized summary artifact/columns are missing

### Task 2: Implement Baseline Residualization In The Calibration Harness

**Files:**
- Modify: `src/portfolio_os/alpha/discovery_calibration.py`

- [ ] **Step 1: Add the residualization helper**

Implement a helper with this signature:

```python
def build_baseline_residualized_expression_summary(
    *,
    returns_panel: pd.DataFrame,
    universe_reference: pd.DataFrame,
    expression_ids: list[str],
) -> pd.DataFrame:
    ...
```

The helper should:

```python
baseline_frame = build_calibration_signal_frame(
    returns_panel=returns_panel,
    universe_reference=universe_reference,
    expression_id="CTRL3_BASELINE_MIMIC",
)
forward_return_frame = build_monthly_forward_return_frame(
    returns_panel=returns_panel,
    universe_reference=universe_reference,
)

for expression_id in expression_ids:
    signal_frame = build_calibration_signal_frame(
        returns_panel=returns_panel,
        universe_reference=universe_reference,
        expression_id=expression_id,
    )
    merged = signal_frame.merge(
        baseline_frame.rename(columns={"signal_value": "baseline_signal_value"}),
        on=["date", "ticker"],
        how="inner",
    ).merge(
        forward_return_frame.loc[:, ["date", "ticker", "forward_return"]],
        on=["date", "ticker"],
        how="inner",
    )
    # per-date cross-sectional residualization:
    # residual_signal = signal_value - beta * baseline_signal_value
    # evaluate residual_signal against forward_return
```

Return one row per expression with:

```python
{
    "expression_id": expression_id,
    "residualized_evaluation_month_count": ...,
    "residualized_mean_rank_ic": ...,
    "residualized_rank_ic_t": ...,
    "residualized_mean_top_bottom_spread": ...,
}
```

- [ ] **Step 2: Thread the residualized summary into the main runner**

Update `run_us_residual_momentum_calibration(...)` so it:

```python
expression_ids = registry_frame.loc[registry_frame["role"] == "expression", "expression_id"].astype(str).tolist()
residualized_summary = build_baseline_residualized_expression_summary(
    returns_panel=returns_panel,
    universe_reference=universe_reference,
    expression_ids=expression_ids,
)
summary_frame = summary_frame.merge(residualized_summary, on="expression_id", how="left")
residualized_summary.to_csv(output_path / "residualized_vs_baseline_summary.csv", index=False)
summary_payload["residualized_expression_count"] = int(len(residualized_summary))
```

- [ ] **Step 3: Update the calibration note rendering**

Add residualization readouts for the best live expression:

```python
f"- baseline-residualized rank IC t-stat: `{float(best_expression['baseline_residualized_rank_ic_t']):.4f}`",
f"- baseline-residualized mean top-bottom spread: `{float(best_expression['baseline_residualized_mean_top_bottom_spread']):.4%}`",
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```powershell
python -m pytest tests/test_alpha_discovery_calibration.py -q
```

Expected:
- PASS

### Task 3: Rerun The Calibration Slice And Update Notes

**Files:**
- Modify: `docs/strategy/us_residual_momentum_calibration_setup_note_2026_04_16.md`
- Modify: `docs/strategy/us_alpha_discovery_v2_memory.md`

- [ ] **Step 1: Rerun the calibration runner**

Run:

```powershell
python scripts/run_us_residual_momentum_calibration.py --output-dir outputs/us_residual_momentum_calibration/2026-04-16
```

Expected:
- artifacts include `residualized_vs_baseline_summary.csv`
- `note.md` shows the best expression's residualized read

- [ ] **Step 2: Update the setup note**

Record:

```md
- the harness now includes a residualization-against-frozen-baseline read
- clarify whether the best live residual expression survives or collapses after baseline removal
- update the practical interpretation:
  - if residualized reads remain weak, the machine is now failing on null separation, winner stability, and baseline-incrementality
  - if residualized reads stay meaningful, the machine still has incremental family content even if winner dominance is weak
```

- [ ] **Step 3: Update branch-local memory**

Add the same residualization conclusion to:

```md
docs/strategy/us_alpha_discovery_v2_memory.md
```

- [ ] **Step 4: Run the full targeted verification**

Run:

```powershell
python -m pytest tests/test_alpha_discovery_calibration.py tests/test_alpha_qualification.py -q
python scripts/run_us_residual_momentum_calibration.py --output-dir outputs/us_residual_momentum_calibration/2026-04-16
```

Expected:
- tests pass
- runner completes
- docs match generated artifacts

- [ ] **Step 5: Commit**

```powershell
git add src/portfolio_os/alpha/discovery_calibration.py tests/test_alpha_discovery_calibration.py docs/strategy/us_residual_momentum_calibration_setup_note_2026_04_16.md docs/strategy/us_alpha_discovery_v2_memory.md docs/superpowers/plans/2026-04-16-calibration-residualization-slice.md
git commit -m "feat: add calibration baseline residualization read"
```
