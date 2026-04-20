# Signal Consistency Diagnostic Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a narrow diagnostic that compares production alpha outputs against the canonical long-horizon operational cross-section without introducing new signal engineering or statistical overreach.

**Architecture:** Reuse the existing real-alpha audit backtest rerun and the long-horizon operational cross-section builder, then join both views month-by-month on ticker intersections. The new module should emit one structured report with per-month detail, pooled consistency metrics, and provenance metadata, while the runner writes reproducible artifacts under `outputs/`.

**Tech Stack:** Python 3.11, pandas, numpy, SciPy rank correlations already exposed through pandas, existing PortfolioOS snapshot writers, existing long-horizon and package-audit helpers.

---

## File Structure

- Create: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\alpha\signal_consistency_diagnostic.py`
  - Build per-month and pooled signal-consistency outputs from canonical and production cross-sections.
- Create: `C:\Users\14574\Quant\PortfolioOS\scripts\run_signal_consistency_diagnostic.py`
  - Rerun the manifest, build baseline and `signed_spread` production views, build canonical long-horizon cross-sections, collect git/provenance metadata, and write artifacts.
- Create: `C:\Users\14574\Quant\PortfolioOS\tests\test_signal_consistency_diagnostic.py`
  - Unit-test pooled/per-month metrics, ragged production views, overlap thresholds, and markdown/metadata output.
- Modify: `C:\Users\14574\Quant\PortfolioOS\scripts\run_us_long_horizon_signal_extension.py`
  - Expose canonical signal-spec constants so provenance is read programmatically instead of hand-copied.
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\alpha\package_audit.py`
  - Expose a small public helper for building one counterfactual production alpha panel so the new runner does not duplicate guard logic.

### Task 1: Freeze canonical and production-view seams

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\scripts\run_us_long_horizon_signal_extension.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\alpha\package_audit.py`
- Test: `C:\Users\14574\Quant\PortfolioOS\tests\test_real_alpha_package_audit.py`

- [ ] **Step 1: Write the failing helper-surface test**

```python
def test_build_counterfactual_alpha_panel_promotes_signed_spread_months_without_mutating_input() -> None:
    alpha_panel = pd.DataFrame(
        {
            "date": ["2025-11-28"] * 4,
            "ticker": ["AAA", "BBB", "CCC", "DDD"],
            "expected_return": [0.0, 0.0, 0.0, 0.0],
            "quantile": [5, 4, 2, 1],
            "alpha_zscore": [1.5, 0.5, -0.5, -1.5],
            "signal_strength_confidence": [0.25] * 4,
            "annualized_top_bottom_spread": [0.0] * 4,
            "period_top_bottom_spread": [0.0] * 4,
            "decision_horizon_days": [1] * 4,
            "raw_mean_top_bottom_spread": [-0.004] * 4,
            "negative_spread_protocol": ["floor_to_zero"] * 4,
            "alpha_protocol_status": ["spread_floor_to_zero"] * 4,
        }
    )

    rebuilt, promoted_dates = build_counterfactual_alpha_panel(
        alpha_panel=alpha_panel,
        negative_spread_mode="signed_spread",
        forward_horizon_days=21,
        max_abs_expected_return=0.2,
    )

    assert set(promoted_dates) == {"2025-11-28"}
    assert bool((rebuilt["expected_return"].abs() > 0).any())
    assert bool((alpha_panel["expected_return"].abs() == 0).all())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_real_alpha_package_audit.py::test_build_counterfactual_alpha_panel_promotes_signed_spread_months_without_mutating_input -q`

Expected: FAIL with `ImportError` or `NameError` because `build_counterfactual_alpha_panel` is not public yet.

- [ ] **Step 3: Implement the minimal public seams**

```python
# run_us_long_horizon_signal_extension.py
CANONICAL_SIGNAL_SPEC = {
    "reversal_lookback_days": 21,
    "momentum_lookback_days": 84,
    "momentum_skip_days": 21,
    "forward_horizon_days": 21,
    "reversal_weight": 0.0,
    "momentum_weight": 1.0,
}

# package_audit.py
def build_counterfactual_alpha_panel(
    *,
    alpha_panel: pd.DataFrame,
    negative_spread_mode: AlphaNegativeSpreadProtocol | str | None,
    forward_horizon_days: int,
    max_abs_expected_return: float,
) -> tuple[pd.DataFrame, set[str]]:
    protocol = None if negative_spread_mode is None else AlphaNegativeSpreadProtocol(str(negative_spread_mode))
    return _build_counterfactual_alpha_panel(
        alpha_panel=alpha_panel,
        negative_spread_mode=protocol,
        forward_horizon_days=forward_horizon_days,
        max_abs_expected_return=max_abs_expected_return,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_real_alpha_package_audit.py -q`

Expected: PASS with all existing package-audit tests green.

- [ ] **Step 5: Commit**

```bash
git add C:\Users\14574\Quant\PortfolioOS\scripts\run_us_long_horizon_signal_extension.py C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\alpha\package_audit.py C:\Users\14574\Quant\PortfolioOS\tests\test_real_alpha_package_audit.py
git commit -m "refactor: expose signal consistency inputs"
```

### Task 2: Add the signal consistency report builder

**Files:**
- Create: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\alpha\signal_consistency_diagnostic.py`
- Test: `C:\Users\14574\Quant\PortfolioOS\tests\test_signal_consistency_diagnostic.py`

- [ ] **Step 1: Write the failing report-builder test**

```python
def test_build_signal_consistency_report_supports_ragged_views_and_pooled_metrics() -> None:
    canonical = pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-10-31"] * 4 + ["2025-11-28"] * 4),
            "ticker": ["AAA", "BBB", "CCC", "DDD"] * 2,
            "alpha_score": [0.4, 0.1, -0.1, -0.4, 0.5, 0.2, -0.2, -0.5],
        }
    )
    baseline = pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-10-31"] * 4),
            "ticker": ["AAA", "BBB", "CCC", "DDD"],
            "alpha_score": [0.4, 0.1, -0.1, -0.4],
            "expected_return": [0.03, 0.01, -0.01, -0.03],
        }
    )
    signed = pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-10-31"] * 4 + ["2025-11-28"] * 4),
            "ticker": ["AAA", "BBB", "CCC", "DDD"] * 2,
            "alpha_score": [0.4, 0.1, -0.1, -0.4, 0.5, 0.2, -0.2, -0.5],
            "expected_return": [0.03, 0.01, -0.01, -0.03, 0.02, 0.01, -0.01, -0.02],
        }
    )

    report = build_signal_consistency_report(
        canonical_cross_section=canonical,
        production_views={"baseline": baseline, "signed_spread": signed},
    )

    assert set(report.per_month_frame["production_view"]) == {"baseline", "signed_spread"}
    assert report.pooled_summary_frame.loc[
        report.pooled_summary_frame["production_view"] == "signed_spread",
        "pooled_alpha_vs_canonical_spearman",
    ].iloc[0] == pytest.approx(1.0)
    assert "portfolioos_head_sha" in report.metadata
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_signal_consistency_diagnostic.py::test_build_signal_consistency_report_supports_ragged_views_and_pooled_metrics -q`

Expected: FAIL because the module and report type do not exist yet.

- [ ] **Step 3: Implement the minimal report builder**

```python
@dataclass(frozen=True)
class SignalConsistencyReport:
    per_month_frame: pd.DataFrame
    pooled_summary_frame: pd.DataFrame
    metadata: dict[str, Any]

    def to_markdown(self) -> str:
        ...

def build_signal_consistency_report(
    *,
    canonical_cross_section: pd.DataFrame,
    production_views: dict[str, pd.DataFrame],
) -> SignalConsistencyReport:
    # normalize dates and tickers
    # build month/view ragged detail rows even when overlap < threshold
    # compute per-month Spearman + overlap metrics on ticker intersections
    # compute pooled metrics with concat_then_correlate
    # return one structured report
```

- [ ] **Step 4: Add edge-case tests before refactor**

```python
def test_build_signal_consistency_report_marks_low_overlap_month_as_nan() -> None:
    ...

def test_build_signal_consistency_report_includes_empty_view_month_rows() -> None:
    ...

def test_build_signal_consistency_report_uses_concat_then_correlate_pooling() -> None:
    ...
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_signal_consistency_diagnostic.py -q`

Expected: PASS with all new diagnostic tests green.

- [ ] **Step 6: Commit**

```bash
git add C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\alpha\signal_consistency_diagnostic.py C:\Users\14574\Quant\PortfolioOS\tests\test_signal_consistency_diagnostic.py
git commit -m "feat: add signal consistency report builder"
```

### Task 3: Add the diagnostic runner and artifact contract

**Files:**
- Create: `C:\Users\14574\Quant\PortfolioOS\scripts\run_signal_consistency_diagnostic.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\tests\test_signal_consistency_diagnostic.py`

- [ ] **Step 1: Write the failing runner smoke test**

```python
def test_signal_consistency_runner_writes_expected_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    output_dir = tmp_path / "signal_consistency"
    # monkeypatch manifest loading, backtest result, canonical cross-section, and git metadata
    main(["--output-dir", str(output_dir)])

    assert (output_dir / "signal_consistency_per_month.csv").exists()
    assert (output_dir / "signal_consistency_pooled_summary.csv").exists()
    assert (output_dir / "signal_consistency_summary.json").exists()
    assert (output_dir / "signal_consistency_note.md").exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_signal_consistency_diagnostic.py::test_signal_consistency_runner_writes_expected_artifacts -q`

Expected: FAIL because the runner does not exist yet.

- [ ] **Step 3: Implement the runner with explicit provenance**

```python
def _git_head_metadata(repo_root: Path) -> dict[str, object]:
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo_root, text=True).strip()
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=repo_root, text=True).strip())
    return {
        "portfolioos_head_sha": f"{sha}-dirty" if dirty else sha,
        "working_tree_clean": not dirty,
        "pooled_method": "concat_then_correlate",
        "min_overlap_threshold": 10,
    }
```

```python
def main(argv: list[str] | None = None) -> None:
    # rerun manifest -> alpha_panel
    # build baseline and signed_spread production view frames
    # rebuild canonical cross-section using long-horizon helper/constants
    # call build_signal_consistency_report(...)
    # write csv/json/md artifacts
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_signal_consistency_diagnostic.py -q`

Expected: PASS with runner smoke coverage included.

- [ ] **Step 5: Commit**

```bash
git add C:\Users\14574\Quant\PortfolioOS\scripts\run_signal_consistency_diagnostic.py C:\Users\14574\Quant\PortfolioOS\tests\test_signal_consistency_diagnostic.py
git commit -m "feat: add signal consistency diagnostic runner"
```

### Task 4: Full verification and artifact generation

**Files:**
- Modify: none beyond previous tasks
- Test: `C:\Users\14574\Quant\PortfolioOS\tests\test_signal_consistency_diagnostic.py`

- [ ] **Step 1: Run the focused verification suite**

Run: `python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_signal_consistency_diagnostic.py C:\Users\14574\Quant\PortfolioOS\tests\test_real_alpha_package_audit.py C:\Users\14574\Quant\PortfolioOS\tests\test_alpha_long_horizon.py -q`

Expected: PASS with zero failures.

- [ ] **Step 2: Run syntax verification**

Run: `python -m py_compile C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\alpha\signal_consistency_diagnostic.py C:\Users\14574\Quant\PortfolioOS\scripts\run_signal_consistency_diagnostic.py C:\Users\14574\Quant\PortfolioOS\scripts\run_us_long_horizon_signal_extension.py C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\alpha\package_audit.py`

Expected: no output, exit code 0.

- [ ] **Step 3: Run the diagnostic runner for real artifacts**

Run: `python C:\Users\14574\Quant\PortfolioOS\scripts\run_signal_consistency_diagnostic.py`

Expected: the script prints one output directory and writes:
- `signal_consistency_per_month.csv`
- `signal_consistency_pooled_summary.csv`
- `signal_consistency_summary.json`
- `signal_consistency_note.md`

- [ ] **Step 4: Review the generated note for the required claim split**

Check that `signal_consistency_note.md` explicitly separates:
- long-horizon `B1 claim`
- small-sample production claim
- pipeline-mechanical consistency claim

- [ ] **Step 5: Commit**

```bash
git add C:\Users\14574\Quant\PortfolioOS\outputs\signal_consistency_diagnostic_* C:\Users\14574\Quant\PortfolioOS\TASK_MEMORY.md
git commit -m "analysis: record signal consistency diagnostic"
```

## Self-Review

- Spec coverage:
  - provenance discipline covered in Task 1 + Task 3
  - per-month + pooled correlations and overlap metrics covered in Task 2
  - ragged baseline/signed-spread production views covered in Task 2
  - artifact runner and note language covered in Task 3 + Task 4
- Placeholder scan:
  - no `TODO`, `TBD`, or “similar to above” placeholders remain
- Type consistency:
  - report type stays `SignalConsistencyReport`
  - public builder stays `build_signal_consistency_report`
  - counterfactual helper stays `build_counterfactual_alpha_panel`

Plan complete and saved to `docs/superpowers/plans/2026-04-16-signal-consistency-diagnostic.md`.

User already approved inline implementation in this session, so proceed with execution in an isolated worktree rather than pausing for execution mode selection.
