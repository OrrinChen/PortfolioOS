# US Left-Tail B2 Cohort Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the B2 bad-month cohort study so PortfolioOS can compare worst-quintile outer vs inner left-tail months against non-bad bootstrap cohorts without reviving the invalid low-frequency crash-mode framing.

**Architecture:** Extend the existing long-horizon research helpers with cohort construction, bootstrap null benchmarking, temporal-distribution summaries, and cross-sectional comparison utilities. Then wire those helpers into the existing US long-horizon runner so B2 artifacts sit beside Layer A and B1/B4 outputs and can be reviewed in one note.

**Tech Stack:** Python 3.11, pandas, numpy, pytest, yfinance

---

### Task 1: Add B2 helper tests and implementations

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\tests\test_alpha_long_horizon.py`
- Modify: `C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\alpha\long_horizon.py`

- [ ] **Step 1: Write the failing helper tests**

Add focused tests for:

```python
def test_build_bad_month_cohorts_splits_worst_quintile_into_outer_and_inner() -> None:
    ...

def test_build_bootstrap_null_summary_reports_percentile_and_ratio() -> None:
    ...

def test_build_temporal_distribution_summary_reports_median_year_and_iqr() -> None:
    ...

def test_build_leg_concentration_metrics_respects_small_loss_guard() -> None:
    ...
```

- [ ] **Step 2: Run the helper test file to verify red**

Run:

```powershell
python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_alpha_long_horizon.py -q
```

Expected:

- `FAIL` because the new B2 helper functions do not exist yet

- [ ] **Step 3: Write the minimal helper implementation**

Implement focused helpers in `long_horizon.py` for:

- cohort ranking from raw `operational 21d top_bottom_spread`
- fixed `worst quintile -> outer 23 / inner 24` split
- non-bad bootstrap comparisons with:
  - `5000` iterations
  - without-replacement sampling
  - fixed seed carried into metadata
- temporal distribution summaries
- size proxy using `adjusted_close * historical_shares`
- static sector / industry label handling with explicit time caveat
- top-leg and bottom-leg HHI / effective-N with the `0.5%` small-loss guard
- long/short attribution vector distance

- [ ] **Step 4: Run the helper test file to verify green**

Run:

```powershell
python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_alpha_long_horizon.py -q
```

Expected:

- `PASS`

### Task 2: Integrate B2 into the long-horizon runner

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\scripts\run_us_long_horizon_signal_extension.py`

- [ ] **Step 1: Add runner logic for B2 artifacts**

Wire the runner to produce:

- cohort membership table
- temporal-distribution summary
- bootstrap metadata
- dimension comparison tables for:
  - size
  - sector / industry
  - pre-crash return
  - pre-crash volatility
  - top-leg / bottom-leg concentration
  - long/short attribution

Use raw `operational 21d top_bottom_spread` as the ranking basis and preserve the hypothesis-level interpretation ceiling in the markdown note.

- [ ] **Step 2: Run the script once to verify it completes**

Run:

```powershell
python C:\Users\14574\Quant\PortfolioOS\scripts\run_us_long_horizon_signal_extension.py
```

Expected:

- script exits `0`
- output directory contains new `layer_b2_*` artifacts

- [ ] **Step 3: Check artifact integrity**

Inspect the output directory and confirm:

- bootstrap metadata contains the fixed seed and iteration count
- size coverage sanity check exists
- sector / industry caveat appears in the note
- outer vs inner and bad vs non-bad comparisons are all present

### Task 3: Verify the slice end to end

**Files:**
- Modify: `C:\Users\14574\Quant\PortfolioOS\TASK_MEMORY.md`

- [ ] **Step 1: Run focused verification**

Run:

```powershell
python -m pytest C:\Users\14574\Quant\PortfolioOS\tests\test_alpha_long_horizon.py -q
```

Expected:

- all B2 helper tests pass

- [ ] **Step 2: Run syntax verification**

Run:

```powershell
python -m py_compile C:\Users\14574\Quant\PortfolioOS\src\portfolio_os\alpha\long_horizon.py C:\Users\14574\Quant\PortfolioOS\scripts\run_us_long_horizon_signal_extension.py
```

Expected:

- no output

- [ ] **Step 3: Update memory with the new B2 read**

Add a compact note to `TASK_MEMORY.md` covering:

- B2 framing rewrite away from `N=3 crash-month mode`
- whether outer and inner look same-type or different-type
- whether non-bad bootstrap supports a real cross-sectional pattern
- whether B3 is now opened, deferred, or rewritten

- [ ] **Step 4: Prepare the branch for closeout**

Stage only the B2 implementation files, tests, spec, plan, and any directly related output notes when the slice is verified.
