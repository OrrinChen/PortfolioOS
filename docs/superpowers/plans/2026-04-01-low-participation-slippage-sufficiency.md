# Low Participation Slippage Sufficiency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow TCA overlay readiness to become `sufficient` for the real low-participation trading regime without weakening the broader-span promotion guardrails.

**Architecture:** Keep the existing broad-span sufficiency path intact, then add a second low-participation overlay path keyed off `fit_sample_count >= 30`, bidirectional side coverage, and the existing signal/quality guardrails. Surface the selected path and the low-participation applicability note in summary, diagnostic, and markdown artifacts.

**Tech Stack:** Python 3.11, pandas, pytest

---

### Task 1: Lock the Low-Participation Behavior With Tests

**Files:**
- Modify: `C:/Users/14574/Quant/PortfolioOS/tests/test_slippage_calibration.py`
- Test: `C:/Users/14574/Quant/PortfolioOS/tests/test_slippage_calibration.py`

- [ ] **Step 1: Write the failing low-participation sufficiency test**

```python
def test_low_participation_dense_coverage_can_be_sufficient_without_span(tmp_path: Path) -> None:
    ...
    assert result.summary["sufficient_participation_span"] is False
    assert result.summary["sufficient_low_participation_coverage"] is True
    assert result.summary["overlay_readiness"] == "sufficient"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_slippage_calibration.py::test_low_participation_dense_coverage_can_be_sufficient_without_span -q`
Expected: FAIL because the current implementation still requires participation span for `overlay_readiness = sufficient`.

- [ ] **Step 3: Add assertions for the new audit note fields**

```python
assert "0-0.1%" in result.summary["participation_range_note"]
assert "0-0.1%" in result.diagnostic_manifest["participation_range_note"]
assert "participation_range_note" in result.report_markdown
```

- [ ] **Step 4: Run the focused slippage calibration tests**

Run: `python -m pytest tests/test_slippage_calibration.py -q`
Expected: FAIL only on the new low-participation sufficiency assertions before implementation.

### Task 2: Implement the Dual-Path Sufficiency Logic

**Files:**
- Modify: `C:/Users/14574/Quant/PortfolioOS/src/portfolio_os/execution/slippage_calibration.py`
- Test: `C:/Users/14574/Quant/PortfolioOS/tests/test_slippage_calibration.py`

- [ ] **Step 1: Add a machine-readable low-participation overlay path**

```python
low_participation_coverage_ok = (
    int(fit_sample_count) >= 30
    and bool(bidirectional_fit_coverage)
)
```

- [ ] **Step 2: Preserve the existing broad-span path**

```python
broad_span_overlay_ok = enough_orders and enough_span and positive_signal_ok and metrics_improved
```

- [ ] **Step 3: Make overlay readiness sufficient when either path is satisfied**

```python
if broad_span_overlay_ok:
    ...
elif low_participation_coverage_ok and positive_signal_ok and metrics_improved:
    return ("sufficient", "apply_as_paper_overlay", "low_participation_overlay_guardrails_satisfied")
```

- [ ] **Step 4: Keep default-promotion recommendation stricter than overlay readiness**

```python
elif low_participation_coverage_ok and candidate_k is not None and candidate_k > 0:
    recommendation = "provisional_only"
    recommendation_reason = "low_participation_overlay_only"
```

- [ ] **Step 5: Add summary and diagnostic fields**

```python
"bidirectional_fit_coverage": bidirectional_fit_coverage,
"sufficient_low_participation_coverage": low_participation_coverage_ok,
"participation_range_note": participation_range_note,
```

- [ ] **Step 6: Render the note in the markdown report**

```python
f"- participation_range_note: {summary.get('participation_range_note', 'N/A')}",
```

### Task 3: Verify and Update Memory

**Files:**
- Modify: `C:/Users/14574/Quant/PortfolioOS/TASK_MEMORY.md`
- Test: `C:/Users/14574/Quant/PortfolioOS/tests/test_slippage_calibration.py`

- [ ] **Step 1: Run focused regression**

Run: `python -m pytest tests/test_slippage_calibration.py -q`
Expected: PASS

- [ ] **Step 2: Run full regression**

Run: `python -m pytest -q`
Expected: PASS with the current machine-specific warning/skip profile.

- [ ] **Step 3: Update handoff memory**

```markdown
- low-participation overlay sufficiency now exists for `fit_sample_count >= 30`
- broad-span sufficiency remains intact for wider-range calibration decisions
- artifacts now carry a `participation_range_note`
```
