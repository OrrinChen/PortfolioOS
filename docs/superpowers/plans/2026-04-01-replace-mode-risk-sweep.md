# Replace-Mode Risk Sweep Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the calibrated expanded-US risk-aversion sweep in `replace` mode so we can isolate whether the problem is specific to `augment` mode or fundamental to the current risk-only objective.

**Architecture:** Keep the new risk sweep tooling unchanged. Add one calibrated research config plus one matching backtest manifest, then run the same real-data multiplier grid against that manifest and compare the resulting frontier with the earlier `augment`-mode run.

**Tech Stack:** YAML configs/manifests, PortfolioOS backtest sweep CLI, pytest for lightweight regression confidence.

---

### Task 1: Create Replace-Mode Research Inputs

**Files:**
- Create: `config/us_expanded_tca_calibrated_replace.yaml`
- Create: `data/backtest_samples/manifest_us_expanded_tca_calibrated_replace.yaml`

- [ ] **Step 1: Add the replace-mode calibrated config**

Create `config/us_expanded_tca_calibrated_replace.yaml` by mirroring `config/us_expanded_tca_calibrated.yaml`, but set `risk_model.integration_mode: replace` and make the active objective weights explicit for replace mode:

```yaml
slippage:
  k: 3.498400399110418

objective_weights:
  risk_term: 1.0
  tracking_error: 1.0
  transaction_cost: 1.0

risk_model:
  enabled: true
  integration_mode: replace
  returns_path: ../data/risk_inputs_us_expanded/returns_long.csv
  factor_exposure_path: ../data/risk_inputs_us_expanded/factor_exposure.csv
```

- [ ] **Step 2: Add the matching manifest**

Create `data/backtest_samples/manifest_us_expanded_tca_calibrated_replace.yaml` by mirroring `data/backtest_samples/manifest_us_expanded_tca_calibrated.yaml` and pointing `config:` at the new replace-mode config:

```yaml
name: us_expanded_monthly_tca_calibrated_replace
description: "Expanded US monthly backtest what-if with low-participation calibrated TCA slippage k in replace-mode risk integration"
config: config/us_expanded_tca_calibrated_replace.yaml
```

### Task 2: Run The Replace-Mode Sweep

**Files:**
- Create: `outputs/risk_sweep_us_expanded_tca_calibrated_replace/...`

- [ ] **Step 1: Execute the same multiplier grid**

Run:

```bash
python -c "import sys; sys.path.insert(0, 'src'); from portfolio_os.api.cli import risk_sweep_app; sys.argv = ['portfolio-os-risk-sweep', '--manifest', 'data/backtest_samples/manifest_us_expanded_tca_calibrated_replace.yaml', '--output-dir', 'outputs/risk_sweep_us_expanded_tca_calibrated_replace', '--risk-aversion-multiplier', '1.0', '--risk-aversion-multiplier', '100.0', '--risk-aversion-multiplier', '1000.0', '--risk-aversion-multiplier', '10000.0', '--risk-aversion-multiplier', '100000.0']; risk_sweep_app()"
```

Expected: `risk_sweep_summary.csv`, `risk_aversion_frontier_report.md`, and `risk_sweep_manifest.json` are written under `outputs/risk_sweep_us_expanded_tca_calibrated_replace/`.

### Task 3: Verify And Record The Interpretation

**Files:**
- Modify: `TASK_MEMORY.md`

- [ ] **Step 1: Run focused regression confidence**

Run:

```bash
python -m pytest tests/test_backtest.py -k "risk_sweep" -q
```

Expected: existing risk sweep tests still pass.

- [ ] **Step 2: Compare the frontier**

Inspect:

```text
outputs/risk_sweep_us_expanded_tca_calibrated_replace/risk_sweep_summary.csv
outputs/risk_sweep_us_expanded_tca_calibrated_replace/risk_aversion_frontier_report.md
```

Record whether:
- annualized volatility still declines as the multiplier rises
- Sharpe shows any non-monotonic sweet spot
- `optimizer_vs_naive_ending_nav_delta` improves, stabilizes, or worsens versus the earlier augment-mode sweep

- [ ] **Step 3: Update memory**

Append a `TASK_MEMORY.md` section with:
- the new replace-mode config and manifest paths
- the exact output root
- the multiplier-by-multiplier summary
- the diagnostic conclusion: `replace` mode helped or it did not
