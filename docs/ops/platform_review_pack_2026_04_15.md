# Platform Review Pack

Date: 2026-04-15  
Scope: research-governance surface + paper-calibration surface

## Purpose

This pack is a compact reviewer-facing snapshot of the two strongest current platform artifacts:

1. a real archived-research bundle rendered through the new promotion-registry surface
2. the current neutral paper-calibration read with dedicated pre-trade reference capture

This is not a promotion decision memo and not an alpha claim.
It is a platform-review artifact.

## Section 1: Promotion Registry Read

### Command

```powershell
python -m portfolio_os.api.cli promotion-registry --input-root C:\Users\14574\Quant\qlib_spikes\portfolioos_signal_probe_01\.worktrees\ashare-a1\outputs\ashare_stage3_promotion_bundle --output-dir C:\Users\14574\Quant\PortfolioOS\outputs\promotion_registry_smoke_2026-04-15_ashare_stage3
```

### Output Root

- `C:\Users\14574\Quant\PortfolioOS\outputs\promotion_registry_smoke_2026-04-15_ashare_stage3`

### Reviewer Read

- bundle id:
  - `ashare_stage3_candidate_2026-04-08`
- research line:
  - `ashare`
- candidate status:
  - `stage3_candidate_not_promoted`
- signals:
  - `anti_mom_21_5`
  - `institutional_crowding`
- Stage 4 eligible:
  - `False`
- blocking reason:
  - shared-sample combo uplift is positive, but second-half IR does not exceed the best single signal

### Interpretation Boundary

- this confirms that PortfolioOS can consume a real archived research bundle without importing branch-local code
- this does **not** mean the candidate was promoted
- current registry semantics remain advisory only

## Section 2: Paper Calibration Read

### Canonical Single-Run Root

- `C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_live_2026-04-15_v3`

### Canonical Single-Run Read

- fill rate:
  - `100%`
- partial fills:
  - `0`
- rejected orders:
  - `0`
- dedicated reference snapshot:
  - `captured_ticker_count = 1`
  - `fallback_reference_count = 0`
- quoted mid:
  - `697.33`
- average fill:
  - `697.36`
- reconciliation:
  - `matched_count = 12`
  - `mismatched_count = 0`

### Repeated Tranche Root

- `C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_tranche_2026-04-15_session1`

### Repeated Tranche Read

- run count:
  - `3`
- observation count:
  - `3`
- reference source mix:
  - `{'mid_price': 3}`
- fallback reference count:
  - `0`
- time-of-day coverage:
  - `14:30-16:00` only

### Interpretation Boundary

- the repeated artifact chain is working on real multi-run data
- current sample size is still too small for any drift-regime claim
- next valid step remains `session2` with phase-diversified coverage

## Section 3: Platform-Level Read

The current platform story is:

- research outputs can now be exported as reviewable bundles
- the platform can validate and summarize those bundles through a stable contract
- the execution side can now capture an independent pre-trade reference and compare it with paper fills
- both surfaces are auditable and bounded

The current platform does **not** yet claim:

- production alpha promotion
- live enforcement gating from the registry
- simulator recalibration from the current tiny paper sample
- readiness for RL execution work

## Section 4: Immediate Next Steps

1. Run `paper-calibration` `session2` during market hours on `2026-04-16` using the fixed four-run phase-diversified plan.
2. Re-aggregate once cumulative observations move toward `N = 10-12`.
3. Export a second real promotion bundle through the same registry path before considering any richer reviewer registry surface.
