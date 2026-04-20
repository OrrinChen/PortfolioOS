# US Residual Momentum Residualization Confirmation Note (2026-04-17)

## Purpose

This note closes the final calibration-family slice required before Alpha Discovery v2 can leave calibration and open the primary family.

The scope here is intentionally narrow. This is **not** a residualization optimization program and **not** a family-rescue effort. It answers only three questions:

1. Is the current residualization implementation algorithmically correct for the current monthly rank-based evaluation?
2. Is the residualization output compatible with `P-001: Exposure-Conditioned Adversarial Nulls`?
3. What is the correct closure state for `FM-001: Null-Consistent Residualization Strengthening`?

## Inputs

- Canonical implementation:
  - `src/portfolio_os/alpha/discovery_calibration.py`
- Canonical hardened artifacts:
  - `outputs/us_residual_momentum_calibration/2026-04-16/residualization_placebo_null_distribution_exposure_conditioned.csv`
  - `outputs/us_residual_momentum_calibration/2026-04-16/baseline_exposure_tercile_null_comparison.csv`
  - `outputs/us_residual_momentum_calibration/2026-04-16/residualized_vs_baseline_exposure_conditioned_summary.csv`

## Q1. Algorithmic Correctness

### Review target

The relevant implementation is:

- `_residualize_against_baseline(...)`
- `_build_residualized_signal_frame(...)`

### Review outcome

The current implementation is **algorithmically acceptable for the present use case**.

Observed properties:

- residualization is performed cross-sectionally, one date at a time
- signal and baseline are aligned on the same `(date, ticker)` intersection before residualization
- the regression slope is estimated from demeaned signal and demeaned baseline
- if overlap is too small or the baseline has zero cross-sectional variance, the routine falls back to the raw signal rather than fabricating a residual
- the output preserves the same date/ticker grid needed by the downstream monthly rank-IC evaluator

Important qualification:

- this implementation is being judged for **monthly cross-sectional ranking use**
- this note does **not** claim that the residualized output is a level-preserving economic forecast
- it only claims that there is no discovered algorithmic defect large enough to justify a redesign before leaving calibration

### Practical conclusion

No broad residualization rewrite is justified by the current evidence.

## Q2. Compatibility With `P-001`

### Question

Does the residualization output interact cleanly with `P-001: Exposure-Conditioned Adversarial Nulls`?

### Answer

Yes.

The hardened null now preserves the same baseline-exposure tercile structure that the residualized live expressions inherit from the baseline-mimic control. Under that conditioned null:

- `RM1_MKT_RESIDUAL` exposure-conditioned percentile: `7%`
- `RM2_SECTOR_RESIDUAL` exposure-conditioned percentile: `8%`
- `RM3_VOL_MANAGED` exposure-conditioned percentile: `15%`

This is the key calibration read:

- once the null is conditioned on the same exposure structure, all three residualized live expressions become plainly non-exceptional
- the earlier apparent residualization "strengthening" no longer survives the adversarial comparison

### Practical conclusion

`P-001` is sufficient for this residualization context.

The current machine state supports the following rule:

> When expression generation alters the exposure distribution, the null must be conditioned on that same exposure partition before any family-level interpretation is allowed.

No additional residualization-specific null machinery is required before opening the primary family.

## Q3. Closure State For `FM-001`

### Observed facts

`FM-001` was created because `RM3_VOL_MANAGED` looked stronger after baseline residualization while still remaining plausibly null-consistent.

After adversarial hardening:

- unconditional null percentile for residualized `RM3`: `81%`
- exposure-conditioned null percentile for residualized `RM3`: `15%`

Exposure tercile read:

- `low` tercile:
  - `rank_ic_t percentile = 1%`
- `mid` tercile:
  - `rank_ic_t percentile = 76%`
- `high` tercile:
  - `rank_ic_t percentile = 56%`

### Interpretation

The combination above means:

- the old "strengthening" read is not family evidence
- the main ambiguity is explained by the missing exposure conditioning in the old null
- however, the tercile asymmetry does not disappear completely and should remain recorded as a known boundary behavior

### Closure state

`FM-001` should now be marked:

- `partially explained`

Meaning:

- explained enough to stop calibration from expanding into a rescue program
- not explained so completely that the entry should be retired

### Practical consequence

`FM-001` stays in the taxonomy as a reusable boundary pattern, but it no longer blocks calibration exit.

## Calibration Exit Decision

Calibration exit conditions are now satisfied:

1. adversarial layer can produce credible conditional-null reads for exposure-biased expressions
2. residualization layer has been reviewed and classified
3. machine governance assets exist:
   - `P-001`
   - `FM-001`

Therefore:

- calibration-family closeout is complete
- `US residual momentum / residual reversal` remains permanently out of scope as an alpha winner claim
- the next lane is the primary family:
  - `A-share state-transition microstructure`

## Prior Shift Note Before Opening The Primary Family

Calibration and primary mining should not use the same prior.

Calibration prior:

- suspicious reads are presumed machine-boundary behavior until proven otherwise

Primary-family prior:

1. first suspect family-specific event/data structure
2. then suspect a mismatch between the new family and an existing machine principle
3. only then conclude that the family has no signal

This shift is required so that the calibration-family discipline does not over-penalize genuinely novel structure in the primary family.
