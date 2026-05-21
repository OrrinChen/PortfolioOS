# Factor Discovery Design Layer Spec

FD-D0 adds a mandatory design layer before any candidate factor formula is
treated as a validation target.

The rule is simple:

```text
market pain point
-> mechanism hypothesis
-> observable pre-formula diagnostics
-> formula as measurement
-> placebo / failure / cost-capacity design
-> FD validation
```

Formula construction is not the start of factor discovery. It is the
measurement step after the market behavior and failure modes have been stated.

## Required Contract Fields

Every new factor or candidate family must define:

```text
market_pain_point
mechanism_hypothesis
investor_constraint_or_behavior
expected_universe
expected_regime
why_not_arbitraged_away
observable_pre_formula_diagnostics
formula_measurement_role
placebo_design
cost_capacity_risks
expected_failure_modes
```

The contract must also set:

```text
design_review_required = true
pre_formula_evidence_required = true
formula_is_measurement_not_thesis = true
not_alpha_evidence = true
direct_q2_entry_allowed = false
```

Standalone candidate-family runners must write:

```text
candidate_design_manifest.json
```

before they compute signals or validation metrics. That manifest is required
for one-off candidate families such as small-cap residual momentum,
momentum-minus-volatility, and revision-confirmed earnings underreaction. If
the manifest is missing or fails validation, the runner must not proceed to
FD-S or FD-R validation.

FD-D0 also has a repository-wide output audit:

```text
make factor-discovery-fd-wide-design-audit
```

The audit scans candidate output directories for candidate/family summary or
decision artifacts and requires a valid same-directory
`candidate_design_manifest.json`. It is meant to catch formula-first or
diagnostic-only outputs that bypassed the runner-level manifest guard.

## Governance Boundary

FD-D0 is a Factor Discovery design gate. It does not approve a factor, enter Q1,
enter Q2, update Alpha Registry, open allocator paths, or make production
claims. It only prevents formula-first candidates from entering later FD
validation phases without a documented mechanism and pain-point thesis.

## Practical Use

When a future candidate fails, the diagnosis should map back to the design
contract:

```text
If placebo dominates:
  the mechanism or placebo design was too weak.

If cost/capacity dominates:
  the pain point may exist but be untradeable in the selected universe.

If exposure attribution dominates:
  the formula measured beta / size / liquidity / sector exposure, not the
  stated mechanism.

If signal timing fails:
  the observable pattern does not match the proposed behavioral or structural
  lag.
```

This makes factor discovery mechanism-first and keeps validation from becoming
formula mining.
