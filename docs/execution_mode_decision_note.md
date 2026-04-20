# Execution Mode Decision Note

Version: 1.0  
Status: accepted  
Decision date: 2026-03-27

## Decision

Accept `impact_aware` as the default execution simulation mode for local execution preflight.

Keep `participation_twap` available as an explicit override and comparison baseline.

## Scope

This decision applies to:

- local execution simulation
- preflight and decision-support reporting
- execution A/B comparison workflows

This decision does not claim:

- broker-realized production superiority
- live-routing superiority
- venue-level microstructure optimality

## Evidence Basis

The supporting comparison was evaluated under one unified cost-accounting rule:

- both modes keep their own planning logic
- both modes are compared using the same bucket-level `evaluated_cost`

The relevant experiment output lives under:

- `outputs/execution_mode_ab_us_30d`

## Independent Basket Interpretation

The 30-trading-day execution-mode A/B window produced repeated values for each static sample.

Therefore the correct interpretation is:

- not `90` independent observations
- but `3` independent basket structures repeated over a 30-day window

Decision-making is based on the three basket structures, not on the repeated row count alone.

## Acceptance Rationale

`impact_aware` is accepted because:

1. downside is zero under the unified `evaluated_cost` rule
   - across the three independent basket structures, `impact_aware` never underperformed `participation_twap`
2. improvement is strongest when participation pressure is higher
   - the largest basket showed the clearest cost improvement
   - lower-ADV / higher-participation names also showed meaningful improvement in basis-point terms
3. ordinary baskets are effectively flat, not worse
   - smaller baskets showed only small absolute gains
   - no fill-rate deterioration or residual-risk deterioration was introduced

## Supporting Basket-Level Summary

Mean `delta_evaluated_cost = evaluated_cost_twap - evaluated_cost_impact_aware`:

- `sample_us_01`: `31.41253086566792`
- `sample_us_02`: `0.3348439746527702`
- `sample_us_03`: `0.6444102001688785`

Interpretation:

- `sample_us_01` demonstrates that the new planner matters when bucket-level participation pressure creates a larger cost gradient
- `sample_us_02` and `sample_us_03` show that when the optimization opportunity is smaller, `impact_aware` remains effectively neutral rather than harmful

## Configuration Decision

The execution simulation default is now:

- `simulation.mode = impact_aware`

The effective code-level default lives in:

- `src/portfolio_os/execution/simulator.py`

The documentation-alignment config hint also lives in:

- `config/us_default.yaml`

This YAML field is currently documentary and future-facing; the active default source remains the simulator config model.

## Explicit Non-Decision

This note does not accept:

- replacing the existing slippage default config with the paper-trading candidate `k`
- removing `participation_twap`
- claiming that the current `greedy` allocator is the final production optimizer

## Follow-On Work

Priority after this decision:

1. expand the US universe / ticket pool
2. continue passive fill accumulation for calibration quality
3. only then consider replacing the greedy allocator with a richer multi-period optimizer
