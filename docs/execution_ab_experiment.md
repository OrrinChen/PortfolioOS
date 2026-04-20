# Execution A/B Experiment Spec

Version: 1.0  
Status: proposed execution-mode A/B protocol  
Scope: US pilot execution preflight comparison over a 30-trading-day window

## Objective

Compare two local execution-planning modes over the same rolling 30-trading-day US sample window:

- `participation_twap`
- `impact_aware`

The goal is not to prove broker-realized alpha. The goal is to test whether the new `impact_aware` allocator produces better execution-preflight cost estimates than the baseline when both are evaluated under the same bucket-level participation accounting rule.

## Experiment Principle

Execution A/B must use one execution path and one evaluation path:

- planning path:
  - each mode is allowed to produce its own child-order schedule
- evaluation path:
  - both schedules must be re-priced under the same bucket-level cost function

Decision-making must use:

- `evaluated_cost`

Decision-making must not use:

- raw `planned_cost` across modes

`planned_cost` is still useful for model debugging, but it is mode-internal and not cross-mode comparable.

## Variants

### Policy A: Baseline

- mode: `participation_twap`
- bucket filling rule: sequential bucket progression
- cost reporting:
  - keep `planned_cost`
  - compare on `evaluated_cost`

### Policy B: Candidate

- mode: `impact_aware`
- allocation method: `greedy`
- meaning:
  - bucket quantities are assigned by preferring lower expected impact buckets first
  - this is a local convex-friendly heuristic, not a full dynamic-programming or Almgren-Chriss solver
- cost reporting:
  - keep `planned_cost`
  - compare on `evaluated_cost`

## Market Curve Assumption

`market_curve` in the current system is:

- a fixed declarative intraday volume-shape template
- stored in calibration profiles or inline request YAML
- not estimated from rolling historical intraday tape in the current MVP

Current built-in profiles are heuristic shapes such as:

- `balanced_day`
- `liquid_midday`
- `tight_liquidity`
- `low_liquidity_stress`
- `low_liquidity_stress_strict`

For the A/B experiment, curve choice must be held fixed across both modes for every daily comparison.

## Sample Universe

Use the existing US pilot samples as the starting point:

- `data/samples/us/sample_us_01`
- `data/samples/us/sample_us_02`
- `data/samples/us/sample_us_03`

Recommended daily comparison set:

- all US samples that pass the input and approval prerequisites for that business date

If a sample is missing required execution inputs on a given day, mark it ineligible for both modes and exclude it from the daily decision metric.

## Window Definition

- window length: 30 trading days
- market: `us`
- cadence: one comparison bundle per trading day
- evaluation window:
  - day-level comparisons
  - rolling 30-day aggregate summary

Trading-day generation should follow the same operational calendar logic already used by:

- `scripts/pilot_historical_replay.py`

## Controlled Inputs

These inputs must be identical across both modes on every daily run:

- frozen OMS basket
- audit payload
- market snapshot
- portfolio state
- execution profile
- calibration profile
- market curve
- participation limit
- volume shock multiplier
- allow-partial-fill flag
- force-completion flag

Only the execution-planning mode may differ.

## Unified Evaluation Rule

For every bucket in every child-order schedule:

- bucket participation ratio:
  - `filled_qty / bucket_available_volume`
- bucket available volume:
  - `ADV * bucket.volume_share`
- evaluated slippage:
  - the standard PortfolioOS slippage function
  - applied with `bucket_available_volume`
  - multiplied by `bucket.slippage_multiplier`

This rule must be used for:

- `participation_twap`
- `impact_aware`

### Primary Metric

- `evaluated_total_cost`

### Secondary Metrics

- `evaluated_total_slippage`
- `evaluated_total_fee`
- `fill_rate`
- `partial_fill_count`
- `unfilled_order_count`
- `inactive_bucket_count`
- `evaluated_cost_per_ordered_notional`
- bucket concentration metrics:
  - percentage of notional routed to each bucket

### Diagnostics

Keep these for debugging, not for the A/B winner decision:

- `planned_cost`
- `planned_slippage`
- `planned_fee`
- planner-selected child-order schedule

## Eligibility Gate

A daily A/B comparison is eligible only when all are true:

- both modes consume the same frozen basket and source audit
- both modes run successfully
- both modes use the same resolved market curve
- both modes use the same participation limit and volume shock multiplier
- both modes expose both:
  - `planned_cost`
  - `evaluated_cost`

A daily comparison is ineligible when any are false.

Ineligible days should be logged explicitly and excluded from winner counts.

## Daily Output Contract

Each comparison day should write:

- one mode-specific output root for `participation_twap`
- one mode-specific output root for `impact_aware`
- one comparison bundle with:
  - input metadata
  - eligibility decision
  - primary metric comparison
  - secondary metric comparison
  - bucket-level route comparison

Recommended comparison artifacts:

- `execution_mode_ab_daily.csv`
- `execution_mode_ab_daily.json`
- `execution_mode_ab_summary.md`

Suggested per-day fields:

```csv
date,sample_id,eligible,eligibility_reason,basket_notional,mode_a,mode_b,planned_cost_a,evaluated_cost_a,planned_cost_b,evaluated_cost_b,fill_rate_a,fill_rate_b,partial_fill_count_a,partial_fill_count_b,unfilled_order_count_a,unfilled_order_count_b,winner,notes
```

`winner` should be one of:

- `participation_twap`
- `impact_aware`
- `tie`
- `ineligible`

## 30-Day Decision Rule

The 30-day experiment should summarize:

- eligible day count
- ineligible day count
- win count by mode using `evaluated_cost`
- median `evaluated_cost` delta
- average `evaluated_cost` delta
- 25/50/75 percentile deltas
- rate of worse fill outcomes:
  - higher partial-fill count
  - higher unfilled count

Recommended success criterion for `impact_aware`:

- eligible days >= 20
- median `evaluated_cost_delta = evaluated_cost_twap - evaluated_cost_impact_aware` > 0
- mean `evaluated_cost_delta` > 0
- no material deterioration in fill rate
- no material increase in unfilled orders

## Material Deterioration Guard

Even if `impact_aware` lowers cost, it should not be accepted if it materially worsens execution completeness.

Suggested guard:

- fail if average fill-rate delta is worse than `-1%`
- fail if unfilled-order count is higher on more than `20%` of eligible days

## Stress Slice

In addition to baseline daily runs, each mode should be re-evaluated under the same stress calibration profile:

- `config/calibration_profiles/low_liquidity_stress_strict.yaml`

Stress comparison should remain secondary. The primary winner decision still uses the baseline `evaluated_cost` path.

## Force Completion Policy

`force_completion=true` should not be part of the baseline experiment unless both modes require it.

If it is enabled:

- log it per day
- emit a warning-style note whenever residual quantity is pushed into the final bucket
- record:
  - `forced_completion_order_count`
  - estimated extra evaluated slippage attributable to the final bucket override

## Current Limitations

- current `impact_aware` allocation method is `greedy`, not a global optimizer
- current `market_curve` is a fixed declarative shape, not a historical intraday estimator
- current simulator is still a local preflight model, not a broker execution engine

These limitations do not block the 30-day A/B, but they must be documented in the report footer.

## Proposed Execution Path

Phase 1:

- implement a daily comparison wrapper around the existing execution simulator
- produce paired outputs for both modes
- emit unified-evaluation comparison artifacts

Phase 2:

- run over a 30-trading-day historical window using the same date orchestration style as `scripts/pilot_historical_replay.py`

Phase 3:

- summarize the rolling 30-day results in one decision report

## Acceptance Criteria

- same request inputs can be run in both modes
- both mode outputs expose `planned_cost` and `evaluated_cost`
- daily winner selection uses only `evaluated_cost`
- ineligible days are explicit and excluded from decision metrics
- 30-day summary reports eligible count, win count, median delta, mean delta, and fill-quality guard results
