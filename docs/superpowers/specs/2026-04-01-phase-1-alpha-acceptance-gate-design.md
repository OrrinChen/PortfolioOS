# Phase 1 Alpha Acceptance Gate Design

## Objective

This design closes Phase 1 with a reproducible, machine-checkable research gate rather than a subjective "looks promising" judgment.

Phase 1 should end in one of two states:

- `accepted`
- `rejected_but_infrastructure_complete`

Both outcomes are valid. The success criterion is not "force a winning alpha." The success criterion is "build a deterministic alpha research loop that can evaluate candidate recipes, enforce explicit gates, and produce a defensible closeout decision."

## Scope

Included in this design:

- a configuration-driven alpha recipe sweep
- chronological development and holdout evaluation
- explicit relative and absolute acceptance gates
- automatic iteration for up to three rounds
- machine-readable decision artifacts
- markdown closeout artifacts

Excluded from this design:

- optimizer integration
- expected-return integration into portfolio construction
- factor neutralization or sector neutralization
- ML model training
- RL or execution changes
- multi-market abstraction finalization

This remains a Phase 1 deliverable. It is still alpha research infrastructure, not alpha-aware portfolio construction.

## Data Source Policy

### Phase 1 Policy

Phase 1 US daily data is defined as:

- live ingest source: `yfinance`
- acceptance source of truth: frozen local research snapshot

The acceptance gate evaluates only the frozen snapshot artifacts, not live API pulls.

Canonical Phase 1 snapshot inputs:

- `data/risk_inputs_us_expanded/returns_long.csv`
- `data/risk_inputs_us_expanded/factor_exposure.csv`
- `data/risk_inputs_us_expanded/risk_inputs_manifest.json`

All Phase 1 acceptance or rejection conclusions must reference the exact snapshot manifest used for evaluation.

### Phase 2+ Policy

The approved forward data-source roadmap is:

- Phase 2+ US: `Polygon.io`
- Phase 4+ CN: existing `Tushare` path

This design does not implement the provider migration. It only records the source policy so Phase 1 work stays reproducible and Phase 2 work has a clear starting point.

## Snapshot Constraint That Affects The Gate

The current frozen expanded-US snapshot records:

- `actual_trading_days = 251`
- date range `2025-03-28` through `2026-03-27`

Because of that history length, a literal `12_1` momentum baseline using a 252-trading-day lookback plus skip window would leave too little evaluation history for the planned development/holdout split and the `evaluation_date_count >= 40` holdout gate.

Therefore Phase 1 uses a snapshot-feasible first-round baseline instead of a literal `12_1` recipe.

## Acceptance Target

Phase 1 is considered complete when the project can:

1. define alpha recipes declaratively
2. evaluate them on a frozen snapshot
3. compare them on a common time split
4. enforce explicit gates
5. automatically iterate when gates fail
6. emit a final `accepted` or `rejected_but_infrastructure_complete` decision

## Candidate Recipe Model

Each recipe is a deterministic configuration over the existing alpha research workflow. At minimum, a recipe must define:

- `recipe_name`
- `reversal_lookback_days`
- `momentum_lookback_days`
- `momentum_skip_days`
- `forward_horizon_days`
- `reversal_weight`
- `momentum_weight`
- `quantiles`
- `min_assets_per_date`

The implementation should reuse the existing `run_alpha_research(...)` logic as much as possible rather than building a second evaluation engine.

## First-Round Recipe Set

Round 1 must stay small and momentum-first. It should evaluate exactly these five recipes:

1. `equal_weight_momentum_6_1`
   - `reversal_lookback_days = 21`
   - `momentum_lookback_days = 126`
   - `momentum_skip_days = 21`
   - `forward_horizon_days = 5`
   - `reversal_weight = 0.0`
   - `momentum_weight = 1.0`
2. `momentum_heavy_10_90`
   - same windows
   - `reversal_weight = 0.1`
   - `momentum_weight = 0.9`
3. `momentum_heavy_25_75`
   - same windows
   - `reversal_weight = 0.25`
   - `momentum_weight = 0.75`
4. `current_50_50`
   - same windows
   - `reversal_weight = 0.5`
   - `momentum_weight = 0.5`
5. `alt_momentum_4_1`
   - `reversal_lookback_days = 21`
   - `momentum_lookback_days = 84`
   - `momentum_skip_days = 21`
   - `forward_horizon_days = 5`
   - `reversal_weight = 0.0`
   - `momentum_weight = 1.0`

## Development And Holdout Split

### Common Evaluation Window

Candidate recipes may produce different evaluation windows because their lookback requirements differ.

To keep recipe comparisons fair:

- each round must align all candidate diagnostics to the intersection of available evaluation dates across the recipes in that round
- recipe ranking and gate evaluation must use only that common date set

If the common evaluation window is too short for the planned split, the round fails for data insufficiency rather than silently comparing recipes on mismatched histories.

### Chronological Split

The common evaluation dates are sorted chronologically and split:

- first 60% -> development slice
- last 40% -> holdout slice

The holdout slice is the only slice that may trigger final gate passage.

Development metrics may rank candidates, but they may not by themselves accept a candidate.

## Metrics Used For The Gate

For each recipe and each slice, the system must compute:

- `mean_ic`
- `mean_rank_ic`
- `positive_rank_ic_ratio`
- `mean_top_bottom_spread`
- `evaluation_date_count`
- `mean_monthly_factor_turnover`

### Monthly Factor Turnover Definition

Phase 1 turnover is a research proxy, not a live portfolio turnover estimate.

Define it as:

1. take the last available trading date of each calendar month within the slice
2. on each monthly date, form an equal-weight long-only top-quantile basket using the recipe score
3. convert that basket into weights
4. compute monthly turnover as `0.5 * sum(abs(w_t - w_t-1))`
5. average that value across consecutive monthly rebalance pairs

If a slice contains fewer than two monthly rebalance dates, `mean_monthly_factor_turnover` is treated as unavailable and the recipe cannot pass the absolute gate on that slice.

## Gate Design

### Binary Outcomes

The final run status must be exactly one of:

- `accepted`
- `rejected_but_infrastructure_complete`

No softer terminal labels should be used for the Phase 1 acceptance engine.

### Relative Gate

Each round evaluates candidates relative to a baseline on the holdout slice.

Baseline rule:

- round 1 baseline = `equal_weight_momentum_6_1`
- if a recipe passes in a round, that accepted winner becomes the baseline for future reruns of the gate on the same snapshot family
- within a single run, if no recipe passes, the current round baseline stays unchanged

A holdout candidate passes the relative gate only if:

- `candidate mean_rank_ic > baseline mean_rank_ic`
- `candidate mean_top_bottom_spread > baseline mean_top_bottom_spread`
- `candidate positive_rank_ic_ratio >= baseline positive_rank_ic_ratio - 0.02`

The positive-ratio tolerance exists so a candidate is not rejected for immaterial noise when the other holdout metrics improve.

### Absolute Gate

A holdout candidate passes the absolute gate only if:

- `mean_rank_ic >= 0.01`
- `positive_rank_ic_ratio >= 0.52`
- `mean_top_bottom_spread > 0`
- `evaluation_date_count >= 40`
- `mean_monthly_factor_turnover <= 0.8`

These thresholds are intentionally modest. They represent a Phase 1 "minimum research viability" standard, not a final institutional promotion standard.

### Final Acceptance Rule

A candidate is `accepted` only if it passes both:

- the relative gate
- the absolute gate

If no candidate passes after all allowed rounds, the final status is:

- `rejected_but_infrastructure_complete`

## Automatic Iteration

### Maximum Iterations

The engine may run at most three rounds.

### Round 2 Expansion Rule

If round 1 finds no accepted recipe:

- rank candidates on the development slice by:
  1. `mean_rank_ic`
  2. `positive_rank_ic_ratio`
  3. `mean_top_bottom_spread`
- take the top two development candidates
- generate a momentum-first local expansion around them using only:
  - `momentum_lookback_days in {84, 126, 168}`
  - `momentum_skip_days in {10, 21}`
  - `reversal_weight in {0.0, 0.1, 0.25}`
- set `momentum_weight = 1.0 - reversal_weight`
- deduplicate against already-tested recipes

### Round 3 Expansion Rule

If round 2 still finds no accepted recipe:

- repeat the same ranking rule
- take the top two development candidates from round 2
- generate a narrower local expansion using only:
  - `momentum_lookback_days` one step shorter, equal, or one step longer than the parent candidate, bounded to `[63, 189]`
  - `momentum_skip_days in {5, 10, 21}`
  - `reversal_weight in {0.0, 0.05, 0.1, 0.25}`
- set `momentum_weight = 1.0 - reversal_weight`
- deduplicate against already-tested recipes

### Iteration Stop Conditions

Stop immediately when:

- a candidate is accepted
- no new valid recipes can be generated
- the common evaluation window cannot satisfy the holdout-date gate
- round 3 completes without an accepted candidate

## Artifacts

Each acceptance run should write:

- `alpha_sweep_summary.csv`
- `alpha_sweep_manifest.json`
- `alpha_acceptance_decision.json`
- `alpha_acceptance_note.md`

Recommended structure:

- summary CSV:
  - one row per recipe per slice
- manifest JSON:
  - snapshot paths
  - snapshot manifest hash or metadata
  - round definitions
  - recipe registry
  - run timestamps
- decision JSON:
  - final status
  - accepted recipe if any
  - baseline recipe
  - gate outcomes
  - stop reason
- note markdown:
  - human-readable closeout summary
  - acceptance or rejection explanation
  - next recommended action

## Decision Semantics

### If Accepted

The note must say:

- which recipe won
- which baseline it beat
- which holdout metrics cleared the gate
- that Phase 1 is complete
- that the next step is Phase 1.5 expected-return integration

### If Rejected

The note must say:

- that the infrastructure is complete
- that no recipe cleared the gate within the allowed search budget
- that this is a valid research conclusion, not a project failure
- that Phase 1 closes without optimizer integration

## Testing Expectations

The implementation should be covered by tests for:

- deterministic recipe comparison
- chronological development/holdout split
- common-date alignment across recipes
- relative gate evaluation
- absolute gate evaluation including turnover
- round expansion and deduplication
- final terminal states:
  - `accepted`
  - `rejected_but_infrastructure_complete`
- artifact writing and CLI output

## Success Criteria For This Design

This design is successful if the implemented Phase 1 gate can:

- run on the existing frozen US snapshot
- compare candidate recipes reproducibly
- stop within three rounds
- emit one of the two terminal statuses without manual judgment
- provide a defensible acceptance or rejection note

## Next Step After This Design

After this design is approved and implemented:

- if the gate accepts a winner, move to Phase 1 closeout and then Phase 1.5 planning
- if the gate rejects all recipes, close Phase 1 as infrastructure complete and keep optimizer integration blocked until a better alpha family is available
