# Phase 1.5 Alpha Integration Design

## Goal

Phase 1.5 integrates the accepted Phase 1 recipe `alt_momentum_4_1` into the optimizer so the optimizer has a first explicit reason to trade beyond target tracking, cost suppression, and risk control.

The phase closes only when PortfolioOS can run a walk-forward backtest with three comparable strategies:

- `naive_pro_rata`
- `alpha_only_top_quintile`
- `optimizer`

and produce a machine-readable result showing whether `optimizer` beats `naive_pro_rata` on net Sharpe after realistic transaction costs.

`alpha_only_top_quintile` is treated as a stretch benchmark, not the blocking gate. Failing to beat `alpha_only_top_quintile` does not invalidate the research run, but it blocks promotion and must be called out explicitly in the decision note.

## Non-Negotiable Constraints

- Do not modify `config/default.yaml`
- Do not modify `config/us_expanded.yaml`
- Do not modify legacy baseline samples `sample_us_01` through `sample_us_03`
- `alpha_weight = 0.0` must preserve existing optimizer behavior exactly
- Existing backtest manifests without alpha settings must keep their current behavior
- Full `python -m pytest -q` must remain green

## Architectural Decision

Phase 1.5 uses manifest-driven, walk-forward alpha injection.

It does not place a static expected-return vector in runtime config. Expected returns are date-varying and must be recomputed at each rebalance using only information available on that rebalance date.

This creates four clear layers:

1. Objective layer
   Adds an optional alpha reward term `- alpha_weight * mu'w`

2. Alpha bridge layer
   Converts accepted recipe scores into walk-forward per-ticker expected returns

3. Backtest orchestration layer
   Generates the alpha panel, injects expected returns into the optimizer universe, and runs the alpha-only baseline

4. Reporting layer
   Surfaces optimizer-vs-naive and optimizer-vs-alpha-only comparisons, plus alpha diagnostics and artifacts

## Objective Design

### New Term

Inside `build_objective()`:

```text
alpha_reward = mu'w
objective += - alpha_weight * alpha_reward
```

Where:

- `mu` is the per-ticker annualized expected return vector aligned to the optimizer universe
- `w` is the post-trade portfolio weight vector
- `alpha_weight` is a new scalar in `ObjectiveWeights`

### Compatibility Rule

If either condition is true:

- `alpha_weight == 0.0`
- `expected_return` is absent from the working universe

then the alpha term must evaluate to zero and the objective must remain behaviorally identical to the current implementation.

### Objective Decomposition

The objective decomposition payload must expose:

- `alpha_reward`
- `alpha_weighted_reward`

so the optimizer can be audited later for whether alpha actually influenced the solution.

## Alpha Bridge Design

### Accepted Recipe

Phase 1.5 hard-codes the accepted Phase 1 recipe:

- reversal lookback: `21`
- momentum lookback: `84`
- momentum skip: `21`
- forward horizon: `5`
- reversal weight: `0.0`
- momentum weight: `1.0`
- quantiles: `5`
- min assets per date: `20`

The bridge may be generalized later, but Phase 1.5 should implement only this accepted recipe.

### Input Data

The bridge consumes only the existing frozen backtest returns history:

- `returns_long.csv`

No new required data sources are introduced for Phase 1.5.

### Score Construction

For each rebalance date `t`:

1. Load only returns history up to and including `t`
2. Build the accepted recipe signal using the existing alpha research primitives
3. Extract the latest available cross-section of `alpha_score`
4. Normalize that cross-section into a stable continuous signal

### Cross-Sectional Normalization

For each ticker with a valid current score:

1. Compute cross-sectional percentile rank using deterministic first-rank ordering
2. Convert percentile rank to inverse-normal score with `NormalDist().inv_cdf`
3. Winsorize the z-score at `[-3.0, +3.0]`
4. Demean the cross-section

This produces the working normalized signal `z_i`.

### Signal-Strength Estimation

Expected returns must not be set by an arbitrary fixed constant. They must be scaled by signal strength estimated only from information available strictly before the rebalance date.

For each rebalance date `t`:

1. Compute historical accepted-recipe evaluation rows using the existing alpha IC workflow
2. Keep only evaluation dates strictly earlier than `t`
3. Require at least `20` historical evaluation dates; otherwise the signal strength is zero
4. Estimate:
   - trailing mean top-bottom spread
   - trailing mean rank IC
   - trailing rank-IC t-statistic

### Annualized Expected-Return Mapping

Let:

- `spread_5d` = trailing mean top-bottom spread
- `t_stat` = trailing rank-IC t-statistic
- `confidence = clip(t_stat / 3.0, 0.0, 1.0)`
- `annualized_spread = max(spread_5d, 0.0) * (252.0 / 5.0)`
- `z_gap` = current top-quintile mean z minus bottom-quintile mean z

Then:

```text
expected_return_i = clip(confidence * annualized_spread * z_i / max(z_gap, 1e-6), -0.30, +0.30)
```

Design rationale:

- uses only observable historical evidence
- shrinks weak signals toward zero
- does not reverse signal direction when historical evidence is negative
- produces an annualized `mu` vector with intuitive long-short interpretation
- caps extreme expected returns for optimizer stability

### Alpha Panel Artifact

Each backtest run with alpha enabled must write an audit panel containing at least:

- `date`
- `ticker`
- `alpha_score`
- `alpha_rank_pct`
- `alpha_zscore`
- `expected_return`
- `quantile`
- `signal_strength_confidence`
- `annualized_top_bottom_spread`

This artifact lives in the backtest output directory, not in the repo.

## Backtest Manifest Design

Backtest manifests gain an optional `alpha_model` section. Existing manifests without this section must remain valid.

Phase 1.5 manifest fields:

```yaml
alpha_model:
  enabled: true
  recipe_name: alt_momentum_4_1
  quantiles: 5
  forward_horizon_days: 5
  min_evaluation_dates: 20
  zscore_winsor_limit: 3.0
  t_stat_full_confidence: 3.0
  max_abs_expected_return: 0.30
  write_alpha_panel: true
  add_alpha_only_baseline: true
```

These settings belong to the manifest because they are backtest research controls, not production default config.

## Alpha-Only Baseline Design

Phase 1.5 adds:

- `alpha_only_top_quintile`

Definition at each rebalance date:

1. Compute current accepted-recipe `alpha_score`
2. Rank the eligible universe cross-section
3. Select top quintile using `quantiles = 5`
4. Set target weights to equal weight across selected names and zero elsewhere
5. Rebalance with the same order-building, repair, execution cost, and compliance handling path used elsewhere

This is intentionally naive:

- no optimizer
- no risk-aware weighting
- no score-proportional sizing

It is the cleanest "signal only" benchmark.

## Backtest Strategy Semantics

### `optimizer`

- uses original target weights from the manifest
- uses existing constraints and cost model
- uses risk model if enabled by the selected config
- uses expected-return alpha term only when `alpha_weight > 0` and `alpha_model.enabled = true`

### `naive_pro_rata`

- unchanged

### `alpha_only_top_quintile`

- uses alpha-derived equal-weight target weights
- pays the same explicit transaction costs as the other strategies

## Reporting And Decision Semantics

Backtest summary and report outputs must include:

- optimizer vs naive comparisons
- optimizer vs alpha-only comparisons
- alpha-only vs naive comparisons
- alpha-enabled metadata:
  - alpha enabled or disabled
  - accepted recipe name
  - alpha weight
  - alpha panel path

The Phase 1.5 hard gate is:

```text
optimizer net Sharpe > naive_pro_rata net Sharpe
```

Stretch benchmark:

```text
optimizer net Sharpe >= alpha_only_top_quintile net Sharpe
```

If the stretch benchmark fails, the run is still valid research output, but the decision note must explicitly say that the optimizer has not yet demonstrated value beyond the naive alpha baseline.

## Testing Requirements

Phase 1.5 tests must prove:

1. `alpha_weight = 0.0` preserves current optimizer outputs
2. objective decomposition includes alpha components when alpha is enabled
3. alpha bridge is walk-forward and does not look ahead
4. alpha-only target weights use top quintile equal weighting
5. backtest manifest loading remains backward compatible
6. existing backtest fixtures still pass unchanged
7. alpha-enabled backtest writes alpha panel artifacts
8. reports include the new comparison fields

## Deliverables

Phase 1.5 is complete when the repo contains:

- alpha objective integration
- walk-forward expected-return bridge
- alpha-enabled backtest manifest support
- `alpha_only_top_quintile` baseline
- alpha panel audit artifact
- updated reporting
- a dedicated Phase 1.5 research manifest for expanded US
- a decision note summarizing whether optimizer+alpha beat naive and how it compared with alpha-only

## Explicit Non-Goals

Phase 1.5 does not:

- promote alpha into `config/default.yaml`
- promote alpha into `config/us_expanded.yaml`
- generalize to multiple alpha recipes
- add RL
- add ML beyond the accepted deterministic alpha bridge
- integrate alpha into live execution or scenario workflows yet
