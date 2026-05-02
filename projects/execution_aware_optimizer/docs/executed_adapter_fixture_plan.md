# Q2 Executed Adapter Fixture Plan

## Decision

Q2 should add one tiny executed PortfolioOS adapter fixture in the next implementation phase.

The fixture scope is deliberately narrow:

- invoke `execution_aware_optimizer.ladder.run_alpha_decay_ladder` directly through a library call
- set `portfolioos.allow_portfolioos_run=true` only inside the test or dedicated fixture config
- use the existing local manifest `data/backtest_samples/manifest_us_expanded_alpha_phase_1_5.yaml`
- request only layers that PortfolioOS can currently map from period attribution:
  - `raw_top_alpha_equal_weight`
  - `full_execution_aware_cost_adjusted`
- keep intermediate layers explicit as unavailable unless stable PortfolioOS stage hooks are added
- write no default report artifacts and do not mutate Q2 default configs

## Evidence From Inspection

The current Q2 adapter maps PortfolioOS `period_attribution` rows in
`projects/execution_aware_optimizer/src/execution_aware_optimizer/ladder.py`.

The stable mapping is:

| Q2 layer | PortfolioOS source strategy | status |
|---|---|---|
| `raw_top_alpha_equal_weight` | `alpha_only_top_quintile` | available through period attribution |
| `full_execution_aware_cost_adjusted` | `optimizer` | available through period attribution |

The existing local manifest produced mapped rows in a direct read-only probe. It did not require live data, broker access, paid APIs, or report writes.

## Not In Scope

This fixture must not:

- enable PortfolioOS execution in default Q2 configs
- add live FMP, SEC, WRDS, Tushare, Alpaca, or broker calls
- run a CLI subprocess chain from Q2 tests
- fabricate intermediate layer values
- claim binding constraints, dual values, liquidity slack, or risk exposure diagnostics that PortfolioOS does not expose
- store paid data or generated research payloads in the repository

## Recommended Implementation

Add a focused Q2 test that:

1. builds an `ExperimentConfig` with `allow_portfolioos_run=true`
2. uses the local manifest `data/backtest_samples/manifest_us_expanded_alpha_phase_1_5.yaml`
3. restricts layers to `raw_top_alpha_equal_weight`, one unavailable intermediate layer, and `full_execution_aware_cost_adjusted`
4. calls `run_alpha_decay_ladder(config, backtest_runner=portfolio_os.backtest.engine.run_backtest)`
5. asserts:
   - at least one raw row has observed `net_return`
   - at least one full execution-aware row has observed `net_return`
   - the intermediate layer remains unavailable with an explicit `infeasibility_reason`
   - no default Q2 report file is written by the test

This gives Q2 one real PortfolioOS-backed proof point without changing default project behavior.
