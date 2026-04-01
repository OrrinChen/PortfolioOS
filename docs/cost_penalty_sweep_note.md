# Cost Penalty Sweep Note

Version: 1.0  
Status: accepted for research follow-up only  
Decision date: 2026-03-30

## Decision

Keep the current expanded-US default unchanged.

Do not change `config/us_expanded.yaml` based on this single-sample sweep.

Accept `0.5x` as the current research candidate for a lower cost-penalty setting and capture it in a separate research config.

## Scope

This note applies to:

- the expanded-US backtest research path
- transaction-cost penalty sensitivity inside the Phase 13 backtest loop
- research-only config variants used for parameter comparison

This note does not apply to:

- production defaults
- live broker execution settings
- slippage calibration overlays

## Evidence Basis

The supporting sweep output lives under:

- `outputs/backtest_us_expanded_cost_sweep_phase13b`

The sweep used:

- base manifest: `data/backtest_samples/manifest_us_expanded.yaml`
- multipliers: `0.1x`, `0.3x`, `0.5x`, `1.0x`, `2.0x`
- fixed expanded-US universe and monthly rebalance schedule
- a deterministic cost-bundle scale across:
  - `objective_weights.transaction_cost`
  - `objective_weights.transaction_fee`
  - `objective_weights.turnover_penalty`
  - `objective_weights.slippage_penalty`

## Summary Table

| Multiplier | Ending NAV | Annualized Return | Sharpe | Max Drawdown | Total Turnover | Total Cost | Vs Naive Ending NAV Delta |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0.1x | 2229057.93 | 14.62% | 1.26 | -8.35% | 0.4965 | 817.11 | 2854.70 |
| 0.3x | 2222781.37 | 14.29% | 1.26 | -8.10% | 0.4096 | 669.72 | -3421.86 |
| 0.5x | 2223675.60 | 14.34% | 1.28 | -7.93% | 0.3595 | 588.00 | -2527.63 |
| 1.0x | 2200618.38 | 13.14% | 1.24 | -7.43% | 0.2439 | 394.94 | -25584.85 |
| 2.0x | 2152740.75 | 10.66% | 1.11 | -6.65% | 0.1090 | 173.39 | -73462.48 |

## Interpretation

The sweep result is clean and useful:

1. Lower cost penalties materially improved ending NAV on this sample.
2. The Sharpe curve is concave across the tested range.
3. `0.5x` is the best risk-adjusted point in this sample.
4. `0.1x` has the best ending NAV, but with visibly higher turnover, higher explicit cost, and deeper drawdown.
5. `2.0x` is too conservative on this upward-trending sample and gives up too much holding return.

The most important observation is the shape of the frontier:

- moving from `2.0x` toward `0.5x` improves both return and Sharpe
- moving further from `0.5x` to `0.1x` still improves ending NAV, but no longer improves Sharpe

That makes `0.5x` the current best research candidate when the objective is better risk-adjusted performance rather than pure ending NAV.

## Why The Default Stays Unchanged

This sweep is still:

- one universe
- one target construction
- one rebalance schedule
- one historical window

The sample is also broadly upward-trending, which favors more aggressive rebalancing. The drawdown profile confirms the trade-off:

- `0.5x` improved Sharpe versus `2.0x`
- but max drawdown still widened from `-6.65%` to `-7.93%`

Without out-of-sample validation, changing the default would be premature and would risk overfitting the research config to this single window.

## Configuration Outcome

The configuration policy after this sweep is:

- keep `config/us_expanded.yaml` unchanged
- add `config/us_expanded_aggressive.yaml` as a research-only variant
- treat `0.5x` as a candidate for future validation, not an accepted default

## Required Next Validation

Before any default change is considered, run at least one additional out-of-sample check:

- a weaker market or drawdown-heavy window
- the same expanded-US framework
- the same multiplier set, or a tighter grid around `0.5x`

Only if the preferred region remains stable across samples should the default be reconsidered.
