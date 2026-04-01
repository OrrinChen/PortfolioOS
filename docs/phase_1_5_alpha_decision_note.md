# Phase 1.5 Alpha Decision Note

Version: 1.0  
Status: infrastructure accepted, research gate failed  
Decision date: 2026-04-01

## Decision

Do not promote Phase 1.5 alpha integration into any default expanded-US config.

Keep the implementation and research inputs as dedicated research assets:

- `config/us_expanded_alpha_phase_1_5.yaml`
- `data/backtest_samples/manifest_us_expanded_alpha_phase_1_5.yaml`

Treat Phase 1.5 as an engineering closeout and a research negative-result closeout:

- the walk-forward alpha bridge is implemented correctly
- the optimizer can now ingest per-date expected returns
- the alpha-only baseline and audit panel exist
- but the accepted Phase 1 signal did not improve portfolio construction on the frozen expanded-US backtest

## Scope

This note applies to:

- the accepted Phase 1 recipe `alt_momentum_4_1`
- the Phase 1.5 expected-return bridge
- the alpha-enabled expanded-US monthly backtest path
- the comparison among `optimizer`, `naive_pro_rata`, and `alpha_only_top_quintile`
- the question of whether alpha now gives the optimizer a real reason to trade

This note does not approve:

- promotion into `config/default.yaml`
- promotion into `config/us_expanded.yaml`
- live alpha-driven execution
- any claim that the accepted Phase 1 signal is robust enough for production portfolio construction

## Evidence Basis

Primary runtime artifacts:

- `outputs/phase1_5_alpha_us_expanded/backtest_results.json`
- `outputs/phase1_5_alpha_us_expanded/nav_series.csv`
- `outputs/phase1_5_alpha_us_expanded/period_attribution.csv`
- `outputs/phase1_5_alpha_us_expanded/backtest_report.md`
- `outputs/phase1_5_alpha_us_expanded/alpha_panel.csv`

Supporting sensitivity probe:

- `outputs/phase1_5_alpha_weight_probe/alpha_weight_probe_summary.csv`

Research inputs:

- `data/risk_inputs_us_expanded/returns_long.csv`
- `data/backtest_samples/manifest_us_expanded_alpha_phase_1_5.yaml`
- `config/us_expanded_alpha_phase_1_5.yaml`

## Implemented Architecture

Phase 1.5 added four durable capabilities:

1. objective-level alpha support via `- alpha_weight * mu'w`
2. walk-forward mapping from accepted alpha score to per-ticker expected return
3. alpha-only top-quintile equal-weight baseline
4. alpha audit artifacts and pairwise reporting in the backtest stack

This closes the original engineering goal: the optimizer can now receive a date-varying expected-return vector generated only from information available at the rebalance date.

## Main Backtest Result

Run configuration:

- manifest: `data/backtest_samples/manifest_us_expanded_alpha_phase_1_5.yaml`
- slippage model: calibrated `k = 3.498400399110418`
- integration mode: `augment`
- alpha weight: `1.0`
- accepted recipe: `alt_momentum_4_1`

Headline results:

| Strategy | Ending NAV | Ann. Return | Sharpe | Max DD | Turnover | Cost |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `optimizer` | `2069840.99` | `6.37%` | `0.78` | `-5.51%` | `0.00` | `0.00` |
| `naive_pro_rata` | `2226203.23` | `14.47%` | `1.24` | `-8.47%` | `0.55` | `904.72` |
| `alpha_only_top_quintile` | `1948934.28` | `0.10%` | `0.06` | `-11.10%` | `2.64` | `4542.32` |

Pairwise outcome:

- hard gate failed:
  - `optimizer` Sharpe `0.78` vs `naive_pro_rata` Sharpe `1.24`
- stretch benchmark passed mechanically:
  - `optimizer` Sharpe `0.78` vs `alpha_only_top_quintile` Sharpe `0.06`

Interpretation:

- the optimizer still materially underperformed naive
- alpha-only was much worse than both optimizer and naive
- passing the stretch benchmark does not rescue the result, because the alpha-only baseline itself was weak

## Structural Findings

### 1. Alpha Integration Did Not Change Optimizer Behavior

The most important Phase 1.5 result is not the Sharpe gap itself. It is that alpha integration did not change the optimizer's portfolio path at all.

Observed optimizer behavior:

- total turnover: `0.0`
- total transaction cost: `0.0`
- ending NAV, annualized return, Sharpe, and drawdown all exactly matched the no-trade `buy_and_hold` path

This means the optimizer did not use the new alpha input to create a meaningfully different portfolio.

### 2. Alpha-Weight Probe Showed Complete Invariance

An ad hoc sensitivity probe swept:

- `alpha_weight = 0`
- `1`
- `10`
- `100`
- `1000`
- `10000`

All six runs produced identical optimizer results:

- ending NAV `2069840.99`
- annualized return `6.37%`
- Sharpe `0.78`
- turnover `0.0`
- optimizer-vs-naive Sharpe delta `-0.46`

This is decisive evidence that the Phase 1.5 failure is not merely "`alpha_weight = 1.0` was too small."

Within the tested range, the current expected-return signal still does not move the solution.

### 3. Effective Alpha Coverage Was Sparse

Alpha-model diagnostics from the main run:

- total rebalance dates: `12`
- cold-start dates without enough history: `6`
- signal-ready dates: `6`
- alpha panel rows: `300`

More importantly, nonzero expected-return support appeared on only `2` of the `12` rebalance dates:

- `2025-10-31`
- `2026-02-27`

The other signal-ready dates produced zero effective `expected_return` because the trailing top-bottom spread estimate was clipped to zero.

Interpretation:

- the accepted Phase 1 signal is too episodic in this walk-forward mapping on the frozen expanded-US sample
- even after the cold-start window passes, the bridge often shrinks the signal fully to zero

### 4. Alpha-Only Baseline Was Not Investable-Useful

The alpha-only benchmark answered the clean question:

> If we trust the accepted signal and simply buy the top quintile equal-weight, what happens?

Observed result:

- ending NAV `1948934.28`
- annualized return `0.10%`
- Sharpe `0.06`
- max drawdown `-11.10%`
- total turnover `2.64`
- total transaction cost `4542.32`

Interpretation:

- the accepted Phase 1 signal did not survive translation into a realistic, costed top-quintile portfolio benchmark on this sample
- the signal may still contain research information, but not enough to justify direct portfolio construction in its current form

## Research Conclusion

Phase 1.5 answers the original question clearly:

> Did accepted alpha give the optimizer a useful reason to trade?

Current answer:

- engineering: yes, the channel exists
- research: no, the signal did not create value in the expanded-US backtest

The outcome is stronger than a simple gate failure:

- the optimizer did not beat naive
- the alpha-only benchmark was weak
- alpha-weight scaling up to `10000` did not alter the optimizer result
- effective nonzero expected returns appeared on only two rebalance dates

This points to a signal-quality / signal-translation bottleneck more than an optimizer-plumbing bottleneck.

## Promotion Decision

Configuration policy after Phase 1.5:

- keep `config/us_expanded.yaml` unchanged
- keep `config/us_expanded_tca_calibrated.yaml` as the calibrated no-alpha research baseline
- keep `config/us_expanded_alpha_phase_1_5.yaml` as the research record for alpha integration
- do not promote alpha-enabled portfolio construction into defaults

## Recommended Next Step

The next high-value step is not more optimizer tuning on this exact signal.

Recommended direction:

1. extend signal validation on a longer or broader US sample
2. inspect why the bridge produces nonzero expected returns on only two rebalance dates
3. improve the signal layer before further optimizer promotion attempts

Concrete implication:

- Phase 1.5 should be treated as "alpha integration infrastructure complete"
- the next research bottleneck is no longer plumbing
- the next bottleneck is whether the accepted alpha remains strong enough after walk-forward shrinkage and realistic cost
