# Cost Model Decision Note

Version: 1.1  
Status: accepted for research closeout only  
Decision date: 2026-04-01

## Decision

Do not promote `slippage.k = 3.498400399110418` into `config/us_expanded.yaml` at this time.

Keep the calibrated value in separate research configs:

- `config/us_expanded_tca_calibrated.yaml`
- `config/us_expanded_tca_calibrated_replace.yaml`

Treat the TCA loop as operationally closed for the current low-participation regime, but treat default-config promotion as blocked by optimizer-structure evidence rather than by missing calibration evidence.

## Scope

This note applies to:

- the expanded-US monthly backtest research path
- the low-participation TCA calibration result
- cost-model selection for `config/us_expanded.yaml`
- the relationship between calibrated transaction cost and optimizer behavior
- the follow-up risk-aversion diagnosis in both `augment` and `replace` modes

This note does not approve:

- default promotion of the calibrated `k`
- a permanent cost-bundle multiplier change in `config/us_expanded.yaml`
- continued tuning of the current risk-only objective for production use
- extrapolation of the calibrated `k` beyond the observed low-participation regime

## Evidence Basis

The decision uses seven linked research artifacts:

- baseline sweep with default `k = 0.015`
  - `outputs/backtest_us_expanded_cost_sweep_phase13b`
- directional TCA sweep with `k = 1.445696451694286`
  - `outputs/backtest_us_expanded_cost_sweep_tca_directional_k_phase13c`
- zero-cost probe under the directional TCA `k`
  - `outputs/backtest_us_expanded_zero_cost_probe_phase13c`
- low-participation live calibration closure
  - `outputs/live_fill_analysis/slippage_calibration_low_participation_20260401_fit_eligible_gate`
- calibrated-`k` sweep with `k = 3.498400399110418`
  - `outputs/backtest_us_expanded_cost_sweep_tca_calibrated_k_phase13d`
- augment-mode risk aversion sweep
  - `outputs/risk_sweep_us_expanded`
- replace-mode calibrated risk aversion sweep
  - `outputs/risk_sweep_us_expanded_tca_calibrated_replace`

## Calibration Conclusion

The TCA workflow now has a usable low-participation calibration outcome:

- calibrated `k = 3.498400399110418`
- `fit_eligible_count = 32`
- `fit_sample_count = 16`
- `overlay_readiness = sufficient`
- `next_recommended_action = apply_as_paper_overlay`
- observed participation coverage:
  - `0-0.1%` only

Applicability boundary:

- this calibration is valid only for the observed `0-0.1%` participation regime
- it must not be extrapolated to higher-participation trading

Important methodological caveat:

- the current estimator still fits `k` from `positive_signal` observations only
- negative-signal eligible fills are not yet included in the fit
- this likely biases `k` upward
- therefore `3.50` should be interpreted as a usable research estimate, not a final unbiased production estimate

## Frontier Comparison

The most important result is not the exact `k` value, but the shape of the frontiers taken together.

### Best Point By Cost Sweep

| Sweep | `k` | Best NAV Multiplier | Best Sharpe Multiplier | Best NAV | Best Sharpe |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline | `0.015` | `0.1x` | `0.5x` | `2229057.93` | `1.28` |
| Directional TCA | `1.445696451694286` | `0.1x` | `0.1x` | `2218442.92` | `1.28` |
| Calibrated TCA | `3.498400399110418` | `0.1x` | `0.1x` | `2184286.35` | `1.20` |

### Same Cost Multiplier, Three `k` Values

| Multiplier | NAV at `k=0.015` | NAV at `k=1.45` | NAV at `k=3.50` | Sharpe at `k=0.015` | Sharpe at `k=1.45` | Sharpe at `k=3.50` |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `0.1x` | `2229057.93` | `2218442.92` | `2184286.35` | `1.26` | `1.28` | `1.20` |
| `0.3x` | `2222781.37` | `2168771.10` | `2103740.52` | `1.26` | `1.17` | `0.93` |
| `0.5x` | `2223675.60` | `2124560.37` | `2089188.57` | `1.28` | `1.02` | `0.87` |
| `1.0x` | `2200618.38` | `2087695.94` | `2073629.18` | `1.24` | `0.86` | `0.79` |
| `2.0x` | `2152740.75` | `2071827.04` | `2069840.99` | `1.11` | `0.79` | `0.78` |

### Calibrated-`k` Cost Sweep Detail

| Multiplier | Ending NAV | Annualized Return | Sharpe | Max Drawdown | Total Turnover | Total Cost | Vs Naive NAV Delta |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `0.1x` | `2184286.35` | `12.30%` | `1.20` | `-7.10%` | `0.2385` | `386.19` | `-41916.88` |
| `0.3x` | `2103740.52` | `8.12%` | `0.93` | `-6.01%` | `0.0704` | `112.71` | `-122462.70` |
| `0.5x` | `2089188.57` | `7.37%` | `0.87` | `-5.82%` | `0.0444` | `72.01` | `-137014.66` |
| `1.0x` | `2073629.18` | `6.56%` | `0.79` | `-5.63%` | `0.0126` | `20.86` | `-152574.05` |
| `2.0x` | `2069840.99` | `6.37%` | `0.78` | `-5.51%` | `0.0000` | `0.00` | `-156362.24` |

## Optimizer Behavior Diagnosis

The three cost sweeps already revealed a stronger conclusion than "the multiplier should be lower." As `k` rises from `0.015` to `1.45` to `3.50`, the entire efficient frontier shifts downward. This is not one bad multiplier point. It is a systematic degradation across the whole tested grid.

That pattern implies:

1. the current problem is not primarily multiplier selection
2. the optimizer does not currently have a strong non-cost lever
3. once the cost model becomes more realistic, the optimizer mostly responds by trading less

The zero-cost probe is the decisive structural evidence:

- under directional `k = 1.445696451694286`
- with cost bundle multiplier `0.0x`
- the optimizer matched `naive_pro_rata` exactly

Observed equality in the zero-cost probe:

- ending NAV: `2226203.2295169644`
- Sharpe: `1.2387708151885233`
- max drawdown: `-8.470949068755251%`
- turnover: `0.5477750189321283`
- optimizer vs naive ending NAV delta: `0.0`

This means that, in the current expanded-US monthly setup, the optimizer's main active lever was cost-driven trade suppression rather than a genuinely different construction choice.

In other words:

- this is not a calibration failure
- this is an objective-function structure failure

The current optimizer is not yet doing a real portfolio-quality versus cost tradeoff. It is mainly doing "naive portfolio, but with fewer trades."

## Risk Sweep Follow-Up

The risk-aversion follow-up was completed in two forms:

- `augment` mode on the expanded-US baseline config
- `replace` mode on the calibrated-TCA research config

### Augment-Mode Result

The augment-mode sweep confirmed that the risk term can move portfolio construction away from naive:

- annualized volatility fell from `9.54%` at `1x` to `6.83%` at `100000x`
- max drawdown improved from `-6.65%` to `-4.95%`
- `optimizer_vs_naive_ending_nav_delta` moved from `-73462.48` to `-144308.86`

That is, the optimizer did build a different, lower-volatility portfolio. But it did so with monotonically worse return quality:

- annualized return fell from `10.66%` to `6.99%`
- Sharpe fell from `1.11` to `1.03`
- no tested multiplier produced a Sharpe sweet spot

Conclusion from augment mode:

- the risk term is active
- but without an alpha / expected-return term, greater risk aversion is pure drag rather than an improved tradeoff

### Replace-Mode Result

The replace-mode sweep tested whether removing the legacy `target_deviation` term would rescue the frontier.

It did not.

Observed behavior:

- multipliers `1x`, `100x`, `1000x`, and `10000x` all collapsed to the exact same point
- that point had:
  - ending NAV `2069840.99`
  - annualized return `6.37%`
  - annualized volatility `8.40%`
  - Sharpe `0.78`
  - turnover `0.0000`
  - total cost `0.00`
  - vs naive delta `-156362.24`
- only `100000x` moved to a new point, and that point was worse:
  - ending NAV `2048388.03`
  - annualized return `5.25%`
  - annualized volatility `7.23%`
  - Sharpe `0.75`
  - vs naive delta `-177815.20`

Conclusion from replace mode:

- removing legacy target-tracking pressure was not enough
- the current risk-only construction objective still does not produce a Sharpe-improving frontier
- replace mode is weaker than augment mode across the tested grid

### Combined Interpretation

The two risk sweeps tighten the diagnosis:

- augment mode: risk aversion works mechanically, but worsens Sharpe monotonically
- replace mode: the objective nearly degenerates, then worsens once forced harder

This is strong evidence for the following statement:

- without an explicit alpha / expected-return signal, the current risk-aware objective does not create a useful production frontier

## Configuration Decision

Configuration policy after this note:

- keep `config/us_expanded.yaml` unchanged
- keep `config/us_expanded_tca_calibrated.yaml` as the research record for the calibrated low-participation cost model
- keep `config/us_expanded_tca_calibrated_replace.yaml` as the research record for the replace-mode diagnostic
- do not promote `k = 3.498400399110418` into the default expanded-US config
- do not change the default cost-bundle setting based on the current calibrated sweep alone

Reason:

- the calibrated TCA result is strong enough to close the calibration loop
- it is not strong enough to justify a default promotion into an optimizer that currently lacks a meaningful non-cost construction lever
- the follow-up risk sweeps failed to find a Sharpe-improving sweet spot in either objective mode

## Research Direction

The risk-aversion sweep has now been completed, and it changes the recommended next step.

The evidence does not support continuing immediately into a larger two-dimensional sweep of:

- higher `risk_term`
- lower `target_deviation`

That path remains possible in theory, but it would add search complexity before there is evidence that the current objective family can produce a Sharpe-improving frontier at all.

The stronger current recommendation is Path C:

- archive the current result as evidence
- keep the risk sweep tooling as reusable infrastructure
- defer further risk-aware portfolio-construction tuning until one of the following exists:
  - an explicit alpha / expected-return signal
  - a redesigned objective that encodes a meaningful portfolio-quality reward, not just lower variance and lower trading

In practical terms:

1. keep the calibrated low-participation `k` as a research-only input
2. keep both augment and replace risk sweep outputs as structural evidence
3. do not continue tuning the current risk-only objective for production promotion
4. revisit risk-aware construction only after the objective can reward beneficial departures from naive

## Explicit Closeout

Phase 13 is considered closed on the following basis:

- backtest loop exists and is reusable
- attribution and cost sweeps exist
- TCA calibration loop is operational
- low-participation sufficiency is achieved with live data
- calibrated cost-model impact has been tested in the backtest loop
- augment-mode risk sweep has been completed
- replace-mode risk sweep has been completed
- no Sharpe-improving risk-aversion sweet spot was found in either mode
- the remaining blocker is no longer data plumbing or calibration readiness
- the remaining blocker is optimizer objective structure
