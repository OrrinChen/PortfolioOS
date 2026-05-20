# SUE Market-Regime Placebo Filter Check

H1E.2 validates the market-regime attribution only.
It reruns filtered score-gate summaries and placebo curves after excluding March 2020, high-volatility weeks, and low-liquidity weeks.
It does not select a SUE score, run Q2, run optimizer-path evaluation, promote Alpha Registry state, open paper/live/broker/order workflows, or approve production use.

## Summary

- schema_version: `sue_regime_filter_placebo_check.v1`
- score_name: `surprise_pct_actual_eps`
- interpretation: `market_regime_filter_reduces_but_does_not_resolve_placebo_failure`
- selected_score: `None`
- high_volatility_week_count: `12`
- low_liquidity_week_count: `12`
- low_liquidity_filter_source: `price_observation_count_proxy_missing_volume`
- q2_evaluation_ran: `False`
- optimizer_path_evaluation_ran: `False`
- production_approval_claimed: `False`

## Filtered Score-Gate Summary

| Filter | Events | Excluded | Live Spread | Best Placebo Shift | Best Placebo Spread | Shift Passed |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `baseline` | 15365 | 0 | 0.020484 | -5 | 0.089401 | `False` |
| `exclude_march_2020` | 14267 | 1098 | 0.026393 | -5 | 0.091431 | `False` |
| `exclude_high_volatility_weeks` | 13114 | 2251 | 0.028235 | -5 | 0.091213 | `False` |
| `exclude_low_liquidity_weeks` | 15356 | 9 | 0.020484 | -5 | 0.089401 | `False` |
| `exclude_market_regime_weeks` | 13105 | 2260 | 0.028235 | -5 | 0.091213 | `False` |

## Regime Week Classification

- week_count: `117`
- high_volatility_week_count: `12`
- low_liquidity_week_count: `12`

## Boundaries

- This phase validates the H1E.1 market-regime attribution only.
- Missing denominator, price, return, or liquidity coverage remains unavailable/no_view and is not encoded as zero alpha.
- Downstream typed projection, Q2, optimizer-path evaluation, and any paper-stage work require a separate explicit reopen.
