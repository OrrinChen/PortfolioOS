# FD Small-Cap Quality Residual Momentum

not alpha evidence
allocator entry: blocked
Q1 entry: blocked
Q2 entry: blocked
Alpha Registry update: blocked
production approval: not claimed

- family: small_cap_quality_residual_momentum_v1
- primary signal: small_cap_quality_residual_momentum_6m_ex1m
- decision: reject_placebo_failure
- learned weighting used: false

- test 1m: rank_ic=0.018224, spread=0.004729
- test 3m: rank_ic=0.019106, spread=0.010827
- validation 1m: rank_ic=-0.045896, spread=-0.031068
- validation 3m: rank_ic=-0.034198, spread=-0.031904

## Negative Controls
- random_same_mcap_adv_coverage: control_beats_live=false
- size_bucket_shuffled_signal: control_beats_live=false
- sector_shuffled_signal: control_beats_live=false
- rebalance_date_shifted_signal: control_beats_live=true
- delisting_return_removed_sensitivity: control_beats_live=false
- equal_weight_vs_value_weight_comparison: control_beats_live=true

## Dominance Diagnosis
- rebalance_date_shifted_signal: driver=signal_persistence_or_monthly_update_noise, score_lag_rank_corr=0.760942
- equal_weight_vs_value_weight_comparison: driver=payoff_concentrated_in_larger_more_capacity_names, score_market_cap_rank_corr=0.088683

## Exposure Attribution
- market_beta: -0.042408
- smb_beta: -0.090036
- sector_exposure: 0.368552
- liquidity_exposure: 0.009185
- microcap_exposure: 0.000000
- quality_exposure: -0.046908

## Cost / Capacity Pre-Gate
- status: fail_cost_adjusted_spread
- gross mean spread: 0.007778
- cost-adjusted mean spread: -0.004068
- capacity USD at 1% ADV: 29746.309909
