# FD-S4.1 Small-Cap Lag / Capacity Dominance Diagnosis

not alpha evidence
allocator entry: blocked
Q1 entry: blocked
Q2 entry: blocked
Alpha Registry update: blocked
production approval: not claimed

- decision: diagnostic_only_cost_blocked
- learned weighting used: false

## Signal Decay
- live_signal: spread_1m=0.002724, spread_3m=0.010541
- lag_1m_signal: spread_1m=0.003386, spread_3m=0.011721
- lag_2m_signal: spread_1m=0.000221, spread_3m=0.009521
- lag_3m_signal: spread_1m=0.004785, spread_3m=0.012614
- rolling_3m_mean_signal: spread_1m=0.004102, spread_3m=0.013565
- rolling_3m_median_signal: spread_1m=0.005134, spread_3m=0.017032
- stale_signal_carry_forward: spread_1m=0.003386, spread_3m=0.011721
- live_minus_lag_update_component: spread_1m=-0.000828, spread_3m=-0.008027

## Holding / Rebalance
- monthly 1m: gross=0.002724, net=-0.008682
- monthly 3m: gross=0.010541, net=-0.000884
- monthly 6m: gross=unavailable, net=unavailable
- quarterly 1m: gross=-0.013031, net=-0.024859
- quarterly 3m: gross=0.005659, net=-0.006169
- quarterly 6m: gross=unavailable, net=unavailable

## Capacity
- market_cap/low: gross=-0.001502, net=-0.021189, cost_drag=0.019686, active_count=29032
- market_cap/mid: gross=0.006966, net=-0.002041, cost_drag=0.009007, active_count=29032
- market_cap/high: gross=0.007851, net=0.003481, cost_drag=0.004370, active_count=29032
- adv/low: gross=-0.001303, net=-0.023950, cost_drag=0.022647, active_count=29032
- adv/mid: gross=-0.000192, net=-0.006786, cost_drag=0.006594, active_count=29032
- adv/high: gross=0.008125, net=0.001905, cost_drag=0.006221, active_count=29032
- spread/low: gross=0.003423, net=0.001215, cost_drag=0.002208, active_count=29032
- spread/mid: gross=0.003773, net=-0.002408, cost_drag=0.006180, active_count=29034
- spread/high: gross=-0.002837, net=-0.028685, cost_drag=0.025848, active_count=29030
- price/price_unavailable: gross=0.002724, net=-0.008682, cost_drag=0.011406, active_count=87096

## Fixed Weighting
- equal_weight: gross=0.002724, net=-0.008682, learned_weighting=false
- value_weight: gross=0.006196, net=-0.005209, learned_weighting=false
- sqrt_market_cap_weight: gross=0.004546, net=-0.006859, learned_weighting=false
- adv_weight: gross=-0.003672, net=-0.015078, learned_weighting=false
- capacity_capped_equal_weight: gross=0.002724, net=-0.008682, learned_weighting=false
- capacity_capped_value_weight: gross=0.004320, net=-0.007086, learned_weighting=false

## Cost Drag
- gross spread: 0.002724
- net spread: -0.004210
