# FD-S4.2 Small-Cap Slow / Capacity-Filtered Diagnostic

not alpha evidence
allocator entry: blocked
Q1 entry: blocked
Q2 entry: blocked
Alpha Registry update: blocked
production approval: not claimed

- decision: reject_temporal_noise_confirmed
- six month target available: true
- learned weighting used: false

## Best Test Rows
- live_signal 3m quarterly adv_weight_within_bucket: gross=0.103442, net=0.101153, placebo=passed_placebo_gate
- live_signal 3m monthly adv_weight_within_bucket: gross=0.083921, net=0.081791, placebo=passed_placebo_gate
- live_signal 6m quarterly capacity_capped_equal_weight: gross=0.078291, net=0.076128, placebo=passed_placebo_gate
- live_signal 6m quarterly equal_weight_within_bucket: gross=0.078291, net=0.076128, placebo=passed_placebo_gate
- lag_1m_signal 3m quarterly adv_weight_within_bucket: gross=0.073463, net=0.071377, placebo=passed_placebo_gate
- rolling_3m_median_signal 6m quarterly capacity_capped_equal_weight: gross=0.072096, net=0.070083, placebo=passed_placebo_gate
- rolling_3m_median_signal 6m quarterly equal_weight_within_bucket: gross=0.072096, net=0.070083, placebo=passed_placebo_gate
- rolling_3m_median_signal 6m monthly equal_weight_within_bucket: gross=0.071085, net=0.069001, placebo=passed_placebo_gate
