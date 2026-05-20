# FD-S4.3 Small-Cap Capacity-Filtered Live Signal Preregistration Gate

not alpha evidence
allocator entry: blocked
Q1 entry: blocked
Q2 entry: blocked
Alpha Registry update: blocked
production approval: not claimed

- decision: reject_capacity_filter_hypothesis
- slow signal branch closed: true
- confirmation available: true
- split method: nested_time_split
- signal: live_signal / 3m / quarterly
- shortability unknown: true

## Confirmation Rows
- confirmation adv_weight_within_bucket: gross=0.084620, net=0.082164, subperiod=0.750000, placebos=failed_or_unavailable_placebo_gate/failed_or_unavailable_placebo_gate/failed_or_unavailable_placebo_gate
- confirmation capacity_capped_equal_weight: gross=0.005628, net=0.003257, subperiod=0.500000, placebos=passed_placebo_gate/passed_placebo_gate/failed_or_unavailable_placebo_gate
- hypothesis_generation adv_weight_within_bucket: gross=0.028817, net=0.026828, subperiod=0.500000, placebos=passed_placebo_gate/failed_or_unavailable_placebo_gate/passed_placebo_gate
- hypothesis_generation capacity_capped_equal_weight: gross=0.065188, net=0.063269, subperiod=0.750000, placebos=passed_placebo_gate/passed_placebo_gate/passed_placebo_gate

## Placebos
- adv_weight_within_bucket same_coverage_placebo: live=0.082164, placebo=0.111557, status=failed_or_unavailable_placebo_gate
- adv_weight_within_bucket capacity_matched_placebo: live=0.082164, placebo=0.141751, status=failed_or_unavailable_placebo_gate
- adv_weight_within_bucket rebalance_shifted_placebo: live=0.082164, placebo=0.085864, status=failed_or_unavailable_placebo_gate
- capacity_capped_equal_weight same_coverage_placebo: live=0.003257, placebo=-0.006555, status=passed_placebo_gate
- capacity_capped_equal_weight capacity_matched_placebo: live=0.003257, placebo=-0.003943, status=passed_placebo_gate
- capacity_capped_equal_weight rebalance_shifted_placebo: live=0.003257, placebo=0.024169, status=failed_or_unavailable_placebo_gate
