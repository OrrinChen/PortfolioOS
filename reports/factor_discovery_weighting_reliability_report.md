# FD-R4.1 / FD-R5.2 Weighting Reliability Report

not alpha evidence
allocator entry: blocked
Q1 entry: blocked
Q2 entry: blocked
Alpha Registry update: blocked
production approval: not claimed

- decision: close
- placebo status: failed_placebo_gate
- rolling ICIR overfit/noise failure: true
- best estimator: equal_weight_all

## Estimator Comparison
- equal_weight_all: 1m_ic=0.021083, 3m_ic=0.031714, spread_1m=0.006373, spread_3m=0.019243, survival=1.000
- family_equal_weight: 1m_ic=0.016620, 3m_ic=0.023776, spread_1m=0.008149, spread_3m=0.018109, survival=1.000
- shrunk_rolling_icir_lambda_24: 1m_ic=0.008100, 3m_ic=0.013261, spread_1m=0.002037, spread_3m=0.005088, survival=0.500
- shrunk_rolling_icir_lambda_12: 1m_ic=0.008102, 3m_ic=0.013235, spread_1m=0.001988, spread_3m=0.005088, survival=0.500
- shrunk_rolling_icir_lambda_6: 1m_ic=0.008101, 3m_ic=0.013218, spread_1m=0.001988, spread_3m=0.005088, survival=0.500
- ridge_weighting_alpha_10: 1m_ic=-0.007398, 3m_ic=0.004852, spread_1m=-0.003163, spread_3m=-0.014950, survival=0.250
- ridge_weighting_alpha_1: 1m_ic=-0.007339, 3m_ic=0.004816, spread_1m=-0.003813, spread_3m=-0.014582, survival=0.250
- signed_shrunk_rolling_icir_lambda_6: 1m_ic=-0.012458, 3m_ic=-0.021128, spread_1m=-0.003923, spread_3m=-0.020784, survival=0.000
- rolling_icir_current: 1m_ic=-0.012462, 3m_ic=-0.021133, spread_1m=-0.003923, spread_3m=-0.020784, survival=0.000
- signed_shrunk_rolling_icir_lambda_24: 1m_ic=-0.012452, 3m_ic=-0.021110, spread_1m=-0.003923, spread_3m=-0.020961, survival=0.000
- signed_shrunk_rolling_icir_lambda_12: 1m_ic=-0.012457, 3m_ic=-0.021123, spread_1m=-0.003923, spread_3m=-0.020961, survival=0.000

## Boundary
- no allocator, Q1, Promotion Gate, Q2, Alpha Registry, broker/order/live workflow, or production approval path is opened
