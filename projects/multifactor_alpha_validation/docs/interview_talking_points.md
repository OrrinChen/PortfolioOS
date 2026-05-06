# Multi-Factor Alpha Validation Interview Talking Points

- Built a PIT-safe multi-factor validation engine that starts from timestamped
  factor contracts instead of formulas.
- Separated raw, neutralized, residual, cost-adjusted, and capacity-adjusted
  evidence.
- Added redundancy and marginal-value gates so high standalone IC cannot route a
  factor into allocation by itself.
- Applied posterior expected-return shrinkage and covariance stabilization
  before factor allocation.
- Produced zero-weight attribution for low confidence, redundancy, cost,
  turnover, capacity, and no-view cases.
- Packaged the result as a registry, static read-only dashboard, final report,
  and reproducible local validation target.

