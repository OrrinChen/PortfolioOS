# AlphaView Contract Note

Phase 35 introduces AlphaView as the first typed alpha-expression boundary.

Allowed output:

- typed predictive view metadata
- timestamp and visibility assumptions
- coverage and explicit abstain information
- expected-return view entries with active/no-view states
- PIT safety report
- provenance

Forbidden output:

- orders
- broker output
- live performance
- trading recommendations
- trading instructions
- hidden Q2 results

The contract intentionally stops before event evaluation, projection,
promotion-v2, Q2 execution survival, or paper overlay calibration. Those are
later phases and must consume AlphaView through explicit adapters.
