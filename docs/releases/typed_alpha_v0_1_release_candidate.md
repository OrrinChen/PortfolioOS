# Typed Alpha v0.1 Release Candidate

## Scope

This release candidate freezes the local Phase 35-42 typed-alpha demo surface:

- AlphaView contract
- event-aware evaluation contract
- Alpha Projection Bridge v2
- Promotion Gate v2
- Q2 typed alpha execution matrix
- paper overlay readiness lane
- SUE integration benchmark
- demo-v2 static dashboard

The implementation range is the typed-alpha roadmap commit span
`f189afd..ff8d8e1`, plus Phase 43 hardening work.

## What Is Complete

- Typed alpha views can be represented as deterministic local JSON artifacts.
- Event alpha can be separated from fixed-horizon alpha through event evidence
  contracts.
- Projection artifacts can convert typed AlphaViews into expected-return panel
  inputs with explicit abstain reporting.
- Promotion Gate v2 preserves the Q1/Q2 boundary and emits a Q2 input contract
  without running Q2.
- Q2 typed matrix rows can consume projected alpha diagnostics while keeping
  execution rows explicitly unavailable.
- Paper overlay readiness is recorded as execution-environment calibration
  only.
- `make demo-v2` writes the local typed-alpha artifact chain under
  `outputs/demo_v2/`.

## What Is Intentionally Not Claimed

- no production alpha approval
- no live trading
- no broker integration
- no order generation
- no realized alpha performance
- no live SUE deployment
- no promotion into production PortfolioOS configs
- no claim that unavailable Q2 rows are performance results
- no claim that paper overlay calibration validates alpha

## How To Reproduce

```bash
make validate
make demo-v2
```

`make demo-v2` is local-only and writes ignored artifacts under
`outputs/demo_v2/`.

## Known Limitations

- Q2 typed matrix rows may remain unavailable until a typed Q2 execution adapter
  exists.
- The SUE pilot is an integration benchmark, not production approval.
- Paper overlay readiness uses local observations only unless a separate,
  explicit paper sampling workflow is requested.
- The dashboard is static read-only and does not expose workflow controls.
- Schema migration support is not implemented beyond the v0.1 version lock.

## Next Optional Branches

- demo-v2 golden snapshot checks
- typed-alpha closeout report
- dashboard readability polish
- read-only artifact browsing hardening
