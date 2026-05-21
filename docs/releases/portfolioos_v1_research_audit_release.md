# PortfolioOS v1 Research-Audit Release

## Scope

This release packages the current PortfolioOS typed-alpha research-audit surface
after Phase 56A closeout. It is a local, reproducible decision-evaluation
release, not a trading system release.

## Q1 Alpha Evidence / Triage Boundary

Q1 remains the alpha-evidence and hypothesis triage boundary. It validates
hypothesis, signal, evaluator, and event-registry contracts with leakage-safe
fixtures and dry-run planning only. Q1 does not run PortfolioOS workflows, emit
execution instructions, or directly modify portfolio logic.

## Evidence Bundle / Promotion Gate Boundary

Evidence Bundle and Promotion Gate artifacts keep research validation separate
from execution evaluation. A promotion decision can produce a typed Q2 input
contract, but it does not run Q2, place a candidate into production, or bypass
the Alpha Registry v2 decision state machine.

## Typed Alpha View Contract

The typed AlphaView contract represents alpha as a timestamped predictive view
with mechanism, horizon, PIT safety, coverage, abstain, decay, confidence, and
provenance. Missing coverage remains explicit abstain/no_view; it is not
silently encoded as zero alpha.

## SUE Local Typed-Q2 Pilot

The local SUE typed-Q2 pilot proves that a SUE expected-return projection can
reach the local optimizer input snapshot and map to observed local Q2 rows where
stable fixture adapter hooks exist. SUE remains a canonical pilot and typed-Q2
candidate only.

## SUE Expanded Deterministic Typed-Q2 Candidate Benchmark

Phase 56A expands the deterministic SUE fixture to 120 event-name rows across
12 rebalance dates. The current release records `event_count=120`,
`rebalance_date_count=12`, `active_rebalance_count=12`, `q2_observed_rows=30`,
and `production_approval_claimed=false`.

This benchmark is deterministic expanded fixture evidence only. It does not
prove real historical SUE alpha. Q2 observed rows still come through existing
local fixture adapter hooks, and missing coverage remains explicit
abstain/no_view, not zero alpha.

## Alpha Registry v2 Decision State Machine

Alpha Registry v2 is the source of truth for alpha state. SUE is recorded as
`canonical_pilot` with typed-Q2 candidate history, including
`sue_expanded_fixture_q2_observed_survives` from deterministic expanded fixture
evidence. It is not recorded as production-promoted.

Revision remains a real shadow branch without marginal-value promotion in the
default fixture. Composite work remains closed by default. Residual momentum is
calibration-only, and A-share remains background unless explicitly reopened
through its charter.

## Dashboard / Audit / Provenance / No-Network Safeguards

The release includes static dashboard, audit report, provenance manifest,
structured trace, schema compatibility, golden demo, and no-network safeguards.
The dashboard is read-only. The no-network guard is part of `make validate`.
Generated demo and registry artifacts are local and reproducible.

## Release Artifacts

- `README.md`
- `ROADMAP.md`
- `TASK_MEMORY.md`
- `VALIDATION.md`
- `reports/alpha_registry_report.md`
- `reports/sue_expanded_typed_q2_closeout.md`
- `reports/sue_expanded_typed_q2_survival_report.md`
- `outputs/alpha_registry_v2/alpha_registry.yaml`
- `outputs/alpha_registry_v2/alpha_registry_decision_table.csv`

## Reproducibility

```bash
make alpha-registry-v2
make sue-expanded-typed-q2-survival
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python scripts/build_sue_expanded_q2_attribution.py
make validate
```

## Validation Summary

The release hygiene path is guarded by:

- Alpha Registry v2 generation and registry tests
- expanded SUE typed-Q2 survival smoke and attribution rebuild
- PortfolioOS v1 release hygiene tests
- `make validate`, including no-network, example validation, audit report,
  core tests, Promotion Gate tests, Q2 focused tests, typed-alpha pilot tests,
  and audit report tests

## Explicit Non-Goals

- no production approval
- no live trading
- no broker/order path
- no paper-ready alpha claim
- no production-promoted SUE status
- no new alpha research
- no optimizer retuning
- no paper canary approval
- no autonomous trading workflow
- no investment advice or recommendation

## Next State

Phase 65 packages the release hygiene surface. Future work should be limited to
Phase 66 maintenance freeze, bug fixes, schema migration, documentation
corrections, or separately approved paper-stage and research-import governance
paths.
