# Promotion Gate

Promotion Gate is a standalone contract layer between Evidence Bundle validation and
Q2 execution-aware evaluation.

It answers:

> Is this evidence package safe enough to hand off as an input contract for execution evaluation?

It does not run Q2, call PortfolioOS workflows, place orders, call brokers, or
perform live data access.

## Current Decisions

`PromotionDecision.decision` can be:

- `promote_to_execution_eval`
- `reject`
- `needs_more_evidence`

Promotion can produce a `Q2InputContract`, but that contract only describes
allowed future input columns. It does not trigger Q2 execution.

Phase 38 adds Promotion Gate v2:

- consumes `EvidenceBundle + AlphaView + ProjectionManifest`
- checks typed horizon semantics, explicit abstain, projection manifest
  consistency, active coverage reporting, and revision marginal-value
  disclosure
- emits `Q2InputContractV2` for projected expected-return panel artifacts
- writes optional local artifacts:
  - `promotion_decision_v2.json`
  - `q2_input_contract_v2.json`
  - `promotion_explanation_v2.md`

The v2 gate still does not run Q2, call PortfolioOS workflows, place orders,
call brokers, or report live performance.

## Required Gate Checks

Current Phase 22 checks include:

- evidence bundle schema validation
- PIT-safety validation through the evidence bundle schema
- leakage validation through the evidence bundle schema
- coverage requirements are declared
- cost assumptions are declared
- evaluation horizon is bounded to a day/month horizon
- forbidden output keys are absent
- direct Q2 execution is not allowed

Current horizon sanity is intentionally conservative: it requires an explicit
bounded day or month phrase such as `5 trading days` before a bundle can move to
execution evaluation. This is a promotion-precondition check, not a backtest or
performance claim.

## Validation

Run from the repository root:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/promotion_gate/src:projects/evidence_bundle/src poetry run pytest projects/promotion_gate/tests -q
```
