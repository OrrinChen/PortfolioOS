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
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/promotion_gate/src:projects/evidence_bundle/src poetry run pytest projects/promotion_gate/tests -q
```
