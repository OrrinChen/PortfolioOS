# Typed Alpha Pilot

This project area builds local typed-alpha pilot artifacts.

The first pilot is SUE / PEAD as an integration benchmark:

```text
AlphaView -> Event Evidence -> Projection Bridge v2 -> Promotion Gate v2 -> Q2 Typed Matrix -> Audit Report
```

It is not production approval, not an alpha promotion, and not a trading
instruction. It does not call brokers, run live services, or generate orders.

## Validation

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/typed_alpha_pilot/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src poetry run pytest projects/typed_alpha_pilot/tests -q
```
