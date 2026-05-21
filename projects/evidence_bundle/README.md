# Evidence Bundle

Evidence Bundle is a standalone project area for typed Q1 evidence packages.

It supports the PortfolioOS platform story:

```text
Q1 Alpha Triage -> Evidence Bundle -> Promotion Gate -> Q2 Execution-Aware Evaluation
```

An evidence bundle is not a trading recommendation, a backtest result, or a Q2 execution artifact. It is a deterministic, schema-backed record that captures whether a research candidate has enough point-in-time, leakage-safe evidence to be reviewed for later promotion.

## Current Schema

The Phase 21 schema includes:

- `bundle_id`
- `hypothesis_id`
- `signal_id`
- `evaluation_id`
- `status`
- `pit_safety`
- `leakage_checks`
- `required_columns`
- `planned_tests`
- `coverage_requirements`
- `cost_assumptions`
- `evaluation_horizon`
- `rejection_reasons`
- `promotion_eligibility`

## Safety Boundaries

Evidence bundles must not contain:

- trading recommendations
- orders
- broker output
- live performance
- hidden Q2 results
- direct Q2 execution output

Unsafe bundles are rejected when they include forward-return leakage, missing timestamp safety, or an anchor trade date that precedes signal visibility.

## Validation

Run from the repository root:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/evidence_bundle/src poetry run pytest projects/evidence_bundle/tests -q
```
