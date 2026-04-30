# Agentic Alpha Hypothesis Triage System

Q1 asks: **Is this alpha real?**

This project is a standalone hypothesis triage shell. It is not an autonomous trading bot and does not place orders, run PortfolioOS workflows, or modify trading code.

The purpose is to force every alpha idea through explicit contracts before it reaches a leakage-safe evaluator.

## Relationship To Q2

Q2 is separate and asks: **Can this alpha be traded?**

Q1 may later export a standard `alpha_score.csv` file:

```text
date,symbol,alpha_score,alpha_source,alpha_confidence
```

Q2 can read that file as a plain input artifact, but Q2 must not depend on Q1 agent loops, SEC/FMP ingestion, schema modules, or hypothesis-generation code.

## Current Scope

This first integration only provides schemas and contracts:

- `Hypothesis`
- `SignalContract`
- `EvaluationContract`
- `EventRegistryEntry`

There are no LLM agent loops, no live API calls, and no paid data artifacts.

## Example Artifacts

The `examples/` directory contains one valid contract set:

- `hypothesis_guidance_raise_drift.yaml`
- `signal_guidance_raise_drift.yaml`
- `evaluation_guidance_raise_drift.yaml`

These examples are intentionally small. They show the required structure for a timestamp-safe hypothesis, signal contract, and leakage-aware evaluation contract. They are not evidence that the alpha works.

## Future Workflow

The intended path is:

1. Generate alpha hypotheses from timestamp-safe SEC/FMP or other point-in-time sources.
2. Express each hypothesis under the strict `Hypothesis` schema.
3. Implement a signal only if it satisfies `SignalContract`.
4. Evaluate through `EvaluationContract` with required leakage and placebo tests.
5. Reject hypotheses that fail timestamp, placebo, cost, liquidity, or stability checks.
6. Optionally export an `alpha_score.csv` artifact for Q2.

## Safety Notes

- `FMP_API_KEY` is referenced only as an environment variable name in config.
- Do not commit paid FMP payloads.
- Do not use non-point-in-time data for event labels.
- Do not treat schema validation as proof that an alpha exists.

## Tests

Run from the repository root:

```bash
PYTHONPATH=projects/agentic_alpha_triage/src poetry run pytest projects/agentic_alpha_triage/tests -q
```

Validate the committed examples:

```bash
PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/validate_examples.py
```
