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

This integration provides schemas, contracts, static examples, and a local dry-run planner:

- `Hypothesis`
- `SignalContract`
- `EvaluationContract`
- `EventRegistryEntry`
- `EventRegistryExample`
- `EvaluatorFixture`
- `EvaluatorPlan`

There are no LLM agent loops, no live API calls, and no paid data artifacts.

## Example Artifacts

The `examples/` directory contains one valid contract set:

- `hypothesis_guidance_raise_drift.yaml`
- `signal_guidance_raise_drift.yaml`
- `evaluation_guidance_raise_drift.yaml`
- `event_registry/valid/guidance_raise_event.yaml`
- `evaluator_fixtures/valid/guidance_raise_drift.yaml`

These examples are intentionally small. They show the required structure for a timestamp-safe hypothesis, signal contract, and leakage-aware evaluation contract. They are not evidence that the alpha works.

The `examples/event_registry/invalid/` and `examples/evaluator_fixtures/invalid/` directories contain negative fixtures that must be rejected by the loaders. These are committed as guardrails for timestamp and leakage-risk examples, not as runnable research inputs.

## Event Registry Examples

Q1 event registry examples describe timestamped events before any evaluator consumes them. They make the market-visible timestamp, source record, and anchor trade date explicit. The committed negative examples show two unsafe cases:

- missing `event_available_timestamp`
- `anchor_trade_date` before event visibility

## Evaluator Fixtures Versus Q2 Checks

Q1 evaluator fixtures describe whether an alpha idea can be evaluated without timestamp leakage or placebo-test shortcuts. They focus on event availability, anchor dates, allowed feature columns, required placebo tests, and cost assumptions.

Q2 execution checks are separate. They ask whether an already-produced `alpha_score` panel survives risk, sector, turnover, liquidity, and transaction-cost constraints. Q2 may read a Q1-exported CSV as a plain artifact, but it does not import Q1 evaluator fixtures or execute Q1 hypothesis logic.

## Evaluator Runner Boundary

The local evaluator runner contract is documented in `docs/evaluator_runner_contract.md`.

The implemented runner in `src/agentic_alpha_triage/evaluator_planner.py` is a dry-run planner only. It assembles local schema-backed Q1 artifacts into a leakage-safe evaluation plan, but it does not call live SEC/FMP services, run LLM agent loops, execute PortfolioOS workflows, compute trading results, or export directly to Q2.

Minimal local usage:

```python
from agentic_alpha_triage import build_evaluator_plan

plan = build_evaluator_plan(
    "projects/agentic_alpha_triage/examples/evaluator_fixtures/valid/guidance_raise_drift.yaml",
    event_registry_dir="projects/agentic_alpha_triage/examples/event_registry/valid",
)
```

CLI dry-run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator.py --fixture projects/agentic_alpha_triage/examples/evaluator_fixtures/valid/guidance_raise_drift.yaml --event-registry-dir projects/agentic_alpha_triage/examples/event_registry/valid
```

By default, local contract disagreements still exit nonzero. For audit workflows that need structured rejection metadata, add `--emit-rejected-json`:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator.py --fixture projects/agentic_alpha_triage/examples/evaluator_fixtures/invalid/guidance_raise_forward_return_leakage.yaml --event-registry-dir projects/agentic_alpha_triage/examples/event_registry/valid --emit-rejected-json
```

The rejected JSON is audit-only. It includes fixture paths, event-registry paths, status, and rejection reasons; it does not include realized returns, alpha performance, orders, trading instructions, PortfolioOS workflow output, or Q2 exports.

## Evaluator Plan Manifest

`examples/evaluator_plan_manifest.yaml` lists local evaluator-plan dry-run targets. It currently includes:

- one valid guidance-raise fixture expected to produce `ready_for_local_evaluation`
- one negative leakage fixture expected to produce `rejected`

The manifest is only an index of local fixture paths and expected planner statuses. Loading it validates the manifest schema and referenced local paths, but it does not execute evaluations, call live services, run agent loops, run PortfolioOS workflows, produce trading results, or export anything to Q2.

Batch dry-run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator_manifest.py --manifest projects/agentic_alpha_triage/examples/evaluator_plan_manifest.yaml
```

The batch output is an ordered JSON object with one ready or rejected planner payload per manifest entry. It is still local audit metadata only; it contains no realized returns, alpha performance, orders, trading instructions, PortfolioOS workflow output, or Q2 export.

## Future Workflow

The intended path is:

1. Generate alpha hypotheses from timestamp-safe SEC/FMP or other point-in-time sources.
2. Express each hypothesis under the strict `Hypothesis` schema.
3. Implement a signal only if it satisfies `SignalContract`.
4. Build a local `EvaluatorPlan` from compatible hypothesis, signal, evaluation, event, and evaluator fixture artifacts.
5. Evaluate through `EvaluationContract` with required leakage and placebo tests.
6. Reject hypotheses that fail timestamp, placebo, cost, liquidity, or stability checks.
7. Optionally export an `alpha_score.csv` artifact for Q2.

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

This also validates `examples/evaluator_plan_manifest.yaml`.

Print the committed local dry-run evaluator plan:

```bash
PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator.py --fixture projects/agentic_alpha_triage/examples/evaluator_fixtures/valid/guidance_raise_drift.yaml --event-registry-dir projects/agentic_alpha_triage/examples/event_registry/valid
```

Print an audit-only rejected evaluator-plan payload:

```bash
PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator.py --fixture projects/agentic_alpha_triage/examples/evaluator_fixtures/invalid/guidance_raise_forward_return_leakage.yaml --event-registry-dir projects/agentic_alpha_triage/examples/event_registry/valid --emit-rejected-json
```

Print the committed batch evaluator-plan manifest payload:

```bash
PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator_manifest.py --manifest projects/agentic_alpha_triage/examples/evaluator_plan_manifest.yaml
```
