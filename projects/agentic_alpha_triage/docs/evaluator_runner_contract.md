# Q1 Evaluator Runner Contract

## Purpose

The Q1 evaluator runner will be a local dry-run planner for answering:

> Is this alpha real enough to evaluate?

It is not a trading workflow, an agent loop, or a PortfolioOS execution path. Its job is to assemble already-declared Q1 contracts into a leakage-safe evaluation plan and reject unsafe inputs before any research code computes results.

## Inputs

The runner may read only local, schema-backed artifacts:

- `Hypothesis`
- `SignalContract`
- `EvaluationContract`
- `EventRegistryExample`
- `EvaluatorFixture`

The first supported fixture family is the committed guidance-raise example under `projects/agentic_alpha_triage/examples`.

## Required Assembly Checks

A runner plan must verify:

- the evaluator fixture references existing hypothesis, signal, and evaluation contract files
- the fixture `hypothesis_id` matches the referenced hypothesis
- the fixture `signal_name` matches the referenced signal contract
- event registry examples share the same `hypothesis_id`
- event `anchor_trade_date` is not before `event_available_timestamp`
- signal `timestamp_column` is included in the fixture required input columns
- signal `output_column` is present as a planned output, not as a feature input
- `label_column` is excluded from `feature_columns`
- `uses_future_data_as_feature` is false
- `entry_after_event_available` is true
- placebo and leakage checks are required before promotion
- cost assumptions are explicitly declared

## Planned Output Schema

The dry-run planner returns a plain local record with:

| field | meaning |
|---|---|
| `plan_id` | Stable plan identifier derived from the fixture id |
| `fixture_id` | Source evaluator fixture |
| `hypothesis_id` | Source hypothesis id |
| `signal_name` | Source signal contract name |
| `event_registry_ids` | Referenced compatible event registries |
| `required_input_columns` | Required columns before signal evaluation |
| `feature_columns` | Allowed feature columns |
| `label_column` | Evaluation label column |
| `holding_windows` | Evaluation horizons |
| `benchmark` | Evaluation benchmark |
| `cost_assumptions` | Explicit evaluation cost assumptions |
| `leakage_checks` | Required leakage checks |
| `placebo_tests` | Required placebo tests |
| `status` | `ready_for_local_evaluation` or `rejected` |
| `rejection_reasons` | Non-empty only when status is `rejected` |

This schema is a planning artifact only. It must not contain realized returns, backtest performance, or trading instructions.

The current `build_evaluator_plan` function raises `ValueError` for rejected plans. A future CLI wrapper may serialize those failures into `status: rejected` records for audit logs.

## Non-Responsibilities

The runner must not:

- call FMP, SEC, WRDS, broker, or paid external services
- invoke LLM agent loops
- generate hypotheses
- implement signal formulas
- run PortfolioOS workflows
- place orders or produce orders
- export to Q2 directly
- fabricate alpha scores or evaluator results
- treat schema validation as alpha validation

## Q2 Boundary

Q1 may later write a plain `alpha_score.csv` artifact after a separate evaluation phase succeeds. Q2 can consume that file as ordinary input, but Q2 must not import this runner or depend on Q1 schemas.

## Phase 14 Implementation

The Phase 14 implementation lives in `src/agentic_alpha_triage/evaluator_planner.py`. It loads the valid fixture family and emits the planned output schema above. It rejects contract disagreements before any evaluation can run.
