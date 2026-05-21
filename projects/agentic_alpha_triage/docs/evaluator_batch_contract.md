# Q1 Evaluator Batch Contract

## Purpose

The Q1 evaluator batch wrapper is a local dry-run audit surface for answering:

> Which declared alpha-evaluation fixtures are ready for local evaluation planning, and which are rejected before evaluation?

It is not a real evaluator, a backtest runner, a trading workflow, an LLM agent loop, or a PortfolioOS execution path. Its job is to consume a local manifest, call the existing dry-run planner for each manifest entry, and emit deterministic planning/audit metadata.

## Batch Inputs

The batch wrapper may read only local, schema-backed artifacts:

- `EvaluatorPlanManifest`
- `EvaluatorPlanManifestEntry`
- `EvaluatorFixture`
- `EventRegistryExample`
- referenced `Hypothesis`, `SignalContract`, and `EvaluationContract` files used by the existing dry-run planner

The primary CLI input is:

| input | meaning |
|---|---|
| `--manifest` | Local evaluator-plan manifest YAML path |

Each manifest entry supplies:

| field | meaning |
|---|---|
| `entry_id` | Stable local manifest entry id |
| `fixture_path` | Manifest-relative evaluator fixture path |
| `event_registry_dir` | Manifest-relative event-registry directory |
| `expected_status` | Expected dry-run status: `ready_for_local_evaluation` or `rejected` |
| `description` | Human-readable local audit description |

## Allowed Batch Detail Output

Detailed batch dry-run output may include only local planning/audit metadata:

| field | meaning |
|---|---|
| `manifest_id` | Source manifest id |
| `entry_id` | Source manifest entry id |
| `expected_status` | Expected local planner status |
| `observed_status` | Observed local planner status |
| `matched_expected_status` | Whether expected and observed status match |
| `fixture_path` | Referenced fixture path from the manifest |
| `event_registry_dir` | Referenced event-registry directory from the manifest |
| `planner_payload` | Existing ready or rejected dry-run planner payload |

The ready planner payload may include planned input columns, feature columns, label column, holding windows, benchmark, declared cost assumptions, leakage checks, and placebo tests. These are plan metadata only.

The rejected planner payload may include rejection reasons and referenced fixture paths. Rejection reasons are for audit/debugging and are not model performance.

## Allowed Summary Output

Summary output may include only:

- `manifest_id`
- total entry count
- ready count
- rejected count
- mismatch count
- expected-status mismatched entry ids

In JSON field form, this is:

| field | meaning |
|---|---|
| `manifest_id` | Source manifest id |
| `total_entries` | Total manifest entries evaluated by the dry-run wrapper |
| `ready_count` | Number of entries with observed status `ready_for_local_evaluation` |
| `rejected_count` | Number of entries with observed status `rejected` |
| `expected_status_mismatch_count` | Number of entries whose observed status differs from manifest expectation |
| `expected_status_mismatches` | Entry ids whose expected status did not match observed status |

The prose terms for the same summary are: ready count, rejected count, mismatch count, rejection reasons, and referenced fixture paths. Rejection reasons and referenced fixture paths belong to detail output, not summary output.

## Forbidden Output

Q1 batch dry-run wrappers must not output:

- realized return
- realized returns
- alpha performance
- orders
- order lists
- trading instructions
- trading recommendations
- broker output
- PortfolioOS workflow output
- Q2 exports
- hidden Q2 results
- live performance

They must also not produce `alpha_score.csv` or any direct Q2 input artifact.

## Forbidden Behavior

Q1 batch dry-run wrappers must not:

- make live FMP/SEC calls
- call WRDS, Tushare, broker, or paid external services
- run LLM agent loops
- generate hypotheses
- implement signal formulas
- run real evaluator code
- run PortfolioOS workflows
- run Q2 execution-aware evaluation
- place orders or simulate broker routing
- mutate trading code
- write paid data payloads

## Difference From Real Evaluation

The batch wrapper answers whether declared local artifacts are structurally ready for later evaluation planning. It does not answer whether an alpha is true, profitable, stable, tradable, or robust.

A real evaluator would need separate, leakage-safe data access, point-in-time feature construction, labels, placebo tests, cost checks, stability checks, and explicit promotion criteria. Those are outside this batch dry-run contract.

## Q2 Boundary

Q2 may later consume a plain promoted artifact through a separate promotion contract. Q1 batch dry-run output must not call Q2, import Q2 workflows, export Q2 inputs, or treat Q2 execution checks as part of Q1 validation.

The current Q1 batch wrapper remains an audit-only planner boundary.
