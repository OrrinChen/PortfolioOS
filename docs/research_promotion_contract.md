# Research Promotion Contract

PortfolioOS does not merge branch-local research code into the shared platform by default.

The shared platform and the research worktrees now meet through a narrow promotion contract instead of through repo-level code movement. The purpose of this contract is to separate two concerns that are currently at different maturity levels. Research workspaces need freedom to iterate on hypotheses, evaluators, and closeout discipline. PortfolioOS needs stable interfaces, predictable artifacts, and low-risk platform evolution.

This means the current project stage is not repository consolidation. It is research convergence and promotion-contract hardening.

## What The Contract Is For

The contract is the minimal evidence-bearing bundle that a research line exports when it believes a signal family, combo, or candidate package is mature enough to be reviewed by the platform layer.

The contract is intentionally narrower than a full strategy integration. It is a review interface, not an automatic promotion switch.

The platform should be able to answer these questions from the bundle alone:

1. What research line produced this candidate?
2. What is the candidate status?
3. Which audited signals are included?
4. What Stage 3 or later combo evidence exists?
5. Why is the candidate blocked from or admitted to Stage 4?
6. Where are the canonical branch-local memory and ledger artifacts?

## Current Rule

Research code stays in its own workspace.

PortfolioOS only absorbs:

- the promotion-contract schema and validator,
- stable adapters that consume validated bundle outputs,
- production-ready artifacts after a candidate actually clears the Stage 4 gate.

Do not merge whole research repos or branch-local audit runners into PortfolioOS while the underlying tranche is still exploratory or only partially positive.

## Manifest Shape

Each promotion bundle is a directory containing `promotion_bundle.json` plus copied artifacts.

The current contract version is `1.0`.

Required top-level fields:

- `contract_type`
- `contract_version`
- `bundle_id`
- `created_at`
- `research_line`
- `candidate_status`
- `thesis`
- `signals`
- `combo`
- `artifacts`

Required `thesis` fields:

- `summary`
- `universe_name`

Required `signals[*]` fields:

- `name`
- `stage_bucket`
- `audit_summary_path`

Required `combo` fields:

- `summary_path`
- `eligible_for_stage4`
- `blocking_reason`
- `full_sample_ir`
- `second_half_ir`

Required `artifacts` fields:

- `memory_path`
- `ledger_path`

All artifact paths must be relative to the bundle root so that the bundle remains portable.

## First Example

The first concrete bundle for this contract is the current A-share Stage 3 candidate exported from the A-share worktree. It is an evidence package for review, not a Stage 4 promotion.

Practical read:

- `anti_mom_21_5` is `partially_real`
- `H18 institutional crowding` is `partially_real`
- the pair is close to orthogonal
- the combo has mild full-sample uplift
- the combo does not improve second-half IR enough to justify Stage 4

That is exactly the kind of borderline-but-real state the contract is meant to preserve without forcing premature platform integration.

## What Comes Next

This stage is complete when the contract becomes a stable review interface.

The next upgrades should be small:

1. export more than one bundle through the same schema,
2. validate them from PortfolioOS without branch-specific code,
3. only then decide whether a reusable adapter layer belongs in the platform.

Repository merge is a later question. Repeated successful promotion through the same interface is the precondition for that question, not the consequence.
