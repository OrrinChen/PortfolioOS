# Promotion Registry Review Surface Design

**Date:** 2026-04-15  
**Workspace:** `C:\Users\14574\Quant\PortfolioOS`  
**Status:** Approved-for-implementation

## Goal

Turn the existing research promotion contract from a library-only validator into a reviewer-facing platform surface.

The first version should let a reviewer point PortfolioOS at one or more promotion bundles and get a compact registry view that answers:

- which bundles were found
- which research line produced each bundle
- what candidate status each bundle is in
- which audited signals are included
- whether the combo is eligible for Stage 4
- why a bundle is blocked
- where the canonical memory and ledger artifacts live

## Why This Is The Right Next Step

PortfolioOS already has:

- research closeout notes
- a promotion contract document
- a validator for `promotion_bundle.json`
- paper-calibration infrastructure

What it does not yet have is a clean reviewer surface that makes those assets legible from the platform layer.

This feature therefore improves the platform's governance and review story without reopening research discovery or pretending Phase 1 alpha is already promotable.

## Roadmap Position

This work belongs to the platform-governance side of the roadmap, not to fresh alpha discovery.

It strengthens the boundary between:

- branch-local research workspaces
- shared platform review and operating surfaces

It also supports the current project-wide operating mode:

- research remains frozen by default
- the platform should consume stable evidence bundles rather than branch-local code

## Non-Goals

This first version does **not**:

- promote any candidate automatically
- merge research code into PortfolioOS
- reopen US or A-share research
- redesign optimizer or alpha mainline paths
- introduce a model registry with champion-challenger semantics
- build a web UI or dashboard

## User-Facing Shape

The reviewer-facing entrypoint is a CLI command that scans one root directory for promotion bundles.

### Inputs

- `input_root`: directory to scan recursively for `promotion_bundle.json`
- `output_dir`: directory for generated registry artifacts

### Outputs

- `promotion_registry.csv`
- `promotion_registry_manifest.json`
- `promotion_registry_summary.md`

## Registry Semantics

Each discovered bundle is validated through the existing promotion-contract loader.

For each valid bundle, the registry should expose:

- bundle id
- created at
- research line
- candidate status
- thesis summary
- universe name
- signal names
- signal stage buckets
- combo Stage 4 eligibility
- combo blocking reason
- combo full-sample IR
- combo second-half IR
- memory path
- ledger path
- bundle directory

The summary markdown should include:

- generation timestamp
- scanned root
- bundle count
- research-line counts
- candidate-status counts
- a compact table of all validated bundles

## Failure Rules

If no promotion bundles are found under the scan root, the command should fail loudly instead of generating an empty but misleading registry.

If a bundle is found but invalid, the command should also fail loudly and surface the validation problem. Silent skipping is not acceptable in v1 because the surface is supposed to be review-safe.

## Architecture

Keep the implementation thin.

### Existing Components To Reuse

- `src/portfolio_os/alpha/promotion_contract.py`
- `src/portfolio_os/api/cli.py`
- existing artifact-writing helpers in `portfolio_os.storage` / `portfolio_os.storage.snapshots`

### New Components

- a workflow module that scans for bundles, validates them, and builds registry outputs
- a small CLI command that invokes the workflow
- tests for recursive discovery, output generation, and CLI behavior

## Success Criteria

This feature is successful if:

- a reviewer can run one command against a bundle root
- PortfolioOS validates all discovered bundles
- the resulting markdown and CSV are enough to review candidate state without opening branch-local code first
- the output is deterministic and test-covered

## Follow-Up, But Not In This Slice

Natural next steps after v1:

- support direct `--bundle-dir` inputs in addition to recursive roots
- add a review-status overlay or manual decision sheet
- add a portfolio-facing registry of accepted vs blocked candidates
- connect the registry to future model-governance / champion-challenger work
