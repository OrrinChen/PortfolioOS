# Testing Policy (Layered L1/L2/L3)

This document defines the expected test strategy by change layer.
It complements the TDD + SDD workflow in `docs/standards/engineering_workflow.md`.

## L1: Core Logic

Scope examples:

- optimizer objective and constraints
- order quantization and repair
- risk and cost metric computations

Required tests:

- deterministic unit tests for new behavior
- at least one failure-path test
- at least one boundary-path test
- regression test when fixing a known bug

Rules:

- apply strict Red -> Green -> Refactor per change slice
- avoid mixing broad refactors with behavior changes

## L2: Orchestration / Script Chain

Scope examples:

- CLI wrappers
- cross-process script orchestration
- replay/pilot execution control

Required tests:

- parameter passthrough tests for every new propagated flag
- timeout and retry behavior tests
- partial-success and resume semantics tests when applicable
- command-construction tests for subprocess invocations

Rules:

- standardize subprocess Python execution via `sys.executable`
- standardize path composition via `pathlib.Path`
- encode error semantics in assertions (message + exit behavior)

## L3: External Integrations

Scope examples:

- data providers and market-data connectors
- broker adapters and third-party APIs

Required tests:

- default offline unit-test path with mocks/stubs
- focused integration smoke tests only for critical signals
- deterministic fallback tests for permission/availability degradation

Rules:

- CI correctness must not depend on unstable third-party availability
- external failures must degrade predictably with clear status artifacts

## Minimum Case Matrix (All Layers)

Every non-trivial change must include:

1. happy path
2. failure path
3. boundary path

If a change crosses script boundaries, also include passthrough + timeout/retry/resume semantics.
