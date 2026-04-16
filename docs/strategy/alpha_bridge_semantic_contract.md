# Alpha Bridge Semantic Contract

Status: frozen semantic contract  
Decision date: 2026-04-15

## Purpose

This document fixes the meaning of alpha-bridge outputs before any further optimizer or backtest promotion work.

The goal is not to describe every possible implementation. The goal is to make the downstream semantic promise explicit:

- what an active alpha snapshot means
- what an individual ticker `expected_return = 0` means
- what a negative trailing-spread guard means
- what downstream consumers must do with each state

## Selected Protocol

The selected protocol for **negative trailing top-bottom spread** is:

- `explicit_abstain`

Meaning:

- when the trailing bridge-level spread estimate is negative, the alpha bridge does **not** emit an active alpha view for that rebalance date
- the bridge does **not** reinterpret the signal as a reversal package
- the bridge does **not** use a zero-valued cross-section as the semantic representation of the guard state

## Why This Protocol Was Chosen

Offline guard-event comparison on the current expanded-US Phase 1.5 sample produced three facts:

1. `signed_spread` was materially different
   - it turned the guard into an active reversal
   - it increased mean guard-event turnover from about `1.15%` to about `2.17%`
   - it added about `$21.5k` mean gross traded notional versus the abstain baseline

2. `floor_to_zero` and `explicit_abstain` were behaviorally identical under the current objective
   - same turnover
   - same hold-through
   - same post-trade cash behavior
   - the difference was semantic, not behavioral

3. Because `floor_to_zero` and `explicit_abstain` tie behaviorally today, the cleaner semantic contract should win
   - `explicit_abstain` says "no alpha view"
   - `floor_to_zero` says "active view with zero expected return"
   - the first statement is the more faithful interpretation of the guard

## Contract Meanings

### 1. Active Alpha Snapshot

An active alpha snapshot means:

- the bridge has enough history to form a signal-strength estimate
- the trailing bridge-level spread estimate is admissible under the chosen protocol
- downstream may consume `expected_return` as an active alpha view

In this state:

- positive per-ticker `expected_return` means relative overweight preference
- negative per-ticker `expected_return` means relative underweight preference inside the active cross-section
- per-ticker `expected_return = 0` means neutral within an active alpha snapshot, not global abstention

### 2. Explicit Abstain

An explicit abstain state means:

- the bridge has chosen to emit **no alpha view** for this rebalance date
- downstream should fall back to the no-alpha prior/default path

Operationally, downstream should treat this as:

- no alpha snapshot injected into the optimizer universe for that date
- no claim that the expected return of every name is literally zero
- no reversal inference

### 3. Signed Reversal Is Not The Default Guard

The selected contract does **not** treat negative trailing spread as a reversal package.

That interpretation remains a separate research alternative, not the default bridge semantics.

## Downstream Obligations

### Optimizer

When the bridge abstains:

- consume no alpha term for that rebalance
- treat the date as ordinary portfolio construction without an active alpha overlay

When the bridge is active:

- consume the provided cross-sectional `expected_return` vector as the alpha term

### Backtest And Reporting

Backtest/reporting layers should distinguish:

- active alpha snapshot
- explicit abstain
- insufficient history / not ready

These states should not be collapsed into a single generic "zero alpha" label.

### Future Registry / Promotion Logic

Promotion or audit tooling should interpret:

- `explicit_abstain` as missing alpha coverage for that date
- not as evidence that the bridge actively predicted zero return

## Current Implementation Status

As of this contract:

- the runtime implementation still uses `floor_to_zero`
- on the current objective stack, that is behaviorally equivalent to `explicit_abstain` for guard dates
- therefore no immediate production change is required to preserve current optimizer behavior

But the semantic contract is now:

- negative trailing spread means `explicit_abstain`

If a future downstream consumer starts to distinguish zero-valued alpha vectors from missing alpha coverage, implementation should be aligned to this contract explicitly rather than relying on the current accidental behavioral equivalence.

## Contract-Runtime Gap

There is currently an **explicit semantic gap** between the selected contract and the runtime representation:

- contract meaning:
  - negative trailing spread = `explicit_abstain`
- current runtime representation:
  - negative trailing spread still flows through `floor_to_zero`

This is acceptable **only because** the current objective stack treats the two representations as behaviorally equivalent on guard dates.

This equivalence is a property of the current downstream stack, not a permanent truth about the alpha bridge.

### What This Means Today

- do not treat the current runtime `floor_to_zero` path as proof that the semantic contract is "active zero prediction"
- do not infer a distinct active-zero protocol from today's guard-date zero vectors
- treat the current runtime behavior as an implementation detail that happens to match the abstain contract under the present objective

### Migration Triggers

Runtime should be aligned explicitly to the `explicit_abstain` contract if any of the following become true:

1. a downstream module needs to distinguish missing alpha coverage from zero-valued active predictions
   - examples:
     - prior-based fallback logic
     - confidence-aware objective scaling
     - signal-health or coverage metrics

2. a second signal is combined into the same bridge/downstream alpha layer
   - reason:
     - multi-signal composition needs to distinguish "this signal abstained" from "this signal actively predicts zero"

3. promotion, registry, or audit tooling starts to report signal activity / inactivity as a formal metric
   - reason:
     - activity accounting becomes ambiguous if guard-date zero vectors are left semantically overloaded

Until one of those triggers fires, the gap is intentional technical debt with an explicit record, not a current bug.
