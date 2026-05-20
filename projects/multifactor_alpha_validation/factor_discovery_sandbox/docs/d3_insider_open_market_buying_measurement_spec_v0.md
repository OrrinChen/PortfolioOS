# D3 Insider Open-Market Buying MeasurementSpec v0

Status: frozen after real D2 aggregate observability.

This note documents why `open_market_insider_buying_post_2023_v0` is now allowed
to have a D3 MeasurementSpec. It is not Q1 evidence, Q2 entry, portfolio
construction, paper workflow, live workflow, broker/order workflow, or production
approval.

## Source D2 Replay

The D2 real replay used local SEC Form 4 archive batches only. No network fetch
was used. The replay was aggregated at:

```text
outputs/factor_discovery/insider_disclosure/d2_real_archive_batched_aggregate/
```

Aggregate readout:

```text
event_count: 84442
event_month_count: 51
overall_decision: observable
allow_d3_charter_for: open_market_insider_buying_post_2023
open_market_buy_event_count: 1798
open_market_buy_covered_count: 1458
open_market_buy_coverage_share: 0.810901
open_market_buy_covered_event_month_count: 39
open_market_buy_covered_cluster_count: 820
```

The first archive batch alone was blocked by coverage, so D3 is based on the
batched aggregate decision, not on a cherry-picked passing segment.

## Frozen Measurement Scope

The only frozen primary measurement is:

```text
open_market_insider_buying_post_2023_v0
```

It measures Form 4 code `P` common-stock purchases after SEC filing acceptance.
Form 4 transaction date is never the return anchor. The return anchor is the
next regular market open after the EDGAR accepted timestamp.

Out of scope for this MeasurementSpec:

```text
discretionary sell alpha
planned sell alpha
sell-side composite
10b5-1 plan-event formula
Q1 handoff
Q2 handoff
optimizer or portfolio path
```

## No-View Rule

Missing market join, missing price/volume controls, unsupported transaction
codes, unknown post-2023 10b5-1 sell flags, and non-event issuers remain
explicit no-view / abstain states. They are not zero alpha.

## Hard Falsifiers

The MeasurementSpec inherits these D2 falsifiers:

- shifted filing dates beat live filing dates
- same-coverage random events beat live
- randomized role labels beat live
- compensation controls beat code `P` purchases
- pre-filing drift dominates post-filing window
- market, sector, or liquidity controls explain the full read

If any hard falsifier fires in the next stage, the path stops before Q1.
