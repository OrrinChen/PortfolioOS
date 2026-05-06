# SUE Historical Event Panel Report

This is a WRDS PIT-safe or PIT-labeled SUE event panel builder.
It does not prove SUE alpha success by itself.
It does not approve paper trading, live trading, broker workflows, orders, or production deployment.
Downstream typed event evidence and Q2 optimizer-path evaluation require separate explicit reopen phases.

## Build Summary

- mode: `smoke`
- event_count: `60`
- rebalance_date_count: `60`
- linked_rows: `57`
- unlinked_rows: `3`
- missing_estimates: `3`
- missing_actuals: `3`
- missing_prices: `2`
- diagnostic_only_rows: `11`

## PIT Rules

- event_available_timestamp must be <= tradable_timestamp.
- estimate_snapshot_date must be <= event_available_timestamp.
- return windows start after tradable_timestamp.
- missing expected EPS remains diagnostic no_view/abstain; it is not encoded as zero SUE.
- FMP frozen estimate history is not accepted as PIT-safe substitute without visibility snapshots.

## Data Lineage

- primary source family: WRDS / IBES / CRSP
- source tables: `ibes.actu_epsus, ibes.statsum_epsus, ibes.idsum, crsp.dsf`
- query_timestamp: `2026-05-06T00:00:00Z`
