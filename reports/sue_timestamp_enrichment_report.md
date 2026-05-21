# SUE Timestamp Source Enrichment Report

This is timestamp-source enrichment only.
It does not prove SUE alpha.
It does not select a SUE score.
It does not run Q2 or optimizer-path evaluation.
-5/-10 shifted windows are not tradable unless public availability is proven.
No paper/live/broker/order/production workflow is approved.

## Decision

- schema_version: `sue_timestamp_enrichment.v1`
- decision_label: `timestamp_enrichment_no_repair_sue_blocked`
- event_count: `17027`
- repairable_event_count: `0`
- selected_score: `None`
- q2_evaluation_ran: `False`
- optimizer_path_evaluation_ran: `False`
- production_approval_claimed: `False`

## Source Coverage

- ibes_anndats_act_count: `17027`
- compustat_rdq_count: `12987`
- exact_release_timestamp_count: `0`
- sec_filing_timestamp_count: `0`
- date_only_no_repair_count: `12990`
- no_auditable_source_count: `4037`

## Boundaries

- Repair candidates are written for later review only; H1E is not rerun here.
- Date-only source fields are audit evidence but not exact tradable timestamps.
- SEC filing timestamps are cross-checks unless a source proves first public release.
