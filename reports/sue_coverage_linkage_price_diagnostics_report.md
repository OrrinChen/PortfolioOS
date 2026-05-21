# SUE Coverage / Linkage / Price Diagnostics

This report diagnoses coverage, linkage, and price-window loss in the expanded WRDS/PIT SUE panel.
This does not run Q2 or optimizer-path evaluation.
This does not prove SUE alpha success by itself.
This does not approve paper trading, live trading, broker workflows, orders, or production deployment.
Missing SUE or price coverage remains explicit diagnostic/no_view and is not encoded as zero alpha.

## Summary

- event_count: `17027`
- final_pit_safe_rows: `15365`
- unlinked_ibes_crsp_rows: `1661`
- missing_price_rows: `1`
- missing_return_windows: `1662`
- diagnostic_only_rows: `1662`
- crsp_cache_rows: `2325833`
- crsp_cache_start_date: `2020-01-02`
- crsp_cache_end_date: `2022-03-25`
- recommended_next_action: `rescue_linkage_and_price_coverage_before_q2`

## Price Gap Classification

- return_window_after_crsp_cache_end: `1`

## Boundaries

- q2_evaluation_ran: `False`
- optimizer_path_evaluation_ran: `False`
- alpha_registry_promoted: `False`
- production_approval_claimed: `False`
- missing_coverage_encoded_as_zero_alpha: `False`
