# FD-S0 Small-Cap Data Admission

not alpha evidence
allocator entry: blocked
Q1 entry: blocked
Q2 entry: blocked
Alpha Registry update: blocked
production approval: not claimed

- small-cap research admitted: true
- candidate family run allowed: true
- delisting handling status: pass
- liquidity cost data status: pass

## Checks
- manifest_schema: pass - research_mode_dataset_manifest.v1 is required
- historical_pit_universe: pass - historical PIT membership is required
- pit_market_cap: pass - PIT market cap or computable close * PIT shares is required
- shares_outstanding_or_float: pass - shares outstanding or float is required
- adjusted_and_raw_prices: pass - adjusted and raw prices are required
- volume: pass - volume is required
- corporate_action_handling: pass - adjustment convention or corporate action handling is required
- delisting_return_or_event_handling: pass - explicit delisting returns or event handling is required
- exchange_share_class_filters: pass - exchange and share-class filters are required
- sector_or_industry: pass - sector or industry classification is required
- benchmark_returns: pass - small-cap or market benchmark returns are required
- adv: pass - ADV must be computable from price and volume
- spread_or_spread_proxy: pass - spread or spread proxy is preferred for cost diagnostics
- timestamp_policy: pass - same-close trading must be disabled
