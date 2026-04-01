# TASK_MEMORY

This file is a handoff note for continuing or reviewing the PortfolioOS MVP implementation.

## Current Status

Implementation is complete.

Phase 2 benchmark comparison support is also complete.
Phase 3 batch replay support is now complete.
Phase 4 structured compliance and OMS-style export support is now complete.
Phase 5 data-quality, explainability, and evidence-pack support is now complete.
Phase 6 scenario analysis and decision-pack support is now complete.
Phase 7 approval, freeze, and handoff support is now complete.
Phase 8 execution simulation support is now complete.
Phase 9 pilot integration and calibration support is now complete.
Phase 10 data feed preparation support is now complete.
Phase 11 Tushare provider and snapshot bundle support is now complete.
Phase 11.5 permission-aware real-feed UX support is now complete.
Phase 12 paper-trading fill collection, reference-price-aware slippage calibration, and impact-aware multi-period execution support is now complete.

Recent 2026-03-30 state:

- runtime default execution simulation mode is now explicitly `impact_aware`
- `participation_twap` remains intentionally preserved in example request YAMLs as the baseline comparison sample mode
- the execution-mode decision note is captured in `docs/execution_mode_decision_note.md`
- Phase 13 planning has started for multi-period backtest plus PnL attribution
- current Phase 13 architectural conclusion:
  - run the backtest loop via library calls, not CLI subprocesses
  - the optimizer core already exposes a public Python entrypoint: `run_rebalance(universe, config, *, input_findings=None)`
  - a public single-run orchestration API now exists in `src/portfolio_os/workflow/single_run.py`
  - the backtest path should reuse `run_single_rebalance(...)` or its lower-level context builder rather than importing CLI helpers
  - CLI single-run orchestration now routes through the shared workflow service instead of owning that logic directly
- the first Phase 13 extraction step is now complete:
  - new public workflow module: `src/portfolio_os/workflow/single_run.py`
  - public entrypoints:
    - `load_single_run_context(...)`
    - `run_single_rebalance(...)`
  - CLI main now delegates single-run orchestration to `run_single_rebalance(...)`
  - existing data-quality findings, import-profile mapping, benchmark generation, summary generation, audit payload construction, and risk-model loading behavior remain preserved
- new workflow regression coverage now exists in `tests/test_single_run_workflow.py`:
  - data-quality findings survive through the public workflow service
  - import-profile mapping works through the public workflow service
  - CLI main delegates to the new workflow service
- expanded US offline universe preparation is now in place:
  - `scripts/generate_risk_inputs.py` supports `--universe-file`
  - a frozen 50-ticker US expanded universe now lives under `data/universe/`
  - expanded US risk inputs now live under `data/risk_inputs_us_expanded/`
  - new US samples `sample_us_04`, `sample_us_05`, and `sample_us_06` now live under `data/samples/us/`
- expanded US Step A trade-coverage perturbation is now applied:
  - `sample_us_04` holdings were offset on `AAPL`, `NVDA`, `AMZN`, `TSLA`, and `BAC`
  - `sample_us_06` holdings were offset on `SOFI`, `PLTR`, `SNAP`, `RIVN`, and `AES`
  - matching replay copies under `data/replay_samples/sample_us_04` and `data/replay_samples/sample_us_06` were updated in lockstep
- expanded US replay was rerun after the Step A holdings offsets:
  - `outputs/replay_us_expanded`
  - `sample_us_04` replay portfolio_os basket now shows 3 orders
  - `sample_us_05` replay portfolio_os basket shows 3 orders
  - `sample_us_06` replay portfolio_os basket now shows 5 orders
- expanded US single-run plus execution coverage is now complete for all three new samples:
  - `outputs/single_us_04` -> solver `optimal`, 3 OMS orders, `gross_traded_notional=48772.210004`
  - `outputs/single_us_05` -> solver `optimal`, 3 OMS orders, `gross_traded_notional=19753.240178`
  - `outputs/single_us_06` -> solver `optimal`, 5 OMS orders, `gross_traded_notional=28098.119906`
  - `outputs/execution_us_04` -> `fill_rate=1.0`, `total_filled_notional=48772.45637377115`
  - `outputs/execution_us_05` -> `fill_rate=1.0`, `total_filled_notional=19753.2493200748`
  - `outputs/execution_us_06` -> `fill_rate=1.0`, `total_filled_notional=28098.00812631607`

Verified on this machine:

- `C:\\Users\\14574\\AppData\\Local\\Programs\\Python\\Python311\\python.exe -m poetry install`
- `C:\\Users\\14574\\AppData\\Local\\Programs\\Python\\Python311\\python.exe -m poetry run pytest`
- `C:\\Users\\14574\\AppData\\Local\\Programs\\Python\\Python311\\python.exe -m poetry run portfolio-os --holdings data/sample/holdings_example.csv --target data/sample/target_example.csv --market data/sample/market_example.csv --reference data/sample/reference_example.csv --portfolio-state data/sample/portfolio_state_example.yaml --constraints config/constraints/public_fund.yaml --config config/default.yaml --execution-profile config/execution/conservative.yaml --output-dir outputs/demo_run`
- `C:\\Users\\14574\\AppData\\Local\\Programs\\Python\\Python311\\python.exe -m poetry run portfolio-os-replay --manifest data/replay_samples/manifest.yaml --constraints config/constraints/public_fund.yaml --config config/default.yaml --execution-profile config/execution/conservative.yaml --output-dir outputs/replay_demo`
- `C:\\Users\\14574\\AppData\\Local\\Programs\\Python\\Python311\\python.exe -m poetry run portfolio-os-scenarios --manifest data/scenario_samples/manifest.yaml --output-dir outputs/scenario_demo`
- `C:\\Users\\14574\\AppData\\Local\\Programs\\Python\\Python311\\python.exe -m poetry run portfolio-os-approve --request data/approval_samples/approval_request_example.yaml --output-dir outputs/approval_demo`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\python.exe -m pip install -e .`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\pytest.exe -q`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os.exe --holdings data/sample/holdings_example.csv --target data/sample/target_example.csv --market data/sample/market_example.csv --reference data/sample/reference_example.csv --portfolio-state data/sample/portfolio_state_example.yaml --constraints config/constraints/public_fund.yaml --config config/default.yaml --execution-profile config/execution/conservative.yaml --output-dir outputs/demo_run`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os-replay.exe --manifest data/replay_samples/manifest.yaml --constraints config/constraints/public_fund.yaml --config config/default.yaml --execution-profile config/execution/conservative.yaml --output-dir outputs/replay_demo`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os-scenarios.exe --manifest data/scenario_samples/manifest.yaml --output-dir outputs/scenario_demo`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os-approve.exe --request data/approval_samples/approval_request_example.yaml --output-dir outputs/approval_demo`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os-execute.exe --request data/execution_samples/execution_request_example.yaml --output-dir outputs/execution_demo`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os.exe --holdings data/import_profile_samples/custodian_style_a/holdings.csv --target data/import_profile_samples/custodian_style_a/target.csv --market data/import_profile_samples/custodian_style_a/market.csv --reference data/import_profile_samples/custodian_style_a/reference.csv --portfolio-state data/sample/portfolio_state_example.yaml --constraints config/constraints/public_fund.yaml --config config/default.yaml --execution-profile config/execution/conservative.yaml --import-profile config/import_profiles/custodian_style_a.yaml --output-dir outputs/demo_run_mapped`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os-execute.exe --request data/execution_samples/execution_request_calibrated.yaml --calibration-profile config/calibration_profiles/balanced_day.yaml --output-dir outputs/execution_demo`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os-build-market.exe --tickers-file data/sample/tickers.txt --as-of-date 2026-03-23 --provider mock --output data/generated/market.csv`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os-build-reference.exe --tickers-file data/sample/tickers.txt --as-of-date 2026-03-23 --provider mock --overlay data/sample/reference_overlay_example.csv --output data/generated/reference.csv`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os-build-target.exe --index-code 000300.SH --as-of-date 2026-03-23 --provider mock --output data/generated/target.csv`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os.exe --holdings data/sample/holdings_example.csv --target data/generated/target.csv --market data/generated/market.csv --reference data/generated/reference.csv --portfolio-state data/sample/portfolio_state_example.yaml --constraints config/constraints/public_fund.yaml --config config/default.yaml --execution-profile config/execution/conservative.yaml --output-dir outputs/demo_run_built --skip-benchmarks`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os-build-snapshot.exe --tickers-file data/sample/tickers.txt --index-code 000300.SH --as-of-date 2026-03-23 --provider mock --reference-overlay data/sample/reference_overlay_example.csv --output-dir data/generated/snapshot_mock`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os.exe --holdings data/sample/holdings_example.csv --target data/generated/snapshot_mock/target.csv --market data/generated/snapshot_mock/market.csv --reference data/generated/snapshot_mock/reference.csv --portfolio-state data/sample/portfolio_state_example.yaml --constraints config/constraints/public_fund.yaml --config config/default.yaml --execution-profile config/execution/conservative.yaml --output-dir outputs/demo_run_snapshot --skip-benchmarks`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os-build-market.exe --tickers-file data/sample/tickers.txt --as-of-date 2026-03-23 --provider tushare --output data/generated/market_real.csv`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os-build-market.exe --tickers-file data/sample/tickers.txt --as-of-date 2026-03-23 --provider tushare --provider-token DUMMY_TOKEN_FOR_CONNECTIVITY_TEST --output data/generated/market_real_dummy.csv`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os-build-market.exe --tickers-file data/sample/tickers.txt --as-of-date 2026-03-23 --provider tushare --provider-token <real token via user env relay> --output data/generated/market_real.csv`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os-build-reference.exe --tickers-file data/sample/tickers.txt --as-of-date 2026-03-23 --provider tushare --provider-token <real token via user env relay> --overlay data/sample/reference_overlay_example.csv --output data/generated/reference_real.csv`
- `C:\\Users\\14574\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\portfolioos-Su-HS-3U-py3.11\\Scripts\\portfolio-os.exe --holdings data/sample/holdings_example.csv --target data/sample/target_example.csv --market data/generated/market_real.csv --reference data/generated/reference_real.csv --portfolio-state data/sample/portfolio_state_example.yaml --constraints config/constraints/public_fund.yaml --config config/default.yaml --execution-profile config/execution/conservative.yaml --output-dir outputs/demo_run_real_feed --skip-benchmarks`
- `python -m pytest -q`
- `python scripts/generate_risk_inputs.py --market us --universe-file data/universe/us_equity_expanded_tickers.txt --lookback-days 252 --end-date 2026-03-27 --output-dir data/risk_inputs_us_expanded --cool-down 0.0`
- `C:\Users\14574\AppData\Local\pypoetry\Cache\virtualenvs\portfolioos-Su-HS-3U-py3.11\Scripts\portfolio-os-replay.exe --manifest data/replay_samples/manifest_us_expanded.yaml --constraints config/constraints/us_public_fund.yaml --config config/us_expanded.yaml --execution-profile config/execution/conservative.yaml --output-dir outputs/replay_us_expanded`
- `C:\Users\14574\AppData\Local\pypoetry\Cache\virtualenvs\portfolioos-Su-HS-3U-py3.11\Scripts\portfolio-os-execute.exe --request data/execution_samples/execution_request_us_04.yaml --output-dir outputs/execution_us_04`
- `C:\Users\14574\AppData\Local\pypoetry\Cache\virtualenvs\portfolioos-Su-HS-3U-py3.11\Scripts\portfolio-os-execute.exe --request data/execution_samples/execution_request_us_06.yaml --output-dir outputs/execution_us_06`
- `C:\Users\14574\AppData\Local\pypoetry\Cache\virtualenvs\portfolioos-Su-HS-3U-py3.11\Scripts\pytest.exe tests\test_single_run_workflow.py -q`

Latest known result:

- all 252 pytest tests passed, with 2 skipped
- sample CLI run completed successfully
- mapped-input sample CLI run completed successfully
- replay CLI run completed successfully
- scenario CLI run completed successfully
- approval CLI run completed successfully
- execution simulation CLI run completed successfully
- market builder CLI run completed successfully
- reference builder CLI run completed successfully
- target builder CLI run completed successfully
- snapshot builder CLI run completed successfully
- builder-generated inputs were consumed successfully by `portfolio-os`
- snapshot-bundle outputs were consumed successfully by `portfolio-os`
- tushare builder path reached clear token validation and live API auth error paths
- real tushare market builder completed successfully
- real tushare reference builder completed successfully via fallback away from restricted endpoints
- real market + real reference files were consumed successfully by `portfolio-os`
- full real snapshot bundle was still blocked by `index_weight` permission limits on the current Tushare account
- manifests now distinguish `success`, `success_with_degradation`, `failed_permission`, `failed_data`, and `failed_runtime`
- snapshot manifests now preserve partial-success state and recommended continuation paths
- client-provided `target.csv` is now documented as the formal real-feed pilot path when `index_weight` is unavailable
- artifacts were written under `outputs/demo_run`
- scenario artifacts were written under `outputs/scenario_demo`
- approval/freeze artifacts were written under `outputs/approval_demo`
- execution simulation artifacts were written under `outputs/execution_demo`
- mapped-input artifacts were written under `outputs/demo_run_mapped`
- builder-generated input files were written under `data/generated`
- builder-consumer run artifacts were written under `outputs/demo_run_built`
- snapshot bundle outputs were written under `data/generated/snapshot_mock`
- snapshot-bundle consumer run artifacts were written under `outputs/demo_run_snapshot`
- real tushare market output was written under `data/generated/market_real.csv`
- real tushare reference output was written under `data/generated/reference_real.csv`
- real-feed consumer run artifacts were written under `outputs/demo_run_real_feed`
- single-run artifacts now include `orders_oms.csv`
- benchmark artifacts now include `benchmark_comparison.json` and `benchmark_comparison.md`
- replay artifacts now include `suite_results.json`, `suite_summary.md`, and `sample_results/...`
- single-run summary now includes blocked-reason and repair-reason summaries
- replay suite summary now includes best/worst highlights and finding-pattern summaries
- scenario outputs now include `scenario_comparison.json`, `scenario_comparison.md`, `decision_pack.md`, and per-scenario artifacts
- approval outputs now include `approval_record.json`, `approval_summary.md`, `freeze_manifest.json`, `handoff_checklist.md`, and frozen `final_*` files
- execution outputs now include `execution_report.json`, `execution_report.md`, `execution_fills.csv`, `execution_child_orders.csv`, and `handoff_checklist.md`
- mapped and calibrated runs now record resolved `import_profile` and `resolved_calibration` metadata in audit/report or manifest outputs
- data builders now produce standard `market.csv`, `reference.csv`, `target.csv`, plus sidecar manifests with provider, hash, row count, and approximation notes
- snapshot bundle now produces `snapshot_manifest.json` that links child manifests and output hashes
- tushare provider now supports permission-aware fallbacks:
  - `stk_limit` unavailable -> pre_close plus board-limit heuristic
  - `stock_basic` / `daily_basic` constrained -> `bak_basic` fallback for reference snapshot
- builder and snapshot manifests now include:
  - `provider_capability_status`
  - `fallback_notes`
  - `permission_notes`
  - `recommended_alternative_path`
  - `build_status`
- live US Alpaca paper-fill collection now reached 20 auditable real fills across:
  - minimal 1-share sell validation
  - repeated large-cap 1-share sell batches
  - higher-participation low-ADV buy and sell batches
- live fill artifacts were written under:
  - `outputs/alpaca_fill_collection_live`
  - `outputs/broker_state_inspection_live`
  - `outputs/pre_submission_checks_live`
  - `outputs/live_fill_analysis`
- the fill-collection pipeline now preserves a distinct `reference_price` field end-to-end rather than forcing calibration to reuse `estimated_price`
- Windows BOM-tolerant JSON loading is now required for calibration and broker-state artifacts
- reference-price-aware paper-trading calibration now supports:
  - synthetic offline calibration fixtures
  - explicit pre-submission checks
  - bootstrap-style confidence review outside the main JSON payload when needed
- latest paper-trading pilot-gate calibration result:
  - current default US `k` remains `0.015`
  - paper-trading candidate `k` reached approximately `1.445696451694286`
  - this candidate is acceptable only as a paper-trading overlay / pilot input
  - do not overwrite `config/us_default.yaml` with this value
- latest paper-trading pilot-gate calibration artifacts were written under:
  - `outputs/live_fill_analysis/slippage_calibration_pilot_gate/...`
  - `outputs/live_fill_analysis/slippage_calibration_live_reference_fixed/...`
- execution simulation now supports two local preflight modes:
  - `participation_twap`
  - `impact_aware`
- `impact_aware` mode now routes through the multi-period allocator and prefers cheaper liquidity buckets while preserving the existing report/output schema
- runtime execution-mode policy is now explicitly:
  - default simulation mode: `impact_aware`
  - keep `participation_twap` sample requests as the baseline comparison mode
  - decision note: `docs/execution_mode_decision_note.md`
- baseline sample request files now include header comments explaining why they still pin `participation_twap`:
  - `data/execution_samples/execution_request_example.yaml`
  - `data/execution_samples/execution_request_calibrated.yaml`
- expanded US universe is now frozen as a 50-ticker deterministic list grouped by sector with coarse liquidity buckets:
  - `data/universe/us_equity_expanded_tickers.txt`
  - `data/universe/us_universe_reference.csv`
  - `data/universe/us_universe_market_2026-03-27.csv`
- expanded US risk inputs now exist separately from the original 12-name baseline set:
  - `data/risk_inputs_us_expanded/returns_long.csv`
  - `data/risk_inputs_us_expanded/factor_exposure.csv`
  - `data/risk_inputs_us_expanded/risk_inputs_manifest.json`
- original 12-name US baseline remains preserved in `data/risk_inputs_us/`
- expanded US sample structure is now:
  - `sample_us_04`: sector-diversified core, 33 tickers
  - `sample_us_05`: concentrated technology bet, 6 tickers
  - `sample_us_06`: liquidity stress mix, 15 tickers
- the ticker union across `sample_us_04` / `sample_us_05` / `sample_us_06` now covers all 50 expanded-universe names
- a generation summary for the expanded-universe refresh was written to:
  - `outputs/us_expanded_universe_generation_summary.json`
- latest expanded-US replay sync result after Step A:
  - `outputs/replay_us_expanded/suite_results.json`
  - replay portfolio_os order counts are now `sample_us_04=3`, `sample_us_05=3`, `sample_us_06=5`
  - replay summary still shows positive cost savings versus naive across all 3 samples
- latest expanded-US end-to-end pipeline result:
  - replay: updated suite synced to the Step A holdings perturbations
  - single-run: `sample_us_04`, `sample_us_05`, and `sample_us_06` all report `solver_status=optimal`
  - execution: all three samples now have complete execution outputs with `fill_rate=1.0`
  - execution filled notionals:
    - `sample_us_04`: `48772.45637377115`
    - `sample_us_05`: `19753.2493200748`
    - `sample_us_06`: `28098.00812631607`
- latest targeted verification on this machine:
  - `py -3.11 -m pytest tests\\test_execution_simulation.py tests\\test_multi_period.py -q`
  - `py -3.11 -m pytest tests\\test_pilot_ops_script.py tests\\test_fill_collection.py tests\\test_slippage_calibration.py -q`
  - `py -3.11 -m pytest tests\\test_multi_period.py tests\\test_execution_simulation.py tests\\test_pilot_ops_script.py tests\\test_fill_collection.py tests\\test_slippage_calibration.py -q`
  - all 70 targeted pytest checks passed
- latest full regression verification on this machine:
  - `C:\Users\14574\AppData\Local\pypoetry\Cache\virtualenvs\portfolioos-Su-HS-3U-py3.11\Scripts\pytest.exe -q`
  - `255 passed, 2 skipped, 2 warnings in 6.87s`

## Project Goal

Build a brand-new Python project at `C:\Users\14574\Quant\PortfolioOS` named `PortfolioOS`.

The MVP is a compliance-aware A-share portfolio rebalance, pilot file-mapping, data-preparation, snapshot-locking, and execution-preflight CLI:

- input: holdings CSV, target CSV, market CSV, reference CSV, portfolio_state YAML, constraints YAML, default config YAML, execution profile YAML
- output: `orders.csv`, `orders_oms.csv`, `audit.json`, `summary.md`, `benchmark_comparison.json`, `benchmark_comparison.md`, plus replay, scenario, approval/freeze, execution-simulation, handoff-checklist, data-builder, and snapshot-bundle outputs
- stack: Python 3.11+, Poetry, pydantic v2, pandas, numpy, cvxpy, pyyaml, typer, pytest
- market: A-share cash equities
- scope: single account, local files, no broker API, no web UI, no ML/RL implementation
- disclaimer must always be visible: auxiliary decision-support tool only, not investment advice

## Frozen Spec Decisions

### CLI

Required CLI flags:

- `--holdings`
- `--target`
- `--market`
- `--reference`
- `--portfolio-state`
- `--constraints`
- `--config` default `config/default.yaml`
- `--execution-profile` default `config/execution/conservative.yaml`
- `--output-dir`

### Portfolio State

`portfolio_state.yaml` fields:

- `account_id`
- `as_of_date`
- `available_cash`
- `min_cash_buffer`
- `account_type`

Do not use `initial_cash`.

### Market / Reference Validation

For every ticker that appears in holdings or target:

- `market.csv` must contain required market fields, otherwise fail fast
- `reference.csv` must contain `industry`, otherwise fail fast

Only holdings + target may outer-join.
Do not silently backfill market or industry data.

### Boolean Parsing

Flexible CSV boolean parsing is required.
Accepted values, case-insensitive and trimmed:

- `true`
- `false`
- `1`
- `0`
- `yes`
- `no`

Used for:

- `tradable`
- `upper_limit_hit`
- `lower_limit_hit`
- `blacklist_buy`
- `blacklist_sell`

### Turnover And Cost Definitions

These definitions are frozen and must stay consistent in optimizer, repair, summary, audit, and tests:

- `gross_traded_notional = sum(abs(quantity) * estimated_price)`
- `turnover = gross_traded_notional / pre_trade_nav`
- `fee = commission + transfer_fee + sell_stamp_duty`
- `slippage = price * k * |q| * sqrt(|q| / adv_shares)`

Optimization, repair cash checks, and exported metrics must use the same formulas.

### Double Ten

- single-fund 10%: real hard constraint
- manager aggregate 10%: warning only
- 10-day remediation: report marker only

Effective single-name cap:

- `effective_single_name_limit = min(single_name_max_weight, double_ten.single_fund_limit)`

Manager aggregate input lives in `reference.csv` optional columns:

- `manager_aggregate_qty`
- `issuer_total_shares`

Important data meaning:

- `manager_aggregate_qty` excludes the current account position
- aggregate warning formula:
  - `(manager_aggregate_qty + current_account_post_trade_qty) / issuer_total_shares`

## Files Already Created

### Root / Config / Docs / Data

Created:

- `README.md`
- `pyproject.toml`
- `.gitignore`
- `config/default.yaml`
- `config/constraints/public_fund.yaml`
- `config/constraints/private_fund.yaml`
- `config/constraints/quant_fund.yaml`
- `config/execution/conservative.yaml`
- `config/execution/aggressive.yaml`
- `data/sample/holdings_example.csv`
- `data/sample/target_example.csv`
- `data/sample/market_example.csv`
- `data/sample/reference_example.csv`
- `data/sample/portfolio_state_example.yaml`
- `data/sample/tickers.txt`
- `data/sample/reference_overlay_example.csv`
- `data/generated/.gitkeep`
- `docs/providers.md`
- `docs/snapshot_bundle.md`
- `docs/math.md`
- `docs/constraints.md`
- `docs/audit.md`
- `docs/approval.md`
- `docs/demo_script.md`
- `docs/data_builders.md`
- `docs/import_profiles.md`
- `docs/calibration_profiles.md`
- `docs/execution_simulation.md`
- `docs/execution_mode_decision_note.md`
- `outputs/live_manual_inputs/.gitkeep` (conceptual workspace for manual live pilot baskets; not necessarily committed)
- `notebooks/01_data_exploration.ipynb`
- `notebooks/02_cost_model_fit.ipynb`
- `notebooks/03_rebalance_demo.ipynb`
- `notebooks/04_constraint_demo.ipynb`
- `data/scenario_samples/manifest.yaml`
- `data/scenario_samples/base_holdings.csv`
- `data/approval_samples/approval_request_example.yaml`
- `data/execution_samples/execution_request_example.yaml`
- `data/execution_samples/execution_request_calibrated.yaml`
- `data/execution_samples/execution_request_impact_aware.yaml`
- `data/universe/us_equity_expanded_tickers.txt`
- `data/universe/us_universe_reference.csv`
- `data/universe/us_universe_market_2026-03-27.csv`
- `data/risk_inputs_us_expanded/returns_long.csv`
- `data/risk_inputs_us_expanded/factor_exposure.csv`
- `data/risk_inputs_us_expanded/risk_inputs_manifest.json`
- `data/samples/us/sample_us_04/holdings.csv`
- `data/samples/us/sample_us_04/target.csv`
- `data/samples/us/sample_us_04/market.csv`
- `data/samples/us/sample_us_04/reference.csv`
- `data/samples/us/sample_us_04/portfolio_state.yaml`
- `data/samples/us/sample_us_04/tickers.txt`
- `data/samples/us/sample_us_05/holdings.csv`
- `data/samples/us/sample_us_05/target.csv`
- `data/samples/us/sample_us_05/market.csv`
- `data/samples/us/sample_us_05/reference.csv`
- `data/samples/us/sample_us_05/portfolio_state.yaml`
- `data/samples/us/sample_us_05/tickers.txt`
- `data/samples/us/sample_us_06/holdings.csv`
- `data/samples/us/sample_us_06/target.csv`
- `data/samples/us/sample_us_06/market.csv`
- `data/samples/us/sample_us_06/reference.csv`
- `data/samples/us/sample_us_06/portfolio_state.yaml`
- `data/samples/us/sample_us_06/tickers.txt`
- `data/import_profile_samples/custodian_style_a/holdings.csv`
- `data/import_profile_samples/custodian_style_a/target.csv`
- `data/import_profile_samples/custodian_style_a/market.csv`
- `data/import_profile_samples/custodian_style_a/reference.csv`
- `config/import_profiles/standard.yaml`
- `config/import_profiles/custodian_style_a.yaml`
- `config/calibration_profiles/liquid_midday.yaml`
- `config/calibration_profiles/balanced_day.yaml`
- `config/calibration_profiles/tight_liquidity.yaml`

### Code Created / Extended So Far

Key modules now in use:

- `src/portfolio_os/__init__.py`
- `src/portfolio_os/domain/enums.py`
- `src/portfolio_os/domain/errors.py`
- `src/portfolio_os/domain/models.py`
- `src/portfolio_os/utils/config.py`
- `src/portfolio_os/utils/logging.py`
- `src/portfolio_os/data/builders/market_builder.py`
- `src/portfolio_os/data/builders/reference_builder.py`
- `src/portfolio_os/data/builders/snapshot_builder.py`
- `src/portfolio_os/data/builders/target_builder.py`
- `src/portfolio_os/data/builders/common.py`
- `src/portfolio_os/data/loaders.py`
- `src/portfolio_os/data/import_profiles.py`
- `src/portfolio_os/data/portfolio.py`
- `src/portfolio_os/data/providers/base.py`
- `src/portfolio_os/data/providers/mock.py`
- `src/portfolio_os/data/providers/tushare_provider.py`
- `src/portfolio_os/data/market.py`
- `src/portfolio_os/data/reference.py`
- `src/portfolio_os/data/universe.py`
- `src/portfolio_os/execution/calibration.py`
- `src/portfolio_os/execution/fill_collection.py`
- `src/portfolio_os/execution/reporting.py`
- `src/portfolio_os/execution/simulator.py`
- `src/portfolio_os/execution/slicer.py`
- `src/portfolio_os/execution/slippage_calibration.py`
- `src/portfolio_os/explain/handoff.py`
- `src/portfolio_os/optimizer/multi_period.py`
- `src/portfolio_os/storage/runs.py`
- `scripts/generate_risk_inputs.py`

These modules are already present and should be extended carefully rather than replaced.

## Sample Data Intent

The sample set is designed to force:

- at least one normal buy
- at least one normal sell
- at least one compliance finding
- at least one blocked trade

Sample tickers currently used:

- `600519`
- `300750`
- `601318`
- `000333`
- `600276`
- `000858`
- `601012`

Key sample behaviors:

- `600276`: suspended / not tradable
- `000858`: upper limit hit
- `601012`: blacklist buy
- `000333`: manager aggregate 10% warning should be triggerable

## Remaining Work

No blocking implementation work remains for the MVP.

Only optional follow-on work remains, for example:

- improve solver quality beyond `SCS`
- add richer regulatory rules
- add broker adapters
- add multi-account support
- extend multi-period support beyond the current local impact-aware allocator if the product scope changes toward richer dynamic programming, horizon coupling, or production execution scheduling
- extend simulation and benchmark modules beyond static snapshot comparison if the product scope changes
- extend scenario scoring or governance rules only if the product scope explicitly changes
- extend execution simulation only if the product scope explicitly changes toward richer intraday modeling, production execution scheduling, or broker connectivity
- extend import profiles only if the product scope explicitly changes toward richer ETL or spreadsheet ingestion
- extend calibration profiles only if the product scope explicitly changes toward richer realized-fill feedback loops, confidence reporting, or venue-aware reference-price policies
- extend data builders only if the product scope explicitly changes toward richer provider integrations or scheduled data preparation
- extend Tushare integration only if the product scope explicitly changes toward broader asset coverage, richer fallback logic, or higher-throughput data acquisition

## Implementation Guidance For The Next Agent

- Keep the implementation simple and stable.
- Use pandas DataFrames for working state and pydantic models for validated interfaces and serialization.
- Use `cvxpy` with SCS.
- Repair logic matters: blocked trades, sell clipping, participation clipping, board-lot rounding, min-notional cleanup, then cash repair.
- Use the same fee/slippage functions everywhere.
- Keep import profiles declarative: columns, defaults, booleans, and simple numeric scaling only.
- Keep data builders lightweight: provider abstraction first, mock provider by default, and output files that exactly match the main CLI schema.
- Keep Tushare token handling explicit and external: CLI token first, then `TUSHARE_TOKEN`, and never persist the raw token.
- Treat permission-limited real feeds as product states, not hidden failures: preserve manifests, partial outputs, and recommended alternative paths.
- Keep execution simulation lightweight: recover source data from the frozen audit package, apply a simple bucket curve, and do not turn it into a live market simulator.
- Keep the new `impact_aware` multi-period allocator lightweight and local: it is a bucketed cost-aware planner, not a dynamic-programming execution engine.
- Preserve `reference_price` separately from `estimated_price` in any new fill-collection or calibration work; calibration should prefer `reference_price` when available.
- Keep calibration profiles static and reviewable; do not turn them into ML fitting logic.
- Keep the expanded US universe deterministic and reviewable:
  - `data/universe/us_equity_expanded_tickers.txt` is now the frozen ticker source for the expanded 50-name risk-input generation path
  - `data/universe/us_universe_reference.csv` is the frozen metadata seed for expanded US samples
  - do not overwrite the original 12-name baseline under `data/risk_inputs_us/` when refreshing expanded-universe assets
- Treat paper-trading calibration overlays as pilot artifacts only:
  - acceptable as `paper_trading` / pilot overlays
  - not acceptable as silent replacements for `config/default.yaml` or `config/us_default.yaml`
- `server.py` and `o32_stub.py` can still be lightweight stubs, but `multi_period.py` is now a real local planner and should be extended rather than reset to a placeholder.
- Use `apply_patch` for file edits in this environment.
- Avoid reverting any existing work.

## Environment Notes

- Current workspace root: `C:\Users\14574\Quant`
- Project root: `C:\Users\14574\Quant\PortfolioOS`
- Python launcher exists: `py -3.11`
- `python` alias was not available earlier
- `poetry` is installed, but in this environment it may still resolve through the broken Windows Store `python.exe` alias; the reliable path is the existing virtualenv under `C:\Users\14574\AppData\Local\pypoetry\Cache\virtualenvs\portfolioos-Su-HS-3U-py3.11\Scripts\`
- `rg` was not usable in this environment, so PowerShell commands may be needed instead

## Next Recommended Steps

1. If continuing, read `README.md` and inspect `outputs/demo_run`.
2. Re-run `C:\Users\14574\AppData\Local\pypoetry\Cache\virtualenvs\portfolioos-Su-HS-3U-py3.11\Scripts\pytest.exe -q` before making changes.
3. Keep the frozen spec decisions in this file unchanged unless the user explicitly revises them.
4. Preserve the Phase 2 benchmark definitions unless the user explicitly changes them:
   - `naive_target_rebalance`
   - `cost_unaware_rebalance`
   - `portfolio_os_rebalance`
5. Preserve the Phase 3 replay positioning:
   - static snapshot batch replay only
   - not an event-driven backtest
   - not a market simulator
6. Preserve the Phase 4 product-facing additions:
   - structured findings with code/category/severity/rule_source/blocking/repair_status
   - OMS-friendly `orders_oms.csv`
   - template-level `severity_policy`, `report_labels`, and `blocked_trade_policy`
7. Preserve the Phase 5 trust/explainability additions:
   - fail-fast data-quality checks for invalid inputs
   - non-blocking `data_quality` findings for softer issues
   - no-order explanations via `no_order_due_to_constraint`
   - replay evidence summaries with blocked-reason and highlight sections
8. Preserve the Phase 6 scenario analysis positioning:
   - scenario manifests reuse one shared snapshot
   - overrides are whitelist-only
   - recommendation is a transparent workflow score, not an investment recommendation
9. Preserve the Phase 7 approval positioning:
   - approval requests consume scenario outputs rather than rerunning the engine
   - approval statuses are local workflow states, not legal sign-offs
   - freeze is implemented as copy plus hash recording
10. Preserve the Phase 8 execution positioning:
   - execution simulation consumes a frozen or scenario-level OMS basket plus its audit package
   - keep it as a local preflight, not a live execution engine or broker adapter
   - `fill_rate` is reference-notional based, while `total_filled_notional` uses simulated fill prices
   - keep `participation_twap` as the stable baseline mode when changing execution logic
   - `impact_aware` is a local cost-aware bucket allocator layered onto the same report/output schema, not a production scheduler
11. Preserve the Phase 9 pilot-integration positioning:
   - import profiles are declarative mappings only
   - no arbitrary scripts, UI forms, or hidden ETL layer
   - mapped inputs must still land in the same standard PortfolioOS domain models
12. Preserve the Phase 9 calibration positioning:
   - request overrides > calibration profile > execution-profile default calibration
   - resolved calibration must remain explicit in the execution report
   - keep `handoff_checklist.md` as a file output, not a task system
   - preserve `reference_price` in fill telemetry and prefer it over `estimated_price` during calibration when available
   - treat paper-trading `k` updates as overlays, not default-config replacements, unless the user explicitly changes product policy
13. Preserve the Phase 10 data-preparation positioning:
   - builders prepare standard input files only
   - provider abstraction should stay simple and replaceable
   - default tests must remain offline through the mock provider
14. Preserve the Phase 11 real-provider positioning:
   - `tushare` is the first real provider path, but mock remains the default
   - manifests should record provider token source, never the raw token
   - snapshot bundles should remain file-based locks, not a database-backed cache
15. Preserve the Phase 11.5 UX positioning:
   - permission-limited steps should still leave artifacts and recommended next actions
   - client-provided `target.csv` is a first-class real pilot path, not a workaround
   - partial success should be visible in manifests rather than hidden behind one exception
16. Useful starting files for future work:
   - scenario manifest: `data/scenario_samples/manifest.yaml`
   - approval request sample: `data/approval_samples/approval_request_example.yaml`
   - approval workflow docs: `docs/approval.md`
   - execution request sample: `data/execution_samples/execution_request_example.yaml`
   - calibrated execution request sample: `data/execution_samples/execution_request_calibrated.yaml`
   - impact-aware execution request sample: `data/execution_samples/execution_request_impact_aware.yaml`
   - execution-mode decision note: `docs/execution_mode_decision_note.md`
   - expanded universe ticker list: `data/universe/us_equity_expanded_tickers.txt`
   - expanded universe reference seed: `data/universe/us_universe_reference.csv`
   - expanded universe market snapshot: `data/universe/us_universe_market_2026-03-27.csv`
   - expanded US risk inputs manifest: `data/risk_inputs_us_expanded/risk_inputs_manifest.json`
   - expanded US sample roots:
     - `data/samples/us/sample_us_04`
     - `data/samples/us/sample_us_05`
     - `data/samples/us/sample_us_06`
   - execution simulation docs: `docs/execution_simulation.md`
   - import profile docs: `docs/import_profiles.md`
   - calibration profile docs: `docs/calibration_profiles.md`
   - data builder docs: `docs/data_builders.md`
   - provider docs: `docs/providers.md`
   - snapshot bundle docs: `docs/snapshot_bundle.md`
   - sample ticker list: `data/sample/tickers.txt`
   - sample reference overlay: `data/sample/reference_overlay_example.csv`
   - latest live paper-fill root: `outputs/alpaca_fill_collection_live`
   - latest live fill analysis root: `outputs/live_fill_analysis`

## 2026-03-30 Phase 13 Progress Update

- Phase 13 prerequisite is complete:
  - public single-run workflow service lives in `src/portfolio_os/workflow/single_run.py`
  - CLI single-run path in `src/portfolio_os/api/cli.py` delegates to `run_single_rebalance(...)`
  - regression coverage exists in `tests/test_single_run_workflow.py`
- Phase 13 minimal backtest engine is now in place:
  - new package: `src/portfolio_os/backtest/`
  - added `manifest.py`, `baseline.py`, `engine.py`, and package exports in `__init__.py`
  - `run_backtest(...)` now supports:
    - monthly rebalance schedule
    - optimizer vs `naive_pro_rata` vs `buy_and_hold`
    - in-memory T+1 close fills
    - commission from app config
    - fixed half-spread assumption from execution profile via `execution.backtest_fixed_half_spread_bps`
    - daily NAV export plus summary JSON payload
- Added minimal backtest CLI surface:
  - new Typer app export: `backtest_app`
  - new Poetry script: `portfolio-os-backtest`
  - CLI writes `backtest_results.json` and `nav_series.csv`
- Added first frozen backtest sample manifest:
  - `data/backtest_samples/manifest_us_expanded.yaml`
- Execution-profile schema now includes:
  - `backtest_fixed_half_spread_bps: float = 5.0`
  - `config/execution/conservative.yaml` now sets it explicitly to `5.0`
- Added red-green coverage in `tests/test_backtest.py` for:
  - manifest loading
  - engine smoke test
  - CLI artifact writing
- Latest verification status:
  - `python -m pytest tests/test_backtest.py -q` -> `3 passed`
  - `python -m pytest tests/test_backtest.py tests/test_single_run_workflow.py -q` -> `6 passed`
  - `python -m pytest -q` -> `258 passed, 2 skipped, 6 warnings`
- Real expanded-sample smoke run completed:
  - `run_backtest(data/backtest_samples/manifest_us_expanded.yaml)` returned `rebalance_count = 12`
  - strategies present: `optimizer`, `naive_pro_rata`, `buy_and_hold`
  - sample summary snapshot:
    - `optimizer`: `rebalance_count=12`, `ending_nav≈2152740.75`, `total_turnover≈0.10898`
    - `naive_pro_rata`: `rebalance_count=12`, `ending_nav≈2226203.23`, `total_turnover≈0.547775`
    - `buy_and_hold`: `rebalance_count=0`, `ending_nav≈2069840.99`
- Known follow-up boundary for the next Phase 13 slice:
  - attribution/report layer is not implemented yet
  - current outputs are limited to `backtest_results.json` and `nav_series.csv`
  - sample manifest is a smoke-test starting point, not yet a production research manifest with dedicated `initial_holdings.csv` / `target_weights.csv`

## 2026-03-30 Phase 13 Closeout Update

- Phase 13 is now closed out to the originally approved scope:
  - added `src/portfolio_os/backtest/attribution.py`
  - added `src/portfolio_os/backtest/report.py`
  - extended `BacktestResult` to carry:
    - `nav_series`
    - `period_attribution`
    - `summary`
    - `report_markdown`
- `run_backtest(...)` now exports auditable period attribution with:
  - `holding_pnl`
  - `active_trading_pnl`
  - `trading_cost_pnl`
  - `period_pnl`
  - `period_return`
  - `gross_traded_notional`
  - `turnover`
  - `optimizer_vs_naive_period_pnl_delta` on optimizer rows
- Backtest summary now includes:
  - `annualized_return`
  - `sharpe`
  - `max_drawdown`
  - `period_count`
  - optimizer-vs-naive comparison deltas for:
    - ending NAV
    - total return
    - annualized return
    - total cost
    - total turnover
- `portfolio-os-backtest` CLI now writes four artifacts:
  - `backtest_results.json`
  - `nav_series.csv`
  - `period_attribution.csv`
  - `backtest_report.md`
- `tests/test_backtest.py` now covers:
  - summary-stat presence
  - attribution column presence and additive period PnL identity
  - CLI artifact creation for CSV + markdown
- Fresh verification evidence for the closeout:
  - `python -m pytest tests/test_backtest.py -q` -> `3 passed`
  - `python -m pytest tests/test_backtest.py tests/test_single_run_workflow.py -q` -> `6 passed`
  - `python -m pytest -q` -> `258 passed, 2 skipped, 6 warnings`
- Real CLI smoke run completed with artifact output:
  - output dir: `outputs/backtest_us_expanded_phase13`
  - manifest: `data/backtest_samples/manifest_us_expanded.yaml`
  - artifact set present:
    - `backtest_results.json`
    - `nav_series.csv`
    - `period_attribution.csv`
    - `backtest_report.md`
  - smoke summary:
    - `rebalance_count = 12`
    - optimizer ending NAV: `2152740.75`
    - naive ending NAV: `2226203.23`
    - buy-and-hold ending NAV: `2069840.99`
    - optimizer vs naive ending NAV delta: `-73462.48`
    - optimizer total turnover: `0.10898`
    - naive total turnover: `0.547775`
    - optimizer total cost: `173.39`
    - naive total cost: `904.72`
- Interpretation boundary for this finished Phase 13 slice:
  - optimizer traded much less and paid much lower explicit cost
  - on the current expanded-universe smoke sample it still finished behind naive on ending NAV
  - this is a backtest research result, not a product-policy decision by itself

## 2026-03-30 Phase 13b Cost Sweep Update

- Added deterministic cost-bundle sweep support on top of the backtest engine:
  - new module: `src/portfolio_os/backtest/sweep.py`
  - report extension in `src/portfolio_os/backtest/report.py`
  - new public exports in `src/portfolio_os/backtest/__init__.py`
  - new CLI entry surface in `src/portfolio_os/api/cli.py`
  - new Poetry script: `portfolio-os-backtest-sweep`
- Sweep semantics:
  - scales the full cost-side objective bundle together:
    - `objective_weights.transaction_cost`
    - `objective_weights.transaction_fee`
    - `objective_weights.turnover_penalty`
    - `objective_weights.slippage_penalty`
  - leaves `risk_term`, `tracking_error`, and `target_deviation` unchanged
  - archives one full backtest run per multiplier under the sweep output root
- Added sweep outputs:
  - `sweep_summary.csv`
  - `efficient_frontier_report.md`
  - `backtest_sweep_manifest.json`
  - per-multiplier run directories with:
    - `config_scaled.yaml`
    - `manifest_scaled.yaml`
    - `backtest_results.json`
    - `nav_series.csv`
    - `period_attribution.csv`
    - `backtest_report.md`
- Added sweep regression coverage in `tests/test_backtest.py` for:
  - cost bundle scaling
  - sweep CLI artifact writing
  - preservation of relative `risk_model` paths via absolute rewrite in derived configs
- Root cause fixed during implementation:
  - first real sweep failed because derived `config_scaled.yaml` changed the base directory for relative `risk_model` paths
  - fix: sweep now rewrites `risk_model.returns_path` and `risk_model.factor_exposure_path` to absolute paths when generating scaled configs
- Fresh verification evidence after the fix:
  - `python -m pytest tests/test_backtest.py -q` -> `6 passed`
  - `python -m pytest tests/test_backtest.py tests/test_single_run_workflow.py -q` -> `9 passed`
  - `python -m pytest -q` -> `261 passed, 2 skipped, 14 warnings`
- Real sweep run completed:
  - output dir: `outputs/backtest_us_expanded_cost_sweep_phase13b`
  - base manifest: `data/backtest_samples/manifest_us_expanded.yaml`
  - multipliers run: `0.1`, `0.3`, `0.5`, `1.0`, `2.0`
- Real sweep summary:
  - `0.1x`: ending NAV `2229057.93`, annualized return `14.62%`, Sharpe `1.26`, MDD `-8.35%`, turnover `0.4965`, cost `817.11`, vs naive ending NAV delta `+2854.70`
  - `0.3x`: ending NAV `2222781.37`, annualized return `14.29%`, Sharpe `1.26`, MDD `-8.10%`, turnover `0.4096`, cost `669.72`, vs naive delta `-3421.86`
  - `0.5x`: ending NAV `2223675.60`, annualized return `14.34%`, Sharpe `1.28`, MDD `-7.93%`, turnover `0.3595`, cost `588.00`, vs naive delta `-2527.63`
  - `1.0x`: ending NAV `2200618.38`, annualized return `13.14%`, Sharpe `1.24`, MDD `-7.43%`, turnover `0.2439`, cost `394.94`, vs naive delta `-25584.85`
  - `2.0x`: ending NAV `2152740.75`, annualized return `10.66%`, Sharpe `1.11`, MDD `-6.65%`, turnover `0.1090`, cost `173.39`, vs naive delta `-73462.48`
- Frontier interpretation from this sample:
  - lower cost penalties materially improve ending NAV on this upward-trending sample
  - `0.1x` gives the best ending NAV and is the only point that beats naive on ending NAV
  - `0.5x` gives the best Sharpe in this sweep
  - the current effective baseline (`2.0x` in this research setup) is likely too conservative for this sample

## 2026-03-30 Phase 13b Research Closeout Update

- Sweep closeout artifacts are now formalized without changing defaults:
  - decision note: `docs/cost_penalty_sweep_note.md`
  - research-only config: `config/us_expanded_aggressive.yaml`
- Configuration policy after the sweep is now explicit:
  - keep `config/us_expanded.yaml` unchanged as the current baseline
  - do not promote `0.5x` to a default without out-of-sample validation
  - treat `config/us_expanded_aggressive.yaml` as a research comparison config only
- Sweep interpretation now captured in a durable note:
  - `0.5x` is the best Sharpe point on the current sample
  - `0.1x` is the best ending-NAV point on the current sample
  - the frontier shape is concave, which makes `0.5x` the current research sweet spot rather than an obvious production default
  - the sweep window is still one expanded-US sample and must not be overfit into default policy
- Next research line after Phase 13b closeout:
  - move to TCA / slippage-calibration follow-on work
  - build on the existing execution stack rather than inventing a parallel pipeline
  - first reuse targets:
    - `src/portfolio_os/execution/slippage_calibration.py`
    - `src/portfolio_os/execution/fill_collection.py`
    - `src/portfolio_os/execution/fill_collection_campaign.py`
    - `src/portfolio_os/execution/reporting.py`
    - `docs/execution_simulation.md`
  - expected boundary:
    - improve realized-vs-simulated residual workflow and overlay artifacts
    - do not mutate production defaults from paper-fill evidence without an explicit decision note
- Fresh closeout verification on this machine:
  - `C:\Users\14574\AppData\Local\pypoetry\Cache\virtualenvs\portfolioos-Su-HS-3U-py3.11\Scripts\python.exe` successfully loaded `config/us_expanded_aggressive.yaml` through `load_app_config(...)`
  - resolved research weights confirmed:
    - `transaction_cost = 0.5`
    - `transaction_fee = 0.5`
    - `turnover_penalty = 0.015`
    - `slippage_penalty = 0.5`
    - `risk_model.enabled = true`
    - `risk_model.integration_mode = augment`
  - `C:\Users\14574\AppData\Local\pypoetry\Cache\virtualenvs\portfolioos-Su-HS-3U-py3.11\Scripts\pytest.exe -q` -> `261 passed, 2 skipped, 14 warnings`
- TCA kickoff recon confirmed the existing reusable path:
  - `src/portfolio_os/execution/slippage_calibration.py` already contains:
    - prep skeleton generation
    - offline synthetic calibration fixtures
    - dataset / residual / overlay / diagnostic artifact flow
  - `docs/execution_simulation.md` already documents the execution request, calibration precedence, and output schema
  - next TCA work should extend these existing artifacts rather than introducing a second calibration workflow

## 2026-03-30 TCA Vertical Slice Update

- The first explicit TCA decision layer is now in place on top of the existing slippage-calibration workflow.
- `src/portfolio_os/execution/slippage_calibration.py` now emits machine-readable TCA decision fields in both `summary` and `diagnostic_manifest`:
  - `overlay_readiness`
    - enum: `sufficient`, `directional_only`, `insufficient`
  - `overlay_readiness_reason`
  - `next_recommended_action`
    - enum currently reachable through the workflow:
      - `apply_as_paper_overlay`
      - `collect_more_fills`
    - reserved path when default-promotion mode is explicitly requested:
      - `promote_to_default`
- Calibration outputs now also include machine-readable audit coverage fields:
  - `fit_reason_counts`
  - `status_counts`
  - `side_counts`
  - `eligible_side_counts`
  - `positive_signal_side_counts`
  - `coverage_by_side`
  - `coverage_by_participation_bucket`
  - `coverage_by_notional_bucket`
- Markdown report output now explicitly surfaces:
  - overlay readiness
  - next recommended action
  - fit eligibility breakdown
  - status coverage
  - side coverage
- `scripts/pilot_ops.py` now includes a dedicated TCA entrypoint:
  - `calibrate-slippage`
  - arguments:
    - `--fill-collection-root`
    - `--source-run-root`
    - `--output-dir`
    - `--alpha`
    - `--min-filled-orders`
    - `--min-participation-span`
  - this command reuses:
    - `slippage_calibration.calibrate_slippage(...)`
    - `slippage_calibration.write_slippage_calibration_artifacts(...)`
  - it does not mutate defaults; it only writes auditable TCA artifacts and prints the key machine-readable decision fields
- Fresh verification evidence after the TCA slice:
  - `pytest tests/test_slippage_calibration.py tests/test_pilot_ops_script.py -q` -> `59 passed`
  - `pytest -q` -> `264 passed, 2 skipped, 14 warnings`
- First real CLI proof for the new TCA entrypoint completed:
  - command:
    - `python scripts/pilot_ops.py calibrate-slippage --fill-collection-root outputs/alpaca_fill_collection_reference_aligned --source-run-root outputs/live_fill_analysis/source_run_root_reference_aligned --output-dir outputs/live_fill_analysis/slippage_calibration_tca_vertical_slice_20260330`
  - artifact root:
    - `outputs/live_fill_analysis/slippage_calibration_tca_vertical_slice_20260330`
  - observed machine-readable outcome:
    - `overlay_readiness = directional_only`
    - `next_recommended_action = collect_more_fills`
    - `recommendation = INSUFFICIENT_DATA_FOR_DEFAULT_UPDATE`
    - `recommendation_reason = insufficient_participation_span`
    - `candidate_k = 1.445696451694286`
  - real coverage snapshot from that run:
    - `filled_order_count = 20`
    - `positive_signal_count = 12`
    - `negative_signal_count = 7`
    - `participation_span = 0.003331720430107527`
  - interpretation:
    - current real fills are strong enough to show directional slippage signal
    - current real fills are not broad enough across participation buckets to justify overlay sufficiency or default promotion

## 2026-03-30 Step 2 Directional-`k` What-If Sweep Update

- Step 2 is now executed using the current directional TCA candidate `k` from the real TCA slice:
  - source TCA artifact:
    - `outputs/live_fill_analysis/slippage_calibration_tca_vertical_slice_20260330`
  - directional candidate:
    - `candidate_k = 1.445696451694286`
- Important config wiring note captured during execution:
  - this backtest path consumes `slippage.k` from the app config, not from the execution-profile YAML
- Added research-only backtest inputs for the what-if run:
  - config: `config/us_expanded_tca_directional.yaml`
  - manifest: `data/backtest_samples/manifest_us_expanded_tca_directional.yaml`
- Real what-if sweep run completed:
  - output root: `outputs/backtest_us_expanded_cost_sweep_tca_directional_k_phase13c`
  - comparison report: `outputs/backtest_us_expanded_cost_sweep_tca_directional_k_phase13c/frontier_comparison_report.md`
- Directional-`k` sweep summary:
  - `0.1x`: ending NAV `2218442.92`, annualized return `14.07%`, Sharpe `1.28`, MDD `-7.70%`, turnover `0.3635`, cost `595.00`, vs naive delta `-7760.31`
  - `0.3x`: ending NAV `2168771.10`, annualized return `11.49%`, Sharpe `1.17`, MDD `-6.77%`, turnover `0.1817`, cost `292.23`, vs naive delta `-57432.13`
  - `0.5x`: ending NAV `2124560.37`, annualized return `9.20%`, Sharpe `1.02`, MDD `-6.29%`, turnover `0.0968`, cost `154.95`, vs naive delta `-101642.86`
  - `1.0x`: ending NAV `2087695.94`, annualized return `7.29%`, Sharpe `0.86`, MDD `-5.82%`, turnover `0.0403`, cost `64.92`, vs naive delta `-138507.29`
  - `2.0x`: ending NAV `2071827.04`, annualized return `6.47%`, Sharpe `0.79`, MDD `-5.58%`, turnover `0.0058`, cost `9.50`, vs naive delta `-154376.19`
- Key research conclusion from Step 2:
  - the earlier `0.5x` Sharpe sweet spot does not survive once `k` is raised to the directional TCA candidate
  - under directional `k`, `0.1x` is now the best point on both ending NAV and Sharpe within the tested grid
  - even that best tested point still underperforms naive on ending NAV
  - the frontier now appears boundary-constrained at the low end, which implies the next multiplier sweep should extend below `0.1x`
- Fresh verification evidence after the Step 2 sweep:
  - `pytest -q` -> `264 passed, 2 skipped, 14 warnings`

## 2026-03-30 Zero-Cost Probe Update

- A single-point zero-cost probe was run to separate cost-side effects from risk-term effects under the directional TCA `k`.
- Probe setup:
  - base config: `config/us_expanded_tca_directional.yaml`
  - manifest: `data/backtest_samples/manifest_us_expanded_tca_directional.yaml`
  - cost bundle multiplier: `0.0`
  - output root: `outputs/backtest_us_expanded_zero_cost_probe_phase13c`
  - note: `outputs/backtest_us_expanded_zero_cost_probe_phase13c/zero_cost_probe_note.md`
- Real probe outcome:
  - optimizer ending NAV: `2226203.2295169644`
  - naive ending NAV: `2226203.2295169644`
  - optimizer vs naive ending NAV delta: `0.0`
  - optimizer turnover: `0.5477750189321283`
  - naive turnover: `0.5477750189321283`
  - optimizer total transaction cost: `904.7164397421116`
  - naive total transaction cost: `904.7164397421116`
- Research interpretation:
  - on this sample and this configuration, once cost-side weights are zeroed, `augment` mode does not pull the optimizer away from naive
  - therefore the directional-`k` underperformance in the earlier sweep is attributable to cost-side weights, not to the risk term
  - stronger interpretation:
    - current `risk_term` / `tracking_error` contribution is too small relative to `target_deviation` to create a portfolio distinct from naive
    - in the current setup, the optimizer is functioning as "trade less than naive when cost weights are positive", not yet as "construct a meaningfully different risk-adjusted portfolio"
- immediate implication:
    - do not pivot to risk-weight tuning yet before TCA closes
    - do not spend more time extending the directional-`k` cost-multiplier sweep below `0.1x`; `0.0x = naive` already bounds that axis
    - after TCA reaches `overlay_readiness = sufficient` and calibrated `k` is available, the next high-value research step is a risk-aversion sweep so the risk term actually participates in portfolio construction

## 2026-03-30 Fill Collection Batch Generator Update

- Added a new pre-submit planning path in `scripts/pilot_ops.py`:
  - CLI subcommand: `generate-fill-collection-batch`
  - purpose: generate deterministic fill-collection batch inputs directly as `orders_oms.csv`, plus a separate auditable `fill_collection_batch.csv`
- Generator behavior:
  - inputs:
    - market snapshot CSV with `ticker`, price (`close`/`vwap`), `adv_shares`, and optional `tradable`
    - participation bucket grid via `--participation-buckets`
    - side scope via `--side-scope`
    - count per bucket-side via `--orders-per-bucket`
  - reverse-calculates shares as:
    - `floor(target_participation * adv_shares)`
  - never emits fractional shares
  - records both:
    - `target_participation_bucket`
    - `actual_participation = quantity / adv_shares`
  - writes extra audit metadata while keeping the generated `orders_oms.csv` compatible with the existing `collect-fills --orders-oms ...` path
- Added artifact set for the generator:
  - `fill_collection_batch.csv`
  - `orders_oms.csv`
  - `fill_collection_batch_manifest.json`
  - `fill_collection_batch_report.md`
- Determinism / selection policy:
  - candidates are ranked by:
    - lowest estimated notional
    - then smallest participation gap from the requested bucket
    - then stable ticker ordering
  - within the same side, the generator now prefers unused tickers across buckets when enough alternatives exist, so one batch does not collapse to the same ticker repeatedly unless the universe forces it
- Added regression coverage:
  - `tests/test_pilot_ops_script.py`
    - parser coverage for `generate-fill-collection-batch`
    - percent-string parsing for participation buckets
    - whole-share flooring plus `actual_participation`
    - side-scope handling and untradable filtering
    - cross-bucket ticker spreading when alternatives exist
  - `tests/test_fill_collection.py`
    - generated `orders_oms.csv` remains compatible with `load_orders_from_orders_oms(...)`
- Fresh verification evidence:
  - `python -m pytest tests/test_pilot_ops_script.py tests/test_fill_collection.py -q` -> `58 passed`
  - real CLI smoke run completed:
    - command:
      - `python scripts/pilot_ops.py generate-fill-collection-batch --market-file data/universe/us_universe_market_2026-03-27.csv --output-dir outputs/fill_collection_batches --participation-buckets 0.01%,0.1%,1%,5% --orders-per-bucket 1 --side-scope both --sample-id tca_batch_smoke`
    - artifact root:
      - `outputs/fill_collection_batches/fill_collection_batch_20260330T235605_4c61ad01`
    - smoke output summary:
      - generated order count: `8`
      - buy/sell split: `4 / 4`
      - per-bucket selected tickers:
        - `0.01%` -> `UPST`
        - `0.1%` -> `SNAP`
        - `1%` -> `SBAC`
        - `5%` -> `CELH`

## 2026-03-31 Account-Constrained Batch Generator Update

- `generate-fill-collection-batch` now supports two optional execution-state constraints:
  - `--broker-positions-file <path>`
    - CSV input
    - accepts `ticker`/`symbol` plus `shares`/`quantity`/`qty`
    - sell-side generation is restricted to these tickers
    - generated sell quantity is capped at available broker shares
  - `--buying-power <float>`
    - buy-side total notional cap
    - buckets are processed in ascending participation order
    - once the budget is exhausted, remaining buy buckets stop generating orders
- Backward compatibility remains unchanged:
  - if neither flag is passed, generator behavior stays unconstrained
- Audit fields added to generator manifest/report:
  - `broker_positions_source`
  - `broker_positions_ticker_count`
  - `buying_power_budget`
  - `buy_budget_remaining`
  - `budget_exhausted`
- Sell-side constrained selection semantics were tightened:
  - after capping to available position size, sell candidates are ranked by:
    - smallest participation gap to the target bucket
    - then lower estimated notional
    - then stable ticker ordering
  - this improves bucket realism compared with blindly ranking capped sells by cheapest notional
- Added regression coverage in `tests/test_pilot_ops_script.py` for:
  - parser support for `--broker-positions-file`
  - parser support for `--buying-power`
  - sell-only generation restricted to provided positions and capped by holdings
  - buy-only generation respecting a total buying-power budget and stopping in bucket order
- Fresh verification evidence:
  - `python -m pytest tests/test_pilot_ops_script.py tests/test_fill_collection.py -q` -> `61 passed`
- Real constrained sell-only batch was generated from the latest live fill snapshot:
  - positions source:
    - `outputs/alpaca_fill_collection/orders_oms_20260331T134654_f5b9cc0c/broker_positions_after.csv`
  - latest post-batch account snapshot from that run:
    - `buying_power = 1216.96`
    - `cash = -106523.36`
    - positions present across 15 tickers
  - constrained sell batch artifact root:
    - `outputs/fill_collection_batches/fill_collection_batch_20260331T140821_8ae45673`
  - generated result:
    - `side_scope = sell-only`
    - `orders_per_bucket = 3`
    - `generated_order_count = 12`
    - `broker_positions_source = provided`
  - interpretation boundary:
    - the batch is now truly executable against current inventory
    - however, current inventory size is still too small relative to ADV to reach the intended higher participation buckets on many names
    - this is now an account-state limitation, not a generator-state limitation
- A manual submit-ready shortlist was also created from that constrained sell batch:
  - shortlist root:
    - `outputs/fill_collection_batches/fill_collection_batch_20260331T141500_sell_shortlist`
  - selected tickers:
    - `RIVN`
    - `AES`
    - `MP`
    - `MRNA`
    - `AAPL`
    - `GOOGL`
    - `AMZN`
    - `NVDA`
  - dropped names:
    - `CELH`
    - `AFRM`
    - `ROKU`
    - `SBAC`
  - shortlist intent:
    - keep names with relatively better effective participation and/or larger cash-release value
    - remove tiny residual sell orders that are unlikely to add much TCA value

## 2026-03-31 Slippage Calibration ADV Join Fix

- Root cause investigation outcome:
  - `missing_adv = 23` was not caused by ticker-format mismatches
  - recent live fill runs record `collection_source_path` as a generated `orders_oms.csv`
    - example family:
      - `outputs/fill_collection_batches/fill_collection_batch_20260331T133958_fcdc3352/orders_oms.csv`
      - sibling `fill_collection_batch.csv` contains `adv_shares`
  - `build_slippage_calibration_dataset(...)` already uses `collection_source_path` when `source_run_root` is omitted
  - but `_build_source_order_lookup(...)` only knew how to read the old directory-style `source_run_root/samples/...` layout
  - when given a direct `orders_oms.csv` file path, source lookup came back empty
  - result:
    - `source_adv_shares` stayed null
    - filled rows were classified as `missing_adv`
    - participation coverage was understated by a broken join, not by missing live fills
- Minimal code fix applied in `src/portfolio_os/execution/slippage_calibration.py`:
  - added `_build_orders_lookup_from_frame(...)`
  - extended `_build_source_order_lookup(...)` so file-style source paths are supported
  - new file-path behavior:
    - read the `orders_oms.csv`
    - look for sibling `fill_collection_batch.csv` first
    - fall back to sibling `market.csv` if batch sidecar is absent
    - recover `adv_shares` and rebuild the same `(sample_id, ticker)` lookup used by calibration
- Regression coverage added in `tests/test_slippage_calibration.py`:
  - `test_build_dataset_recovers_adv_from_orders_oms_source_path_with_batch_sidecar`
  - coverage intent:
    - live-style `collection_source_path` points to `orders_oms.csv`
    - `adv_shares` is recovered from sibling batch metadata
    - filled rows no longer get incorrectly marked as `missing_adv`
- Focused verification completed immediately after the fix:
  - `python -m pytest tests/test_slippage_calibration.py tests/test_pilot_ops_script.py -q`
  - result:
    - `68 passed`
- Real calibration rerun after the fix:
  - output root:
    - `outputs/live_fill_analysis/slippage_calibration_after_adv_fix_20260331`
  - summary before/after:
    - `missing_adv_count: 23 -> 2`
    - `fit_eligible_count: 3 -> 24`
    - `fit_sample_count: 3 -> 14`
    - `positive_signal_count: 3 -> 14`
    - `negative_signal_count: 0 -> 10`
    - `candidate_k: 14418.45969258086 -> 3.498400399110418`
  - current truthful calibration state after the join fix:
    - `filled_order_count = 26`
    - `overlay_readiness = directional_only`
    - `next_recommended_action = collect_more_fills`
    - `participation_span = 0.008003562024978718`
    - `coverage_by_participation_bucket` still only contains `0-0.1%`
  - interpretation:
    - the ADV join bug is fixed
    - the remaining blocker is now real participation coverage, not broken metadata recovery
- Remaining `missing_adv = 2` rows were isolated to old demo artifacts, not the recent live batch data:
  - collection run:
    - `outputs/alpaca_fill_collection/collect_fills_demo_20260326`
  - tickers:
    - `AAPL`
    - `MSFT`
  - manifest points to a temp source path under:
    - `C:\Users\14574\AppData\Local\Temp\...`
  - practical implication:
    - these rows are not informative for current live calibration quality
    - if needed later, old demo runs can be excluded from calibration roots without affecting current live TCA conclusions

## 2026-04-01 Universe ADV Reachability Check

- The frozen universe file `data/universe/us_universe_market_2026-03-27.csv` was reviewed directly.
- Schema confirmed:
  - `ticker`
  - `close`
  - `vwap`
  - `adv_shares`
  - `tradable`
- Universe size:
  - `50` tickers
- Lowest-ADV names in the current frozen universe:
  - `SBAC`: `968,935` shares ADV
  - `VRTX`: `1,462,480` shares ADV
  - `COST`: `1,728,475` shares ADV
  - `GS`: `2,432,855` shares ADV
  - `CAT`: `2,704,915` shares ADV
- Reachability check using a single-ticket budget of `$5,000`:
  - methodology:
    - use `close` as the execution-price proxy from the universe file
    - compute executable shares as `floor(5000 / close)`
    - compute participation as `shares / adv_shares`
  - highest reachable names under this budget:
    - `UPST`: `0.003898%`
    - `SNAP`: `0.003101%`
    - `SBAC`: `0.002993%`
    - `CELH`: `0.002110%`
    - `AFRM`: `0.002034%`
  - count of names that can reach `1%+` participation:
    - `0`
  - count of names that can reach `5%+` participation:
    - `0`
- Practical interpretation:
  - the current 50-name universe is nowhere close to supporting `1%` or `5%` participation buckets with a `$5,000` order size
  - this is a physical market/account-size limitation, not a generator bug
  - even the lowest-ADV name in the universe (`SBAC`) would require about `$1.62M` notional to reach `1%` participation and about `$8.09M` to reach `5%`
  - if higher participation coverage is still required, the next strategy must change one of:
    - use a materially lower-ADV universe
    - increase deployable order notional dramatically
    - redefine target buckets to match the current account scale
- Fresh verification on this machine:
  - `python -m pytest tests/test_pilot_ops_script.py tests/test_fill_collection.py -q` -> `61 passed, 1 warning`
  - `python -m pytest -q` -> `277 passed, 14 warnings`
  - note on count drift versus the older `274 passed, 2 skipped` baseline:
    - this machine currently has `ALPACA_API_KEY` and `ALPACA_SECRET_KEY` present
    - the conditional Alpaca tests therefore executed instead of skipping

## 2026-04-01 Low-Participation Overlay Sufficiency Update

- TCA readiness logic was adjusted to reflect the real account/universe regime discovered in the ADV reachability check.
- Broad-span sufficiency remains intact:
  - `overlay_readiness = sufficient` still triggers on the old path when:
    - filled-order guardrail passes
    - participation-span guardrail passes
    - signal-quality and model-improvement guardrails pass
- Added a second low-participation overlay path for the current production regime:
  - `overlay_readiness = sufficient` now also triggers when:
    - `fit_eligible_count >= 30`
    - fit-eligible coverage includes both `buy` and `sell`
    - observed fit participation buckets stay within `0-0.1%`
    - signal-quality and model-improvement guardrails still pass
- Important boundary:
  - this new path does **not** relax default-promotion logic into a broad extrapolation claim
  - low-participation sufficiency yields:
    - `overlay_readiness = sufficient`
    - `next_recommended_action = apply_as_paper_overlay`
    - `recommendation = provisional_only`
    - `recommendation_reason = low_participation_overlay_only`
- New machine-readable audit fields now emitted in summary/diagnostic artifacts:
  - `sufficient_low_participation_coverage`
  - `bidirectional_fit_coverage`
  - `participation_range_note`
- The low-participation applicability note is now explicit:
  - `calibration validated only for 0-0.1% participation; do not extrapolate to higher-participation scenarios`
- Regression coverage added in `tests/test_slippage_calibration.py`:
  - low-participation dense-coverage fixture
  - sufficiency path with `sufficient_participation_span = false`
  - note propagation into summary, diagnostic manifest, and markdown report
- Focused verification completed immediately after the change:
  - `python -m pytest tests/test_slippage_calibration.py::test_low_participation_dense_coverage_can_be_sufficient_without_span -q` -> `1 passed`
  - `python -m pytest tests/test_slippage_calibration.py -q` -> `12 passed`
  - `python -m pytest -q` -> `277 passed, 14 warnings`

## 2026-04-01 Low-Participation Live Fill Closure Update

- A new constrained low-participation batch was generated and submitted during regular market hours:
  - generated batch root:
    - `outputs/fill_collection_batches/fill_collection_batch_20260401T173256_20916ee5`
  - live pre-submit check:
    - `outputs/pre_submission_checks_live/pre_submission_check_20260401T173309_41a253b5`
    - result: `ready_to_submit`
  - live fill collection run:
    - `outputs/alpaca_fill_collection/orders_oms_20260401T173321_8c1cad8c`
- Execution outcome for this batch:
  - submitted orders: `8`
  - filled orders: `5`
  - partially filled orders: `3`
  - unfilled orders: `0`
  - rejected orders: `0`
- Calibration rerun after including this batch:
  - artifact root:
    - `outputs/live_fill_analysis/slippage_calibration_low_participation_20260401_fit_eligible_gate`
  - observed machine-readable outcome:
    - `fit_eligible_count = 32`
    - `fit_sample_count = 16`
    - `bidirectional_fit_coverage = true`
    - `sufficient_low_participation_coverage = true`
    - `overlay_readiness = sufficient`
    - `next_recommended_action = apply_as_paper_overlay`
    - `recommendation = provisional_only`
    - `recommendation_reason = low_participation_overlay_only`
    - `participation_range_note = calibration validated only for 0-0.1% participation; do not extrapolate to higher-participation scenarios`
    - `candidate_k = 3.498400399110418`
- Important methodological note captured for the next step:
  - the current `k` fitting path still uses `positive_signal` observations only
  - this means sufficiency is now closed for the low-participation overlay decision, but the estimator itself is still likely upward-biased
  - next methodological follow-up:
    - revisit `fit_slippage_k(...)` so negative-signal eligible fills also participate in the `k` estimate, or explicitly document why a one-sided estimator is still desired

## 2026-04-01 Cost Model Decision Closeout

- A calibrated-`k` expanded-US sweep was run with:
  - research config:
    - `config/us_expanded_tca_calibrated.yaml`
  - manifest:
    - `data/backtest_samples/manifest_us_expanded_tca_calibrated.yaml`
  - calibrated `k`:
    - `3.498400399110418`
  - output root:
    - `outputs/backtest_us_expanded_cost_sweep_tca_calibrated_k_phase13d`
- Calibrated-`k` sweep summary:
  - `0.1x`: ending NAV `2184286.35`, annualized return `12.30%`, Sharpe `1.20`, MDD `-7.10%`, turnover `0.2385`, cost `386.19`, vs naive delta `-41916.88`
  - `0.3x`: ending NAV `2103740.52`, annualized return `8.12%`, Sharpe `0.93`, MDD `-6.01%`, turnover `0.0704`, cost `112.71`, vs naive delta `-122462.70`
  - `0.5x`: ending NAV `2089188.57`, annualized return `7.37%`, Sharpe `0.87`, MDD `-5.82%`, turnover `0.0444`, cost `72.01`, vs naive delta `-137014.66`
  - `1.0x`: ending NAV `2073629.18`, annualized return `6.56%`, Sharpe `0.79`, MDD `-5.63%`, turnover `0.0126`, cost `20.86`, vs naive delta `-152574.05`
  - `2.0x`: ending NAV `2069840.99`, annualized return `6.37%`, Sharpe `0.78`, MDD `-5.51%`, turnover `0.0000`, cost `0.00`, vs naive delta `-156362.24`
- Sweep interpretation:
  - best tested multiplier remains `0.1x` on both ending NAV and Sharpe
  - however, all tested calibrated-`k` points underperform naive by a wide margin
  - comparing the three sweep rounds (`k = 0.015`, `k = 1.4457`, `k = 3.4984`) shows the entire frontier shifting downward as `k` rises
  - this indicates the core issue is not multiplier choice alone
- Structural optimizer diagnosis:
  - the earlier zero-cost probe already showed:
    - cost bundle `0.0x` -> optimizer matched naive exactly
  - therefore, in the current expanded-US monthly setup:
    - the risk term is not materially moving construction away from naive
    - the optimizer's main active lever is cost-driven trade suppression
    - once costs become realistic, suppressing trades sacrifices more holding return than it saves in explicit cost
- Decision outcome:
  - do **not** promote calibrated `k = 3.498400399110418` into `config/us_expanded.yaml`
  - keep the calibrated value in:
    - `config/us_expanded_tca_calibrated.yaml`
  - write the decision rationale to:
    - `docs/cost_model_decision_note.md`
- Priority after Phase 13 closeout:
  - next high-value research step is a risk-aversion / risk-term sweep
  - goal:
    - make the optimizer construct portfolios materially different from naive before re-evaluating default cost-model promotion

## 2026-04-01 Risk Aversion Sweep Implementation And Real-Data Run

- A parallel risk-aversion sweep path is now implemented for the historical backtest stack.
- Additive code surface:
  - `src/portfolio_os/backtest/sweep.py`
    - added:
      - `_RISK_WEIGHT_KEYS = ("risk_term",)`
      - `RiskSweepRunResult`
      - `RiskAversionSweepResult`
      - `_risk_scaled_config_payload(...)`
      - `_build_risk_sweep_summary_frame(...)`
      - `run_backtest_risk_sweep(...)`
  - `src/portfolio_os/backtest/report.py`
    - added:
      - `render_risk_sweep_report(...)`
  - `src/portfolio_os/backtest/__init__.py`
    - exports now include:
      - `RiskAversionSweepResult`
      - `run_backtest_risk_sweep`
      - `render_risk_sweep_report`
  - `src/portfolio_os/api/cli.py`
    - added:
      - `risk_sweep_app`
  - `pyproject.toml`
    - added script entrypoint:
      - `portfolio-os-risk-sweep`
- Regression coverage added in `tests/test_backtest.py` for:
  - risk-term scaling behavior
  - CLI output generation
  - risk-model relative-path preservation
  - summary-frame annualized volatility field
  - protection against accidental cost-weight scaling during risk sweeps
- Focused verification after implementation:
  - `python -m pytest tests/test_backtest.py -k "risk_sweep" -q` -> `5 passed, 6 deselected, 14 warnings`
- Full regression completed during implementation:
  - `python -m pytest tests/ -x -q` -> `282 passed, 28 warnings`

- Real expanded-US risk-aversion sweep was then executed against:
  - manifest:
    - `data/backtest_samples/manifest_us_expanded.yaml`
  - output root:
    - `outputs/risk_sweep_us_expanded`
  - tested multipliers:
    - `1.0`
    - `100.0`
    - `1000.0`
    - `10000.0`
    - `100000.0`
- Environment note:
  - `portfolio-os-risk-sweep` was not available on the current shell PATH
  - the sweep was executed successfully via a direct Python invocation of `risk_sweep_app` from the local `src/` tree with the same argument set
- All five tested multipliers completed successfully:
  - each run directory under `outputs/risk_sweep_us_expanded/runs/` produced `backtest_results.json`
  - no solver infeasibility or runtime failure was observed in this sweep
- Real-data sweep summary:
  - `1.0x`: ending NAV `2152740.75`, annualized return `10.66%`, annualized volatility `9.54%`, Sharpe `1.11`, MDD `-6.65%`, turnover `0.1090`, cost `173.39`, vs naive delta `-73462.48`
  - `100.0x`: ending NAV `2152681.31`, annualized return `10.66%`, annualized volatility `9.54%`, Sharpe `1.11`, MDD `-6.65%`, turnover `0.1089`, cost `173.26`, vs naive delta `-73521.92`
  - `1000.0x`: ending NAV `2149983.75`, annualized return `10.52%`, annualized volatility `9.52%`, Sharpe `1.10`, MDD `-6.62%`, turnover `0.1059`, cost `167.74`, vs naive delta `-76219.48`
  - `10000.0x`: ending NAV `2135885.84`, annualized return `9.79%`, annualized volatility `9.02%`, Sharpe `1.08`, MDD `-6.28%`, turnover `0.0954`, cost `150.61`, vs naive delta `-90317.39`
  - `100000.0x`: ending NAV `2081894.37`, annualized return `6.99%`, annualized volatility `6.83%`, Sharpe `1.03`, MDD `-4.95%`, turnover `0.1056`, cost `168.27`, vs naive delta `-144308.86`
- Immediate interpretation:
  - the risk term now appears to be materially participating in portfolio construction:
    - `optimizer_vs_naive_ending_nav_delta` moves farther away from zero as the multiplier rises
    - annualized volatility declines steadily from `9.54%` at `1.0x` to `6.83%` at `100000.0x`
  - the sweep does show a risk-return tradeoff axis:
    - higher multipliers reduce volatility and drawdown
    - but they also reduce ending NAV, annualized return, and Sharpe
  - best tested point inside this grid remains the low-risk-weight edge:
    - best ending NAV multiplier: `1.0x`
    - best Sharpe multiplier: `1.0x`
    - lowest volatility multiplier: `100000.0x`
  - practical conclusion:
    - risk-weight tuning can now move the optimizer away from naive
    - however, in this first tested range, stronger risk aversion improves defensiveness at the cost of enough return that optimizer relative performance versus naive becomes even worse, not better
- Artifacts to inspect next:
  - `outputs/risk_sweep_us_expanded/risk_sweep_summary.csv`
  - `outputs/risk_sweep_us_expanded/risk_aversion_frontier_report.md`

## 2026-04-01 Replace-Mode Risk Sweep Follow-Up

- Path A diagnostic was executed to test whether the poor augment-mode frontier was primarily caused by the legacy `target_deviation` term dominating the objective.
- New research inputs created:
  - config:
    - `config/us_expanded_tca_calibrated_replace.yaml`
  - manifest:
    - `data/backtest_samples/manifest_us_expanded_tca_calibrated_replace.yaml`
- Replace-mode config semantics:
  - calibrated slippage:
    - `k = 3.498400399110418`
  - `risk_model.integration_mode = replace`
  - explicit active objective weights:
    - `risk_term = 1.0`
    - `tracking_error = 1.0`
    - `transaction_cost = 1.0`
- Focused regression confidence before the run:
  - `python -m pytest tests/test_backtest.py -k "risk_sweep" -q` -> `5 passed, 6 deselected, 14 warnings`
- Real replace-mode risk sweep was run against:
  - manifest:
    - `data/backtest_samples/manifest_us_expanded_tca_calibrated_replace.yaml`
  - output root:
    - `outputs/risk_sweep_us_expanded_tca_calibrated_replace`
  - tested multipliers:
    - `1.0`
    - `100.0`
    - `1000.0`
    - `10000.0`
    - `100000.0`
- Environment note:
  - the shell still did not expose `portfolio-os-risk-sweep` directly on PATH
  - the run completed successfully via a direct Python invocation of `risk_sweep_app` from `src/`
- All five replace-mode run directories produced `backtest_results.json`; there was no solver infeasibility or runtime failure.
- Replace-mode sweep summary:
  - `1.0x`: ending NAV `2069840.99`, annualized return `6.37%`, annualized volatility `8.40%`, Sharpe `0.78`, MDD `-5.51%`, turnover `0.0000`, cost `0.00`, vs naive delta `-156362.24`
  - `100.0x`: ending NAV `2069840.99`, annualized return `6.37%`, annualized volatility `8.40%`, Sharpe `0.78`, MDD `-5.51%`, turnover `0.0000`, cost `0.00`, vs naive delta `-156362.24`
  - `1000.0x`: ending NAV `2069840.99`, annualized return `6.37%`, annualized volatility `8.40%`, Sharpe `0.78`, MDD `-5.51%`, turnover `0.0000`, cost `0.00`, vs naive delta `-156362.24`
  - `10000.0x`: ending NAV `2069840.99`, annualized return `6.37%`, annualized volatility `8.40%`, Sharpe `0.78`, MDD `-5.51%`, turnover `0.0000`, cost `0.00`, vs naive delta `-156362.24`
  - `100000.0x`: ending NAV `2048388.03`, annualized return `5.25%`, annualized volatility `7.23%`, Sharpe `0.75`, MDD `-5.08%`, turnover `0.0675`, cost `107.71`, vs naive delta `-177815.20`
- Direct diagnostic conclusion:
  - replace mode did **not** uncover a better frontier or a Sharpe sweet spot
  - multipliers `1.0x` through `10000.0x` collapsed to the exact same outcome:
    - same NAV
    - same return
    - same volatility
    - zero turnover
    - zero cost
  - only at `100000.0x` did the optimizer move to a new point, and that point was worse on:
    - ending NAV
    - annualized return
    - Sharpe
    - vs-naive ending NAV delta
- Comparison versus the earlier augment-mode sweep:
  - augment mode already showed a monotonic risk-return tradeoff with no Sharpe sweet spot
  - replace mode is even less attractive:
    - lower Sharpe than augment at every tested multiplier
    - much worse `optimizer_vs_naive_ending_nav_delta` even at the low-weight edge
    - near-degenerate behavior across `1x` to `10000x`
- Strongest current interpretation:
  - removing legacy `target_deviation` was **not** enough to rescue the objective
  - in the current monthly expanded-US setup, a risk-only objective without an explicit alpha / expected-return signal still behaves as drag rather than as a better optimizer
  - this pushes the research program toward Path C:
    - keep the risk sweep tooling as infrastructure
    - archive the result as evidence that no Sharpe-improving sweet spot was found in either augment or replace mode
    - defer further risk-aware portfolio construction work until an explicit alpha / expected-return signal or a different construction objective is introduced
- Artifacts:
  - `outputs/risk_sweep_us_expanded_tca_calibrated_replace/risk_sweep_summary.csv`
  - `outputs/risk_sweep_us_expanded_tca_calibrated_replace/risk_aversion_frontier_report.md`

## 2026-04-01 ML+RL Platform Roadmap And Phase 1 Alpha Core Kickoff

- The project now has an explicit platform-level roadmap for the longer-term strategy:
  - `docs/platform_ml_rl_roadmap.md`
- Design and implementation planning for the first alpha slice were also written into project docs:
  - design:
    - `docs/superpowers/specs/2026-04-01-phase-1-us-alpha-core-design.md`
  - implementation plan:
    - `docs/superpowers/plans/2026-04-01-phase-1-us-alpha-core.md`
- Strategic direction is now explicit:
  - shared research core
  - market-specific adapters
  - US-first depth
  - US/CN portability later
  - ML for alpha/risk/cost prediction
  - optimizer for allocation
  - RL for sequential execution and control

- Phase 1 first implementation slice is now live:
  - new package:
    - `src/portfolio_os/alpha/`
  - public baseline workflow:
    - `run_alpha_research(...)`
  - standalone CLI:
    - `alpha_research_app`
    - poetry entrypoint:
      - `portfolio-os-alpha-research`
- Current Phase 1 scope is intentionally research-only:
  - consumes `returns_long.csv`
  - builds deterministic baseline signals:
    - short-horizon reversal
    - medium-horizon momentum with skip window
  - blends signals into `alpha_score`
  - computes forward-return labels
  - emits:
    - per-security signal panel
    - per-date IC diagnostics
    - JSON summary
    - markdown report
- Important boundary:
  - alpha is **not** yet connected into the optimizer objective
  - this first slice establishes the reusable forecast-and-evaluation layer before portfolio-construction integration

- New files added for the implementation:
  - `src/portfolio_os/alpha/research.py`
  - `src/portfolio_os/alpha/report.py`
  - `src/portfolio_os/alpha/__init__.py`
  - `tests/test_alpha_research.py`
- Existing files extended:
  - `src/portfolio_os/api/cli.py`
  - `pyproject.toml`

- Regression coverage added in `tests/test_alpha_research.py` for:
  - signal panel construction
  - alpha-score ordering on a synthetic trending fixture
  - positive rank-IC and top-bottom spread on the baseline fixture
  - artifact writing
  - CLI writing path

- Verification completed after implementation:
  - `python -m pytest tests/test_alpha_research.py -q` -> `5 passed`
  - `python -m pytest tests/test_alpha_research.py tests/test_backtest.py -q` -> `16 passed, 26 warnings`

- Real-data smoke run completed on the frozen expanded-US returns set:
  - command path:
    - direct Python invocation of `alpha_research_app` from `src/`
  - input:
    - `data/risk_inputs_us_expanded/returns_long.csv`
  - output root:
    - `outputs/alpha_research_us_expanded_smoke`
  - artifacts written:
    - `alpha_signal_panel.csv`
    - `alpha_ic_by_date.csv`
    - `alpha_research_summary.json`
    - `alpha_research_report.md`
- Smoke summary on current defaults:
  - parameters:
    - reversal lookback `21`
    - momentum lookback `126`
    - momentum skip `21`
    - forward horizon `5`
    - equal weights `0.5 / 0.5`
  - universe:
    - `50` tickers
    - `100` evaluation dates
  - diagnostics:
    - `mean_ic = -0.0029675551791283235`
    - `mean_rank_ic = 0.0023249785139220847`
    - `positive_rank_ic_ratio = 0.46`
    - `mean_top_bottom_spread = -0.0052516018996480365`
- Initial interpretation:
  - the baseline signal scaffold works end-to-end
  - but the first equal-weight reversal+momentum blend is not yet a production-quality alpha on the frozen expanded-US sample
  - this is expected and acceptable for Phase 1 kickoff
  - the immediate next step inside Phase 1 is signal iteration:
    - improve feature definitions
    - add neutralization / standardization where appropriate
    - compare alternative blends before connecting alpha into the optimizer

## 2026-04-01 Phase 1 Alpha Component Diagnostics Extension

- Phase 1 alpha research now evaluates signal components side-by-side instead of only reporting the final blended score.
- Research workflow changes:
  - `run_alpha_research(...)` now computes per-date diagnostics for:
    - `reversal_only`
    - `momentum_only`
    - `blended_alpha`
  - the result object now carries:
    - `signal_summary_frame`
  - the workflow now writes one additional artifact:
    - `alpha_signal_summary.csv`
- Reporting changes:
  - `alpha_research_summary.json` now includes:
    - `primary_signal_name`
    - `best_signal_name`
    - `best_signal_mean_rank_ic`
    - `signal_summaries`
  - `alpha_research_report.md` now includes a `Signal Leaderboard` section before the per-date IC table
  - the per-date IC table remains focused on the primary signal:
    - `blended_alpha`
- Code surface touched:
  - `src/portfolio_os/alpha/research.py`
  - `src/portfolio_os/alpha/report.py`
  - `src/portfolio_os/api/cli.py`
  - `tests/test_alpha_research.py`
- Focused verification after the change:
  - `python -m pytest tests/test_alpha_research.py -q` -> `6 passed`
  - `python -m pytest tests/test_alpha_research.py tests/test_backtest.py -q` -> `17 passed, 26 warnings`
- Real-data smoke run completed on the frozen expanded-US returns set:
  - output root:
    - `outputs/alpha_research_us_expanded_component_diagnostics`
  - leaderboard result on current default parameters:
    - `momentum_only`:
      - `mean_rank_ic = 0.0169594237695078`
      - `mean_top_bottom_spread = 0.004812966103636091`
    - `blended_alpha`:
      - `mean_rank_ic = 0.0023249785139220847`
      - `mean_top_bottom_spread = -0.0052516018996480365`
    - `reversal_only`:
      - `mean_rank_ic = -0.011141416566626648`
      - `mean_top_bottom_spread = -0.0048010166983345395`
- Immediate interpretation:
  - on the current frozen expanded-US sample, `momentum_only` is the strongest of the three evaluated signals
  - the current equal-weight blend is worse than pure momentum
  - the current reversal component is directionally harmful on average in this sample
- Recommended next Phase 1 step:
  - iterate the signal recipe around the momentum leg first
  - either reduce or remove the reversal weight before expanding into broader parameter sweeps or optimizer integration
