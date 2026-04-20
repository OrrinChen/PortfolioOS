# Data Builders

Phase 10 adds a lightweight data feed preparation layer for PortfolioOS.
Phase 11 extends it with a real `TushareProvider` path and a snapshot bundle CLI.

The goal is not to build a data platform. The goal is to make static cross-sectional inputs easier to prepare in a pilot workflow.

This layer prepares three standard files that the existing main CLI already understands:

- `market.csv`
- `reference.csv`
- `target.csv`

## Provider Abstraction

PortfolioOS now defines a small provider protocol in `src/portfolio_os/data/providers/base.py`.

Required provider methods:

1. `get_daily_market_snapshot(tickers, as_of_date)`
2. `get_reference_snapshot(tickers, as_of_date)`
3. `get_index_weights(index_code, as_of_date)`

The default built-in provider is:

- `MockDataProvider`

The first real provider path now available is:

- `TushareProvider`

It is intentionally offline and stable so tests do not depend on network access.

This means the current system can run entirely locally, while future work can add alternative providers such as:

- AkShare
- JoinQuant

Those future providers only need to implement the same small protocol.

## Builder CLIs

### `portfolio-os-build-market`

Input:

- `--tickers-file`
- `--as-of-date`
- `--provider`
- optional `--provider-token`
- `--output`

Output schema:

```csv
ticker,close,vwap,adv_shares,tradable,upper_limit_hit,lower_limit_hit
```

Notes:

- `vwap` may be an approximate daily VWAP proxy
- `adv_shares` may be a historical average-volume proxy
- this layer validates basic completeness and positivity, but it is not a full data-governance system

### `portfolio-os-build-reference`

Input:

- `--tickers-file`
- `--as-of-date`
- `--provider`
- optional `--provider-token`
- `--output`
- optional `--overlay`

Output schema:

```csv
ticker,industry,blacklist_buy,blacklist_sell,benchmark_weight,manager_aggregate_qty,issuer_total_shares
```

Provider responsibility:

- `industry`
- optional `benchmark_weight`
- optional `issuer_total_shares`

Overlay responsibility:

- `blacklist_buy`
- `blacklist_sell`
- `manager_aggregate_qty`

### `portfolio-os-build-target`

Input:

- `--index-code`
- `--as-of-date`
- `--provider`
- optional `--provider-token`
- `--output`

Output schema:

```csv
ticker,target_weight
```

Companion output:

- `target_manifest.json`

The manifest records:

- `index_code`
- `as_of_date`
- `provider`
- input weight sum
- output weight sum
- whether normalization was applied

Recommended usage note:

- use `portfolio-os-build-target` when the provider account has index-weight access
- otherwise treat client-provided `target.csv` as the primary real pilot path

### `portfolio-os-build-snapshot`

Input:

- `--tickers-file`
- `--index-code`
- `--as-of-date`
- `--provider`
- optional `--provider-token`
- optional `--reference-overlay`
- `--output-dir`

Output directory:

- `market.csv`
- `reference.csv`
- `target.csv`
- `market_manifest.json`
- `reference_manifest.json`
- `target_manifest.json`
- `snapshot_manifest.json`

## Overlay Merge Mechanism

Reference builder merge order:

1. provider generates the base reference rows
2. optional overlay is loaded locally
3. overlay values overwrite matched tickers for:
   - `blacklist_buy`
   - `blacklist_sell`
   - `manager_aggregate_qty`
4. the final output is written back in the standard `reference.csv` schema

The overlay is file-based and explicit. There is no hidden state or database.

The overlay is applied after provider data is assembled but before the final standard `reference.csv` is written.

## Target Weight Sum Handling

The target builder validates provider weights before writing `target.csv`.

Rules:

- negative weights are rejected
- sums materially above `1.0` are rejected
- small near-`1.0` deviations can be normalized
- the result is recorded in `target_manifest.json`

Current behavior:

- exact or already-valid sums are written as-is
- small floating-point drift near `1.0` can be normalized
- large deviations above tolerance fail fast

This keeps the behavior transparent and avoids silently masking bad provider outputs.

## Client-Provided Target Mode

This is now a formal real-feed pilot path:

1. build `market.csv` from Tushare
2. build `reference.csv` from Tushare
3. keep `target.csv` client-provided
4. run `portfolio-os`

This path matters because automatic target construction from `index_weight` is an enhancement, not the only real-world entry point.

Use client-provided `target.csv` when:

- the desk already has a PM target file
- the provider account does not have `index_weight` access
- the pilot should focus on rebalance, controls, handoff, and execution-preflight rather than benchmark replication

## Builder Manifests

Each builder now records:

- `provider`
- `provider_token_source`
- `as_of_date`
- `request_parameters`
- `output_path`
- `output_sha256`
- `row_count`
- `generated_at`
- `approximation_notes`

This keeps the data-preparation step auditable without turning it into a data platform.

## Sample Inputs

Current sample helpers:

- `data/sample/tickers.txt`
- `data/sample/reference_overlay_example.csv`

You can also use:

- a one-column CSV with `ticker`
- a simple CSV where the first column contains tickers

## Provider Token Configuration

For `tushare`, token priority is:

1. `--provider-token`
2. `TUSHARE_TOKEN`

If neither exists, the CLI fails fast with a clear error.

The token value itself is never written to any manifest.

## Current Default: Mock Provider

The offline mock provider is the current default for demos and tests.

It is useful because:

- no network is required
- results are stable
- builder outputs are immediately consumable by the main PortfolioOS CLI

This is the current pilot-ready story:

- provider interface first
- mock implementation today
- future real providers later

Phase 11 extends that story:

- mock stays the default and testing path
- tushare is the first real provider path
- snapshot bundles lock a full static package for downstream rebalance runs

## What This Is Not

This layer is intentionally limited:

- not a data warehouse
- not a scheduler
- not a historical data platform
- not a complex ETL framework
- not a mandatory online integration

It is a local data preparation layer that helps produce the exact static files PortfolioOS already consumes.
