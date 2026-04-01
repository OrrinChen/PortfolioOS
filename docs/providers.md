# Providers

PortfolioOS uses a small provider abstraction for the data-preparation layer.

The intent is simple:

- builders ask a provider for static snapshot data
- providers return a small standard set of fields
- builders convert that into PortfolioOS input files

This is not a universal market-data framework. It is a minimal interface designed for pilot-ready static workflows.

## Provider Abstraction

Current protocol methods:

1. `get_daily_market_snapshot(tickers, as_of_date)`
2. `get_reference_snapshot(tickers, as_of_date)`
3. `get_index_weights(index_code, as_of_date)`

The protocol lives in:

- `src/portfolio_os/data/providers/base.py`

## Mock Provider

`MockDataProvider` is:

- the default builder provider
- fully offline
- the main testing path
- stable and deterministic

It is useful for:

- CI
- local demos
- validating builder and snapshot-bundle logic

## Tushare Provider

`TushareProvider` is the first real provider path.

It uses the Tushare Pro HTTP endpoint and maps data into the PortfolioOS standard schemas.

Current endpoint usage:

- market:
  - `daily`
  - `stk_limit`
- reference:
  - `stock_basic`
  - `daily_basic`
- target:
  - `index_weight`

### Token Configuration

Token priority:

1. CLI `--provider-token`
2. environment variable `TUSHARE_TOKEN`

If neither is provided, PortfolioOS fails fast with a clear error.

The token value itself is never written into manifests. Only the token source is recorded, for example:

- `cli`
- `env`

## Tushare Permission Dependencies

Current practical dependency map:

- `market`:
  - primary: `daily`
  - optional enhancement: `stk_limit`
- `reference`:
  - primary: `stock_basic`
  - optional enhancement: `daily_basic`
  - fallback: `bak_basic`
- `target`:
  - primary: `index_weight`

What this means operationally:

- missing `stk_limit` is a degradation, not a hard stop
- missing `stock_basic` or `daily_basic` may still be survivable if `bak_basic` is available
- missing `index_weight` blocks automatic target construction, but does not block the broader real pilot path if the client already has `target.csv`

## Current Field Mapping Notes

### Market snapshot

Target fields:

- `ticker`
- `close`
- `vwap`
- `adv_shares`
- `tradable`
- `upper_limit_hit`
- `lower_limit_hit`

Current Tushare mapping:

- `close`: from `daily.close`
- `vwap`: approximated from `daily.amount` and `daily.vol`
- `adv_shares`: approximated from recent daily volume history
- `tradable`: approximated from the existence of a daily row and positive daily volume
- `upper_limit_hit` / `lower_limit_hit`: derived from `stk_limit` and daily close when available, otherwise from `pre_close` plus board-based limit heuristics

Approximation notes matter here:

- `daily.vol` is treated as lot-based volume and converted into shares
- `daily.amount` and `daily.vol` are used to derive an approximate per-share VWAP
- limit-hit status is inferred from close versus daily limit prices
- when `stk_limit` is unavailable, limit-hit status falls back to `pre_close` plus board-based limit heuristics

### Reference snapshot

Target fields:

- `ticker`
- `industry`
- `benchmark_weight`
- `issuer_total_shares`

Current Tushare mapping:

- `industry`: from `stock_basic.industry` when available, otherwise from `bak_basic.industry`
- `issuer_total_shares`: from `daily_basic.total_share` when available, otherwise from `bak_basic.total_share`
- `benchmark_weight`: not populated by `get_reference_snapshot`

Scaling note:

- `daily_basic.total_share` is treated as 10k-share units and scaled into shares
- `bak_basic.total_share` is treated as 亿 shares and scaled into shares

### Index weights

Target fields:

- `ticker`
- `target_weight`

Current Tushare mapping:

- `target_weight`: from `index_weight.weight / 100`
- if the requested date has no exact weight record, the latest available trade date on or before `as_of_date` is used

Permission note:

- `index_weight` still requires the account to have access to that endpoint

## Permission Failure vs System Failure

PortfolioOS now treats these differently:

Permission-limited or quota-limited cases:

- builder manifests record `build_status = failed_permission`
- provider capability status becomes `unavailable` or `degraded`
- manifests include `permission_notes` and `recommended_alternative_path`

Normal data problems:

- builder manifests record `failed_data`

Runtime problems such as network transport errors:

- builder manifests record `failed_runtime`

That distinction matters because permission failures are often a product onboarding issue, not a software defect.

## How To Extend Later

Future providers such as AkShare or JoinQuant should keep the same small protocol:

- return provider-level rows
- let builders remain unchanged
- keep approximation notes explicit

That way the project can gain real provider options without turning into a large integration platform.
