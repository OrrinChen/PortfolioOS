# Import Profiles

Phase 9 adds a lightweight pilot integration layer for mapped CSV inputs.

Import profiles let PortfolioOS accept local client exports that do not already use the native column names, while still converting them into the same standard internal models used by the rest of the workflow.

This is intentionally declarative:

- no scripts
- no database
- no UI
- no custom parser plugins

## Supported File Types

Current profile sections:

- `holdings`
- `target`
- `market`
- `reference`

`portfolio_state.yaml` is unchanged and still uses the native YAML schema.

## Profile Structure

Example:

```yaml
name: custodian_style_a
description: Example pilot mapping for a custodian-style export
holdings:
  required_fields:
    - ticker
    - quantity
  columns:
    ticker: security_code
    quantity: position_qty
    avg_cost: avg_cost_cny
target:
  required_fields:
    - ticker
    - target_weight
  columns:
    ticker: security_code
    target_weight: target_weight_pct
  numeric_scales:
    target_weight: 0.01
market:
  required_fields:
    - ticker
    - close
    - adv_shares
    - tradable
    - upper_limit_hit
    - lower_limit_hit
  columns:
    ticker: security_code
    close: last_px
    vwap: day_vwap
    adv_shares: avg_daily_volume
    tradable: is_tradable
    upper_limit_hit: up_limit_flag
    lower_limit_hit: down_limit_flag
  boolean_values:
    tradable:
      y: true
      n: false
reference:
  required_fields:
    - ticker
    - industry
    - blacklist_buy
    - blacklist_sell
  columns:
    ticker: security_code
    industry: sector_name
    blacklist_buy: buy_restricted
    blacklist_sell: sell_restricted
  defaults:
    blacklist_buy: false
    blacklist_sell: false
```

## Supported Mapping Features

`columns`

- maps internal standard field name to external source column name
- example:
  - internal `ticker`
  - external `security_code`

`defaults`

- fills missing or null values after mapping
- useful for optional flags such as `blacklist_buy` and `blacklist_sell`

`boolean_values`

- per-field declarative boolean normalization
- values are matched case-insensitively after trimming
- useful for `Y/N`, `Open/Blocked`, `1/0` style exports

`numeric_scales`

- multiplies numeric fields after mapping
- useful for percentage exports such as `8` meaning `8%`

`required_fields`

- internal fields that must exist after mapping/default application
- PortfolioOS fails fast when a required mapped field cannot be produced

## Default Behavior

If `--import-profile` is not passed:

- PortfolioOS reads the current native CSV schemas directly
- no remapping is applied
- all existing standard-schema behavior remains unchanged

If `--import-profile config/import_profiles/standard.yaml` is passed:

- the profile behaves as an explicit identity mapping
- this can still be useful for pilot documentation or controlled onboarding

## CLI Usage

Single run:

```bash
py -3.11 -m poetry run portfolio-os ^
  --holdings data/import_profile_samples/custodian_style_a/holdings.csv ^
  --target data/import_profile_samples/custodian_style_a/target.csv ^
  --market data/import_profile_samples/custodian_style_a/market.csv ^
  --reference data/import_profile_samples/custodian_style_a/reference.csv ^
  --portfolio-state data/sample/portfolio_state_example.yaml ^
  --constraints config/constraints/public_fund.yaml ^
  --config config/default.yaml ^
  --execution-profile config/execution/conservative.yaml ^
  --import-profile config/import_profiles/custodian_style_a.yaml ^
  --output-dir outputs/demo_run_mapped
```

Replay and scenarios can also take the same `--import-profile` flag.

## What Is Still Not Supported

- arbitrary transformation scripts
- custom formulas
- row-level joins or multi-file ETL
- Excel workbook parsing magic
- portfolio-state YAML remapping

The goal is to make pilot file onboarding easier, not to turn PortfolioOS into a general integration platform.
