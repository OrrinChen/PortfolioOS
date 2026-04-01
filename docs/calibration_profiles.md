# Calibration Profiles

Phase 9 adds a lightweight execution calibration layer for the local execution simulator.

Calibration profiles are static parameter templates. They help PortfolioOS align execution-preflight assumptions with different client execution styles without adding ML, live connectivity, or a complex market simulator.

## Profile Structure

Example:

```yaml
name: balanced_day
description: Balanced intraday calibration for normal pilot demos
market_curve:
  buckets:
    - label: open
      volume_share: 0.25
      slippage_multiplier: 1.2
    - label: mid_morning
      volume_share: 0.2
      slippage_multiplier: 1.0
    - label: midday
      volume_share: 0.1
      slippage_multiplier: 0.9
    - label: afternoon
      volume_share: 0.2
      slippage_multiplier: 1.0
    - label: close
      volume_share: 0.25
      slippage_multiplier: 1.15
defaults:
  participation_limit: 0.2
  allow_partial_fill: true
  force_completion: false
```

Fields:

- `market_curve.buckets[*].label`
- `market_curve.buckets[*].volume_share`
- `market_curve.buckets[*].slippage_multiplier`
- `defaults.participation_limit`
- `defaults.allow_partial_fill`
- `defaults.force_completion`

`volume_share` must sum to `1.0`.

## Priority Rules

PortfolioOS resolves execution-preflight settings in this order:

1. request overrides
2. selected calibration profile
3. execution-profile default calibration

More specifically:

- request inline `market_curve` overrides the calibration profile curve
- request `simulation.allow_partial_fill` overrides calibration defaults
- request `simulation.force_completion` overrides calibration defaults
- request `simulation.max_bucket_participation_override` overrides default participation assumptions
- `--calibration-profile` overrides any `calibration_profile` path inside the request
- if no calibration profile is passed, PortfolioOS falls back to the execution profile's `default_calibration_profile`
- if the execution profile also does not name one, PortfolioOS falls back to `balanced_day`

The resolved choices are written into `execution_report.json` under `resolved_calibration`.

## Current Templates

`liquid_midday`

- intended for liquid names or desks comfortable with stronger midday participation
- more evenly distributed intraday volume
- slightly lower midday slippage multipliers
- higher default participation assumption

`balanced_day`

- intended as the default pilot profile
- balanced open / close emphasis
- moderate default participation
- good general-purpose demo profile

`tight_liquidity`

- intended for thinner liquidity or more conservative execution review
- heavier open / close concentration
- higher slippage multipliers
- lower default participation assumption

`low_liquidity_stress`

- intended for stress testing low-liquidity execution risk
- very low default participation
- higher open/close slippage multipliers
- useful for baseline-vs-stress comparison in execution reports

## CLI Usage

```bash
py -3.11 -m poetry run portfolio-os-execute ^
  --request data/execution_samples/execution_request_calibrated.yaml ^
  --calibration-profile config/calibration_profiles/balanced_day.yaml ^
  --output-dir outputs/execution_demo
```

## What This Is Not

Calibration profiles are intentionally simple:

- not ML execution fitting
- not realized-fill calibration against broker logs
- not venue routing logic
- not live market microstructure modeling

They are a file-based way to express execution style assumptions for pilot reviews.
