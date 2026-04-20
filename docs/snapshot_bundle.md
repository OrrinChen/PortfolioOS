# Snapshot Bundle

Phase 11 adds a snapshot bundle builder:

- `portfolio-os-build-snapshot`

The purpose is to lock a complete static input package for downstream PortfolioOS runs.

## CLI

Example:

```bash
py -3.11 -m poetry run portfolio-os-build-snapshot ^
  --tickers-file data/sample/tickers.txt ^
  --index-code 000300.SH ^
  --as-of-date 2026-03-23 ^
  --provider mock ^
  --reference-overlay data/sample/reference_overlay_example.csv ^
  --output-dir data/generated/snapshot_mock
```

Optional:

```bash
py -3.11 -m poetry run portfolio-os-build-snapshot ... --allow-partial-build
```

With `--allow-partial-build`, the command returns successfully even if one child step is permission-limited, as long as the partial bundle and manifests were written.

Tushare example:

```bash
py -3.11 -m poetry run portfolio-os-build-snapshot ^
  --tickers-file data/sample/tickers.txt ^
  --index-code 000300.SH ^
  --as-of-date 2026-03-23 ^
  --provider tushare ^
  --provider-token YOUR_TOKEN ^
  --reference-overlay data/sample/reference_overlay_example.csv ^
  --output-dir data/generated/snapshot_real
```

## Output Directory Structure

The snapshot bundle writes:

- `market.csv`
- `reference.csv`
- `target.csv`
- `market_manifest.json`
- `reference_manifest.json`
- `target_manifest.json`
- `snapshot_manifest.json`

If one child step is permission-limited or otherwise fails:

- successful child outputs are still preserved
- child failure manifests are still written
- `snapshot_manifest.json` records step-level status and a recommended continuation path

## Snapshot Manifest

`snapshot_manifest.json` records:

- `provider`
- `provider_token_source`
- `as_of_date`
- `tickers_input_path`
- `index_code`
- `overlay_path`
- `generated_at`
- `child_manifests`
- `output_files`
- `notes`

It now also records:

- `build_status`
- `provider_capability_status`
- `fallback_notes`
- `permission_notes`
- `recommended_alternative_path`
- step-level `build_status` for market, reference, and target

The child manifests are linked by path, size, and sha256 metadata.

The output files are also linked by path, size, and sha256 metadata.

## Partial Success

Typical permission-limited case:

- `market.csv`: success
- `reference.csv`: success
- `target.csv`: failed because `index_weight` is unavailable

In that case:

- `market.csv` and `reference.csv` are preserved
- `target_manifest.json` is still written with a failure status
- `snapshot_manifest.json` explains which step failed and why
- the recommended continuation path is usually:
  - provide `target.csv` from the client side and continue

## Why Snapshot Locking Matters

The bundle provides a lightweight static lock on the exact cross-sectional files used downstream.

That matters because it lets the workflow say:

- these were the exact market, reference, and target files
- these were the exact hashes
- these were the exact provider settings and date inputs
- this was the static package used for the rebalance run

This is useful for pilot workflows because it improves traceability without adding:

- a database
- a task system
- a data platform
- a broker integration layer

## Downstream Consumption

Once built, the snapshot can be consumed directly by the main rebalance CLI:

```bash
py -3.11 -m poetry run portfolio-os ^
  --holdings data/sample/holdings_example.csv ^
  --target data/generated/snapshot_mock/target.csv ^
  --market data/generated/snapshot_mock/market.csv ^
  --reference data/generated/snapshot_mock/reference.csv ^
  --portfolio-state data/sample/portfolio_state_example.yaml ^
  --constraints config/constraints/public_fund.yaml ^
  --config config/default.yaml ^
  --execution-profile config/execution/conservative.yaml ^
  --output-dir outputs/demo_run_built ^
  --skip-benchmarks
```

That is the main value of the bundle:

- build once
- lock once
- consume downstream without remapping

If automatic target construction is permission-limited, the continuation path is:

1. keep the generated `market.csv`
2. keep the generated `reference.csv`
3. add client-provided `target.csv`
4. run `portfolio-os`
