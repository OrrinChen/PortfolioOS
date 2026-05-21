"""D3 SignalBuilder for the frozen open-market insider buying MeasurementSpec."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import pandas as pd
import yaml


SUMMARY_SCHEMA_VERSION = "insider_open_market_buying_signal_builder_summary.v1"
STAGE = "D3-INSIDER-02"
TRANSACTION_CODE_SCOPE = "open_market_or_private_purchase"
PRIVATE_PURCHASE_FILTER_STATUS = "unavailable_from_form4_code_only"
P_CODE_SCOPE_WARNING = (
    "Form 4 code P means open-market or private purchase; this D3 builder "
    "does not prove every code-P event is exchange open-market."
)

FORBIDDEN_INPUT_PATTERNS = (
    "forward_return",
    "future",
    "optimizer",
    "portfolio",
    "expected_return",
)
FORBIDDEN_INPUT_PREFIXES = ("car_", "q1_", "q2_")

ROLE_WEIGHTS = {
    "ceo": 1.30,
    "cfo": 1.30,
    "other_officer": 1.15,
    "director": 1.00,
    "ten_pct_owner": 0.80,
}


@dataclass(frozen=True)
class InsiderOpenMarketBuyingSignalBuilderResult:
    """Artifacts and summary for D3 signal building."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_open_market_buying_signal_builder(
    event_registry_path: str | Path,
    measurement_spec_path: str | Path,
    output_dir: str | Path,
) -> InsiderOpenMarketBuyingSignalBuilderResult:
    """Build timestamp-safe D3 signal artifacts from the frozen MeasurementSpec."""

    event_path = Path(event_registry_path)
    spec_path = Path(measurement_spec_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    event_header = pd.read_csv(event_path, nrows=0)
    forbidden_columns = _forbidden_input_columns(event_header.columns)
    if forbidden_columns:
        raise ValueError(f"forbidden input columns: {', '.join(forbidden_columns)}")

    events = pd.read_csv(event_path).fillna("")
    spec_text = spec_path.read_text(encoding="utf-8")
    spec = yaml.safe_load(spec_text)
    spec_hash = _hash_text(spec_text)
    source_hash = _hash_file(event_path)

    component_panel = _build_component_panel(events, spec_hash, source_hash)
    signal_panel = _build_signal_panel(component_panel)
    coverage_panel = _build_coverage_panel(signal_panel)
    lineage = _build_lineage(signal_panel)
    timestamp_audit = _build_timestamp_audit(signal_panel)
    normalization_audit = _build_normalization_audit(signal_panel)
    no_view_report = _build_no_view_report(coverage_panel)
    falsifier_pack = _build_hard_falsifier_input_pack(artifacts, spec, spec_hash, source_hash)

    signal_panel.to_csv(artifacts["signal_panel"], index=False)
    component_panel.to_csv(artifacts["signal_component_panel"], index=False)
    coverage_panel.to_csv(artifacts["coverage_abstain_panel"], index=False)
    lineage.to_csv(artifacts["event_to_signal_lineage"], index=False)
    timestamp_audit.to_csv(artifacts["signal_timestamp_audit"], index=False)
    normalization_audit.to_csv(artifacts["signal_normalization_audit"], index=False)
    no_view_report.to_csv(artifacts["no_view_reason_report"], index=False)
    _write_json(artifacts["hard_falsifier_input_pack"], falsifier_pack)

    summary = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "stage": STAGE,
        "measurement_spec_id": str(spec["measurement_spec_id"]),
        "measurement_spec_hash": spec_hash,
        "source_manifest_hash": source_hash,
        "event_count": int(len(events)),
        "signal_row_count": int(len(signal_panel)),
        "active_signal_count": int(signal_panel["coverage_state"].eq("active").sum()),
        "no_view_count": int(signal_panel["coverage_state"].eq("no_view").sum()),
        "transaction_code_scope": TRANSACTION_CODE_SCOPE,
        "private_purchase_filter_status": PRIVATE_PURCHASE_FILTER_STATUS,
        "p_code_purchase_scope_warning": P_CODE_SCOPE_WARNING,
        "signal_panel_written": True,
        "expected_return_panel_written": False,
        "not_alpha_evidence": True,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "alpha_registry_update_allowed": False,
        "optimizer_or_portfolio_path_opened": False,
        "paper_workflow_opened": False,
        "broker_order_workflow_opened": False,
        "production_approval_claimed": False,
    }
    _write_json(artifacts["d3_signal_builder_summary"], summary)
    artifacts["d3_open_market_buying_signal_builder_report"].write_text(
        _render_report(summary, artifacts),
        encoding="utf-8",
    )
    return InsiderOpenMarketBuyingSignalBuilderResult(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "signal_panel": output_path / "signal_panel.csv",
        "signal_component_panel": output_path / "signal_component_panel.csv",
        "coverage_abstain_panel": output_path / "coverage_abstain_panel.csv",
        "event_to_signal_lineage": output_path / "event_to_signal_lineage.csv",
        "signal_timestamp_audit": output_path / "signal_timestamp_audit.csv",
        "signal_normalization_audit": output_path / "signal_normalization_audit.csv",
        "no_view_reason_report": output_path / "no_view_reason_report.csv",
        "hard_falsifier_input_pack": output_path / "hard_falsifier_input_pack.json",
        "d3_signal_builder_summary": output_path / "d3_signal_builder_summary.json",
        "d3_open_market_buying_signal_builder_report": output_path / "d3_open_market_buying_signal_builder_report.md",
    }


def _forbidden_input_columns(columns: pd.Index) -> list[str]:
    forbidden: list[str] = []
    for column in columns:
        lower = str(column).lower()
        if lower.startswith(FORBIDDEN_INPUT_PREFIXES) or any(pattern in lower for pattern in FORBIDDEN_INPUT_PATTERNS):
            forbidden.append(str(column))
    return forbidden


def _build_component_panel(events: pd.DataFrame, spec_hash: str, source_hash: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    enriched = events.copy()
    enriched["signal_date"] = pd.to_datetime(enriched["tradable_timestamp"], errors="coerce", utc=True).dt.date.astype(str)
    group_keys = ["ticker", "issuer_cik", "signal_date"]
    open_buys = enriched[
        (enriched["event_subset"] == "open_market_buy")
        & (enriched["transaction_code"] == "P")
        & (enriched["acquired_disposed"] == "A")
    ].copy()
    group_buy_dollars = open_buys.groupby(group_keys)["transaction_dollar_value"].sum().to_dict()
    group_owner_count = open_buys.groupby(group_keys)["reporting_owner_cik"].nunique().to_dict()
    for _, row in enriched.iterrows():
        group_key = (row.get("ticker", ""), row.get("issuer_cik", ""), row.get("signal_date", ""))
        role_bucket = str(row.get("role_bucket", ""))
        role_weight = ROLE_WEIGHTS.get(role_bucket, math.nan)
        market_cap = _safe_float(row.get("market_cap_at_event"))
        adv_20d = _safe_float(row.get("adv_20d"))
        transaction_shares = _safe_float(row.get("transaction_shares"))
        post_holding = _safe_float(row.get("post_transaction_holding"))
        prior_holding = post_holding - transaction_shares
        no_view_reason = _component_no_view_reason(row, market_cap, adv_20d, role_weight, prior_holding)
        distinct_buyers = int(group_owner_count.get(group_key, 0))
        buy_dollar_value = float(group_buy_dollars.get(group_key, 0.0))
        buy_value_pct = buy_dollar_value / market_cap if market_cap > 0 else math.nan
        cluster_weight = math.log1p(distinct_buyers) if distinct_buyers > 0 else math.nan
        holding_change_ratio = min(2.0, transaction_shares / prior_holding) if prior_holding > 0 else math.nan
        holding_change_weight = math.sqrt(1 + holding_change_ratio) if not math.isnan(holding_change_ratio) else math.nan
        raw_buy_conviction = (
            math.log1p(buy_value_pct) * role_weight * cluster_weight * holding_change_weight
            if not no_view_reason
            else math.nan
        )
        rows.append(
            {
                "event_id": row.get("event_id", ""),
                "issuer_id": row.get("ticker", "") or row.get("issuer_cik", ""),
                "ticker": row.get("ticker", ""),
                "cik": row.get("issuer_cik", ""),
                "accession_number": row.get("accession_number", ""),
                "filing_accepted_ts": row.get("filing_accepted_ts", ""),
                "tradable_ts": row.get("tradable_timestamp", ""),
                "signal_date": row.get("signal_date", ""),
                "event_subset": row.get("event_subset", ""),
                "transaction_code": row.get("transaction_code", ""),
                "rule_10b5_1_flag": row.get("rule_10b5_1_flag", ""),
                "role_bucket": role_bucket,
                "buy_dollar_value": buy_dollar_value if not no_view_reason else "",
                "market_cap_at_event": market_cap if market_cap > 0 else "",
                "buy_value_pct": buy_value_pct if not no_view_reason else "",
                "distinct_buying_insider_count": distinct_buyers if not no_view_reason else "",
                "cluster_weight": cluster_weight if not no_view_reason else "",
                "holding_change_ratio": holding_change_ratio if not no_view_reason else "",
                "holding_change_weight": holding_change_weight if not no_view_reason else "",
                "role_weight": role_weight if not math.isnan(role_weight) and not no_view_reason else "",
                "raw_buy_conviction": raw_buy_conviction if not no_view_reason else "",
                "coverage_state": "no_view" if no_view_reason else "active",
                "no_view_reason": no_view_reason,
                "measurement_spec_hash": spec_hash,
                "source_manifest_hash": source_hash,
                "transaction_code_scope": TRANSACTION_CODE_SCOPE,
                "private_purchase_filter_status": PRIVATE_PURCHASE_FILTER_STATUS,
                "p_code_purchase_scope_warning": P_CODE_SCOPE_WARNING,
                "no_view_not_zero_alpha": True,
                "not_alpha_evidence": True,
            },
        )
    return pd.DataFrame(rows)


def _component_no_view_reason(row: Mapping[str, object], market_cap: float, adv_20d: float, role_weight: float, prior_holding: float) -> str:
    if row.get("event_subset") != "open_market_buy" or row.get("transaction_code") != "P" or row.get("acquired_disposed") != "A":
        return "not_code_p_primary_measurement"
    if str(row.get("coverage_state", "")) == "no_view":
        return str(row.get("no_view_reason", "")) or "missing_market_join_or_price_volume_controls"
    if market_cap <= 0 or adv_20d <= 0:
        return "missing_market_join_or_price_volume_controls"
    if math.isnan(role_weight):
        return "unsupported_role_for_primary_measurement"
    if prior_holding <= 0:
        return "missing_holding_baseline"
    return ""


def _build_signal_panel(component_panel: pd.DataFrame) -> pd.DataFrame:
    signal_panel = component_panel.copy()
    active_mask = signal_panel["coverage_state"].eq("active")
    raw = pd.to_numeric(signal_panel.loc[active_mask, "raw_buy_conviction"], errors="coerce")
    if raw.empty:
        signal_panel["winsorized_raw_buy_conviction"] = ""
        signal_panel["normalized_signal"] = ""
        return signal_panel
    lower = raw.quantile(0.01)
    upper = raw.quantile(0.99)
    winsorized = raw.clip(lower=lower, upper=upper)
    std = winsorized.std(ddof=0)
    normalized = (winsorized - winsorized.mean()) / std if std and not pd.isna(std) else winsorized * 0.0
    signal_panel["winsorized_raw_buy_conviction"] = ""
    signal_panel["normalized_signal"] = ""
    signal_panel.loc[active_mask, "winsorized_raw_buy_conviction"] = winsorized.to_numpy()
    signal_panel.loc[active_mask, "normalized_signal"] = normalized.to_numpy()
    return signal_panel


def _build_coverage_panel(signal_panel: pd.DataFrame) -> pd.DataFrame:
    return signal_panel[
        [
            "event_id",
            "ticker",
            "signal_date",
            "event_subset",
            "transaction_code",
            "coverage_state",
            "no_view_reason",
            "no_view_not_zero_alpha",
            "not_alpha_evidence",
        ]
    ].copy()


def _build_lineage(signal_panel: pd.DataFrame) -> pd.DataFrame:
    lineage = signal_panel[
        [
            "event_id",
            "ticker",
            "cik",
            "accession_number",
            "filing_accepted_ts",
            "tradable_ts",
            "signal_date",
            "measurement_spec_hash",
            "source_manifest_hash",
        ]
    ].copy()
    lineage["lineage_status"] = "event_mapped_to_d3_signal_row"
    lineage["q1_entry_allowed"] = False
    lineage["q2_entry_allowed"] = False
    return lineage


def _build_timestamp_audit(signal_panel: pd.DataFrame) -> pd.DataFrame:
    accepted = pd.to_datetime(signal_panel["filing_accepted_ts"], errors="coerce", utc=True)
    tradable = pd.to_datetime(signal_panel["tradable_ts"], errors="coerce", utc=True)
    return pd.DataFrame(
        [
            {
                "row_count": int(len(signal_panel)),
                "missing_filing_accepted_ts_count": int(accepted.isna().sum()),
                "missing_tradable_ts_count": int(tradable.isna().sum()),
                "tradable_not_after_accepted_count": int(((tradable <= accepted) & accepted.notna() & tradable.notna()).sum()),
                "transaction_date_used_as_return_anchor": False,
                "return_anchor": "tradable_timestamp",
                "status": "pass" if int(((tradable <= accepted) & accepted.notna() & tradable.notna()).sum()) == 0 else "fail",
            },
        ],
    )


def _build_normalization_audit(signal_panel: pd.DataFrame) -> pd.DataFrame:
    active = signal_panel[signal_panel["coverage_state"] == "active"]
    raw = pd.to_numeric(active["raw_buy_conviction"], errors="coerce")
    normalized = pd.to_numeric(active["normalized_signal"], errors="coerce")
    return pd.DataFrame(
        [
            {
                "active_signal_count": int(len(active)),
                "raw_min": float(raw.min()) if not raw.empty else 0.0,
                "raw_max": float(raw.max()) if not raw.empty else 0.0,
                "normalized_mean": float(normalized.mean()) if not normalized.empty else 0.0,
                "normalized_std": float(normalized.std(ddof=0)) if not normalized.empty else 0.0,
                "winsorization": "global_1_99_percentile",
                "normalization": "cross_sectional_zscore_over_active_fixture_rows",
                "no_view_rows_normalized": False,
            },
        ],
    )


def _build_no_view_report(coverage_panel: pd.DataFrame) -> pd.DataFrame:
    no_view = coverage_panel[coverage_panel["coverage_state"] == "no_view"].copy()
    if no_view.empty:
        return pd.DataFrame(
            [{"no_view_reason": "", "row_count": 0, "no_view_not_zero_alpha": True}],
        )
    report = no_view.groupby("no_view_reason", dropna=False).size().reset_index(name="row_count")
    report["no_view_not_zero_alpha"] = True
    return report


def _build_hard_falsifier_input_pack(
    artifacts: Mapping[str, Path],
    spec: Mapping[str, object],
    spec_hash: str,
    source_hash: str,
) -> dict[str, object]:
    return {
        "schema_version": "insider_open_market_buying_hard_falsifier_input_pack.v1",
        "stage": STAGE,
        "measurement_spec_id": spec["measurement_spec_id"],
        "measurement_spec_hash": spec_hash,
        "source_manifest_hash": source_hash,
        "source_signal_panel": str(artifacts["signal_panel"]),
        "source_coverage_abstain_panel": str(artifacts["coverage_abstain_panel"]),
        "hard_falsifiers": [
            "shifted_filing_dates",
            "same_coverage_random",
            "randomized_role_labels",
            "compensation_controls",
            "pre_filing_drift_dominance",
            "market_sector_liquidity_controls",
        ],
        "primary_label_window_for_later_q1": "post_1_22_trading_days",
        "diagnostic_windows_for_later_q1": ["post_1_5_trading_days", "post_1_10_trading_days", "post_1_44_trading_days"],
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "expected_return_panel_written": False,
        "not_alpha_evidence": True,
    }


def _render_report(summary: Mapping[str, object], artifacts: Mapping[str, Path]) -> str:
    return "\n".join(
        [
            "# D3 Open-Market Insider Buying SignalBuilder Report",
            "",
            "not alpha evidence",
            "D3 signal builder only",
            "Form 4 code P means open-market or private purchase.",
            P_CODE_SCOPE_WARNING,
            "",
            "## Summary",
            "",
            f"- measurement spec: `{summary['measurement_spec_id']}`",
            f"- signal rows: {summary['signal_row_count']}",
            f"- active signals: {summary['active_signal_count']}",
            f"- no-view rows: {summary['no_view_count']}",
            f"- transaction code scope: `{summary['transaction_code_scope']}`",
            f"- private purchase filter status: `{summary['private_purchase_filter_status']}`",
            "",
            "## Artifacts",
            "",
            f"- signal panel: `{artifacts['signal_panel']}`",
            f"- coverage / abstain panel: `{artifacts['coverage_abstain_panel']}`",
            f"- hard falsifier input pack: `{artifacts['hard_falsifier_input_pack']}`",
            "",
            "## Boundary",
            "",
            "This builder writes a Track A D3 signal panel from the frozen MeasurementSpec. It does not run Q1, Q2, optimizer paths, portfolio construction, Alpha Registry promotion, paper workflows, broker/order workflows, live workflows, or production approval.",
            "Missing coverage remains no-view / abstain and is not encoded as zero.",
            "",
        ],
    )


def _safe_float(value: object) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _hash_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
