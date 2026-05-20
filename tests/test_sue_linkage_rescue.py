from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from portfolio_os.alpha.sue_linkage_rescue import (
    SueLinkageRescueConfig,
    rescue_sue_links_from_crsp_stocknames,
)


class _FakeStocknamesConnection:
    def raw_sql(self, query: str) -> pd.DataFrame:
        assert "crsp.stocknames" in query
        return pd.DataFrame(
            [
                {
                    "permno": 10107,
                    "permco": 8048,
                    "namedt": "2010-01-01",
                    "nameenddt": "2022-12-31",
                    "ncusip": "59491810",
                    "cusip": "59491810",
                    "ticker": "MSFT",
                    "shrcd": 11,
                    "exchcd": 3,
                },
                {
                    "permno": 77777,
                    "permco": 777,
                    "namedt": "2021-01-01",
                    "nameenddt": "2022-12-31",
                    "ncusip": "22222222",
                    "cusip": "22222222",
                    "ticker": "LATE",
                    "shrcd": 11,
                    "exchcd": 3,
                },
            ]
        )


def test_rescues_failed_cusip_links_with_date_valid_stocknames(tmp_path: Path) -> None:
    existing_links = tmp_path / "ibes_links.csv"
    failed_links = tmp_path / "linkage_failure_report.csv"
    output_links = tmp_path / "ibes_links_rescued.csv"
    rescue_report = tmp_path / "linkage_rescue_report.json"
    stocknames_probe = tmp_path / "stocknames_failed_cusip_matches.csv"

    pd.DataFrame(
        [
            {
                "ibes_ticker": "AAPL",
                "cusip": "03783310",
                "permno": 14593,
                "permco": 7,
                "link_method": "ibes_idsum_cusip_sdates_stocknames",
                "link_start_date": "2010-01-01",
                "link_end_date": "2022-12-31",
                "link_validity_flag": True,
            }
        ]
    ).to_csv(existing_links, index=False)
    pd.DataFrame(
        [
            {
                "event_id": "SUE-MSFT-20200115",
                "symbol": "MSFT",
                "ibes_ticker": "MSFT",
                "cusip": "59491810",
                "announcement_date": "2020-01-15",
                "link_method": "unlinked_ibes_idsum_cusip_sdates",
                "pit_safety_status": "diagnostic_unlinked",
                "failure_reason": "unlinked",
            },
            {
                "event_id": "SUE-LATE-20200115",
                "symbol": "LATE",
                "ibes_ticker": "LATE",
                "cusip": "22222222",
                "announcement_date": "2020-01-15",
                "link_method": "unlinked_ibes_idsum_cusip_sdates",
                "pit_safety_status": "diagnostic_unlinked",
                "failure_reason": "unlinked",
            },
        ]
    ).to_csv(failed_links, index=False)

    result = rescue_sue_links_from_crsp_stocknames(
        SueLinkageRescueConfig(
            existing_links_path=str(existing_links),
            linkage_failure_report_path=str(failed_links),
            output_links_path=str(output_links),
            rescue_report_path=str(rescue_report),
            stocknames_probe_path=str(stocknames_probe),
            batch_size=100,
            fetched_at="2026-05-07T00:00:00Z",
        ),
        connection=_FakeStocknamesConnection(),
    )

    assert result["status"] == "completed"
    assert result["failed_event_rows"] == 2
    assert result["rescued_event_rows"] == 1
    assert result["rescued_symbols"] == 1
    assert result["ticker_only_matching_used"] is False
    assert result["production_approval_claimed"] is False
    links = pd.read_csv(output_links)
    assert set(links["ibes_ticker"]) == {"AAPL", "MSFT"}
    rescued = links.loc[links["ibes_ticker"].eq("MSFT")].iloc[0]
    assert int(rescued["permno"]) == 10107
    assert rescued["link_method"] == "crsp_stocknames_exact_cusip_rescue"
    report = json.loads(rescue_report.read_text(encoding="utf-8"))
    assert report["rescued_event_rows"] == 1
    assert report["alpha_registry_promoted"] is False


def test_rerun_preserves_existing_rescued_output_links(tmp_path: Path) -> None:
    existing_links = tmp_path / "ibes_links.csv"
    failed_links = tmp_path / "linkage_failure_report.csv"
    output_links = tmp_path / "ibes_links_rescued.csv"
    rescue_report = tmp_path / "linkage_rescue_report.json"
    stocknames_probe = tmp_path / "stocknames_failed_cusip_matches.csv"

    base_row = {
        "ibes_ticker": "AAPL",
        "cusip": "03783310",
        "permno": 14593,
        "permco": 7,
        "link_method": "ibes_idsum_cusip_sdates_stocknames",
        "link_start_date": "2010-01-01",
        "link_end_date": "2022-12-31",
        "link_validity_flag": True,
    }
    rescued_row = {
        "ibes_ticker": "MSFT",
        "cusip": "59491810",
        "permno": 10107,
        "permco": 8048,
        "link_method": "crsp_stocknames_exact_cusip_rescue",
        "link_start_date": "2010-01-01",
        "link_end_date": "2022-12-31",
        "link_validity_flag": True,
    }
    pd.DataFrame([base_row]).to_csv(existing_links, index=False)
    pd.DataFrame([base_row, rescued_row]).to_csv(output_links, index=False)
    pd.DataFrame(
        [
            {
                "event_id": "SUE-NOMATCH-20200115",
                "symbol": "NOMATCH",
                "ibes_ticker": "NOMATCH",
                "cusip": "99999999",
                "announcement_date": "2020-01-15",
                "link_method": "unlinked_ibes_idsum_cusip_sdates",
                "pit_safety_status": "diagnostic_unlinked",
                "failure_reason": "unlinked",
            }
        ]
    ).to_csv(failed_links, index=False)

    result = rescue_sue_links_from_crsp_stocknames(
        SueLinkageRescueConfig(
            existing_links_path=str(existing_links),
            linkage_failure_report_path=str(failed_links),
            output_links_path=str(output_links),
            rescue_report_path=str(rescue_report),
            stocknames_probe_path=str(stocknames_probe),
            batch_size=100,
            fetched_at="2026-05-07T00:00:00Z",
        ),
        connection=_FakeStocknamesConnection(),
    )

    links = pd.read_csv(output_links)
    assert result["rescued_event_rows"] == 0
    assert result["preserved_exact_cusip_rescue_link_rows"] == 1
    assert result["combined_exact_cusip_rescue_link_rows"] == 1
    assert set(links["ibes_ticker"]) == {"AAPL", "MSFT"}
    assert "crsp_stocknames_exact_cusip_rescue" in set(links["link_method"])
