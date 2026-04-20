from __future__ import annotations

from portfolio_os.simulation.replay import (
    load_replay_manifest,
    run_replay_suite,
    summarize_distribution,
)


def test_manifest_parsing(replay_manifest_path) -> None:
    manifest = load_replay_manifest(replay_manifest_path)

    assert manifest.name == "demo_replay_suite"
    assert len(manifest.samples) == 5
    assert manifest.samples[0] == "sample_01"


def test_distribution_summary_statistics() -> None:
    summary = summarize_distribution([1.0, 2.0, 4.0, 8.0], include_min_max=True, include_positive_rate=True)

    assert summary["mean"] == 3.75
    assert summary["median"] == 3.0
    assert summary["min"] == 1.0
    assert summary["max"] == 8.0
    assert summary["positive_rate"] == 1.0


def test_replay_suite_runs_multiple_samples(project_root, replay_manifest_path) -> None:
    replay_suite = run_replay_suite(
        manifest_path=replay_manifest_path,
        constraints_path=project_root / "config" / "constraints" / "public_fund.yaml",
        config_path=project_root / "config" / "default.yaml",
        execution_profile_path=project_root / "config" / "execution" / "conservative.yaml",
    )

    assert len(replay_suite.sample_runs) == 5
    assert replay_suite.suite_results_payload["suite"]["sample_count"] == 5
    for sample in replay_suite.suite_results_payload["samples"]:
        strategy_names = {strategy["strategy_name"] for strategy in sample["strategies"]}
        assert strategy_names == {
            "naive_target_rebalance",
            "cost_unaware_rebalance",
            "portfolio_os_rebalance",
        }
    assert "portfolio_os_vs_naive" in replay_suite.suite_results_payload["aggregate_summary"]["comparisons"]
    assert "finding_patterns" in replay_suite.suite_results_payload["aggregate_summary"]
    assert "best_blocked_trade_reduction_sample" in replay_suite.suite_results_payload["aggregate_summary"]
    assert "most_common_blocking_category" in replay_suite.suite_summary_markdown
    assert "most_common_blocked_reason" in replay_suite.suite_summary_markdown
    assert "best_blocked_trade_reduction_sample" in replay_suite.suite_summary_markdown
