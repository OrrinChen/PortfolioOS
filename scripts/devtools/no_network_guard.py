#!/usr/bin/env python3
"""Self-test the PortfolioOS no-network validation guard."""

from __future__ import annotations

from portfolio_os.validation import assert_no_network_guard_blocks_connections


def main() -> None:
    assert_no_network_guard_blocks_connections()
    print("no_network_guard: blocked socket connections")


if __name__ == "__main__":
    main()
