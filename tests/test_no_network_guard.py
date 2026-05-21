from __future__ import annotations

import socket

import pytest

from portfolio_os.validation.no_network import NoNetworkViolation, no_network_guard


def test_no_network_guard_blocks_socket_connections() -> None:
    with no_network_guard():
        with pytest.raises(NoNetworkViolation):
            socket.create_connection(("example.com", 80), timeout=0.01)


def test_no_network_guard_blocks_connect_ex() -> None:
    with no_network_guard():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            with pytest.raises(NoNetworkViolation):
                sock.connect_ex(("127.0.0.1", 9))
