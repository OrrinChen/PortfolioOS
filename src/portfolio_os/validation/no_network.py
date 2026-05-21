"""No-network guard for local validation commands."""

from __future__ import annotations

from contextlib import contextmanager
import socket
from types import TracebackType
from typing import Iterator


class NoNetworkViolation(RuntimeError):
    """Raised when validation code attempts a network connection."""


class _SocketGuard:
    def __init__(self) -> None:
        self._original_socket_connect = socket.socket.connect
        self._original_socket_connect_ex = socket.socket.connect_ex
        self._original_create_connection = socket.create_connection

    def __enter__(self) -> "_SocketGuard":
        socket.socket.connect = _blocked_connect  # type: ignore[method-assign]
        socket.socket.connect_ex = _blocked_connect_ex  # type: ignore[method-assign]
        socket.create_connection = _blocked_create_connection  # type: ignore[assignment]
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        socket.socket.connect = self._original_socket_connect  # type: ignore[method-assign]
        socket.socket.connect_ex = self._original_socket_connect_ex  # type: ignore[method-assign]
        socket.create_connection = self._original_create_connection  # type: ignore[assignment]


@contextmanager
def no_network_guard() -> Iterator[None]:
    """Block socket connection attempts inside the context."""

    with _SocketGuard():
        yield


def assert_no_network_guard_blocks_connections() -> None:
    """Self-test the guard without making a real external connection."""

    with no_network_guard():
        try:
            socket.create_connection(("example.com", 80), timeout=0.01)
        except NoNetworkViolation:
            return
    raise AssertionError("no_network_guard did not block socket.create_connection")


def _blocked_connect(self: socket.socket, address: object) -> None:
    raise NoNetworkViolation(f"network connection blocked during validation: {address!r}")


def _blocked_connect_ex(self: socket.socket, address: object) -> int:
    raise NoNetworkViolation(f"network connection blocked during validation: {address!r}")


def _blocked_create_connection(
    address: object,
    timeout: float | object = socket._GLOBAL_DEFAULT_TIMEOUT,
    source_address: tuple[str, int] | None = None,
) -> socket.socket:
    raise NoNetworkViolation(f"network connection blocked during validation: {address!r}")
