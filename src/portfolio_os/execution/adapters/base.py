"""Broker adapter abstraction for execution integration."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from portfolio_os.execution.models import ExecutionResult, ReconciliationReport


class BrokerAdapter(ABC):
    """Abstract broker adapter contract."""

    @abstractmethod
    def connect(self) -> bool:
        """Initialize broker connectivity and return readiness."""

    @abstractmethod
    def submit_orders(self, orders_df: pd.DataFrame) -> ExecutionResult:
        """Submit orders and return execution outcomes."""

    @abstractmethod
    def query_positions(self) -> pd.DataFrame:
        """Fetch current broker positions."""

    @abstractmethod
    def query_account(self) -> dict:
        """Fetch account-level metadata and balances."""

    @abstractmethod
    def cancel_all(self) -> bool:
        """Cancel all open orders."""

    @abstractmethod
    def reconcile(self, expected_positions: pd.DataFrame) -> ReconciliationReport:
        """Reconcile expected positions against broker positions."""

