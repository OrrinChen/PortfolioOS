"""Project-specific exceptions."""

from __future__ import annotations


class PortfolioOSError(Exception):
    """Base exception for PortfolioOS."""


class InputValidationError(PortfolioOSError):
    """Raised when input files or values are invalid."""


class OptimizationError(PortfolioOSError):
    """Raised when the optimization workflow fails."""


class ProviderPermissionError(InputValidationError):
    """Raised when a data provider call fails because of permission or quota limits."""


class ProviderDataError(InputValidationError):
    """Raised when a data provider cannot supply valid data for the request."""


class ProviderRuntimeError(PortfolioOSError):
    """Raised when a data provider fails for transport or runtime reasons."""
