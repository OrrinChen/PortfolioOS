"""Common enumerations used across the PortfolioOS MVP."""

from __future__ import annotations

from enum import Enum


class OrderSide(str, Enum):
    """Trade side used for exported orders."""

    BUY = "BUY"
    SELL = "SELL"


class Urgency(str, Enum):
    """Execution urgency labels."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"


class FindingSeverity(str, Enum):
    """Severity level for compliance findings."""

    INFO = "INFO"
    WARNING = "WARNING"
    BREACH = "BREACH"


class FindingCategory(str, Enum):
    """Finding category used in structured compliance outputs."""

    TRADABILITY = "tradability"
    REGULATORY = "regulatory"
    RISK = "risk"
    CASH = "cash"
    DATA_QUALITY = "data_quality"


class RepairStatus(str, Enum):
    """Repair-state label for one finding."""

    NOT_NEEDED = "not_needed"
    REPAIRED = "repaired"
    PARTIALLY_REPAIRED = "partially_repaired"
    UNRESOLVED = "unresolved"
