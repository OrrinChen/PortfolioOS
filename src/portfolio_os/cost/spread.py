"""Simple spread helpers."""

from __future__ import annotations


def estimate_half_spread_bps(participation_ratio: float, base_spread_bps: float = 4.0) -> float:
    """Return a small heuristic half-spread estimate."""

    return base_spread_bps * (1.0 + max(participation_ratio, 0.0))

