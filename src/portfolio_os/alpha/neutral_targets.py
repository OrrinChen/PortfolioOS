"""Deterministic neutral target helpers for paper calibration."""

from __future__ import annotations

import random

import pandas as pd


def build_neutral_target_frame(
    *,
    tickers: list[str],
    gross_target_weight: float = 1.0,
    perturbation_bps: float = 0.0,
    perturbation_seed: int | None = None,
) -> pd.DataFrame:
    """Build a deterministic trivial target frame."""

    clean_tickers = sorted({str(ticker).strip().upper() for ticker in tickers if str(ticker).strip()})
    if not clean_tickers:
        return pd.DataFrame(columns=["ticker", "target_weight"])

    frame = pd.DataFrame({"ticker": clean_tickers})
    base_weight = float(gross_target_weight) / float(len(frame))
    frame["target_weight"] = base_weight

    if perturbation_bps > 0.0 and len(frame) > 1:
        rng = random.Random(perturbation_seed)
        offsets = [rng.uniform(-perturbation_bps, perturbation_bps) / 10000.0 for _ in range(len(frame))]
        centered_offsets = [offset - (sum(offsets) / len(offsets)) for offset in offsets]
        frame["target_weight"] = frame["target_weight"] + pd.Series(centered_offsets)
        frame["target_weight"] = frame["target_weight"] / float(frame["target_weight"].sum())

    return frame.reset_index(drop=True)


def build_neutral_target_manifest(
    *,
    target_frame: pd.DataFrame,
    strategy_name: str,
    perturbation_bps: float,
    perturbation_seed: int | None,
) -> dict[str, object]:
    """Build a compact manifest for one neutral target."""

    tickers = target_frame.get("ticker", pd.Series(dtype=object)).astype(str).tolist()
    weight_sum = (
        float(pd.to_numeric(target_frame.get("target_weight"), errors="coerce").fillna(0.0).sum())
        if not target_frame.empty
        else 0.0
    )
    return {
        "strategy_name": str(strategy_name),
        "selected_tickers": tickers,
        "selected_count": int(len(target_frame)),
        "target_weight_sum": weight_sum,
        "perturbation_bps": float(perturbation_bps),
        "perturbation_seed": perturbation_seed,
    }


def build_neutral_order_frame(
    *,
    tickers: list[str],
    quantity: float,
    direction: str = "buy",
) -> pd.DataFrame:
    """Build a deterministic neutral order frame for paper calibration."""

    clean_tickers = sorted({str(ticker).strip().upper() for ticker in tickers if str(ticker).strip()})
    if not clean_tickers:
        return pd.DataFrame(columns=["ticker", "direction", "quantity"])
    return pd.DataFrame(
        {
            "ticker": clean_tickers,
            "direction": [str(direction).strip().lower()] * len(clean_tickers),
            "quantity": [float(quantity)] * len(clean_tickers),
        }
    )
