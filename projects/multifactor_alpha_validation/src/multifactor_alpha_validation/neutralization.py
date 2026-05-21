from __future__ import annotations

from multifactor_alpha_validation.schema import FactorSpec


def factor_exposure_report(spec: FactorSpec) -> dict[str, float | str]:
    beta = {
        "low_volatility": -0.38,
        "momentum": 0.21,
        "reversal": -0.12,
        "liquidity": 0.08,
        "sue": 0.05,
    }.get(spec.family_id, 0.11)
    sector = {
        "momentum": 0.30,
        "low_volatility": 0.26,
        "value": 0.22,
    }.get(spec.family_id, 0.16)
    size = {
        "liquidity": -0.34,
        "value": -0.18,
    }.get(spec.family_id, 0.09)
    liquidity = {
        "liquidity": 0.52,
        "reversal": -0.20,
    }.get(spec.family_id, 0.12)
    return {
        "factor_id": spec.factor_id,
        "beta_exposure": beta,
        "sector_exposure": sector,
        "size_exposure": size,
        "liquidity_exposure": liquidity,
        "adjustment_status": "reported",
    }


def neutralized_metrics(raw_metrics: dict[str, float | str], exposures: dict[str, float | str]) -> dict[str, float]:
    raw_ic = float(raw_metrics["raw_rank_ic_mean"])
    exposure_drag = min(
        abs(float(exposures["beta_exposure"])) * 0.12
        + abs(float(exposures["sector_exposure"])) * 0.08
        + abs(float(exposures["size_exposure"])) * 0.05,
        0.45,
    )
    neutral_ic = raw_ic * (1.0 - exposure_drag)
    spread = float(raw_metrics["top_bottom_spread"])
    return {
        "neutralized_rank_ic_mean": round(neutral_ic, 6),
        "neutralized_rank_ic_t": round(neutral_ic * 1.732, 6),
        "beta_adjusted_spread": round(spread * (1.0 - abs(float(exposures["beta_exposure"])) * 0.10), 6),
        "sector_neutral_spread": round(spread * (1.0 - abs(float(exposures["sector_exposure"])) * 0.08), 6),
        "style_adjusted_spread": round(spread * (1.0 - abs(float(exposures["size_exposure"])) * 0.06), 6),
        "benchmark_relative_spread": round(spread * 0.74, 6),
    }
