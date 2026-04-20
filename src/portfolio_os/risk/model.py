"""Risk-model context builders and metrics."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from portfolio_os.data.risk_inputs import load_factor_exposures, load_returns_long
from portfolio_os.domain.errors import InputValidationError
from portfolio_os.utils.config import AppConfig


@dataclass
class RiskModelContext:
    """Resolved risk-model inputs aligned to one optimizer universe."""

    sigma: np.ndarray
    factor_matrix: np.ndarray
    factor_names: list[str]
    target_factor_exposure: np.ndarray
    returns_observation_count: int
    estimator: str


def _sample_covariance(returns_matrix: np.ndarray) -> np.ndarray:
    """Compute a sample covariance matrix from aligned returns."""

    if returns_matrix.ndim != 2:
        raise InputValidationError("returns matrix must be 2D.")
    if returns_matrix.shape[0] < 2:
        raise InputValidationError("At least two returns observations are required for covariance estimation.")
    return np.cov(returns_matrix, rowvar=False, ddof=1)


def _ledoit_wolf_covariance(returns_matrix: np.ndarray) -> np.ndarray:
    """Estimate covariance with Ledoit-Wolf-style linear shrinkage to scaled identity."""

    centered = returns_matrix - returns_matrix.mean(axis=0, keepdims=True)
    n_obs, n_assets = centered.shape
    if n_obs < 2:
        raise InputValidationError("At least two returns observations are required for Ledoit-Wolf estimation.")

    sample = np.matmul(centered.T, centered) / float(n_obs)
    mu = float(np.trace(sample) / n_assets)
    target = mu * np.eye(n_assets, dtype=float)
    delta_hat = float(np.sum(np.square(sample - target)))
    if delta_hat <= 0.0:
        return target

    beta_hat = 0.0
    for row in centered:
        outer = np.outer(row, row)
        beta_hat += float(np.sum(np.square(outer - sample)))
    beta_hat /= float(n_obs * n_obs)
    shrinkage = min(max(beta_hat / delta_hat, 0.0), 1.0)
    return (1.0 - shrinkage) * sample + shrinkage * target


def _make_psd(matrix: np.ndarray, *, diagonal_jitter: float) -> np.ndarray:
    """Project a symmetric matrix to PSD and apply optional diagonal jitter."""

    symmetric = 0.5 * (matrix + matrix.T)
    eigvals, eigvecs = np.linalg.eigh(symmetric)
    clipped = np.maximum(eigvals, 0.0)
    projected = np.matmul(eigvecs, np.matmul(np.diag(clipped), eigvecs.T))
    if diagonal_jitter > 0:
        projected = projected + diagonal_jitter * np.eye(projected.shape[0], dtype=float)
    return 0.5 * (projected + projected.T)


def _resolve_required_path(path_text: str | None, field_name: str) -> Path:
    """Resolve and validate a required risk-input path."""

    if not path_text:
        raise InputValidationError(f"Missing required config field: {field_name}")
    path = Path(path_text)
    if not path.exists():
        raise InputValidationError(f"{field_name} does not exist: {path}")
    return path


def build_risk_model_context(universe: pd.DataFrame, config: AppConfig) -> RiskModelContext | None:
    """Build aligned covariance and factor context if risk mode is enabled."""

    if not config.risk_model.enabled:
        return None
    if universe.empty:
        raise InputValidationError("Risk model cannot run on an empty universe.")

    returns_path = _resolve_required_path(config.risk_model.returns_path, "risk_model.returns_path")
    factor_path = _resolve_required_path(
        config.risk_model.factor_exposure_path,
        "risk_model.factor_exposure_path",
    )
    ordered_tickers = universe["ticker"].astype(str).tolist()
    returns_frame = load_returns_long(
        returns_path,
        required_tickers=ordered_tickers,
        lookback_days=int(config.risk_model.lookback_days),
        min_history_days=int(config.risk_model.min_history_days),
    )
    factor_frame = load_factor_exposures(factor_path, required_tickers=ordered_tickers)
    configured_factors = set(config.constraints.factor_bounds.keys())
    available_factors = set(str(name) for name in factor_frame.columns.tolist())
    missing_factors = sorted(configured_factors - available_factors)
    if missing_factors:
        raise InputValidationError(
            "factor_bounds references unknown factor(s) absent from factor_exposure.csv: "
            + ", ".join(missing_factors)
        )

    returns_matrix = returns_frame.to_numpy(dtype=float)
    if str(config.risk_model.estimator).lower() == "sample":
        sigma = _sample_covariance(returns_matrix)
    else:
        sigma = _ledoit_wolf_covariance(returns_matrix)
    sigma = sigma * float(config.risk_model.annualization_factor)
    sigma = _make_psd(sigma, diagonal_jitter=float(config.risk_model.diagonal_jitter))

    factor_names = [str(name) for name in factor_frame.columns.tolist()]
    factor_matrix = factor_frame.to_numpy(dtype=float)
    target_weights = universe["target_weight"].to_numpy(dtype=float)
    target_factor_exposure = np.matmul(factor_matrix.T, target_weights)
    return RiskModelContext(
        sigma=sigma,
        factor_matrix=factor_matrix,
        factor_names=factor_names,
        target_factor_exposure=target_factor_exposure,
        returns_observation_count=int(returns_matrix.shape[0]),
        estimator=str(config.risk_model.estimator),
    )


def portfolio_variance(
    quantities: np.ndarray,
    prices: np.ndarray,
    nav: float,
    sigma: np.ndarray,
) -> float:
    """Compute portfolio variance from quantities and covariance."""

    if nav <= 0:
        return 0.0
    weights = quantities * prices / nav
    return float(np.matmul(weights, np.matmul(sigma, weights)))


def tracking_error_variance(
    quantities: np.ndarray,
    prices: np.ndarray,
    nav: float,
    target_weights: np.ndarray,
    sigma: np.ndarray,
) -> float:
    """Compute active variance relative to target weights."""

    if nav <= 0:
        return 0.0
    weights = quantities * prices / nav
    active = weights - target_weights
    return float(np.matmul(active, np.matmul(sigma, active)))
