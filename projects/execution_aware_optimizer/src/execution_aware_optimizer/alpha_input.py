"""Alpha-score input adapter for the execution-aware optimizer project."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from portfolio_os.data.loaders import normalize_ticker
from portfolio_os.domain.errors import InputValidationError


REQUIRED_ALPHA_COLUMNS = ("date", "symbol", "alpha_score")
OPTIONAL_ALPHA_COLUMNS = ("alpha_source", "alpha_confidence")


@dataclass(frozen=True)
class AlphaInputReport:
    """Validation and cleaning summary for one alpha panel load."""

    source_name: str
    row_count_input: int
    row_count_output: int
    missing_alpha_score_count: int
    invalid_date_count: int
    dropped_row_count: int
    rank_normalized: bool
    winsorize_quantile: float | None

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serializable report payload."""

        return {
            "source_name": self.source_name,
            "row_count_input": self.row_count_input,
            "row_count_output": self.row_count_output,
            "missing_alpha_score_count": self.missing_alpha_score_count,
            "invalid_date_count": self.invalid_date_count,
            "dropped_row_count": self.dropped_row_count,
            "rank_normalized": self.rank_normalized,
            "winsorize_quantile": self.winsorize_quantile,
        }


@dataclass(frozen=True)
class AlphaInputResult:
    """Cleaned alpha panel plus validation report."""

    panel: pd.DataFrame
    report: AlphaInputReport


def _read_alpha_frame(path: str | Path) -> pd.DataFrame:
    """Read CSV or parquet alpha inputs."""

    source_path = Path(path)
    suffix = source_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(source_path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(source_path)
    raise InputValidationError(f"Unsupported alpha input format for {source_path}. Use CSV or parquet.")


def _ensure_required_columns(frame: pd.DataFrame, source_name: str) -> None:
    """Validate required input columns."""

    missing = [column for column in REQUIRED_ALPHA_COLUMNS if column not in frame.columns]
    if missing:
        raise InputValidationError(
            f"Missing required alpha input columns in {source_name}: {', '.join(missing)}"
        )


def _validate_winsorize_quantile(value: float | None) -> float | None:
    """Validate optional quantile winsorization control."""

    if value is None:
        return None
    quantile = float(value)
    if not 0.0 < quantile < 0.5:
        raise InputValidationError("winsorize_quantile must be greater than 0 and less than 0.5.")
    return quantile


def _winsorize_by_date(scores: pd.Series, quantile: float) -> pd.Series:
    """Clip one date's scores to symmetric quantile bounds."""

    lower = float(scores.quantile(quantile))
    upper = float(scores.quantile(1.0 - quantile))
    return scores.clip(lower=lower, upper=upper)


def _rank_pct_by_date(scores: pd.Series) -> pd.Series:
    """Return 0..1 rank positions inside one date cross-section."""

    count = len(scores)
    if count <= 1:
        return pd.Series(0.5, index=scores.index, dtype=float)
    ranks = scores.rank(method="average")
    return ((ranks - 1.0) / float(count - 1)).astype(float)


def _rank_normalize_by_date(scores: pd.Series) -> pd.Series:
    """Return -1..1 cross-sectional rank-normalized scores."""

    rank_pct = _rank_pct_by_date(scores)
    if len(scores) <= 1:
        return pd.Series(0.0, index=scores.index, dtype=float)
    return (2.0 * rank_pct - 1.0).astype(float)


def clean_alpha_scores(
    frame: pd.DataFrame,
    *,
    source_name: str = "alpha_scores",
    rank_normalize_by_date: bool = False,
    winsorize_quantile: float | None = None,
) -> AlphaInputResult:
    """Validate and normalize a standard alpha-score panel.

    The output keeps the source `symbol`, adds PortfolioOS-compatible `ticker`,
    and preserves `raw_alpha_score` before optional rank normalization.
    """

    _ensure_required_columns(frame, source_name)
    quantile = _validate_winsorize_quantile(winsorize_quantile)
    row_count_input = int(len(frame))

    selected_columns = [*REQUIRED_ALPHA_COLUMNS, *[column for column in OPTIONAL_ALPHA_COLUMNS if column in frame.columns]]
    work = frame.loc[:, selected_columns].copy()
    parsed_dates = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
    invalid_date_count = int(parsed_dates.isna().sum())
    if invalid_date_count:
        raise InputValidationError(f"{source_name} contains {invalid_date_count} invalid date value(s).")
    work["date"] = parsed_dates
    work["symbol"] = work["symbol"].map(normalize_ticker)
    work["ticker"] = work["symbol"]

    numeric_score = pd.to_numeric(work["alpha_score"], errors="coerce")
    missing_alpha_score_count = int(numeric_score.isna().sum())
    work["alpha_score"] = numeric_score.astype(float)
    work = work.dropna(subset=["alpha_score"]).copy()

    if "alpha_confidence" in work.columns:
        confidence = pd.to_numeric(work["alpha_confidence"], errors="coerce")
        invalid_confidence = int(confidence.isna().sum() - work["alpha_confidence"].isna().sum())
        if invalid_confidence > 0:
            raise InputValidationError(f"{source_name} contains non-numeric alpha_confidence value(s).")
        work["alpha_confidence"] = confidence

    if quantile is not None and not work.empty:
        work["alpha_score"] = (
            work.groupby("date", group_keys=False)["alpha_score"]
            .transform(lambda scores: _winsorize_by_date(scores, quantile))
            .astype(float)
        )

    if work.empty:
        clean = pd.DataFrame(
            columns=[
                "date",
                "symbol",
                "ticker",
                "alpha_score",
                "raw_alpha_score",
                "alpha_rank_pct",
                *OPTIONAL_ALPHA_COLUMNS,
            ]
        )
    else:
        work["raw_alpha_score"] = work["alpha_score"].astype(float)
        work["alpha_rank_pct"] = (
            work.groupby("date", group_keys=False)["raw_alpha_score"]
            .transform(_rank_pct_by_date)
            .astype(float)
        )
        if rank_normalize_by_date:
            work["alpha_score"] = (
                work.groupby("date", group_keys=False)["raw_alpha_score"]
                .transform(_rank_normalize_by_date)
                .astype(float)
            )
        output_columns = [
            "date",
            "symbol",
            "ticker",
            "alpha_score",
            "raw_alpha_score",
            "alpha_rank_pct",
            *[column for column in OPTIONAL_ALPHA_COLUMNS if column in work.columns],
        ]
        clean = work.loc[:, output_columns].sort_values(["date", "symbol"]).reset_index(drop=True)

    report = AlphaInputReport(
        source_name=source_name,
        row_count_input=row_count_input,
        row_count_output=int(len(clean)),
        missing_alpha_score_count=missing_alpha_score_count,
        invalid_date_count=invalid_date_count,
        dropped_row_count=int(row_count_input - len(clean)),
        rank_normalized=bool(rank_normalize_by_date),
        winsorize_quantile=quantile,
    )
    return AlphaInputResult(panel=clean, report=report)


def load_alpha_scores(
    path: str | Path,
    *,
    rank_normalize_by_date: bool = False,
    winsorize_quantile: float | None = None,
) -> AlphaInputResult:
    """Load and clean a CSV/parquet alpha-score panel."""

    source_path = Path(path)
    return clean_alpha_scores(
        _read_alpha_frame(source_path),
        source_name=str(source_path),
        rank_normalize_by_date=rank_normalize_by_date,
        winsorize_quantile=winsorize_quantile,
    )
