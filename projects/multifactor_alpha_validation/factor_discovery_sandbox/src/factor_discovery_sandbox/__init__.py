"""Factor Discovery Sandbox package."""

from .teaching_baseline import TeachingBaselineResult, run_teaching_baseline
from .factor_specs import write_price_volume_factor_specs
from .rolling_oos import RollingOOSResult, run_rolling_oos
from .marginal_value import MarginalValueGateResult, run_marginal_value_gate
from .allocator import AllocatorResult, run_allocator
from .survival import SurvivalAnalysisResult, run_survival_analysis

__all__ = [
    "AllocatorResult",
    "MarginalValueGateResult",
    "RollingOOSResult",
    "SurvivalAnalysisResult",
    "TeachingBaselineResult",
    "run_allocator",
    "run_marginal_value_gate",
    "run_rolling_oos",
    "run_survival_analysis",
    "run_teaching_baseline",
    "write_price_volume_factor_specs",
]
