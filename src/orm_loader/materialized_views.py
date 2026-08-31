"""Public materialized-view definition and lifecycle API.

Database lifecycle mechanics belong here. Downstream packages retain domain
grain decisions, construct registries, rebuild policy, and command-line
orchestration.
"""

from .backends.materialized_view_errors import (
    ConcurrentRefreshNotEligibleError,
    MaterializationError,
    MaterializationFailure,
    MaterializationOperation,
    MaterializationOutcome,
    UnsupportedMaterializationDialectError,
)
from .mappers.materialised_view_contracts import (
    MaterializedSelectable,
    MaterializedViewIndex,
    MaterializedViewSpec,
)
from .mappers.materialised_view_mixin import (
    MaterializedViewMixin,
    refresh_all_mvs,
    resolve_mv_refresh_order,
)

__all__ = [
    "ConcurrentRefreshNotEligibleError",
    "MaterializationError",
    "MaterializationFailure",
    "MaterializationOperation",
    "MaterializationOutcome",
    "MaterializedSelectable",
    "MaterializedViewIndex",
    "MaterializedViewMixin",
    "MaterializedViewSpec",
    "UnsupportedMaterializationDialectError",
    "refresh_all_mvs",
    "resolve_mv_refresh_order",
]
