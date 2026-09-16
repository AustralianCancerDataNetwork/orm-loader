"""Public materialized-view contracts and lifecycle helpers."""

# The established module filenames use British spelling (`materialised_*`),
# while public classes and methods use US spelling (`Materialized...`). Keep
# the module names for backwards compatibility.
#
# New code should import from `orm_loader.mappers`, so a future module rename
# will not affect package-level consumers. A later breaking release can rename
# the modules and remove the legacy paths.


from .materialised_view_contracts import (
    CreateMaterializedViewIndex,
    DropMaterializedView,
    MaterializedViewIndex,
)
from .materialised_view_mixin import (
    CreateMaterializedView,
    MaterializedViewMixin,
    refresh_all_mvs,
    resolve_mv_refresh_order,
)
from .materialised_view_errors import (
    ConcurrentRefreshNotEligibleError,
    MaterializationError,
    MaterializationFailure,
    MaterializationOperation,
    UnsupportedMaterializationDialectError,
)

__all__ = [
    "ConcurrentRefreshNotEligibleError",
    "CreateMaterializedView",
    "CreateMaterializedViewIndex",
    "DropMaterializedView",
    "MaterializationError",
    "MaterializationFailure",
    "MaterializationOperation",
    "MaterializedViewIndex",
    "MaterializedViewMixin",
    "UnsupportedMaterializationDialectError",
    "refresh_all_mvs",
    "resolve_mv_refresh_order",
]
