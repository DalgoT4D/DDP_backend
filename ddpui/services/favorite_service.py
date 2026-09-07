"""Shared favorite service for charts, dashboards, and other favoritable resources.

Mirrors Superset's own favorite DAO pattern (superset/daos/chart.py,
superset/daos/dashboard.py): favorite/unfavorite/favorited_ids, scoped to the
current user, against one shared table rather than one per resource type.
"""

from typing import List, Set

from ddpui.models.favorite import Favorite
from ddpui.models.org_user import OrgUser
from ddpui.models.resource_share import ResourceType

# ResourceType also covers REPORT and KPI (for access control elsewhere), but
# favoriting itself is only wired up for these two — enforced here rather than
# left implicit, since FavoriteService has no other guard against a future
# caller favoriting a resource type nothing validates existence for.
SUPPORTED_RESOURCE_TYPES = {ResourceType.CHART, ResourceType.DASHBOARD}


def _assert_supported(resource_type: ResourceType) -> None:
    assert (
        resource_type in SUPPORTED_RESOURCE_TYPES
    ), f"Favoriting is not supported for resource type '{resource_type}'"


class FavoriteService:
    """Service class for favorite operations, shared across resource types"""

    @staticmethod
    def add_favorite(resource_type: ResourceType, resource_id: int, orguser: OrgUser) -> None:
        """Mark a resource as favorited by this user"""
        _assert_supported(resource_type)
        Favorite.objects.get_or_create(
            org_user=orguser, resource_type=resource_type.value, resource_id=resource_id
        )

    @staticmethod
    def remove_favorite(resource_type: ResourceType, resource_id: int, orguser: OrgUser) -> None:
        """Remove this user's favorite on a resource, if any"""
        _assert_supported(resource_type)
        Favorite.objects.filter(
            org_user=orguser, resource_type=resource_type.value, resource_id=resource_id
        ).delete()

    @staticmethod
    def is_favorited(resource_type: ResourceType, resource_id: int, orguser: OrgUser) -> bool:
        """Whether this user has favorited a single resource. Single-resource
        counterpart to get_favorited_ids, for detail/update responses."""
        _assert_supported(resource_type)
        return Favorite.objects.filter(
            org_user=orguser, resource_type=resource_type.value, resource_id=resource_id
        ).exists()

    @staticmethod
    def get_favorited_ids(
        resource_type: ResourceType, resource_ids: List[int], orguser: OrgUser
    ) -> Set[int]:
        """Return the subset of resource_ids this user has favorited"""
        _assert_supported(resource_type)
        if not resource_ids:
            return set()
        return set(
            Favorite.objects.filter(
                org_user=orguser,
                resource_type=resource_type.value,
                resource_id__in=resource_ids,
            ).values_list("resource_id", flat=True)
        )

    @staticmethod
    def remove_favorites_for_resource(resource_type: ResourceType, resource_id: int) -> None:
        """Delete every user's favorite on a resource. resource_id isn't a real
        ForeignKey (it's generic across resource types), so there's no DB-level
        cascade when the chart/dashboard itself is deleted — callers must call
        this explicitly at the point of deletion to avoid orphaned rows."""
        _assert_supported(resource_type)
        Favorite.objects.filter(resource_type=resource_type.value, resource_id=resource_id).delete()

    @staticmethod
    def remove_favorites_for_resources(
        resource_type: ResourceType, resource_ids: List[int]
    ) -> None:
        """Bulk variant of remove_favorites_for_resource, for bulk-delete flows."""
        _assert_supported(resource_type)
        if not resource_ids:
            return
        Favorite.objects.filter(
            resource_type=resource_type.value, resource_id__in=resource_ids
        ).delete()
