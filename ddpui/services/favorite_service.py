"""Shared favorite service for charts, dashboards, and other favoritable resources.

Mirrors Superset's own favorite DAO pattern (superset/daos/chart.py,
superset/daos/dashboard.py): favorite/unfavorite/favorited_ids, scoped to the
current user, against one shared table rather than one per resource type.
"""

from typing import List, Set

from ddpui.models.favorite import Favorite, FavoriteResourceType
from ddpui.models.org_user import OrgUser


class FavoriteService:
    """Service class for favorite operations, shared across resource types"""

    @staticmethod
    def add_favorite(
        resource_type: FavoriteResourceType, resource_id: int, orguser: OrgUser
    ) -> None:
        """Mark a resource as favorited by this user"""
        Favorite.objects.get_or_create(
            org_user=orguser, resource_type=resource_type.value, resource_id=resource_id
        )

    @staticmethod
    def remove_favorite(
        resource_type: FavoriteResourceType, resource_id: int, orguser: OrgUser
    ) -> None:
        """Remove this user's favorite on a resource, if any"""
        Favorite.objects.filter(
            org_user=orguser, resource_type=resource_type.value, resource_id=resource_id
        ).delete()

    @staticmethod
    def get_favorited_ids(
        resource_type: FavoriteResourceType, resource_ids: List[int], orguser: OrgUser
    ) -> Set[int]:
        """Return the subset of resource_ids this user has favorited"""
        if not resource_ids:
            return set()
        return set(
            Favorite.objects.filter(
                org_user=orguser,
                resource_type=resource_type.value,
                resource_id__in=resource_ids,
            ).values_list("resource_id", flat=True)
        )
