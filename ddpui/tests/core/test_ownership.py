"""
Unit tests for the owner-or-Admin delete helper.
Uses mock objects — no DB needed. Ownership is keyed off ``owner_id``,
falling back to ``created_by_id`` when ``owner_id`` is null (pre-backfill
or resources that predate the ``owner`` column).
"""

from unittest.mock import MagicMock

from ddpui.core.ownership import can_delete_resource


def _orguser(org_user_id: int, role_slug: str):
    ou = MagicMock()
    ou.id = org_user_id
    ou.new_role.slug = role_slug
    return ou


def _resource(created_by_id, owner_id=None):
    r = MagicMock()
    r.created_by_id = created_by_id
    r.owner_id = owner_id
    return r


def test_creator_can_delete():
    assert can_delete_resource(_orguser(1, "analyst"), _resource(1)) is True


def test_non_creator_analyst_cannot_delete():
    assert can_delete_resource(_orguser(1, "analyst"), _resource(2)) is False


def test_admin_can_delete_others_resource():
    assert can_delete_resource(_orguser(1, "admin"), _resource(2)) is True


def test_super_admin_can_delete_others_resource():
    assert can_delete_resource(_orguser(1, "super-admin"), _resource(2)) is True


def test_non_creator_with_null_role_is_denied_not_crash():
    ou = _orguser(1, "analyst")
    ou.new_role = None
    assert can_delete_resource(ou, _resource(2)) is False


def test_creator_with_null_role_can_still_delete_own_resource():
    ou = _orguser(1, "analyst")
    ou.new_role = None
    assert can_delete_resource(ou, _resource(1)) is True


def test_resource_with_null_creator_blocks_non_admin():
    assert can_delete_resource(_orguser(1, "analyst"), _resource(None)) is False


def test_resource_with_null_creator_allows_admin():
    assert can_delete_resource(_orguser(1, "admin"), _resource(None)) is True


# ---------------------------------------------------------------------------
# Owner-first behavior: owner_id wins when set; created_by_id is only
# consulted as a fallback when owner_id is null.
# ---------------------------------------------------------------------------


def test_owner_can_delete_even_when_not_creator():
    """owner_id set and matching orguser wins, regardless of created_by_id."""
    resource = _resource(created_by_id=2, owner_id=1)
    assert can_delete_resource(_orguser(1, "analyst"), resource) is True


def test_creator_can_still_delete_when_owner_is_null():
    """Falls back to created_by_id when owner_id is null (pre-backfill row)."""
    resource = _resource(created_by_id=1, owner_id=None)
    assert can_delete_resource(_orguser(1, "analyst"), resource) is True


def test_non_owner_non_creator_non_admin_cannot_delete():
    resource = _resource(created_by_id=2, owner_id=3)
    assert can_delete_resource(_orguser(1, "analyst"), resource) is False


def test_creator_cannot_delete_when_owner_set_to_someone_else():
    """Once owner_id is set, a non-owning creator no longer qualifies."""
    resource = _resource(created_by_id=1, owner_id=2)
    assert can_delete_resource(_orguser(1, "analyst"), resource) is False


def test_admin_can_delete_when_owner_set_to_other():
    resource = _resource(created_by_id=2, owner_id=2)
    assert can_delete_resource(_orguser(1, "admin"), resource) is True
