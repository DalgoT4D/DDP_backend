"""
Unit tests for the owner-or-Admin delete helper.
Uses mock objects — no DB needed.
"""

from unittest.mock import MagicMock

from ddpui.core.ownership import can_delete_resource


def _orguser(org_user_id: int, role_slug: str):
    ou = MagicMock()
    ou.id = org_user_id
    ou.new_role.slug = role_slug
    return ou


def _resource(owner_id):
    r = MagicMock()
    r.owner_id = owner_id
    return r


def test_owner_can_delete():
    assert can_delete_resource(_orguser(1, "analyst"), _resource(1)) is True


def test_non_owner_analyst_cannot_delete():
    assert can_delete_resource(_orguser(1, "analyst"), _resource(2)) is False


def test_admin_can_delete_others_resource():
    assert can_delete_resource(_orguser(1, "admin"), _resource(2)) is True


def test_resource_with_null_owner_blocks_non_admin():
    assert can_delete_resource(_orguser(1, "analyst"), _resource(None)) is False


def test_resource_with_null_owner_allows_admin():
    assert can_delete_resource(_orguser(1, "admin"), _resource(None)) is True
