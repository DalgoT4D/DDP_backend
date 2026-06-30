"""
Unit tests for the creator-or-Admin delete helper.
Uses mock objects — no DB needed. Ownership is keyed off ``created_by``.
"""

from unittest.mock import MagicMock

from ddpui.core.ownership import can_delete_resource


def _orguser(org_user_id: int, role_slug: str):
    ou = MagicMock()
    ou.id = org_user_id
    ou.new_role.slug = role_slug
    return ou


def _resource(created_by_id):
    r = MagicMock()
    r.created_by_id = created_by_id
    return r


def test_creator_can_delete():
    assert can_delete_resource(_orguser(1, "analyst"), _resource(1)) is True


def test_non_creator_analyst_cannot_delete():
    assert can_delete_resource(_orguser(1, "analyst"), _resource(2)) is False


def test_admin_can_delete_others_resource():
    assert can_delete_resource(_orguser(1, "admin"), _resource(2)) is True


def test_resource_with_null_creator_blocks_non_admin():
    assert can_delete_resource(_orguser(1, "analyst"), _resource(None)) is False


def test_resource_with_null_creator_allows_admin():
    assert can_delete_resource(_orguser(1, "admin"), _resource(None)) is True
