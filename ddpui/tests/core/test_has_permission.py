"""Regression tests for the has_permission decorator (issue #1325).

These tests verify that has_permission raises HTTP 403 (not 404) when the
caller lacks the required permissions. A previous bare ``except`` block in
the decorator masked 403 errors as 404, violating HTTP semantics.

These tests do not touch the database, so they intentionally live outside
test_auth.py (which is module-marked with pytest.mark.django_db).
"""

from unittest.mock import Mock

import pytest
from ninja.errors import HttpError

from ddpui.auth import has_permission


def _make_request_with_permissions(permissions):
    request = Mock()
    request.permissions = permissions
    return request


def test_has_permission_raises_403_when_permissions_empty():
    """Empty permissions list must raise 403, not 404."""

    @has_permission(["can_view_charts"])
    def endpoint(request):
        return "ok"

    request = _make_request_with_permissions([])
    with pytest.raises(HttpError) as excinfo:
        endpoint(request)
    assert excinfo.value.status_code == 403
    assert str(excinfo.value) == "not allowed"


def test_has_permission_raises_403_when_required_slug_missing():
    """Missing a required permission slug must raise 403, not 404."""

    @has_permission(["can_edit_charts"])
    def endpoint(request):
        return "ok"

    request = _make_request_with_permissions(["can_view_charts"])
    with pytest.raises(HttpError) as excinfo:
        endpoint(request)
    assert excinfo.value.status_code == 403
    assert str(excinfo.value) == "not allowed"


def test_has_permission_allows_when_caller_has_required_permissions():
    """Happy path: caller has the required slug, endpoint executes."""

    @has_permission(["can_view_charts"])
    def endpoint(request):
        return "ok"

    request = _make_request_with_permissions(["can_view_charts", "can_edit_charts"])
    assert endpoint(request) == "ok"
