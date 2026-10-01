"""Regression: @has_permission must keep async views recognizably async.

Django Ninja awaits a view only when iscoroutinefunction(view) is True. The
original sync-only wrapper hid that, so async endpoints (chat history) returned
an un-awaited coroutine that Ninja tried to JSON-serialize → HTTP 500
('Object of type coroutine is not JSON serializable').
"""

import asyncio
import inspect
import os
from types import SimpleNamespace

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from ninja.errors import HttpError

from ddpui.auth import has_permission


def make_request(permissions):
    return SimpleNamespace(permissions=permissions, orguser=None)


def test_async_view_stays_a_coroutine_function():
    @has_permission(["can_use_chat_with_data"])
    async def view(request):
        return {"ok": True}

    assert inspect.iscoroutinefunction(view)  # ninja awaits only if this holds
    assert asyncio.run(view(make_request(["can_use_chat_with_data"]))) == {"ok": True}


def test_async_view_permission_denied_still_raises():
    @has_permission(["can_use_chat_with_data"])
    async def view(request):
        return {"ok": True}

    with pytest.raises(HttpError):
        asyncio.run(view(make_request(["something_else"])))


def test_sync_views_unchanged():
    @has_permission(["can_use_chat_with_data"])
    def view(request):
        return {"ok": True}

    assert not inspect.iscoroutinefunction(view)
    assert view(make_request(["can_use_chat_with_data"])) == {"ok": True}
