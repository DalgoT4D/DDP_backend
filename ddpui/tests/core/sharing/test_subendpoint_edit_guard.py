"""Task 5b: close the sub-resource WRITE gap the Task 5 review found.

Task 5 added `require_edit_access` (resolver-edit object check) after the
slug gate on the 5 main resource UPDATE endpoints. It missed that several
sub-resource WRITE endpoints on the same routers were still gated ONLY by
role slug (`@has_permission(["can_edit_*"])`) with no object-level check —
so a viewer with only VIEW access to a resource (general access, or an
explicit "view" grant) could still mutate its sub-resources, because the
role slug alone says nothing about THIS object.

This suite covers:
  - Dashboard filters: create/update/delete (`dashboard_native_api.py`)
  - Dashboard locks: lock/refresh/unlock (`dashboard_native_api.py`)
  - KPI notes: create/update/delete (`kpi_api.py`)

Same pattern as `test_update_edit_guard.py`, which this file reuses
fixtures from: an "analyst" persona carries every `can_edit_*` role slug
but must still fail the resolver-edit object check on a resource it only
has VIEW on (general access) — and pass once granted "edit" (ResourceShare)
or when acting as an admin (org-wide override).
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from ninja.errors import HttpError

from ddpui.auth import ADMIN_ROLE
from ddpui.api.dashboard_native_api import (
    create_filter,
    delete_filter,
    lock_dashboard,
    refresh_dashboard_lock,
    unlock_dashboard,
    update_filter,
)
from ddpui.api.kpi_api import create_annotation, delete_annotation, update_annotation
from ddpui.models.dashboard import DashboardFilter, DashboardFilterType, DashboardLock
from ddpui.models.general_access import AccessLevel
from ddpui.schemas.dashboard_schema import FilterCreate, FilterUpdate
from ddpui.schemas.kpi_schema import AnnotationEntryCreate, AnnotationEntryUpdate
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db
from ddpui.tests.core.sharing.test_update_edit_guard import (
    VIEW_ONLY,
    _dashboard,
    _grant_edit,
    _kpi,
    _make_orguser,
    analyst,
    org,
    owner,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def admin(org, seed_db):
    ou = _make_orguser(org, ADMIN_ROLE, "editguard-admin")
    yield ou
    ou.delete()


EDITABLE = {"analyst_level": AccessLevel.NONE, "member_level": AccessLevel.NONE}


def _filter_create_payload(**overrides):
    data = dict(
        name="Region",
        filter_type=DashboardFilterType.VALUE.value,
        schema_name="public",
        table_name="beneficiaries",
        column_name="region",
    )
    data.update(overrides)
    return FilterCreate(**data)


def _make_filter(dashboard):
    return DashboardFilter.objects.create(
        dashboard=dashboard,
        name="Region",
        filter_type=DashboardFilterType.VALUE.value,
        schema_name="public",
        table_name="beneficiaries",
        column_name="region",
    )


# ================================================================================
# Dashboard filters
# ================================================================================


class TestDashboardCreateFilterEditGuard:
    def test_view_only_denied(self, org, owner, analyst):
        dashboard = _dashboard(org, owner, **VIEW_ONLY)
        with pytest.raises(HttpError) as excinfo:
            create_filter(mock_request(analyst), dashboard.id, _filter_create_payload())
        assert excinfo.value.status_code == 403
        assert not DashboardFilter.objects.filter(dashboard=dashboard).exists()

    def test_editor_via_grant_allowed(self, org, owner, analyst):
        dashboard = _dashboard(org, owner, **EDITABLE)
        _grant_edit(org, "dashboard", dashboard, analyst)

        response = create_filter(mock_request(analyst), dashboard.id, _filter_create_payload())
        assert response.name == "Region"

    def test_admin_allowed(self, org, owner, admin):
        dashboard = _dashboard(org, owner, **EDITABLE)

        response = create_filter(mock_request(admin), dashboard.id, _filter_create_payload())
        assert response.name == "Region"


class TestDashboardUpdateFilterEditGuard:
    def test_view_only_denied(self, org, owner, analyst):
        dashboard = _dashboard(org, owner, **VIEW_ONLY)
        filter_obj = _make_filter(dashboard)

        with pytest.raises(HttpError) as excinfo:
            update_filter(
                mock_request(analyst), dashboard.id, filter_obj.id, FilterUpdate(name="renamed")
            )
        assert excinfo.value.status_code == 403
        filter_obj.refresh_from_db()
        assert filter_obj.name == "Region"

    def test_editor_via_grant_allowed(self, org, owner, analyst):
        dashboard = _dashboard(org, owner, **EDITABLE)
        filter_obj = _make_filter(dashboard)
        _grant_edit(org, "dashboard", dashboard, analyst)

        response = update_filter(
            mock_request(analyst), dashboard.id, filter_obj.id, FilterUpdate(name="renamed")
        )
        assert response.name == "renamed"

    def test_admin_allowed(self, org, owner, admin):
        dashboard = _dashboard(org, owner, **EDITABLE)
        filter_obj = _make_filter(dashboard)

        response = update_filter(
            mock_request(admin), dashboard.id, filter_obj.id, FilterUpdate(name="renamed")
        )
        assert response.name == "renamed"


class TestDashboardDeleteFilterEditGuard:
    def test_view_only_denied(self, org, owner, analyst):
        dashboard = _dashboard(org, owner, **VIEW_ONLY)
        filter_obj = _make_filter(dashboard)

        with pytest.raises(HttpError) as excinfo:
            delete_filter(mock_request(analyst), dashboard.id, filter_obj.id)
        assert excinfo.value.status_code == 403
        assert DashboardFilter.objects.filter(id=filter_obj.id).exists()

    def test_editor_via_grant_allowed(self, org, owner, analyst):
        dashboard = _dashboard(org, owner, **EDITABLE)
        filter_obj = _make_filter(dashboard)
        _grant_edit(org, "dashboard", dashboard, analyst)

        response = delete_filter(mock_request(analyst), dashboard.id, filter_obj.id)
        assert response == {"success": True}
        assert not DashboardFilter.objects.filter(id=filter_obj.id).exists()

    def test_admin_allowed(self, org, owner, admin):
        dashboard = _dashboard(org, owner, **EDITABLE)
        filter_obj = _make_filter(dashboard)

        response = delete_filter(mock_request(admin), dashboard.id, filter_obj.id)
        assert response == {"success": True}
        assert not DashboardFilter.objects.filter(id=filter_obj.id).exists()


# ================================================================================
# Dashboard locks
# ================================================================================


class TestDashboardLockEditGuard:
    def test_view_only_denied(self, org, owner, analyst):
        dashboard = _dashboard(org, owner, **VIEW_ONLY)

        with pytest.raises(HttpError) as excinfo:
            lock_dashboard(mock_request(analyst), dashboard.id)
        assert excinfo.value.status_code == 403
        assert not DashboardLock.objects.filter(dashboard=dashboard).exists()

    def test_editor_via_grant_allowed(self, org, owner, analyst):
        dashboard = _dashboard(org, owner, **EDITABLE)
        _grant_edit(org, "dashboard", dashboard, analyst)

        response = lock_dashboard(mock_request(analyst), dashboard.id)
        assert response.locked_by == analyst.user.email

    def test_admin_allowed(self, org, owner, admin):
        dashboard = _dashboard(org, owner, **EDITABLE)

        response = lock_dashboard(mock_request(admin), dashboard.id)
        assert response.locked_by == admin.user.email


class TestDashboardRefreshLockEditGuard:
    def test_view_only_denied(self, org, owner, analyst):
        dashboard = _dashboard(org, owner, **VIEW_ONLY)

        with pytest.raises(HttpError) as excinfo:
            refresh_dashboard_lock(mock_request(analyst), dashboard.id)
        assert excinfo.value.status_code == 403

    def test_editor_via_grant_allowed(self, org, owner, analyst):
        dashboard = _dashboard(org, owner, **EDITABLE)
        _grant_edit(org, "dashboard", dashboard, analyst)
        lock_dashboard(mock_request(analyst), dashboard.id)

        response = refresh_dashboard_lock(mock_request(analyst), dashboard.id)
        assert response.locked_by == analyst.user.email

    def test_admin_allowed(self, org, owner, admin):
        dashboard = _dashboard(org, owner, **EDITABLE)
        lock_dashboard(mock_request(admin), dashboard.id)

        response = refresh_dashboard_lock(mock_request(admin), dashboard.id)
        assert response.locked_by == admin.user.email


class TestDashboardUnlockEditGuard:
    def test_view_only_denied(self, org, owner, analyst):
        dashboard = _dashboard(org, owner, **VIEW_ONLY)

        with pytest.raises(HttpError) as excinfo:
            unlock_dashboard(mock_request(analyst), dashboard.id)
        assert excinfo.value.status_code == 403

    def test_editor_via_grant_allowed(self, org, owner, analyst):
        dashboard = _dashboard(org, owner, **EDITABLE)
        _grant_edit(org, "dashboard", dashboard, analyst)
        lock_dashboard(mock_request(analyst), dashboard.id)

        response = unlock_dashboard(mock_request(analyst), dashboard.id)
        assert response == {"success": True}

    def test_admin_allowed(self, org, owner, admin):
        dashboard = _dashboard(org, owner, **EDITABLE)
        lock_dashboard(mock_request(admin), dashboard.id)

        response = unlock_dashboard(mock_request(admin), dashboard.id)
        assert response == {"success": True}


# ================================================================================
# KPI notes
# ================================================================================


def _annotation_payload(**overrides):
    data = dict(note_type="manual", period_key="2026-01", content="a note")
    data.update(overrides)
    return AnnotationEntryCreate(**data)


def _seed_annotation(kpi, created_by_email):
    entry = {
        "id": 1,
        "note_type": "manual",
        "period_key": "2026-01",
        "period_date": None,
        "content": "original",
        "snapshot_value": None,
        "snapshot_pop_change": None,
        "created_by_email": created_by_email,
        "last_modified_by_email": created_by_email,
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
    }
    kpi.annotations = [entry]
    kpi.save(update_fields=["annotations"])
    return entry


class TestKpiCreateAnnotationEditGuard:
    def test_view_only_denied(self, org, owner, analyst):
        kpi = _kpi(org, owner, **VIEW_ONLY)

        with pytest.raises(HttpError) as excinfo:
            create_annotation(mock_request(analyst), kpi.id, _annotation_payload())
        assert excinfo.value.status_code == 403
        kpi.refresh_from_db()
        assert kpi.annotations == []

    def test_editor_via_grant_allowed(self, org, owner, analyst):
        kpi = _kpi(org, owner, **EDITABLE)
        _grant_edit(org, "kpi", kpi, analyst)

        response = create_annotation(mock_request(analyst), kpi.id, _annotation_payload())
        assert response.content == "a note"

    def test_admin_allowed(self, org, owner, admin):
        kpi = _kpi(org, owner, **EDITABLE)

        response = create_annotation(mock_request(admin), kpi.id, _annotation_payload())
        assert response.content == "a note"


class TestKpiUpdateAnnotationEditGuard:
    def test_view_only_denied(self, org, owner, analyst):
        kpi = _kpi(org, owner, **VIEW_ONLY)
        entry = _seed_annotation(kpi, analyst.user.email)

        with pytest.raises(HttpError) as excinfo:
            update_annotation(
                mock_request(analyst), kpi.id, entry["id"], AnnotationEntryUpdate(content="edited")
            )
        assert excinfo.value.status_code == 403
        kpi.refresh_from_db()
        assert kpi.annotations[0]["content"] == "original"

    def test_editor_via_grant_allowed(self, org, owner, analyst):
        kpi = _kpi(org, owner, **EDITABLE)
        entry = _seed_annotation(kpi, analyst.user.email)
        _grant_edit(org, "kpi", kpi, analyst)

        response = update_annotation(
            mock_request(analyst), kpi.id, entry["id"], AnnotationEntryUpdate(content="edited")
        )
        assert response.content == "edited"

    def test_admin_allowed(self, org, owner, admin):
        kpi = _kpi(org, owner, **EDITABLE)
        entry = _seed_annotation(kpi, admin.user.email)

        response = update_annotation(
            mock_request(admin), kpi.id, entry["id"], AnnotationEntryUpdate(content="edited")
        )
        assert response.content == "edited"


class TestKpiDeleteAnnotationEditGuard:
    def test_view_only_denied(self, org, owner, analyst):
        kpi = _kpi(org, owner, **VIEW_ONLY)
        entry = _seed_annotation(kpi, analyst.user.email)

        with pytest.raises(HttpError) as excinfo:
            delete_annotation(mock_request(analyst), kpi.id, entry["id"])
        assert excinfo.value.status_code == 403
        kpi.refresh_from_db()
        assert len(kpi.annotations) == 1

    def test_editor_via_grant_allowed(self, org, owner, analyst):
        kpi = _kpi(org, owner, **EDITABLE)
        entry = _seed_annotation(kpi, analyst.user.email)
        _grant_edit(org, "kpi", kpi, analyst)

        response = delete_annotation(mock_request(analyst), kpi.id, entry["id"])
        assert response["success"] is True
        kpi.refresh_from_db()
        assert kpi.annotations == []

    def test_admin_allowed(self, org, owner, admin):
        kpi = _kpi(org, owner, **EDITABLE)
        entry = _seed_annotation(kpi, admin.user.email)

        response = delete_annotation(mock_request(admin), kpi.id, entry["id"])
        assert response["success"] is True
        kpi.refresh_from_db()
        assert kpi.annotations == []
