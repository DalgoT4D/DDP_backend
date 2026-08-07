"""Tests for context building: schema derivation (pure) and scope wiring (DB)."""

import os

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from django.contrib.auth.models import User

from ddpui.core.ai.agent import context_builder as context_module
from ddpui.core.ai.agent.context_builder import build_run_context, derive_allowed_schemas
from ddpui.models.chat_with_data import ChatWithDataSession
from ddpui.models.dashboard import Dashboard
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.visualization import Chart


class SchemaWarehouse:
    def __init__(self, schemas):
        self.schemas = schemas

    def execute(self, sql):
        return [{"schema_name": s} for s in self.schemas]


def test_dbt_default_schema_wins_when_it_exists_in_warehouse():
    warehouse = SchemaWarehouse(["prod", "staging", "raw_kobo"])
    assert derive_allowed_schemas(warehouse, "postgres", dbt_default_schema="prod") == ["prod"]


def test_falls_back_to_raw_schemas_when_org_has_no_dbt():
    # decision 2 (research §4): raw-only orgs still get answers
    warehouse = SchemaWarehouse(["raw_kobo", "raw_sheets"])
    assert derive_allowed_schemas(warehouse, "postgres", dbt_default_schema=None) == [
        "raw_kobo",
        "raw_sheets",
    ]


def test_system_schemas_are_never_offered():
    warehouse = SchemaWarehouse(["information_schema", "pg_catalog", "airbyte_internal", "raw_x"])
    assert derive_allowed_schemas(warehouse, "postgres", dbt_default_schema=None) == ["raw_x"]


def test_stale_dbt_schema_falls_back_to_raw():
    # dbt configured but its schema is gone from the warehouse — don't offer a ghost
    warehouse = SchemaWarehouse(["raw_kobo"])
    assert derive_allowed_schemas(warehouse, "postgres", dbt_default_schema="prod") == ["raw_kobo"]


# ── Scope wiring (DB) ───────────────────────────────────────────────────────


@pytest.fixture
def scoped_setup(monkeypatch):
    """Org + warehouse + dashboard(1 chart) + one scoped and one org session."""
    org = Org.objects.create(name="Ctx Test Org", slug="ctx-test")
    OrgWarehouse.objects.create(org=org, wtype="postgres")
    user = User.objects.create(username="ctxuser", email="ctxuser@test.com", password="x")
    orguser = OrgUser.objects.create(user=user, org=org)
    chart = Chart.objects.create(
        title="Surveys by district",
        chart_type="bar",
        computation_type="aggregated",
        schema_name="prod",
        table_name="surveys",
        org=org,
        created_by=orguser,
    )
    dashboard = Dashboard.objects.create(
        title="Field Performance",
        org=org,
        created_by=orguser,
        tabs=[
            {
                "id": "t1",
                "title": "Main",
                "layout_config": {},
                "components": {"c1": {"type": "chart", "config": {"chartId": chart.id}}},
            }
        ],
    )
    monkeypatch.setattr(
        context_module.WarehouseFactory,
        "get_warehouse_client",
        staticmethod(lambda org_warehouse: SchemaWarehouse(["prod", "raw_kobo"])),
    )
    yield orguser, dashboard
    dashboard.delete()
    chart.delete()
    orguser.delete()
    user.delete()
    org.delete()


@pytest.mark.django_db
def test_dashboard_scoped_session_narrows_the_context(scoped_setup):
    orguser, dashboard = scoped_setup
    session = ChatWithDataSession.objects.create(
        org=orguser.org, orguser=orguser, scope_type="dashboard", scope_id=dashboard.id
    )

    ctx = build_run_context(orguser, session=session)

    assert ctx.scope_type == "dashboard"
    assert ctx.allowed_tables == ["prod.surveys"]
    # schemas narrowed to the scoped tables' schemas — no schemata discovery
    assert ctx.allowed_schemas == ["prod"]
    assert '"Field Performance"' in ctx.scope_context


@pytest.mark.django_db
def test_org_session_and_no_session_keep_full_context(scoped_setup):
    orguser, _ = scoped_setup
    org_session = ChatWithDataSession.objects.create(org=orguser.org, orguser=orguser)

    for ctx in (build_run_context(orguser), build_run_context(orguser, session=org_session)):
        assert ctx.scope_type == "org"
        assert ctx.allowed_tables is None
        assert ctx.scope_context == ""
        assert ctx.allowed_schemas == ["prod", "raw_kobo"]
