"""Tests for context building: schema derivation (pure) and scope wiring (DB)."""

import os

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from django.contrib.auth.models import User

from django.core.exceptions import ValidationError

from ddpui.core.ai.agent import context_builder as context_module
from ddpui.core.ai.agent.context_builder import build_run_context, derive_allowed_schemas
from ddpui.models.chat_with_data import ChatWithDataOrgConfig, ChatWithDataSession
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


# ── Context building (DB) ───────────────────────────────────────────────────


@pytest.fixture
def org_setup(monkeypatch):
    """Org + warehouse + one orguser, with a fake two-schema warehouse."""
    org = Org.objects.create(name="Ctx Test Org", slug="ctx-test")
    OrgWarehouse.objects.create(org=org, wtype="postgres")
    user = User.objects.create(username="ctxuser", email="ctxuser@test.com", password="x")
    orguser = OrgUser.objects.create(user=user, org=org)
    monkeypatch.setattr(
        context_module.WarehouseFactory,
        "get_warehouse_client",
        staticmethod(lambda org_warehouse: SchemaWarehouse(["prod", "raw_kobo"])),
    )
    yield orguser
    orguser.delete()
    user.delete()
    org.delete()


@pytest.mark.django_db
def test_context_carries_all_non_system_schemas(org_setup):
    orguser = org_setup
    ctx = build_run_context(orguser)
    assert ctx.allowed_schemas == ["prod", "raw_kobo"]
    assert ctx.org_slug == "ctx-test"


@pytest.mark.django_db
def test_context_uses_defaults_when_org_has_no_config_row(org_setup):
    ctx = build_run_context(org_setup)
    assert ctx.max_result_rows == 100
    assert ctx.query_timeout_s == 30
    assert ctx.pii_rules == []


@pytest.mark.django_db
def test_org_config_row_overrides_schemas_limits_and_pii_rules(org_setup):
    orguser = org_setup
    rule = {"pii_type": "case_id", "detector": r"CASE-\d{6}", "strategy": "redact"}
    ChatWithDataOrgConfig.objects.create(
        org=orguser.org,
        allowed_schemas=["prod"],
        max_result_rows=50,
        query_timeout_s=10,
        pii_rules=[rule],
    )

    ctx = build_run_context(orguser)

    assert ctx.allowed_schemas == ["prod"]  # admin's list wins over derivation
    assert ctx.max_result_rows == 50
    assert ctx.query_timeout_s == 10
    assert ctx.pii_rules == [rule]


@pytest.mark.django_db
def test_org_config_rejects_invalid_pii_rules_at_save_time(org_setup):
    config = ChatWithDataOrgConfig(
        org=org_setup.org,
        pii_rules=[{"pii_type": "email", "detector": "x"}],  # collides with a default
    )
    with pytest.raises(ValidationError, match="built-in"):
        config.full_clean()
