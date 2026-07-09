"""Tests for allowed-schema derivation (pure part of context building)."""

from ddpui.core.chat_with_data.agent.context import derive_allowed_schemas


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
