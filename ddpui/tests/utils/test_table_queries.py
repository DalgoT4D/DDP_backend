from types import SimpleNamespace

from ddpui.utils.warehouse.client.table_queries import _split_bq_schema, list_table_names
from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType


class _FakeBigQueryClient:
    def __init__(self):
        self.engine = SimpleNamespace(url=SimpleNamespace(database="fallback-project"))
        self.last_query = None

    def execute(self, query, params=None):
        self.last_query = query
        return [{"table_name": "events"}]


def test_split_bq_schema_domain_prefixed_project():
    assert _split_bq_schema("example.com:project-id.analytics") == (
        "example.com:project-id",
        "analytics",
    )


def test_split_bq_schema_domain_prefixed_legacy_with_table_suffix():
    assert _split_bq_schema("example.com:project-id:analytics.events") == (
        "example.com:project-id",
        "analytics",
    )


def test_split_bq_schema_domain_prefixed_with_table_suffix():
    assert _split_bq_schema("example.com:project-id.analytics.events") == (
        "example.com:project-id",
        "analytics",
    )


def test_list_table_names_builds_domain_prefixed_query():
    client = _FakeBigQueryClient()

    result = list_table_names(
        client,
        WarehouseType.BIGQUERY.value,
        "example.com:project-id.analytics",
    )

    assert result == ["events"]
    assert "`example.com:project-id.analytics.INFORMATION_SCHEMA.TABLES`" in client.last_query
