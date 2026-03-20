"""Unit tests for dashboard chat warehouse helpers."""

import json
import os
from types import SimpleNamespace
from unittest.mock import patch

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.dashboard_chat.warehouse_tools import (
    DashboardChatWarehouseTools,
    DashboardChatWarehouseToolsError,
)


def _build_bigquery_tools():
    return DashboardChatWarehouseTools(
        org=SimpleNamespace(id=1),
        org_warehouse=SimpleNamespace(
            wtype="bigquery", credentials="warehouse-secret", bq_location="asia-south1"
        ),
        warehouse_client=object(),
    )


def test_quote_bigquery_table_ref_uses_project_id_from_credentials():
    """BigQuery table refs should use project_id from stored credentials, not dataset location."""
    with patch(
        "ddpui.core.dashboard_chat.warehouse_tools.secretsmanager.retrieve_warehouse_credentials",
        return_value={"project_id": "analytics-project"},
    ):
        tools = _build_bigquery_tools()
        assert tools._quote_bigquery_table_ref("analytics", "program_reach") == (
            "`analytics-project.analytics.program_reach`"
        )


def test_quote_bigquery_table_ref_reads_nested_project_id_from_credentials_json():
    """credentials_json payloads should still provide the BigQuery project id."""
    with patch(
        "ddpui.core.dashboard_chat.warehouse_tools.secretsmanager.retrieve_warehouse_credentials",
        return_value={"credentials_json": json.dumps({"project_id": "analytics-project"})},
    ):
        tools = _build_bigquery_tools()
        assert tools._quote_bigquery_table_ref("analytics", "program_reach") == (
            "`analytics-project.analytics.program_reach`"
        )


def test_quote_bigquery_table_ref_requires_project_id():
    """A missing project id should fail explicitly."""
    with patch(
        "ddpui.core.dashboard_chat.warehouse_tools.secretsmanager.retrieve_warehouse_credentials",
        return_value={"dataset_location": "asia-south1"},
    ):
        tools = _build_bigquery_tools()
        with pytest.raises(
            DashboardChatWarehouseToolsError, match="BigQuery project id not configured"
        ):
            tools._quote_bigquery_table_ref("analytics", "program_reach")
