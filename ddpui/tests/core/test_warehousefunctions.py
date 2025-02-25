import os
import re
import pytest
from unittest.mock import Mock, patch

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


from ddpui.models.org import Org, OrgWarehouse
from ddpui.core.warehousefunctions import (
    parse_sql_query_with_limit_offset,
    generate_sql_from_warehouse_rag,
)

pytestmark = pytest.mark.django_db


@pytest.fixture()
def dummy_org_warehouse():
    org = Org.objects.create(name="del", slug="del")
    warehouse = OrgWarehouse.objects.create(org=org, name="fake-warehouse-name")
    yield warehouse

    org.delete()


def test_parse_sql_query_with_limit_offset_invalid_sql_type():
    """We should not support any other queries apart from SELECT"""

    sql = "DELETE FROM table1"

    with pytest.raises(Exception, match="Only SELECT queries are allowed"):
        parse_sql_query_with_limit_offset(sql, 10, 0)


def test_parse_sql_query_with_limit_offset_invalid_only_limit_preset():
    """If OFFSET is not present, the function should add it"""
    sql = "SELECT * FROM table1 LIMIT 10"

    parsed_sql = parse_sql_query_with_limit_offset(sql, 10, 1)

    assert parsed_sql.find("OFFSET 1") > 0


def test_parse_sql_query_with_limit_offset_invalid_only_offset_preset():
    """If LIMIT is not present, the function should add it"""
    sql = "SELECT * FROM table1 OFFSET 0"

    parsed_sql = parse_sql_query_with_limit_offset(sql, 100, 1)

    assert parsed_sql.find("LIMIT 100") > 0


@patch(
    "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
)
def test_generate_sql_from_warehouse_rag_pgvector_creds_not_found(
    mock_retrieve_warehouse_credentials: Mock,
    dummy_org_warehouse: OrgWarehouse,
):
    """Failure because pgvector creds not found"""

    with pytest.raises(Exception, match="Couldn't find the pgvector creds for the org"):
        generate_sql_from_warehouse_rag(
            warehouse=dummy_org_warehouse, user_prompt="How is the weather today ?"
        )

    mock_retrieve_warehouse_credentials.assert_called_once_with(dummy_org_warehouse)


@patch(
    "ddpui.utils.secretsmanager.retrieve_pgvector_credentials",
)
@patch(
    "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
)
def test_generate_sql_from_warehouse_rag_pgvector_creds_not_saved_in_secrets_manager(
    mock_retrieve_warehouse_credentials: Mock,
    mock_retrieve_pgvector_credentials: Mock,
    dummy_org_warehouse: OrgWarehouse,
):
    """Failure because pgvector creds not saved to secrets manager"""
    mock_retrieve_pgvector_credentials.return_value = None

    dummy_org_warehouse.org.pgvector_creds = "pgvectorCreds-1234"

    with pytest.raises(Exception, match="Pg vector creds for the org not created/generated"):
        generate_sql_from_warehouse_rag(
            warehouse=dummy_org_warehouse, user_prompt="How is the weather today ?"
        )

    mock_retrieve_warehouse_credentials.assert_called_once_with(dummy_org_warehouse)
    mock_retrieve_pgvector_credentials.assert_called_once_with(dummy_org_warehouse.org)


@patch(
    "ddpui.utils.secretsmanager.retrieve_pgvector_credentials",
)
@patch(
    "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
)
def test_generate_sql_from_warehouse_rag_pgvector_creds_validation_error(
    mock_retrieve_warehouse_credentials: Mock,
    mock_retrieve_pgvector_credentials: Mock,
    dummy_org_warehouse: OrgWarehouse,
):
    """
    Partial pgvector creds object to check validation error
    """
    mock_retrieve_pgvector_credentials.return_value = {
        "username": "something-suer",
        "host": "localhost",
    }

    dummy_org_warehouse.org.pgvector_creds = "pgvectorCreds-1234"

    with pytest.raises(Exception, match="Pg vector creds are not of the right schema"):
        generate_sql_from_warehouse_rag(
            warehouse=dummy_org_warehouse, user_prompt="How is the weather today ?"
        )

    mock_retrieve_warehouse_credentials.assert_called_once_with(dummy_org_warehouse)
    mock_retrieve_pgvector_credentials.assert_called_once_with(dummy_org_warehouse.org)


@patch("ddpui.core.llm_service.check_if_rag_is_trained")
@patch(
    "ddpui.utils.secretsmanager.retrieve_pgvector_credentials",
)
@patch(
    "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
)
def test_generate_sql_from_warehouse_rag_not_trained_error(
    mock_retrieve_warehouse_credentials: Mock,
    mock_retrieve_pgvector_credentials: Mock,
    mock_check_if_rag_is_trained: Mock,
    dummy_org_warehouse: OrgWarehouse,
):
    mock_retrieve_pgvector_credentials.return_value = {
        "username": "something-suer",
        "host": "localhost",
        "port": 5432,
        "database": "some-db",
        "password": "random-pass",
    }
    mock_check_if_rag_is_trained.return_value = False

    dummy_org_warehouse.org.pgvector_creds = "pgvectorCreds-1234"

    with pytest.raises(
        Exception,
        match=f"Looks like the vector db for the org {re.escape(str(dummy_org_warehouse.org))} has no embeddings generated. Please perform the training",
    ):
        generate_sql_from_warehouse_rag(
            warehouse=dummy_org_warehouse, user_prompt="How is the weather today ?"
        )

    mock_retrieve_warehouse_credentials.assert_called_once_with(dummy_org_warehouse)
    mock_retrieve_pgvector_credentials.assert_called_once_with(dummy_org_warehouse.org)


@patch("ddpui.core.llm_service.ask_vanna_for_sql")
@patch("ddpui.core.llm_service.check_if_rag_is_trained")
@patch(
    "ddpui.utils.secretsmanager.retrieve_pgvector_credentials",
)
@patch(
    "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
)
def test_generate_sql_from_warehouse_rag_success(
    mock_retrieve_warehouse_credentials: Mock,
    mock_retrieve_pgvector_credentials: Mock,
    mock_check_if_rag_is_trained: Mock,
    mock_ask_vanna_for_sql: Mock,
    dummy_org_warehouse: OrgWarehouse,
):
    mock_retrieve_pgvector_credentials.return_value = {
        "username": "something-suer",
        "host": "localhost",
        "port": 5432,
        "database": "some-db",
        "password": "random-pass",
    }
    mock_check_if_rag_is_trained.return_value = True
    mock_retrieve_warehouse_credentials.return_value = "creds"
    mock_ask_vanna_for_sql.return_value = "sql-query"

    dummy_org_warehouse.org.pgvector_creds = "pgvectorCreds-1234"

    result = generate_sql_from_warehouse_rag(
        warehouse=dummy_org_warehouse, user_prompt="How is the weather today ?"
    )

    assert result == "sql-query"

    mock_retrieve_warehouse_credentials.assert_called_once_with(dummy_org_warehouse)
    mock_retrieve_pgvector_credentials.assert_called_once_with(dummy_org_warehouse.org)
    mock_ask_vanna_for_sql.assert_called_once_with(
        user_prompt="How is the weather today ?",
        pg_vector_creds=mock_retrieve_pgvector_credentials.return_value,
        warehouse_creds=mock_retrieve_warehouse_credentials.return_value,
        warehouse_type=dummy_org_warehouse.wtype,
    )
