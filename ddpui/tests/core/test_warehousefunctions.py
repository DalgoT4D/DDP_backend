import os
import re
import pytest
from unittest.mock import Mock, patch

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


from ddpui.models.org import Org, OrgWarehouse, OrgWarehouseRagTraining
from ddpui.core.warehousefunctions import (
    parse_sql_query_with_limit_offset,
    generate_sql_from_warehouse_rag,
    scaffold_rag_training,
    generate_training_sql,
)
from ddpui.schemas.warehouse_api_schemas import WarehouseRagTrainConfig

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


@patch("ddpui.core.warehousefunctions.generate_training_sql")
def test_scaffold_rag_training_create_or_update_test(
    mock_generate_training_sql: Mock, dummy_org_warehouse: OrgWarehouse
):
    """
    Test whether create or update on OrgWarehouseRagTraining works as expected
    """
    mock_generate_training_sql.return_value = "SELECT * FROM INFORMATION_SCHEMA.columns"
    # create
    assert OrgWarehouseRagTraining.objects.filter(warehouse=dummy_org_warehouse).count() == 0

    config = WarehouseRagTrainConfig(exclude_schemas=[], exclude_tables=[], exclude_columns=[])
    scaffold_rag_training(dummy_org_warehouse, config)

    assert OrgWarehouseRagTraining.objects.filter(warehouse=dummy_org_warehouse).count() == 1

    # update
    config = WarehouseRagTrainConfig(
        exclude_schemas=["schema1"], exclude_tables=["table1"], exclude_columns=[]
    )
    scaffold_rag_training(dummy_org_warehouse, config)

    assert OrgWarehouseRagTraining.objects.filter(warehouse=dummy_org_warehouse).count() == 1

    rag_training = OrgWarehouseRagTraining.objects.filter(warehouse=dummy_org_warehouse).first()
    assert rag_training.training_sql == mock_generate_training_sql.return_value
    assert rag_training.exclude.get("schemas") == ["schema1"]
    assert rag_training.exclude.get("tables") == ["table1"]
    assert rag_training.exclude.get("columns") == None


@patch("ddpui.datainsights.warehouse.warehouse_factory.WarehouseFactory.connect")
@patch(
    "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
)
def test_generate_training_sql_success(
    mock_retrieve_warehouse_credentials: Mock,
    mock_warehouse_factory_connect: Mock,
    dummy_org_warehouse: OrgWarehouse,
):
    """
    Test the generate training sql function
    """
    mock_retrieve_warehouse_credentials.return_value = "creds"
    mock_warehouse_factory_connect.return_value = Mock(
        build_rag_training_sql=Mock(return_value="sql-query")
    )

    config = WarehouseRagTrainConfig(exclude_schemas=[], exclude_tables=[], exclude_columns=[])

    sql = generate_training_sql(dummy_org_warehouse, config)

    assert sql == "sql-query"
