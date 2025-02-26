import os
import re
import pytest
from unittest.mock import Mock, patch, _CallList, _Call

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


from ninja.errors import HttpError
from ddpui.models.org import Org, OrgWarehouse, OrgWarehouseRagTraining
from ddpui.core.warehousefunctions import (
    parse_sql_query_with_limit_offset,
    generate_sql_from_warehouse_rag,
    scaffold_rag_training,
    generate_training_sql,
    train_rag_on_warehouse,
    save_pgvector_creds,
    fetch_warehouse_tables,
    get_warehouse_data,
)
from ddpui.schemas.warehouse_api_schemas import WarehouseRagTrainConfig
from ddpui.tests.api_tests.test_user_org_api import mock_request

pytestmark = pytest.mark.django_db


@pytest.fixture(scope="session", autouse=True)
def set_env():
    os.environ["PGVECTOR_PORT"] = "5432"
    os.environ["PGVECTOR_HOST"] = "some-host"


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


@patch(
    "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
)
def test_train_rag_on_warehouse_pgvector_creds_not_found(
    mock_retrieve_warehouse_credentials: Mock,
    dummy_org_warehouse: OrgWarehouse,
):
    """Failure because pgvector creds not found"""

    with pytest.raises(Exception, match="Couldn't find the pgvector creds for the org"):
        train_rag_on_warehouse(warehouse=dummy_org_warehouse)

    mock_retrieve_warehouse_credentials.assert_called_once_with(dummy_org_warehouse)


@patch(
    "ddpui.utils.secretsmanager.retrieve_pgvector_credentials",
)
@patch(
    "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
)
def test_train_rag_on_warehouse_pgvector_pgvector_creds_not_saved_in_secrets_manager(
    mock_retrieve_warehouse_credentials: Mock,
    mock_retrieve_pgvector_credentials: Mock,
    dummy_org_warehouse: OrgWarehouse,
):
    """Failure because pgvector creds not saved to secrets manager"""
    mock_retrieve_pgvector_credentials.return_value = None

    dummy_org_warehouse.org.pgvector_creds = "pgvectorCreds-1234"

    with pytest.raises(Exception, match="Pg vector creds for the org not created/generated"):
        train_rag_on_warehouse(
            warehouse=dummy_org_warehouse,
        )

    mock_retrieve_warehouse_credentials.assert_called_once_with(dummy_org_warehouse)
    mock_retrieve_pgvector_credentials.assert_called_once_with(dummy_org_warehouse.org)


@patch(
    "ddpui.utils.secretsmanager.retrieve_pgvector_credentials",
)
@patch(
    "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
)
def test_train_rag_on_warehouse_pgvector_creds_validation_error(
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
        train_rag_on_warehouse(
            warehouse=dummy_org_warehouse,
        )

    mock_retrieve_warehouse_credentials.assert_called_once_with(dummy_org_warehouse)
    mock_retrieve_pgvector_credentials.assert_called_once_with(dummy_org_warehouse.org)


@patch(
    "ddpui.utils.secretsmanager.retrieve_pgvector_credentials",
)
@patch(
    "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
)
def test_train_rag_on_warehouse_no_training_sql_found(
    mock_retrieve_warehouse_credentials: Mock,
    mock_retrieve_pgvector_credentials: Mock,
    dummy_org_warehouse: OrgWarehouse,
):
    mock_retrieve_pgvector_credentials.return_value = {
        "username": "something-suer",
        "host": "localhost",
        "port": 5432,
        "database": "some-db",
        "password": "random-pass",
    }

    dummy_org_warehouse.org.pgvector_creds = "pgvectorCreds-1234"

    with pytest.raises(
        Exception, match="Rag training sql not found. Please generate to train your warehouse"
    ):
        train_rag_on_warehouse(
            warehouse=dummy_org_warehouse,
        )

    mock_retrieve_warehouse_credentials.assert_called_once_with(dummy_org_warehouse)
    mock_retrieve_pgvector_credentials.assert_called_once_with(dummy_org_warehouse.org)

    OrgWarehouseRagTraining.objects.create(warehouse=dummy_org_warehouse, training_sql=None)

    with pytest.raises(
        Exception, match="Rag training sql not found. Please generate to train your warehouse"
    ):
        train_rag_on_warehouse(
            warehouse=dummy_org_warehouse,
        )

    mock_retrieve_warehouse_credentials.assert_called_with(dummy_org_warehouse)
    mock_retrieve_pgvector_credentials.assert_called_with(dummy_org_warehouse.org)


@patch("ddpui.core.llm_service.train_vanna_on_warehouse")
@patch(
    "ddpui.utils.secretsmanager.retrieve_pgvector_credentials",
)
@patch(
    "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
)
def test_train_rag_on_warehouse_success(
    mock_retrieve_warehouse_credentials: Mock,
    mock_retrieve_pgvector_credentials: Mock,
    mock_train_vanna_on_warehouse: Mock,
    dummy_org_warehouse: OrgWarehouse,
):
    mock_retrieve_pgvector_credentials.return_value = {
        "username": "something-suer",
        "host": "localhost",
        "port": 5432,
        "database": "some-db",
        "password": "random-pass",
    }
    mock_retrieve_warehouse_credentials.return_value = "warehouse-creds"
    mock_train_vanna_on_warehouse.return_value = "result"

    OrgWarehouseRagTraining.objects.create(warehouse=dummy_org_warehouse, training_sql="train-sql")

    dummy_org_warehouse.org.pgvector_creds = "pgvectorCreds-1234"

    train_rag_on_warehouse(
        warehouse=dummy_org_warehouse,
    )

    mock_train_vanna_on_warehouse.assert_called_once_with(
        training_sql="train-sql",
        warehouse_creds=mock_retrieve_warehouse_credentials.return_value,
        pg_vector_creds=mock_retrieve_pgvector_credentials.return_value,
        warehouse_type=dummy_org_warehouse.wtype,
    )

    assert OrgWarehouseRagTraining.objects.filter(warehouse=dummy_org_warehouse).count() == 1

    rag_training = OrgWarehouseRagTraining.objects.filter(warehouse=dummy_org_warehouse).first()
    assert rag_training.last_log == "result"
    assert rag_training.last_trained_at is not None


@patch("ddpui.core.warehousefunctions.generate_hash_id")
@patch("ddpui.utils.secretsmanager.save_pgvector_credentials")
def test_save_pgvector_creds_creation(
    mock_save_pgvector_creds: Mock,
    mock_generate_hash_id: Mock,
    dummy_org_warehouse: OrgWarehouse,
):
    """
    Check if the creds are being created/generated with right username & database names
    """

    mock_save_pgvector_creds.return_value = "saved-to-secret-name"
    mock_generate_hash_id.return_value = "password"

    assert dummy_org_warehouse.org.pgvector_creds is None

    save_pgvector_creds(dummy_org_warehouse.org)

    assert dummy_org_warehouse.org.pgvector_creds == "saved-to-secret-name"
    args_list: _CallList = mock_save_pgvector_creds.call_args_list
    assert "credentials" in args_list[0].kwargs
    assert (
        args_list[0].kwargs["credentials"]["username"] == dummy_org_warehouse.org.slug + "_vector"
    )
    assert (
        args_list[0].kwargs["credentials"]["database"] == dummy_org_warehouse.org.slug + "_vector"
    )
    assert args_list[0].kwargs["credentials"]["password"] == "password"


@patch("ddpui.core.warehousefunctions.generate_hash_id")
@patch("ddpui.utils.secretsmanager.update_pgvector_credentials")
def test_save_pgvector_creds_updation(
    mock_update_pgvector_credentials: Mock,
    mock_generate_hash_id: Mock,
    dummy_org_warehouse: OrgWarehouse,
):
    """
    Check if we are able to reset the creds with new password
    """
    dummy_org_warehouse.org.pgvector_creds = "creds-created-already"

    mock_generate_hash_id.return_value = "new-password"

    # without overwrite
    save_pgvector_creds(dummy_org_warehouse.org)

    mock_update_pgvector_credentials.assert_not_called()

    # with overwrite
    result = save_pgvector_creds(dummy_org_warehouse.org, overwrite=True)

    mock_update_pgvector_credentials.assert_called_once()
    assert result == dummy_org_warehouse.org.pgvector_creds

    args_list: _CallList = mock_update_pgvector_credentials.call_args_list
    assert "credentials" in args_list[0].kwargs
    assert (
        args_list[0].kwargs["credentials"]["username"] == dummy_org_warehouse.org.slug + "_vector"
    )
    assert (
        args_list[0].kwargs["credentials"]["database"] == dummy_org_warehouse.org.slug + "_vector"
    )
    assert args_list[0].kwargs["credentials"]["password"] == "new-password"


@patch("ddpui.core.warehousefunctions.RedisClient")
@patch("ddpui.core.warehousefunctions.get_warehouse_data")
def test_fetch_warehouse_tables(
    mock_get_warehouse_data: Mock, mock_redis_client: Mock, dummy_org_warehouse: OrgWarehouse
):
    """
    Test the tables being fetched
    Test if the data is being stored in redis
    """
    mock_get_warehouse_data.return_value = ["table1"]
    mock_get_warehouse_data.side_effect = (item for item in [["schema1"], ["table11", "table12"]])

    result = fetch_warehouse_tables({}, dummy_org_warehouse, cache_key=None)

    assert mock_get_warehouse_data.call_count == 2
    assert len(result) == 2
    assert_with_result = []
    for schema in ["schema1"]:
        for table in ["table11", "table12"]:
            assert_with_result.append(
                {
                    "schema": schema,
                    "input_name": table,
                    "type": "src_model_node",
                    "id": schema + "-" + table,
                }
            )
    assert result == assert_with_result

    mock_get_warehouse_data.side_effect = (item for item in [["schema1"], ["table11", "table12"]])
    mock_redis_client.return_value = Mock(get_instance=Mock(set=True))

    result = fetch_warehouse_tables({}, dummy_org_warehouse, cache_key="some-cache-key")

    assert result == assert_with_result


@patch("ddpui.core.dbtautomation_service._get_wclient")
def test_get_warehouse_data(mock_get_wclient: Mock, dummy_org_warehouse: OrgWarehouse):
    """
    Test the function against various kwargs
    """
    mock_get_tables = Mock()
    mock_get_wclient.return_value = Mock(get_tables=mock_get_tables)
    kwargs = {"org_warehouse": dummy_org_warehouse}

    # tables
    with pytest.raises(HttpError):
        get_warehouse_data(Mock(), "tables", **kwargs)

    mock_get_wclient.assert_called_once()

    kwargs["schema_name"] = "some-schema"

    get_warehouse_data(Mock(), "tables", **kwargs)

    mock_get_tables.assert_called_once_with("some-schema")

    # schemas
    mock_get_schemas = Mock()
    mock_get_wclient.return_value = Mock(get_schemas=mock_get_schemas)

    get_warehouse_data(Mock(), "schemas", **kwargs)

    mock_get_schemas.assert_called_once()

    # table_columns
    mock_get_table_columns = Mock()
    mock_get_wclient.return_value = Mock(get_table_columns=mock_get_table_columns)

    with pytest.raises(HttpError):
        get_warehouse_data(Mock(), "table_columns", **kwargs)

    kwargs["table_name"] = "some-table"

    get_warehouse_data(Mock(), "table_columns", **kwargs)

    mock_get_table_columns.assert_called_once()

    # table data
    mock_get_table_data = Mock()
    mock_get_wclient.return_value = Mock(get_table_data=mock_get_table_data)

    kwargs["limit"] = 10
    kwargs["page"] = 2
    kwargs["order_by"] = "some-col"
    kwargs["order"] = 1

    get_warehouse_data(Mock(), "table_data", **kwargs)

    mock_get_table_data.assert_called_once_with(
        schema=kwargs["schema_name"],
        table=kwargs["table_name"],
        limit=kwargs["limit"],
        page=kwargs["page"],
        order_by=kwargs["order_by"],
        order=kwargs["order"],
    )

    # some random key of data
    with pytest.raises(HttpError):
        get_warehouse_data(Mock(), "some-random-key", **kwargs)
