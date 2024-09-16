import pytest
from unittest.mock import Mock, patch
from ninja.errors import HttpError
import sqlalchemy
from unittest.mock import _Call

from ddpui.models.org import OrgWarehouse, Org
from ddpui.models.role_based_access import Role, RolePermission, Permission
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.tests.api_tests.test_user_org_api import (
    seed_db,
    orguser,
    mock_request,
    authuser,
    org_without_workspace,
)
from ddpui.api.warehouse_api import (
    get_schema,
    get_table,
    get_table_columns,
    get_table_data,
    post_data_insights,
    get_download_warehouse_data,
    get_warehouse_table_columns_spec,
    post_warehouse_prompt,
    post_save_warehouse_prompt_session,
)
from ddpui.schemas.warehouse_api_schemas import (
    RequestorColumnSchema,
    AskWarehouseRequest,
    SaveLlmSessionRequest,
)
from ddpui.utils.constants import LIMIT_ROWS_TO_SEND_TO_LLM
from ddpui.models.llm import LlmSession, LlmSessionStatus, LlmAssistantType


pytestmark = pytest.mark.django_db


@pytest.fixture
def data_insights_payload():
    return RequestorColumnSchema(
        db_schema="test_schema",
        db_table="test_table",
        column_name="test_column",
        filter={},
        refresh=True,
    )


def test_seed_data(seed_db):
    """a test to seed the database"""
    assert Role.objects.count() == 5
    assert RolePermission.objects.count() > 5
    assert Permission.objects.count() > 5


@patch.multiple(
    "ddpui.api.warehouse_api",
    get_warehouse_data=Mock(return_value=["table1", "table2"]),
)
def test_get_table_success(orguser):
    request = mock_request(orguser)
    schema_name = "test_schema"
    response = get_table(request, schema_name)

    assert response is not None
    assert response == ["table1", "table2"]


@patch.multiple(
    "ddpui.api.warehouse_api",
    get_warehouse_data=Mock(return_value=["schema1", "schema2"]),
)
def test_get_schema_success(orguser):
    request = mock_request(orguser)
    response = get_schema(request)

    assert response is not None
    assert response == ["schema1", "schema2"]


@patch.multiple(
    "ddpui.api.warehouse_api",
    get_warehouse_data=Mock(return_value=["column1", "column2"]),
)
def test_get_table_columns_success(orguser):
    request = mock_request(orguser)
    schema_name = "test_schema"
    table_name = "test_table"
    response = get_table_columns(request, schema_name, table_name)

    assert response is not None
    assert response == ["column1", "column2"]


@patch.multiple(
    "ddpui.api.warehouse_api",
    get_warehouse_data=Mock(
        return_value=[{"column_1": "value_1"}, {"column2": "value2}"}]
    ),
)
def test_get_table_data_success(orguser):
    request = mock_request(orguser)
    schema_name = "test_schema"
    table_name = "test_table"
    response = get_table_data(request, schema_name, table_name)

    assert response is not None
    assert response == [{"column_1": "value_1"}, {"column2": "value2}"}]


def test_data_insights_without_warehouse(orguser, data_insights_payload):
    """Failure case for data insights without warehouse"""

    with pytest.raises(HttpError):
        request = mock_request(orguser)
        post_data_insights(request, data_insights_payload)


def test_data_insights_taskprogress_creation_failed(orguser, data_insights_payload):
    """Test case to handle failure of taskprogress creation; could be due to redis being offline"""

    OrgWarehouse.objects.create(org=orguser.org, name="fake-warehouse-name")

    with patch("ddpui.api.warehouse_api.TaskProgress.__init__") as MockTaskProgress:
        MockTaskProgress.side_effect = HttpError(500, "Redis is offline")
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            post_data_insights(request, data_insights_payload)
            MockTaskProgress.objects.create.assert_called_once()
        assert exc.value.status_code == 500
        assert str(exc.value) == "Redis is offline"


def test_data_insights_success_submitting_to_celery(orguser, data_insights_payload):
    """Test case to handle success in submitting the task to celery"""

    OrgWarehouse.objects.create(org=orguser.org, name="fake-warehouse-name")

    with patch(
        "ddpui.api.warehouse_api.TaskProgress.__init__", return_value=None
    ) as MockTaskProgress, patch(
        "ddpui.api.warehouse_api.TaskProgress.add", return_value=None
    ) as MockTaskProgressAdd:
        request = mock_request(orguser)
        response = post_data_insights(request, data_insights_payload)
        MockTaskProgress.assert_called_once()
        MockTaskProgressAdd.assert_called_once()
        assert "task_id" in response


def test_download_warehouse_data_without_warehouse(orguser):
    """Failure case for download warehouse data without warehouse"""

    with pytest.raises(HttpError) as exc:
        request = mock_request(orguser)
        get_download_warehouse_data(request, "test_schema", "test_table")
    assert exc.value.status_code == 404
    assert str(exc.value) == "Please set up your warehouse first"


def test_download_warehouse_data_success(orguser):
    """Success case for download warehouse data"""

    OrgWarehouse.objects.create(org=orguser.org, name="fake-warehouse-name")

    # mock generator function
    mock_page1 = [
        {"col1": "value1", "col2": "value2"},
        {"col1": "value3", "col2": "value4"},
    ]
    mock_page2 = [{"col1": "value5", "col2": "value6"}]
    mock_page3 = []
    mock_db_pagination = [mock_page1, mock_page2, mock_page3]

    with patch(
        "ddpui.api.warehouse_api.get_warehouse_data", side_effect=mock_db_pagination
    ):
        request = mock_request(orguser)
        response = get_download_warehouse_data(request, "test_schema", "test_table")

        # check response
        content = b"".join(response.streaming_content).decode("utf-8")

        # Assertions to verify the streaming response content
        assert "col1,col2\n" in content  # Check header
        assert "value1,value2\n" in content  # Check first row
        assert "value3,value4\n" in content  # Check second row
        assert "value5,value6\n" in content  # Check second row
        assert content.count("\n") == 4


def test_get_warehouse_table_columns_spec_without_warehouse(orguser):
    """Failure case for get warehouse table columns spec without warehouse"""

    with pytest.raises(HttpError) as exc:
        request = mock_request(orguser)
        get_warehouse_table_columns_spec(request, "test_schema", "test_table")
    assert exc.value.status_code == 404
    assert str(exc.value) == "Please set up your warehouse first"


def test_get_warehouse_table_columns_spec_table_not_found_in_schema(orguser):
    """Failure case for get warehouse table columns spec when table not found in schema"""

    OrgWarehouse.objects.create(org=orguser.org, name="fake-warehouse-name")

    with patch(
        "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
        return_value={"some-creds": "some-value"},
    ) as mock_retrieve_warehouse_credentials, patch(
        "ddpui.datainsights.warehouse.warehouse_factory.WarehouseFactory.connect"
    ) as mock_wclient:
        mock_wclient.return_value.get_table_columns.side_effect = (
            sqlalchemy.exc.NoSuchTableError()
        )
        with pytest.raises(Exception) as exc:
            request = mock_request(orguser)
            get_warehouse_table_columns_spec(request, "test_schema", "test_table")
        assert exc.value.status_code == 404
        assert str(exc.value) == "Table not found"


def test_get_warehouse_table_columns_spec_table_failed_to_connect_to_warehouse(orguser):
    """Failure case for get warehouse table columns spec when connection to warehouse failed"""

    OrgWarehouse.objects.create(org=orguser.org, name="fake-warehouse-name")

    with patch(
        "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
        return_value={"some-creds": "some-value"},
    ) as mock_retrieve_warehouse_credentials, patch(
        "ddpui.datainsights.warehouse.warehouse_factory.WarehouseFactory.connect",
        side_effect=Exception("Warehouse connection failed"),
    ) as mock_wclient:
        with pytest.raises(Exception) as exc:
            request = mock_request(orguser)
            get_warehouse_table_columns_spec(request, "test_schema", "test_table")
        assert exc.value.status_code == 500
        assert str(exc.value) == "Warehouse connection failed"


def test_get_warehouse_table_columns_spec_table_success(orguser):
    """Failure case for get warehouse table columns spec when connection to warehouse failed"""

    OrgWarehouse.objects.create(org=orguser.org, name="fake-warehouse-name")

    with patch(
        "ddpui.utils.secretsmanager.retrieve_warehouse_credentials",
        return_value={"some-creds": "some-value"},
    ) as mock_retrieve_warehouse_credentials, patch(
        "ddpui.datainsights.warehouse.warehouse_factory.WarehouseFactory.connect",
    ) as mock_wclient:
        mock_wclient.return_value.get_table_columns.side_effect = [
            [
                {"name": "col1", "data_type": "int"},
                {"name": "col2", "data_type": "varchar"},
            ]
        ]
        request = mock_request(orguser)
        response = get_warehouse_table_columns_spec(
            request, "test_schema", "test_table"
        )
        assert response == [
            {"name": "col1", "data_type": "int"},
            {"name": "col2", "data_type": "varchar"},
        ]


def test_llm_data_analysis_invalid_sqls(orguser):
    """
    Test cases for llm data analysis with invalid sql
    """
    # only select queries allowed
    payload = AskWarehouseRequest(
        sql="update some_table set col1 = null where 1 = 1",
        user_prompt="Summarize the output",
    )
    with pytest.raises(HttpError) as exc:
        request = mock_request(orguser)
        post_warehouse_prompt(request, payload)
    assert exc.value.status_code == 400
    assert str(exc.value) == "Only SELECT queries are allowed"

    # only 1 sql
    payload = AskWarehouseRequest(
        sql="select * from some_table; select * from some_other_table",
        user_prompt="Summarize the output",
    )
    with pytest.raises(HttpError) as exc:
        request = mock_request(orguser)
        post_warehouse_prompt(request, payload)
    assert exc.value.status_code == 400
    assert str(exc.value) == "Only one query is allowed"

    # no sql
    payload = AskWarehouseRequest(
        sql="",
        user_prompt="Summarize the output",
    )
    with pytest.raises(HttpError) as exc:
        request = mock_request(orguser)
        post_warehouse_prompt(request, payload)
    assert exc.value.status_code == 400
    assert str(exc.value) == "No query provided"


def test_llm_data_analysis_limit_records_sent_to_llm(orguser):
    """
    Make sure the defined limit for no of records is going to the llms for analysis
    """
    OrgWarehouse.objects.create(org=orguser.org, name="fake-warehouse-name")

    payload = AskWarehouseRequest(
        sql=f"select * from some_table limit {LIMIT_ROWS_TO_SEND_TO_LLM + 2000}",
        user_prompt="Summarize the output",
    )

    with pytest.raises(HttpError) as exc:
        request = mock_request(orguser)
        post_warehouse_prompt(request, payload)

    assert exc.value.status_code == 400
    assert str(exc.value) == f"Please make sure the limit in query is less than {LIMIT_ROWS_TO_SEND_TO_LLM}"

    # if the limit is not set default limit will be used
    payload = AskWarehouseRequest(
        sql="select * from some_table",
        user_prompt="Summarize the output",
    )

    sql = "select * from some_table"
    payload = AskWarehouseRequest(
        sql=sql,
        user_prompt="Summarize the output",
    )
    with patch(
        "ddpui.celeryworkers.tasks.summarize_warehouse_results.apply_async",
        return_value=Mock(id="task-id"),
    ) as mock_apply_async:
        request = mock_request(orguser)
        post_warehouse_prompt(request, payload)

        call: _Call = mock_apply_async.call_args
        _, call_kwargs = list(call)

        assert (
            call_kwargs.get("kwargs", {}).get("sql", None)
            == f"{sql} LIMIT {LIMIT_ROWS_TO_SEND_TO_LLM}"
        )


def test_llm_data_analysis_save_new_session(orguser):
    """
    Test the creation of new session for llm data analysis
    """
    request = mock_request(orguser)
    # session pushed by llm service and the long running generation process
    session_id = "some-random-uuid"
    session = LlmSession.objects.create(
        session_id=session_id,
        org=orguser.org,
        orguser=orguser,
        response=[{"prompt": "some prompt", "response": "some response"}],
        session_status=LlmSessionStatus.COMPLETED,
        session_type=LlmAssistantType.LONG_TEXT_SUMMARIZATION,
    )

    # session not found
    payload = SaveLlmSessionRequest(
        session_name="save session with this name", overwrite=False, old_session_id=None
    )
    with pytest.raises(HttpError) as exc:
        post_save_warehouse_prompt_session(request, "some-fake-id", payload)

    assert exc.value.status_code == 404
    assert str(exc.value) == "Session not found"

    # save the session
    post_save_warehouse_prompt_session(request, session_id, payload)
    assert (
        LlmSession.objects.filter(session_name="save session with this name").count()
        == 1
    )


def test_llm_data_analysis_save_and_overwrite_session(orguser):
    """
    Test the save & overwrite session for llm data analysis
    """
    request = mock_request(orguser)
    old_session_id = "some-random-uuid"
    old_session = LlmSession.objects.create(
        session_id=old_session_id,
        org=orguser.org,
        orguser=orguser,
        response=[{"prompt": "some prompt", "response": "some response"}],
        session_status=LlmSessionStatus.COMPLETED,
        session_type=LlmAssistantType.LONG_TEXT_SUMMARIZATION,
        session_name="old session name; to be overwritten",
    )

    # new ghost session pushed by llm service and celery
    new_session_id = "new-session-id"
    new_session = LlmSession.objects.create(
        session_id=new_session_id,
        org=orguser.org,
        orguser=orguser,
        response=[{"prompt": "some prompt", "response": "some response"}],
        session_status=LlmSessionStatus.COMPLETED,
        session_type=LlmAssistantType.LONG_TEXT_SUMMARIZATION,
    )
    payload = SaveLlmSessionRequest(
        session_name="new session name", overwrite=True, old_session_id=old_session_id
    )

    post_save_warehouse_prompt_session(request, new_session.session_id, payload)

    assert LlmSession.objects.filter(session_id=old_session_id).count() == 0
    assert (
        LlmSession.objects.filter(
            session_id=new_session_id, session_name=payload.session_name
        ).count()
        == 1
    )
