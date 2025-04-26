import os
import django

from unittest.mock import Mock, patch
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser, OrgUserRole
from ddpui.api.airbyte_api import (
    get_airbyte_source_definitions,
    get_airbyte_source_definition_specifications,
    post_airbyte_source,
    put_airbyte_source,
    get_airbyte_source,
    get_airbyte_sources_v1,
    post_airbyte_check_source,
    post_airbyte_check_source_for_update,
    get_airbyte_sources,
    get_airbyte_source_schema_catalog,
    get_airbyte_destination_definitions,
    get_airbyte_destination_definition_specifications,
    post_airbyte_destination,
    post_airbyte_check_destination,
    post_airbyte_check_destination_for_update,
    get_airbyte_destinations,
    get_airbyte_destination,
    get_job_status,
)

from ddpui.models.role_based_access import Role, RolePermission, Permission
from ddpui.ddpairbyte.schema import (
    AirbyteSourceCreate,
    AirbyteSourceUpdate,
    AirbyteSourceUpdateCheckConnection,
    AirbyteDestinationCreate,
    AirbyteDestinationUpdateCheckConnection,
)
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui import ddpprefect
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request
from ddpui.utils.custom_logger import CustomLogger


logger = CustomLogger("ddpui-pytest")

pytestmark = pytest.mark.django_db


# ================================================================================
@pytest.fixture
def authuser():
    """a django User object"""
    user = User.objects.create(
        username="tempusername", email="tempuseremail", password="tempuserpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org_without_workspace():
    """a pytest fixture which creates an Org without an airbyte workspace"""
    org = Org.objects.create(airbyte_workspace_id=None, slug="test-org-slug")
    yield org
    org.delete()


@pytest.fixture
def org_with_workspace():
    """a pytest fixture which creates an Org having an airbyte workspace"""
    org = Org.objects.create(airbyte_workspace_id="FAKE-WORKSPACE-ID", slug="test-org-slug")
    yield org
    org.delete()


@pytest.fixture
def orguser(authuser, org_without_workspace):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org_without_workspace,
        role=OrgUserRole.ACCOUNT_MANAGER,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def orguser_workspace(authuser, org_with_workspace):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org_with_workspace,
        role=OrgUserRole.ACCOUNT_MANAGER,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


# ================================================================================


def test_seed_data(seed_db):
    """a test to seed the database"""
    assert Role.objects.count() == 5
    assert RolePermission.objects.count() > 5
    assert Permission.objects.count() > 5


# ================================================================================
def test_get_airbyte_source_definitions_without_airbyte_workspace(
    orguser,
):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_source_definitions(request)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_source_definitions=Mock(
        return_value={
            "sourceDefinitions": [
                {"name": "name1"},
                {"name": "name2"},
                {"name": "name3"},
            ]
        }
    ),
)
def test_get_airbyte_source_definitions_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    result = get_airbyte_source_definitions(request)

    assert len(result) == 3


# ================================================================================
def test_get_airbyte_source_definition_specifications_without_workspace(
    orguser,
):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_source_definition_specifications(request, "fake-sourcedef-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_source_definition_specification=Mock(
        return_value={"connectionSpecification": "srcdefspeec_val"}
    ),
)
def test_get_airbyte_source_definition_specifications_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    result = get_airbyte_source_definition_specifications(request, "fake-sourcedef-id")

    assert result == "srcdefspeec_val"


# ================================================================================
def test_post_airbyte_source_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    fake_payload = AirbyteSourceCreate(name="temp-name", sourceDefId="fake-id", config={})
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_source(request, fake_payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    create_source=Mock(return_value={"sourceId": "fake-source-id"}),
)
def test_post_airbyte_source_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    fake_payload = AirbyteSourceCreate(name="temp-name", sourceDefId="fake-id", config={})
    source = post_airbyte_source(request, fake_payload)

    assert source["sourceId"] == "fake-source-id"


# ================================================================================
def test_put_airbyte_source_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    fake_payload = AirbyteSourceUpdate(name="temp-name", sourceDefId="fake-id", config={})
    with pytest.raises(HttpError) as excinfo:
        put_airbyte_source(request, "fake-source-id", fake_payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    update_source=Mock(return_value={"sourceId": "fake-source-id"}),
)
def test_put_airbyte_source_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    fake_payload = AirbyteSourceUpdate(name="temp-name", sourceDefId="fake-id", config={})
    source = put_airbyte_source(request, "fake-source-id", fake_payload)

    assert source["sourceId"] == "fake-source-id"


# ================================================================================
def test_post_airbyte_check_source_with_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    fake_payload = AirbyteSourceUpdate(name="temp-name", sourceDefId="fake-id", config={})
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_check_source(request, fake_payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    check_source_connection=Mock(
        return_value={"jobInfo": {"succeeded": False, "logs": {"logLines": [1]}}}
    ),
)
def test_post_airbyte_check_source_failure(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    fake_payload = AirbyteSourceUpdate(name="temp-name", sourceDefId="fake-id", config={})
    result = post_airbyte_check_source(request, fake_payload)

    assert result["status"] == "failed"
    assert len(result["logs"]) == 1


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    check_source_connection=Mock(
        return_value={"jobInfo": {"succeeded": True, "logs": {"logLines": [1, 2]}}}
    ),
)
def test_post_airbyte_check_source_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    fake_payload = AirbyteSourceUpdate(name="temp-name", sourceDefId="fake-id", config={})
    result = post_airbyte_check_source(request, fake_payload)

    assert result["status"] == "succeeded"
    assert len(result["logs"]) == 2


# ================================================================================
def test_post_airbyte_check_source_for_update_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    fake_payload = AirbyteSourceUpdateCheckConnection(
        name="temp-name", sourceDefId="fake-id", config={}
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_check_source_for_update(request, "fake-source-id", fake_payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    check_source_connection_for_update=Mock(
        return_value={"jobInfo": {"succeeded": False, "logs": {"logLines": [1]}}}
    ),
)
def test_post_airbyte_check_source_for_update_failure(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    fake_payload = AirbyteSourceUpdateCheckConnection(
        name="temp-name", sourceDefId="fake-id", config={}
    )
    result = post_airbyte_check_source_for_update(request, "fake-source-id", fake_payload)

    assert result["status"] == "failed"
    assert len(result["logs"]) == 1


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    check_source_connection_for_update=Mock(
        return_value={"jobInfo": {"succeeded": True, "logs": {"logLines": [1, 2]}}}
    ),
)
def test_post_airbyte_check_source_for_update_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    fake_payload = AirbyteSourceUpdateCheckConnection(
        name="temp-name", sourceDefId="fake-id", config={}
    )
    result = post_airbyte_check_source_for_update(request, "fake-source-id", fake_payload)

    assert result["status"] == "succeeded"
    assert len(result["logs"]) == 2


# ================================================================================
def test_get_airbyte_sources_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_sources(
            request,
        )

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_sources=Mock(return_value={"sources": [1, 2, 3]}),
)
def test_get_airbyte_sources_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    result = get_airbyte_sources(
        request,
    )

    assert len(result) == 3


# ================================================================================


def test_get_airbyte_sources_v1_without_workspace(orguser):
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_sources_v1(request)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == "Create an Airbyte workspace first"


@patch(
    "ddpui.api.airbyte_api.airbyte_service.get_sources_v1",
    return_value={"sources": [{"name": "source1"}], "total": 1},
)
def test_get_airbyte_sources_v1_success(mock_get_sources_v1, orguser_workspace):
    request = mock_request(orguser_workspace)

    response = get_airbyte_sources_v1(request, limit=10, offset=0, search="source")

    assert response["total"] == 1
    assert len(response["sources"]) == 1
    assert response["sources"][0]["name"] == "source1"


# ================================================================================
def test_get_airbyte_source_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_source(request, "fake-source-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_source=Mock(return_value={"fake-key": "fake-val"}),
)
def test_get_airbyte_source_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    result = get_airbyte_source(request, "fake-source-id")

    assert result["fake-key"] == "fake-val"


# ================================================================================
def test_get_airbyte_source_schema_catalog_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_source_schema_catalog(request, "fake-source-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_source_schema_catalog=Mock(return_value={"fake-key": "fake-val"}),
)
def test_get_airbyte_source_schema_catalog_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    result = get_airbyte_source_schema_catalog(request, "fake-source-id")

    assert result["fake-key"] == "fake-val"


# ================================================================================
def test_get_airbyte_destination_definitions_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_destination_definitions(request)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_destination_definitions=Mock(
        return_value={"destinationDefinitions": [{"name": "dest1"}, {"name": "dest3"}]}
    ),
)
def test_get_airbyte_destination_definitions_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    os.environ["AIRBYTE_DESTINATION_TYPES"] = "dest1,dest2"
    result = get_airbyte_destination_definitions(request)

    assert len(result) == 1
    assert result[0]["name"] == "dest1"


# ================================================================================
def test_get_airbyte_destination_definition_specifications_without_workspace(
    orguser,
):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_destination_definition_specifications(request, "fake-dest-def-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_destination_definition_specification=Mock(
        return_value={"connectionSpecification": {"fake-key": "fake-val"}}
    ),
)
def test_get_airbyte_destination_definition_specifications_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    os.environ["AIRBYTE_DESTINATION_TYPES"] = "dest1,dest2"
    result = get_airbyte_destination_definition_specifications(request, "fake-dest-def-id")

    assert result["fake-key"] == "fake-val"


# ================================================================================
def test_post_airbyte_destination_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    payload = AirbyteDestinationCreate(
        name="fake-dest-name",
        destinationDefId="fake-dest-def-id",
        config={},
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_destination(request, payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    create_destination=Mock(return_value={"destinationId": "fake-dest-id"}),
)
def test_post_airbyte_destination_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    payload = AirbyteDestinationCreate(
        name="fake-dest-name",
        destinationDefId="fake-dest-def-id",
        config={},
    )
    result = post_airbyte_destination(request, payload)

    assert result["destinationId"] == "fake-dest-id"


# ================================================================================
def test_post_airbyte_check_destination_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    payload = AirbyteDestinationCreate(
        name="fake-dest-name",
        destinationDefId="fake-dest-def-id",
        config={},
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_check_destination(request, payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    check_destination_connection=Mock(
        return_value={"jobInfo": {"succeeded": True, "logs": {"logLines": [1, 2, 3]}}}
    ),
)
def test_post_airbyte_check_destination_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    payload = AirbyteDestinationCreate(
        name="fake-dest-name",
        destinationDefId="fake-dest-def-id",
        config={},
    )
    result = post_airbyte_check_destination(request, payload)

    assert result["status"] == "succeeded"
    assert len(result["logs"]) == 3


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    check_destination_connection=Mock(
        return_value={"jobInfo": {"succeeded": False, "logs": {"logLines": [1, 2, 3, 4]}}}
    ),
)
def test_post_airbyte_check_destination_failure(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    payload = AirbyteDestinationCreate(
        name="fake-dest-name",
        destinationDefId="fake-dest-def-id",
        config={},
    )
    result = post_airbyte_check_destination(request, payload)

    assert result["status"] == "failed"
    assert len(result["logs"]) == 4


# ================================================================================
def test_post_airbyte_check_destination_for_update_without_workspace(
    orguser,
):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    payload = AirbyteDestinationUpdateCheckConnection(
        name="fake-dest-name",
        config={},
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_check_destination_for_update(request, "fake-dest-id", payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    check_destination_connection_for_update=Mock(
        return_value={"jobInfo": {"succeeded": True, "logs": {"logLines": [1, 2, 3]}}}
    ),
)
def test_post_airbyte_check_destination_for_update_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    payload = AirbyteDestinationUpdateCheckConnection(
        name="fake-dest-name",
        config={},
    )
    result = post_airbyte_check_destination_for_update(request, "fake-dest-id", payload)

    assert result["status"] == "succeeded"
    assert len(result["logs"]) == 3


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    check_destination_connection_for_update=Mock(
        return_value={"jobInfo": {"succeeded": False, "logs": {"logLines": [1, 2, 3, 4]}}}
    ),
)
def test_post_airbyte_check_destination_for_update_failure(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    payload = AirbyteDestinationUpdateCheckConnection(
        name="fake-dest-name",
        config={},
    )
    result = post_airbyte_check_destination_for_update(request, "fake-dest-id", payload)

    assert result["status"] == "failed"
    assert len(result["logs"]) == 4


# ================================================================================
def test_get_airbyte_destinations_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_destinations(request)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_destinations=Mock(return_value={"destinations": [{"fake-key": "fake-val"}]}),
)
def test_get_airbyte_destinations_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    result = get_airbyte_destinations(request)

    assert len(result) == 1
    assert result[0]["fake-key"] == "fake-val"


# ================================================================================
def test_get_airbyte_destination_without_workspace(orguser):
    """tests GET /source_definitions"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_destination(request, "fake-dest-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_destination=Mock(return_value={"fake-key": "fake-val"}),
)
def test_get_airbyte_destination_success(orguser_workspace):
    """tests GET /source_definitions"""
    request = mock_request(orguser_workspace)

    result = get_airbyte_destination(request, "fake-dest-id")

    assert result["fake-key"] == "fake-val"


# ================================================================================
@pytest.fixture
def warehouse_without_destination(org_with_workspace):
    warehouse = OrgWarehouse.objects.create(org=org_with_workspace)
    yield warehouse
    warehouse.delete()


@pytest.fixture
def warehouse_with_destination(org_with_workspace):
    warehouse = OrgWarehouse.objects.create(
        org=org_with_workspace, airbyte_destination_id="destination-id"
    )
    yield warehouse
    warehouse.delete()


# ================================================================================
@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_job_info=Mock(
        return_value={
            "attempts": [{"logs": {"logLines": [1, 2, 3]}}],
            "job": {"status": "completed"},
        }
    ),
)
def test_get_job_status(orguser):
    request = mock_request(orguser)

    result = get_job_status(request, "fake-job-id")
    assert result["status"] == "completed"
    assert len(result["logs"]) == 3
