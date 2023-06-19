import os
import django
from datetime import datetime

from unittest.mock import Mock, patch
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.api.client.user_org_api import (
    get_current_user,
    post_organization_user,
    get_organization_users,
    put_organization_user_self,
    put_organization_user,
    post_organization,
    post_organization_warehouse,
    delete_organization_warehouses,
    get_organizations_warehouses,
    post_organization_user_invite,
    get_organization_user_invite,
    post_organization_user_accept_invite,
)
from ddpui.models.org import Org, OrgSchema, OrgWarehouseSchema, OrgWarehouse
from ddpui.models.org_user import (
    OrgUser,
    OrgUserCreate,
    OrgUserRole,
    OrgUserUpdate,
    InvitationSchema,
    Invitation,
    AcceptInvitationSchema,
)
from django.contrib.auth.models import User
from ddpui.ddpairbyte.schema import AirbyteWorkspace
from ddpui.utils import timezone


# ================================================================================
@pytest.fixture
def org_without_workspace():
    """a pytest fixture which creates an Org without an airbyte workspace"""
    print("creating org_without_workspace")
    org = Org.objects.create(airbyte_workspace_id=None, slug="test-org-slug")
    yield org
    print("deleting org_without_workspace")
    org.delete()


@pytest.fixture
def org_with_workspace():
    """a pytest fixture which creates an Org having an airbyte workspace"""
    print("creating org_with_workspace")
    org = Org.objects.create(
        name="org-name", airbyte_workspace_id="FAKE-WORKSPACE-ID", slug="test-org-slug"
    )
    yield org
    print("deleting org_with_workspace")
    org.delete()


@pytest.fixture
def authuser():
    """a django User object"""
    while True:
        existing_authuser = User.objects.filter(email="tempuseremail").first()
        if existing_authuser:
            existing_authuser.delete()
        else:
            break
    user = User.objects.create(
        username="tempusername", email="tempuseremail", password="tempuserpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def orguser(authuser, org_without_workspace):
    orguser = OrgUser.objects.create(
        user=authuser, org=org_without_workspace, role=OrgUserRole.ACCOUNT_MANAGER
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def nonadminorguser(authuser, org_without_workspace):
    orguser = OrgUser.objects.create(
        user=authuser, org=org_without_workspace, role=OrgUserRole.REPORT_VIEWER
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def orguserwithoutorg(authuser):
    orguser = OrgUser.objects.create(
        user=authuser, org=None, role=OrgUserRole.REPORT_VIEWER
    )
    yield orguser
    orguser.delete()


# ================================================================================
def test_get_current_user_no_user():
    """tests /worksspace/detatch/"""

    mock_request = Mock()
    mock_request.orguser = None

    with pytest.raises(HttpError) as excinfo:
        get_current_user(mock_request)
    assert str(excinfo.value) == "requestor is not an OrgUser"


def test_get_current_user_has_user(org_with_workspace):
    """tests /worksspace/detatch/"""

    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace
    mock_orguser.user = Mock()
    mock_orguser.user.email = "useremail"
    mock_orguser.user.is_active = True
    mock_orguser.role = 3

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    response = get_current_user(mock_request)
    assert response.email == "useremail"
    assert response.org.name == "org-name"
    assert response.active is True
    assert response.role == 3


# ================================================================================
def test_post_organization_user_wrong_signupcode():
    mock_request = Mock()
    payload = OrgUserCreate(
        email="useremail", password="userpassword", signupcode="wrong-signupcode"
    )
    os.environ["SIGNUPCODE"] = "right-signupcode"
    with pytest.raises(HttpError) as excinfo:
        post_organization_user(mock_request, payload)
    assert str(excinfo.value) == "That is not the right signup code"


def test_post_organization_user_userexists_email(authuser):
    mock_request = Mock()
    payload = OrgUserCreate(
        email="tempuseremail", password="userpassword", signupcode="right-signupcode"
    )
    os.environ["SIGNUPCODE"] = "right-signupcode"
    with pytest.raises(HttpError) as excinfo:
        post_organization_user(mock_request, payload)
    assert str(excinfo.value) == f"user having email {authuser.email} exists"


def test_post_organization_user_userexists_username(authuser):
    mock_request = Mock()
    payload = OrgUserCreate(
        email="tempusername",
        password="userpassword",
        signupcode="right-signupcode",
        role=2,
    )
    os.environ["SIGNUPCODE"] = "right-signupcode"
    with pytest.raises(HttpError) as excinfo:
        post_organization_user(mock_request, payload)
    assert str(excinfo.value) == f"user having email {authuser.username} exists"


def test_post_organization_user_success():
    mock_request = Mock()
    payload = OrgUserCreate(
        email="test-useremail",
        password="test-userpassword",
        signupcode="right-signupcode",
    )
    authuser = User.objects.filter(email=payload.email).first()
    if authuser:
        authuser.delete()

    os.environ["SIGNUPCODE"] = "right-signupcode"
    response = post_organization_user(mock_request, payload)
    assert response.email == payload.email
    assert response.org is None
    assert response.active is True
    assert response.role == OrgUserRole.ACCOUNT_MANAGER

    authuser = User.objects.filter(email=payload.email).first()
    if authuser:
        authuser.delete()


# ================================================================================
def test_get_organization_users_no_org():
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = None

    with pytest.raises(HttpError) as excinfo:
        get_organization_users(mock_request)
    assert str(excinfo.value) == "no associated org"


def test_get_organization_users(orguser):
    mock_request = Mock()
    mock_request.orguser = orguser

    response = get_organization_users(mock_request)
    assert len(response) == 1
    assert response[0].email == orguser.user.email


# ================================================================================
def test_put_organization_user_self(orguser):
    mock_request = Mock()
    mock_request.orguser = orguser

    new_active_status = not orguser.user.is_active
    payload = OrgUserUpdate(
        toupdate_email="unused-param",
        email="newemail",
        active=new_active_status,
    )

    response = put_organization_user_self(mock_request, payload)

    assert response.email == "newemail"
    assert response.active is new_active_status


# ================================================================================
def test_put_organization_user_not_authorized(orguser, nonadminorguser):
    mock_request = Mock()
    mock_request.orguser = nonadminorguser

    payload = OrgUserUpdate(
        toupdate_email=orguser.user.email,
        email="newemail",
    )

    with pytest.raises(HttpError) as excinfo:
        put_organization_user(mock_request, payload)
    assert str(excinfo.value) == "not authorized to update another user"


def test_put_organization_user(orguser, nonadminorguser):
    mock_request = Mock()
    mock_request.orguser = orguser

    payload = OrgUserUpdate(
        toupdate_email=nonadminorguser.user.email,
        email="newemail",
    )

    response = put_organization_user(mock_request, payload)
    assert response.email == payload.email


# ================================================================================
def test_post_organization_has_org(orguser):
    mock_request = Mock()
    mock_request.orguser = orguser

    payload = OrgSchema(name="some-name")
    with pytest.raises(HttpError) as excinfo:
        post_organization(mock_request, payload)
    assert str(excinfo.value) == "orguser already has an associated org"


def test_post_organization_orgexists(orguserwithoutorg, org_without_workspace):
    mock_request = Mock()
    mock_request.orguser = orguserwithoutorg

    payload = OrgSchema(name=org_without_workspace.name)
    with pytest.raises(HttpError) as excinfo:
        post_organization(mock_request, payload)
    assert str(excinfo.value) == "client org already exists"


@patch(
    "ddpui.ddpairbyte.airbytehelpers.setup_airbyte_workspace",
    Mock(
        return_value=AirbyteWorkspace(
            name="workspace-name",
            workspaceId="workspace-id",
            initialSetupComplete=False,
        )
    ),
)
def test_post_organization(orguserwithoutorg):
    mock_request = Mock()
    mock_request.orguser = orguserwithoutorg

    payload = OrgSchema(name="newname")
    response = post_organization(mock_request, payload)
    assert response.name == payload.name

    # now we have an org
    orguserwithoutorg = OrgUser.objects.filter(user=orguserwithoutorg.user).first()
    assert orguserwithoutorg.org is not None
    assert orguserwithoutorg.org.name == "newname"

    orguserwithoutorg.org.delete()


# ================================================================================
def test_post_organization_warehouse_unknownwtype(orguser):
    mock_request = Mock()
    mock_request.orguser = orguser
    payload = OrgWarehouseSchema(
        wtype="unknown", destinationDefId="destinationDefId", airbyteConfig={}
    )

    with pytest.raises(HttpError) as excinfo:
        post_organization_warehouse(mock_request, payload)

    assert str(excinfo.value) == "unrecognized warehouse type unknown"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    create_destination=Mock(
        return_value={
            "destinationId": "destination-id",
        }
    ),
)
@patch.multiple(
    "ddpui.utils.secretsmanager",
    save_warehouse_credentials=Mock(return_value="credentials_lookupkey"),
)
def test_post_organization_warehouse_bigquery(orguser):
    mock_request = Mock()
    mock_request.orguser = orguser
    payload = OrgWarehouseSchema(
        wtype="bigquery",
        destinationDefId="destinationDefId",
        airbyteConfig={"credentials_json": "{}"},
    )

    while True:
        warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
        if warehouse:
            warehouse.delete()
        else:
            break

    response = post_organization_warehouse(mock_request, payload)

    assert response["success"] == 1

    warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    assert warehouse.wtype == "bigquery"
    assert warehouse.airbyte_destination_id == "destination-id"
    assert warehouse.credentials == "credentials_lookupkey"

    warehouse.delete()


# ================================================================================
def test_delete_organization_warehouses(orguser):
    mock_request = Mock()
    mock_request.orguser = orguser

    while True:
        warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
        if warehouse:
            warehouse.delete()
        else:
            break

    OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="airbyte_destination_id",
    )

    assert OrgWarehouse.objects.filter(org=orguser.org).count() == 1
    delete_organization_warehouses(mock_request)
    assert OrgWarehouse.objects.filter(org=orguser.org).count() == 0


# ================================================================================
@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_destination=Mock(
        side_effect=[
            {
                "destination_id": "destination_id_1",
            },
            {
                "destination_id": "destination_id_2",
            },
        ]
    ),
)
def test_get_organizations_warehouses(orguser):
    mock_request = Mock()
    mock_request.orguser = orguser

    while True:
        warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
        if warehouse:
            warehouse.delete()
        else:
            break

    warehouse1 = OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="destination_id_1",
    )
    warehouse2 = OrgWarehouse.objects.create(
        org=orguser.org,
        wtype="postgres",
        airbyte_destination_id="destination_id_2",
    )
    response = get_organizations_warehouses(mock_request)
    assert "warehouses" in response
    assert len(response["warehouses"]) == 2
    assert response["warehouses"][0]["wtype"] == "postgres"
    assert (
        response["warehouses"][0]["airbyte_destination"]["destination_id"]
        == "destination_id_1"
    )
    assert response["warehouses"][1]["wtype"] == "postgres"
    assert (
        response["warehouses"][1]["airbyte_destination"]["destination_id"]
        == "destination_id_2"
    )
    warehouse1.delete()
    warehouse2.delete()


# ================================================================================
def test_post_organization_user_invite_failure(orguser):
    payload = InvitationSchema(
        invited_email="inivted_email",
        invited_role=1,
        invited_by=None,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
    )
    invitation = Invitation.objects.create(
        invited_email=payload.invited_email,
        invited_by=orguser,
        invited_on=timezone.as_ist(datetime.now()),
    )
    mock_request = Mock()
    mock_request.orguser = orguser
    with pytest.raises(HttpError) as excinfo:
        post_organization_user_invite(mock_request, payload)
    assert str(excinfo.value) == f"{payload.invited_email} has already been invited"
    invitation.delete()


def test_post_organization_user_invite(orguser):
    payload = InvitationSchema(
        invited_email="inivted_email",
        invited_role=1,
        invited_by=None,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
    )

    while True:
        invitation = Invitation.objects.filter(
            invited_email=payload.invited_email, invited_by=orguser
        ).first()
        if invitation:
            invitation.delete()
        else:
            break

    mock_request = Mock()
    mock_request.orguser = orguser

    assert (
        Invitation.objects.filter(
            invited_email=payload.invited_email, invited_by=orguser
        ).count()
        == 0
    )
    response = post_organization_user_invite(mock_request, payload)
    assert (
        Invitation.objects.filter(
            invited_email=payload.invited_email, invited_by=orguser
        ).count()
        == 1
    )

    assert response.invited_by.email == orguser.user.email
    assert response.invited_by.role == orguser.role
    assert response.invited_role == payload.invited_role
    assert response.invited_on == payload.invited_on
    assert response.invite_code == payload.invite_code


# ================================================================================
def test_get_organization_user_invite_fail(orguser):
    invited_email = "invited_email"
    while True:
        invitation = Invitation.objects.filter(
            invited_email=invited_email, invited_by=orguser
        ).first()
        if invitation:
            invitation.delete()
        else:
            break

    invitation = Invitation.objects.create(
        invited_email=invited_email,
        invited_by=orguser,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
    )
    mock_request = Mock()
    mock_request.orguser = orguser

    with pytest.raises(HttpError) as excinfo:
        get_organization_user_invite(mock_request, "wrong-code")
    assert str(excinfo.value) == "invalid invite code"

    invitation.delete()


def test_get_organization_user_invite(orguser):
    invited_email = "invited_email"
    while True:
        invitation = Invitation.objects.filter(
            invited_email=invited_email, invited_by=orguser
        ).first()
        if invitation:
            invitation.delete()
        else:
            break

    invitation = Invitation.objects.create(
        invited_email=invited_email,
        invited_by=orguser,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
    )
    mock_request = Mock()
    mock_request.orguser = orguser

    response = get_organization_user_invite(mock_request, "invite_code")
    assert response.invited_email == invitation.invited_email
    assert response.invited_role == invitation.invited_role
    assert response.invited_by.email == invitation.invited_by.user.email
    assert response.invited_on == invitation.invited_on
    assert response.invite_code == invitation.invite_code

    invitation.delete()


# ================================================================================
def test_post_organization_user_accept_invite_fail(orguser):
    mock_request = Mock()
    mock_request.orguser = orguser
    payload = AcceptInvitationSchema(
        invite_code="invalid-invite_code", password="password"
    )

    with pytest.raises(HttpError) as excinfo:
        post_organization_user_accept_invite(mock_request, payload)

    assert str(excinfo.value) == "invalid invite code"


def test_post_organization_user_accept_invite(orguser):
    mock_request = Mock()
    mock_request.orguser = orguser
    payload = AcceptInvitationSchema(invite_code="invite_code", password="password")
    while True:
        invitation = Invitation.objects.filter(
            invited_email="invited_email", invited_by=orguser
        ).first()
        if invitation:
            invitation.delete()
        else:
            break

    invitation = Invitation.objects.create(
        invited_email="invited_email",
        invited_by=orguser,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
    )

    while True:
        orguser = OrgUser.objects.filter(
            user__email="invited_email",
        ).first()
        if orguser:
            orguser.delete()
        else:
            break

    assert (
        OrgUser.objects.filter(
            user__email="invited_email",
        ).count()
        == 0
    )
    response = post_organization_user_accept_invite(mock_request, payload)
    assert response.email == "invited_email"
    assert (
        OrgUser.objects.filter(
            user__email="invited_email",
        ).count()
        == 1
    )
