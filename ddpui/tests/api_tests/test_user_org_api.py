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
    post_forgot_password,
    post_reset_password,
    post_verify_email,
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
    ForgotPasswordSchema,
    ResetPasswordSchema,
    VerifyEmailSchema,
)
from ddpui.ddpairbyte.schema import AirbyteWorkspace
from ddpui.utils import timezone
from django.contrib.auth.models import User

pytestmark = pytest.mark.django_db


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
    user = User.objects.create(
        username="tempusername", email="tempuseremail", password="tempuserpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def orguser(authuser, org_without_workspace):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=authuser, org=org_without_workspace, role=OrgUserRole.ACCOUNT_MANAGER
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def nonadminorguser(authuser, org_without_workspace):
    """a pytest fixture representing an OrgUser having the report-viewer role"""
    orguser = OrgUser.objects.create(
        user=authuser, org=org_without_workspace, role=OrgUserRole.REPORT_VIEWER
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def orguserwithoutorg(authuser):
    """a pytest fixture representing an OrgUser with no associated Org"""
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
    """a failing test, signup without the signup code"""
    mock_request = Mock()
    payload = OrgUserCreate(
        email="useremail", password="userpassword", signupcode="wrong-signupcode"
    )
    os.environ["SIGNUPCODE"] = "right-signupcode"
    with pytest.raises(HttpError) as excinfo:
        post_organization_user(mock_request, payload)
    assert str(excinfo.value) == "That is not the right signup code"


def test_post_organization_user_userexists_email(authuser):
    """a failing test, the email address is already in use"""
    mock_request = Mock()
    payload = OrgUserCreate(
        email="tempuseremail", password="userpassword", signupcode="right-signupcode"
    )
    os.environ["SIGNUPCODE"] = "right-signupcode"
    with pytest.raises(HttpError) as excinfo:
        post_organization_user(mock_request, payload)
    assert str(excinfo.value) == f"user having email {authuser.email} exists"


def test_post_organization_user_userexists_username(authuser):
    """a failing test, the email address is already in use"""
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


def test_post_organization_user_invalid_email(authuser):
    """a failing test, the email address is not valid"""
    mock_request = Mock()
    payload = OrgUserCreate(
        email="invalid_email",
        password="userpassword",
        signupcode="right-signupcode",
        role=2,
    )
    os.environ["SIGNUPCODE"] = "right-signupcode"
    with pytest.raises(HttpError) as excinfo:
        post_organization_user(mock_request, payload)
    assert str(excinfo.value) == "that is not a valid email address"


@patch.multiple("ddpui.utils.sendgrid", send_signup_email=Mock(return_value=1))
def test_post_organization_user_success():
    """a success test"""
    mock_request = Mock()
    payload = OrgUserCreate(
        email="test@useremail.com",
        password="test-userpassword",
        signupcode="right-signupcode",
    )
    the_authuser = User.objects.filter(email=payload.email).first()
    if the_authuser:
        the_authuser.delete()

    os.environ["SIGNUPCODE"] = "right-signupcode"
    response = post_organization_user(mock_request, payload)
    assert response.email == payload.email
    assert response.org is None
    assert response.active is True
    assert response.role == OrgUserRole.ACCOUNT_MANAGER

    the_authuser = User.objects.filter(email=payload.email).first()
    if the_authuser:
        the_authuser.delete()


# ================================================================================
def test_get_organization_users_no_org():
    """a failing test, requestor has no associated org"""
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = None

    with pytest.raises(HttpError) as excinfo:
        get_organization_users(mock_request)
    assert str(excinfo.value) == "no associated org"


def test_get_organization_users(orguser):
    """a success test"""
    mock_request = Mock()
    mock_request.orguser = orguser

    response = get_organization_users(mock_request)
    assert len(response) == 1
    assert response[0].email == orguser.user.email


# ================================================================================
def test_put_organization_user_self(orguser):
    """a success test"""
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
    """a failing test, requestor cannot update another user"""
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
    """a succeas test, requestor updates another user"""
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
    """failing test, user already has an org"""
    mock_request = Mock()
    mock_request.orguser = orguser

    payload = OrgSchema(name="some-name")
    with pytest.raises(HttpError) as excinfo:
        post_organization(mock_request, payload)
    assert str(excinfo.value) == "orguser already has an associated org"


def test_post_organization_orgexists(orguserwithoutorg, org_without_workspace):
    """failing test, org name is already in use"""
    mock_request = Mock()
    mock_request.orguser = orguserwithoutorg

    payload = OrgSchema(name=org_without_workspace.name)
    with pytest.raises(HttpError) as excinfo:
        post_organization(mock_request, payload)
    assert str(excinfo.value) == "client org with this name already exists"


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
    """success test for org creation"""
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
    """a failing test, unrecognized warehouse type"""
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
    """success test, warehouse creation"""
    mock_request = Mock()
    mock_request.orguser = orguser
    payload = OrgWarehouseSchema(
        wtype="bigquery",
        destinationDefId="destinationDefId",
        airbyteConfig={"credentials_json": "{}"},
    )

    response = post_organization_warehouse(mock_request, payload)

    assert response["success"] == 1

    warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    assert warehouse.wtype == "bigquery"
    assert warehouse.airbyte_destination_id == "destination-id"
    assert warehouse.credentials == "credentials_lookupkey"


# ================================================================================
# this needs to be rewritten
# def test_delete_organization_warehouses(orguser):
#     """success test, deleting a warehouse"""
#     mock_request = Mock()
#     mock_request.orguser = orguser

#     orguser.org.airbyte_workspace_id = "workspace-id"
#     OrgWarehouse.objects.create(
#         org=orguser.org,
#         wtype="postgres",
#         airbyte_destination_id="airbyte_destination_id",
#     )

#     assert OrgWarehouse.objects.filter(org=orguser.org).count() == 1
#     delete_organization_warehouses(mock_request)
#     assert OrgWarehouse.objects.filter(org=orguser.org).count() == 0


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
    """success test, fetching all warehouses for an org"""
    mock_request = Mock()
    mock_request.orguser = orguser

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
    """failing test, invitation has already gone out"""
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
    """success test, inviting a new user"""
    payload = InvitationSchema(
        invited_email="inivted_email",
        invited_role=1,
        invited_by=None,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
    )

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
    """failing test, invalid invitation code"""
    invited_email = "invited_email"

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
    """success test, look up an invitation from the invite code"""
    invited_email = "invited_email"

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
    """failing test, invalid invite code"""
    mock_request = Mock()
    mock_request.orguser = orguser
    payload = AcceptInvitationSchema(
        invite_code="invalid-invite_code", password="password"
    )

    with pytest.raises(HttpError) as excinfo:
        post_organization_user_accept_invite(mock_request, payload)

    assert str(excinfo.value) == "invalid invite code"


def test_post_organization_user_accept_invite(orguser):
    """success test, accepting an invitation"""
    mock_request = Mock()
    mock_request.orguser = orguser
    payload = AcceptInvitationSchema(invite_code="invite_code", password="password")

    Invitation.objects.create(
        invited_email="invited_email",
        invited_by=orguser,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
    )

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


# ================================================================================
def test_post_forgot_password_nosuchuser():
    """success test, invalid email address"""
    mock_request = Mock()
    payload = ForgotPasswordSchema(email="no-such-email")
    response = post_forgot_password(mock_request, payload)
    assert response["success"] == 1


@patch.multiple(
    "ddpui.utils.sendgrid",
    send_password_reset_email=Mock(side_effect=Exception("error")),
)
def test_post_forgot_password_emailfailed():
    """failure test, could not send email"""
    mock_request = Mock()
    user = User.objects.create(email="fake-email", username="fake-username")
    temporguser = OrgUser.objects.create(user=user)
    payload = ForgotPasswordSchema(email=temporguser.user.email)
    with pytest.raises(HttpError) as excinfo:
        post_forgot_password(mock_request, payload)
    assert str(excinfo.value) == "failed to send email"


@patch.multiple("ddpui.utils.sendgrid", send_password_reset_email=Mock(return_value=1))
def test_post_forgot_password_success():
    """success test, forgot password email sent"""
    mock_request = Mock()
    user = User.objects.create(email="fake-email", username="fake-username")
    temporguser = OrgUser.objects.create(user=user)
    payload = ForgotPasswordSchema(email=temporguser.user.email)
    response = post_forgot_password(mock_request, payload)
    assert response["success"] == 1


# ================================================================================
@patch.multiple("redis.Redis", get=Mock(return_value=None))
def test_post_reset_password_invalid_reset_code():
    """failure test, invalid code"""
    mock_request = Mock()
    payload = ResetPasswordSchema(token="fake-token", password="new-password")
    with pytest.raises(HttpError) as excinfo:
        post_reset_password(mock_request, payload)
    assert str(excinfo.value) == "invalid reset code"


@patch.multiple("redis.Redis", get=Mock(return_value="98765".encode("utf-8")))
def test_post_reset_password_no_such_orguser():
    """failure test, invalid code"""
    mock_request = Mock()
    payload = ResetPasswordSchema(token="real-token", password="new-password")
    with pytest.raises(HttpError) as excinfo:
        post_reset_password(mock_request, payload)
    assert str(excinfo.value) == "could not look up request from this token"


# ================================================================================
@patch.multiple("redis.Redis", get=Mock(return_value=None))
def test_post_verify_email_invalid_reset_code():
    """failure test, invalid code"""
    mock_request = Mock()
    payload = VerifyEmailSchema(token="fake-token")
    with pytest.raises(HttpError) as excinfo:
        post_verify_email(mock_request, payload)
    assert str(excinfo.value) == "this link has expired"


@patch.multiple("redis.Redis", get=Mock(return_value="98765".encode("utf-8")))
def test_post_verify_email_no_such_orguser():
    """failure test, invalid code"""
    mock_request = Mock()
    payload = VerifyEmailSchema(token="real-token")
    with pytest.raises(HttpError) as excinfo:
        post_verify_email(mock_request, payload)
    assert str(excinfo.value) == "could not look up request from this token"
