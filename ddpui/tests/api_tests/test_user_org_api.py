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
    get_current_user_v2,
    post_organization_user,
    get_organization_users,
    delete_organization_users,
    put_organization_user_self,
    put_organization_user,
    post_transfer_ownership,
    post_organization,
    post_organization_warehouse,
    get_organizations_warehouses,
    post_organization_user_invite,
    post_organization_user_accept_invite,
    post_forgot_password,
    post_reset_password,
    post_verify_email,
    get_invitations,
    post_resend_invitation,
    delete_invitation,
)
from ddpui.models.org import Org, OrgSchema, OrgWarehouseSchema, OrgWarehouse
from ddpui.models.org_user import (
    OrgUser,
    OrgUserCreate,
    OrgUserRole,
    OrgUserUpdate,
    OrgUserNewOwner,
    InvitationSchema,
    Invitation,
    UserAttributes,
    AcceptInvitationSchema,
    ForgotPasswordSchema,
    ResetPasswordSchema,
    VerifyEmailSchema,
    DeleteOrgUserPayload,
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
    org = Org.objects.create(airbyte_workspace_id=None, slug="test-org-WO-slug")
    yield org
    print("deleting org_without_workspace")
    org.delete()


@pytest.fixture
def org_with_workspace():
    """a pytest fixture which creates an Org having an airbyte workspace"""
    print("creating org_with_workspace")
    org = Org.objects.create(
        name="org-name",
        airbyte_workspace_id="FAKE-WORKSPACE-ID",
        slug="test-org-W-slug",
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


def test_get_current_userv2_has_user(
    authuser, org_with_workspace, org_without_workspace
):
    """tests /worksspace/detatch/"""
    orguser1 = OrgUser.objects.create(
        user=authuser, org=org_with_workspace, role=OrgUserRole.REPORT_VIEWER
    )
    orguser2 = OrgUser.objects.create(
        user=authuser, org=org_without_workspace, role=OrgUserRole.ACCOUNT_MANAGER
    )

    mock_request = Mock()
    mock_request.orguser = orguser1

    response = get_current_user_v2(mock_request)
    assert len(response) == 2
    assert response[0].email == authuser.email
    assert response[0].active == authuser.is_active
    assert response[1].email == authuser.email
    assert response[1].active == authuser.is_active

    if response[0].org.slug == org_with_workspace.slug:
        assert response[0].role == orguser1.role
        assert response[1].role == orguser2.role

    elif response[1].org.slug == org_with_workspace.slug:
        assert response[1].role == orguser1.role
        assert response[0].role == orguser2.role


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


def test_post_organization_user_userexists_caps_email(authuser):
    """a failing test, the email address is already in use"""
    mock_request = Mock()
    payload = OrgUserCreate(
        email="TEMPUSEREMAIL", password="userpassword", signupcode="right-signupcode"
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


@patch.multiple("ddpui.utils.sendgrid", send_signup_email=Mock(return_value=1))
def test_post_organization_user_success_lowercase_email():
    """a success test"""
    mock_request = Mock()
    payload = OrgUserCreate(
        email="TEST@useremail.com",
        password="test-userpassword",
        signupcode="right-signupcode",
    )
    the_authuser = User.objects.filter(email__iexact=payload.email).first()
    if the_authuser:
        the_authuser.delete()

    os.environ["SIGNUPCODE"] = "right-signupcode"
    response = post_organization_user(mock_request, payload)
    assert response.email == payload.email.lower()
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
def test_delete_organization_users_no_org():
    """a failing test, requestor has no associated org"""
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = None
    payload = DeleteOrgUserPayload(email="email-dne")

    with pytest.raises(HttpError) as excinfo:
        delete_organization_users(mock_request, payload)
    assert str(excinfo.value) == "no associated org"


def test_delete_organization_users_wrong_org(orguser):
    """a failing test, orguser dne"""
    mock_request = Mock()
    mock_request.orguser = orguser
    payload = DeleteOrgUserPayload(email="email-dne")

    with pytest.raises(HttpError) as excinfo:
        delete_organization_users(mock_request, payload)
    assert str(excinfo.value) == "user does not belong to the org"


def test_delete_organization_users_cant_delete_self(orguser):
    """a failing test, orguser dne"""
    mock_request = Mock()
    mock_request.orguser = orguser
    payload = DeleteOrgUserPayload(email=orguser.user.email)

    with pytest.raises(HttpError) as excinfo:
        delete_organization_users(mock_request, payload)
    assert str(excinfo.value) == "user cannot delete themselves"


def test_delete_organization_users_cant_delete_higher_role(orguser):
    """a failing test, orguser not authorized"""
    mock_request = Mock()
    mock_request.orguser = orguser
    orguser.role = OrgUserRole.PIPELINE_MANAGER
    payload = DeleteOrgUserPayload(email="useremail")
    user = User.objects.create(email=payload.email, username=payload.email)
    OrgUser.objects.create(org=orguser.org, user=user, role=OrgUserRole.ACCOUNT_MANAGER)

    with pytest.raises(HttpError) as excinfo:
        delete_organization_users(mock_request, payload)
    assert str(excinfo.value) == "cannot delete user having higher role"


def test_delete_organization_users_success(orguser):
    """a passing test"""
    mock_request = Mock()
    mock_request.orguser = orguser
    payload = DeleteOrgUserPayload(email="useremail")
    user = User.objects.create(email=payload.email, username=payload.email)
    OrgUser.objects.create(org=orguser.org, user=user)
    assert (
        OrgUser.objects.filter(org=orguser.org, user__email=payload.email).count() == 1
    )
    delete_organization_users(mock_request, payload)
    assert (
        OrgUser.objects.filter(org=orguser.org, user__email=payload.email).count() == 0
    )
    user.delete()


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
def test_post_transfer_ownership_only_account_owner(authuser, org_with_workspace):
    """only an account owner can transfer account ownership"""
    orguser = OrgUser.objects.create(user=authuser, org=org_with_workspace)
    orguser.role = OrgUserRole.PIPELINE_MANAGER
    mock_request = Mock()
    mock_request.orguser = orguser
    payload = OrgUserNewOwner(new_owner_email="new-email")
    with pytest.raises(HttpError) as excinfo:
        post_transfer_ownership(mock_request, payload)
    assert str(excinfo.value) == "only an account owner can transfer account ownership"


def test_post_transfer_ownership_no_such_user(authuser, org_with_workspace):
    """only an account owner can transfer account ownership"""
    orguser = OrgUser.objects.create(user=authuser, org=org_with_workspace)
    orguser.role = OrgUserRole.ACCOUNT_MANAGER
    mock_request = Mock()
    mock_request.orguser = orguser
    payload = OrgUserNewOwner(new_owner_email="new-email")
    with pytest.raises(HttpError) as excinfo:
        post_transfer_ownership(mock_request, payload)
    assert (
        str(excinfo.value)
        == "could not find user having this email address in this org"
    )


def test_post_transfer_ownership_not_pipeline_mgr(authuser, org_with_workspace):
    """only an account owner can transfer account ownership"""
    orguser = OrgUser.objects.create(user=authuser, org=org_with_workspace)
    orguser.role = OrgUserRole.ACCOUNT_MANAGER
    mock_request = Mock()
    mock_request.orguser = orguser
    new_owner_authuser = User.objects.create(username="new-owner", email="new-owner")
    new_owner = OrgUser.objects.create(user=new_owner_authuser, org=org_with_workspace)
    new_owner.role = OrgUserRole.REPORT_VIEWER
    payload = OrgUserNewOwner(new_owner_email="new-owner")
    with pytest.raises(HttpError) as excinfo:
        post_transfer_ownership(mock_request, payload)
    assert str(excinfo.value) == "can only promote pipeline managers"


@patch(
    "ddpui.api.client.user_org_api.transaction.atomic",
    Mock(side_effect=Exception("db error")),
)
def test_post_transfer_ownership_db_error(
    authuser,
    org_with_workspace,
):
    """only an account owner can transfer account ownership"""
    orguser = OrgUser.objects.create(user=authuser, org=org_with_workspace)
    orguser.role = OrgUserRole.ACCOUNT_MANAGER
    mock_request = Mock()
    mock_request.orguser = orguser
    new_owner_authuser = User.objects.create(username="new-owner", email="new-owner")
    new_owner = OrgUser.objects.create(user=new_owner_authuser, org=org_with_workspace)
    new_owner.role = OrgUserRole.PIPELINE_MANAGER
    new_owner.save()
    payload = OrgUserNewOwner(new_owner_email="new-owner")
    with pytest.raises(HttpError) as excinfo:
        post_transfer_ownership(mock_request, payload)
    assert str(excinfo.value) == "failed to transfer ownership"


# ================================================================================
def test_post_organization_orgexists(orguserwithoutorg, org_without_workspace):
    """failing test, org name is already in use"""
    mock_request = Mock()
    UserAttributes.objects.create(user=orguserwithoutorg.user, can_create_orgs=True)
    mock_request.orguser = orguserwithoutorg

    payload = OrgSchema(name=org_without_workspace.name)
    with pytest.raises(HttpError) as excinfo:
        post_organization(mock_request, payload)
    assert str(excinfo.value) == "client org with this name already exists"


def test_post_organization_no_create_org(orguserwithoutorg, org_without_workspace):
    """failing test, org name is already in use"""
    mock_request = Mock()
    mock_request.orguser = orguserwithoutorg

    payload = OrgSchema(name=org_without_workspace.name)
    with pytest.raises(HttpError) as excinfo:
        post_organization(mock_request, payload)
    assert str(excinfo.value) == "Insufficient permissions for this operation"


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
    UserAttributes.objects.create(user=orguserwithoutorg.user, can_create_orgs=True)

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
        wtype="unknown",
        name="warehousename",
        destinationDefId="destinationDefId",
        airbyteConfig={},
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
        name="bigquery",
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
# #skipcq: PY-W0069
# this needs to be rewritten  #skipcq: PY-W0069
# def test_delete_organization_warehouses(orguser):  #skipcq: PY-W0069
#     """success test, deleting a warehouse"""  #skipcq: PY-W0069
#     mock_request = Mock()  #skipcq: PY-W0069
#     mock_request.orguser = orguser  #skipcq: PY-W0069
# skipcq: PY-W0069
#     orguser.org.airbyte_workspace_id = "workspace-id"  #skipcq: PY-W0069
#     OrgWarehouse.objects.create(  #skipcq: PY-W0069
#         org=orguser.org,  #skipcq: PY-W0069
#         wtype="postgres",  #skipcq: PY-W0069
#         airbyte_destination_id="airbyte_destination_id",  #skipcq: PY-W0069
#     )  #skipcq: PY-W0069
# skipcq: PY-W0069
#     assert OrgWarehouse.objects.filter(org=orguser.org).count() == 1  #skipcq: PY-W0069
#     delete_organization_warehouses(mock_request)  #skipcq: PY-W0069
#     assert OrgWarehouse.objects.filter(org=orguser.org).count() == 0  #skipcq: PY-W0069


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
@patch("ddpui.utils.sendgrid.send_invite_user_email", Mock())
def test_post_organization_user_invite_no_org(orguser):
    """failing test, no org"""
    mock_request = Mock()
    orguser.org = None
    mock_request.orguser = orguser
    payload = InvitationSchema(
        invited_email="some-email", invited_role_slug="report_viewer"
    )
    with pytest.raises(HttpError) as excinfo:
        post_organization_user_invite(mock_request, payload)
    assert str(excinfo.value) == "create an organization first"


@patch("ddpui.utils.sendgrid.send_invite_user_email", Mock())
def test_post_organization_user_invite_nosuchrole(orguser):
    """failing test, no such role"""
    mock_request = Mock()
    mock_request.orguser = orguser
    payload = InvitationSchema(
        invited_email="some-email", invited_role_slug="hot_stepper"
    )
    with pytest.raises(HttpError) as excinfo:
        post_organization_user_invite(mock_request, payload)
    assert str(excinfo.value) == "Invalid role"


@patch("ddpui.utils.sendgrid.send_invite_user_email", Mock())
def test_post_organization_user_invite_insufficientrole(orguser):
    """failing test, no such role"""
    mock_request = Mock()
    mock_request.orguser = orguser
    orguser.role = 1
    payload = InvitationSchema(
        invited_email="some-email", invited_role_slug="pipeline_manager"
    )
    with pytest.raises(HttpError) as excinfo:
        post_organization_user_invite(mock_request, payload)
    assert str(excinfo.value) == "Insufficient permissions for this operation"


@patch("ddpui.utils.sendgrid.send_invite_user_email", mock_sendgrid=Mock())
def test_post_organization_user_invite(mock_sendgrid, orguser):
    """success test, inviting a new user"""
    payload = InvitationSchema(
        invited_email="inivted_email",
        invited_role_slug="report_viewer",
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
    mock_sendgrid.assert_called_once()


@patch("ddpui.utils.sendgrid.send_invite_user_email", mock_sendgrid=Mock())
def test_post_organization_user_invite_multiple_open_invites(mock_sendgrid, orguser):
    """success test, inviting a new user"""
    another_org = Org.objects.create(name="anotherorg", slug="anotherorg")
    another_user = User.objects.create(username="anotheruser", email="anotheruser")
    another_org_user = OrgUser.objects.create(
        org=another_org, user=another_user, role=OrgUserRole.PIPELINE_MANAGER
    )
    Invitation.objects.create(
        invited_email="inivted_email",
        invited_role=OrgUserRole.PIPELINE_MANAGER,
        invited_by=another_org_user,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code_existing",
    )
    payload = InvitationSchema(
        invited_email="inivted_email",
        invited_role_slug="report_viewer",
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
    mock_sendgrid.assert_called_once()


@patch("ddpui.utils.sendgrid.send_invite_user_email", mock_sendgrid=Mock())
def test_post_organization_user_invite_lowercase_email(mock_sendgrid, orguser: OrgUser):
    """success test, inviting a new user"""
    payload = InvitationSchema(
        invited_email="INVITED_EMAIL",
        invited_role_slug="report_viewer",
        invited_by=None,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
    )

    mock_request = Mock()
    mock_request.orguser = orguser

    assert (
        Invitation.objects.filter(
            invited_email__iexact=payload.invited_email, invited_by=orguser
        ).count()
        == 0
    )
    response = post_organization_user_invite(mock_request, payload)
    assert (
        Invitation.objects.filter(
            invited_email=payload.invited_email.lower(), invited_by=orguser
        ).count()
        == 1
    )

    assert response.invited_by.email == orguser.user.email
    assert response.invited_by.role == orguser.role
    assert response.invited_role == payload.invited_role
    assert response.invited_on == payload.invited_on
    assert response.invite_code == payload.invite_code
    mock_sendgrid.assert_called_once()


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
    assert UserAttributes.objects.filter(user__email="invited_email").exists()


def test_post_organization_user_accept_invite_lowercase_email(orguser):
    """success test, accepting an invitation"""
    mock_request = Mock()
    mock_request.orguser = orguser
    payload = AcceptInvitationSchema(invite_code="invite_code", password="password")

    Invitation.objects.create(
        invited_email="INVITED_EMAIL",
        invited_by=orguser,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
    )

    assert (
        OrgUser.objects.filter(
            user__email__iexact="invited_email",
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


def test_post_organization_user_accept_invite_firstaccount_fail(orguser):
    """failing test, invalid invite code"""
    mock_request = Mock()
    mock_request.orguser = orguser
    payload = AcceptInvitationSchema(
        invite_code="invite_code",
    )
    Invitation.objects.create(
        invited_email="invited_email",
        invited_by=orguser,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
    )

    with pytest.raises(HttpError) as excinfo:
        post_organization_user_accept_invite(mock_request, payload)

    assert str(excinfo.value) == "password is required"


def test_post_organization_user_accept_invite_secondaccount(orguser):
    """success test, accepting an invitation"""
    mock_request = Mock()
    mock_request.orguser = orguser
    payload = AcceptInvitationSchema(invite_code="invite_code")

    Invitation.objects.create(
        invited_email="invited_email",
        invited_by=orguser,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
    )

    authuser = User.objects.create_user(
        username="invited_email", email="invited_email", password="oldpassword"
    )
    assert User.objects.filter(email="invited_email").count() == 1
    anotherorg = Org.objects.create(name="anotherorg", slug="anotherorg")
    OrgUser.objects.create(user=authuser, org=anotherorg)

    assert (
        OrgUser.objects.filter(
            user__email="invited_email",
        ).count()
        == 1
    )
    response = post_organization_user_accept_invite(mock_request, payload)
    assert response.email == "invited_email"
    assert (
        OrgUser.objects.filter(
            user__email="invited_email",
        ).count()
        == 2
    )
    assert (
        OrgUser.objects.filter(
            user__email="invited_email", org__slug=orguser.org.slug
        ).count()
        == 1
    )
    assert (
        OrgUser.objects.filter(
            user__email="invited_email", org__slug=anotherorg.slug
        ).count()
        == 1
    )
    assert User.objects.filter(email="invited_email").count() == 1


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


# ================================================================================
def test_get_invitations_no_org(orguser):
    """failure - no org"""
    orguser.org = None
    mock_request = Mock()
    mock_request.orguser = orguser
    with pytest.raises(HttpError) as excinfo:
        get_invitations(mock_request)
    assert str(excinfo.value) == "create an organization first"


def test_get_invitations(orguser):
    """success - return invitations"""
    mock_request = Mock()
    mock_request.orguser = orguser
    Invitation.objects.create(
        invited_by=orguser,
        invited_email="invited-email",
        invited_role=1,
        invited_on=timezone.as_ist(datetime(2023, 1, 1, 10, 0, 0)),
    )
    response = get_invitations(mock_request)
    assert len(response) == 1
    assert response[0]["invited_email"] == "invited-email"
    assert response[0]["invited_role_slug"] == "report_viewer"
    assert response[0]["invited_role"] == 1
    assert response[0]["invited_on"] == datetime(
        2023, 1, 1, 4, 30, 0, tzinfo=timezone.pytz.utc
    )


# ================================================================================
def test_post_resend_invitation_no_org(orguser):
    """failure - no org"""
    orguser.org = None
    mock_request = Mock()
    mock_request.orguser = orguser
    with pytest.raises(HttpError) as excinfo:
        post_resend_invitation(mock_request, 1)
    assert str(excinfo.value) == "create an organization first"


@patch("ddpui.utils.sendgrid.send_invite_user_email", sendgrid=Mock())
def test_post_resend_invitation(sendgrid: Mock, orguser):
    """success test"""
    original_invited_on = timezone.as_ist(datetime(2023, 1, 1, 10, 0, 0))
    invitation = Invitation.objects.create(
        invited_on=original_invited_on,
        invited_email="email",
        invite_code="hello",
        invited_by=orguser,
        invited_role=1,
    )
    frontend_url = os.getenv("FRONTEND_URL")
    invite_url = f"{frontend_url}/invitations/?invite_code={invitation.invite_code}"
    mock_request = Mock(orguser=orguser)
    post_resend_invitation(mock_request, invitation.id)
    sendgrid.assert_called_once_with("email", orguser.user.email, invite_url)
    invitation.refresh_from_db()
    assert invitation.invited_on > original_invited_on


# ================================================================================
def test_delete_invitation_no_org(orguser):
    """failure - no org"""
    orguser.org = None
    mock_request = Mock()
    mock_request.orguser = orguser
    with pytest.raises(HttpError) as excinfo:
        delete_invitation(mock_request, 1)
    assert str(excinfo.value) == "create an organization first"


def test_delete_invitation(orguser):
    """success"""
    original_invited_on = timezone.as_ist(datetime(2023, 1, 1, 10, 0, 0))
    invitation = Invitation.objects.create(
        invited_on=original_invited_on,
        invited_email="email",
        invite_code="hello",
        invited_by=orguser,
        invited_role=1,
    )
    mock_request = Mock(orguser=orguser)
    assert Invitation.objects.filter(id=invitation.id).exists()
    delete_invitation(mock_request, invitation.id)
    assert not Invitation.objects.filter(id=invitation.id).exists()
