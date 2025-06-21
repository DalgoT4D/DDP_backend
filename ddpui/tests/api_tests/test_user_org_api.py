import uuid
import os
import django
from datetime import datetime
from django.core.management import call_command

from unittest.mock import Mock, patch
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.api.user_org_api import (
    get_current_user_v2,
    post_organization_user,
    get_organization_users,
    delete_organization_users,
    delete_organization_users_v1,
    put_organization_user_self,
    put_organization_user_self_v1,
    put_organization_user,
    put_organization_user_v1,
    post_transfer_ownership,
    post_organization_warehouse,
    get_organizations_warehouses,
    post_organization_user_invite,
    post_organization_user_invite_v1,
    post_organization_user_accept_invite_v1,
    post_forgot_password,
    post_reset_password,
    post_verify_email,
    get_invitations,
    get_invitations_v1,
    post_resend_invitation,
    delete_invitation,
    post_organization_accept_tnc,
    get_organization_wren,
)
from ddpui.models.org import Org, OrgWarehouseSchema, OrgWarehouse
from ddpui.models.role_based_access import Role, RolePermission, Permission
from ddpui.models.org_user import (
    OrgUser,
    OrgUserCreate,
    OrgUserRole,
    OrgUserUpdate,
    OrgUserUpdatev1,
    OrgUserNewOwner,
    InvitationSchema,
    NewInvitationSchema,
    Invitation,
    UserAttributes,
    AcceptInvitationSchema,
    ForgotPasswordSchema,
    ResetPasswordSchema,
    VerifyEmailSchema,
    DeleteOrgUserPayload,
)
from ddpui.auth import (
    ACCOUNT_MANAGER_ROLE,
    PIPELINE_MANAGER_ROLE,
    GUEST_ROLE,
    SUPER_ADMIN_ROLE,
)
from ddpui.models.orgtnc import OrgTnC
from ddpui.utils import timezone
from ddpui.utils.custom_logger import CustomLogger
from django.contrib.auth.models import User

pytestmark = pytest.mark.django_db


logger = CustomLogger("ddpui-pytest")


@pytest.fixture(scope="session")
def seed_db(django_db_setup, django_db_blocker):
    with django_db_blocker.unblock():
        # Run the loaddata command to load the fixture
        call_command("loaddata", "001_roles.json")
        call_command("loaddata", "002_permissions.json")
        call_command("loaddata", "003_role_permissions.json")


# ================================================================================
@pytest.fixture
def org_without_workspace():
    """a pytest fixture which creates an Org without an airbyte workspace"""
    org = Org.objects.create(
        airbyte_workspace_id=None, slug="test-org-WO-slug", name="test-org-WO-name"
    )
    yield org
    org.delete()


@pytest.fixture
def org_with_workspace():
    """a pytest fixture which creates an Org having an airbyte workspace"""
    org = Org.objects.create(
        name="org-name",
        airbyte_workspace_id="FAKE-WORKSPACE-ID",
        slug="test-org-W-slug",
    )
    yield org
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
        user=authuser,
        org=org_without_workspace,
        role=OrgUserRole.ACCOUNT_MANAGER,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def nonadminorguser(authuser, org_without_workspace):
    """a pytest fixture representing an OrgUser having the report-viewer role"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org_without_workspace,
        role=OrgUserRole.REPORT_VIEWER,
        new_role=Role.objects.filter(slug=GUEST_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def orguserwithoutorg(authuser):
    """a pytest fixture representing an OrgUser with no associated Org"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=None,
        role=OrgUserRole.REPORT_VIEWER,
        new_role=Role.objects.filter(slug=GUEST_ROLE).first(),
    )
    yield orguser
    orguser.delete()


# ================================================================================
def mock_request(orguser: OrgUser = None):
    mock_request = Mock()
    mock_request.orguser = orguser
    mock_request.permissions = []
    if orguser and orguser.new_role:
        permission_slugs = RolePermission.objects.filter(role=orguser.new_role).values_list(
            "permission__slug", flat=True
        )
        mock_request.permissions = list(permission_slugs)

    return mock_request


def test_seed_data(seed_db):
    """a test to seed the database"""
    assert Role.objects.count() == 5
    assert RolePermission.objects.count() > 5
    assert Permission.objects.count() > 5


def test_get_current_userv2_has_user(authuser, org_with_workspace, org_without_workspace):
    """tests /worksspace/detatch/"""
    orguser1 = OrgUser.objects.create(
        user=authuser,
        org=org_with_workspace,
        role=OrgUserRole.REPORT_VIEWER,
        new_role=Role.objects.filter(slug=GUEST_ROLE).first(),
    )
    orguser2 = OrgUser.objects.create(
        user=authuser,
        org=org_without_workspace,
        role=OrgUserRole.ACCOUNT_MANAGER,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )

    request = mock_request(orguser2)

    response = get_current_user_v2(request)

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
def test_post_organization_user_wrong_signupcode(orguser):
    """a failing test, signup without the signup code"""
    request = mock_request(orguser)
    payload = OrgUserCreate(
        email="useremail", password="userpassword", signupcode="wrong-signupcode"
    )
    os.environ["SIGNUPCODE"] = "right-signupcode"
    with pytest.raises(HttpError) as excinfo:
        post_organization_user(request, payload)
    assert str(excinfo.value) == "That is not the right signup code"


def test_post_organization_user_userexists_email(orguser):
    """a failing test, the email address is already in use"""
    request = mock_request(orguser)
    payload = OrgUserCreate(
        email="tempuseremail", password="userpassword", signupcode="right-signupcode"
    )
    os.environ["SIGNUPCODE"] = "right-signupcode"
    with pytest.raises(HttpError) as excinfo:
        post_organization_user(request, payload)
    assert str(excinfo.value) == f"user having email {orguser.user.email} exists"


def test_post_organization_user_userexists_caps_email(orguser):
    """a failing test, the email address is already in use"""
    request = mock_request(orguser)
    payload = OrgUserCreate(
        email="TEMPUSEREMAIL", password="userpassword", signupcode="right-signupcode"
    )
    os.environ["SIGNUPCODE"] = "right-signupcode"
    with pytest.raises(HttpError) as excinfo:
        post_organization_user(request, payload)
    assert str(excinfo.value) == f"user having email {orguser.user.email} exists"


def test_post_organization_user_userexists_username(orguser):
    """a failing test, the email address is already in use"""
    request = mock_request(orguser)
    payload = OrgUserCreate(
        email="tempusername",
        password="userpassword",
        signupcode="right-signupcode",
        role=2,
    )
    os.environ["SIGNUPCODE"] = "right-signupcode"
    with pytest.raises(HttpError) as excinfo:
        post_organization_user(request, payload)
    assert str(excinfo.value) == f"user having email {orguser.user.username} exists"


def test_post_organization_user_invalid_email(orguser):
    """a failing test, the email address is not valid"""
    request = mock_request(orguser)
    payload = OrgUserCreate(
        email="invalid_email",
        password="userpassword",
        signupcode="right-signupcode",
        role=2,
    )
    os.environ["SIGNUPCODE"] = "right-signupcode"
    with pytest.raises(HttpError) as excinfo:
        post_organization_user(request, payload)
    assert str(excinfo.value) == "that is not a valid email address"


@patch.multiple("ddpui.utils.awsses", send_signup_email=Mock(return_value=1))
def test_post_organization_user_success(orguser):
    """a success test"""
    request = mock_request(orguser)
    payload = OrgUserCreate(
        email="test@useremail.com",
        password="test-userpassword",
        signupcode="right-signupcode",
    )
    the_authuser = User.objects.filter(email=payload.email).first()
    if the_authuser:
        the_authuser.delete()

    os.environ["SIGNUPCODE"] = "right-signupcode"
    response = post_organization_user(request, payload)
    assert response.email == payload.email
    assert response.org is None
    assert response.active is True
    assert response.role == OrgUserRole.ACCOUNT_MANAGER

    the_authuser = User.objects.filter(email=payload.email).first()
    if the_authuser:
        the_authuser.delete()


@patch.multiple("ddpui.utils.awsses", send_signup_email=Mock(return_value=1))
def test_post_organization_user_success_lowercase_email(orguser):
    """a success test"""
    request = mock_request(orguser)
    payload = OrgUserCreate(
        email="TEST@useremail.com",
        password="test-userpassword",
        signupcode="right-signupcode",
    )
    the_authuser = User.objects.filter(email__iexact=payload.email).first()
    if the_authuser:
        the_authuser.delete()

    os.environ["SIGNUPCODE"] = "right-signupcode"
    response = post_organization_user(request, payload)
    assert response.email == payload.email.lower()
    assert response.org is None
    assert response.active is True
    assert response.role == OrgUserRole.ACCOUNT_MANAGER

    the_authuser = User.objects.filter(email=payload.email).first()
    if the_authuser:
        the_authuser.delete()


# ================================================================================
def test_get_organization_users_no_org(orguser):
    """a failing test, requestor has no associated org"""
    orguser.org = None
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_organization_users(request)
    assert str(excinfo.value) == "no associated org"


def test_get_organization_users(orguser):
    """a success test"""
    request = mock_request(orguser)

    response = get_organization_users(request)
    assert len(response) == 1
    assert response[0].email == orguser.user.email


# ================================================================================
def test_delete_organization_users_no_org(orguser):
    """a failing test, requestor has no associated org"""
    orguser.org = None
    request = mock_request(orguser)
    payload = DeleteOrgUserPayload(email="email-dne")

    with pytest.raises(HttpError) as excinfo:
        delete_organization_users(request, payload)
    assert str(excinfo.value) == "no associated org"


def test_delete_organization_users_no_org_v1(orguser):
    """a failing test, requestor has no associated org"""
    orguser.org = None
    request = mock_request(orguser)
    payload = DeleteOrgUserPayload(email="email-dne")

    with pytest.raises(HttpError) as excinfo:
        delete_organization_users_v1(request, payload)
    assert str(excinfo.value) == "no associated org"


def test_delete_organization_users_wrong_org(orguser):
    """a failing test, orguser dne"""
    request = mock_request(orguser)
    payload = DeleteOrgUserPayload(email="email-dne")

    with pytest.raises(HttpError) as excinfo:
        delete_organization_users(request, payload)
    assert str(excinfo.value) == "user does not belong to the org"


def test_delete_organization_users_wrong_org_v1(orguser):
    """a failing test, orguser dne"""
    request = mock_request(orguser)
    payload = DeleteOrgUserPayload(email="email-dne")

    with pytest.raises(HttpError) as excinfo:
        delete_organization_users_v1(request, payload)
    assert str(excinfo.value) == "user does not belong to the org"


def test_delete_organization_users_cant_delete_self(orguser):
    """a failing test, orguser dne"""
    request = mock_request(orguser)
    request.orguser = orguser
    payload = DeleteOrgUserPayload(email=orguser.user.email)

    with pytest.raises(HttpError) as excinfo:
        delete_organization_users(request, payload)
    assert str(excinfo.value) == "user cannot delete themselves"


def test_delete_organization_users_cant_delete_self_v1(orguser):
    """a failing test, orguser dne"""
    request = mock_request(orguser)
    request.orguser = orguser
    payload = DeleteOrgUserPayload(email=orguser.user.email)

    with pytest.raises(HttpError) as excinfo:
        delete_organization_users_v1(request, payload)
    assert str(excinfo.value) == "user cannot delete themselves"


def test_delete_organization_users_cant_delete_higher_role(orguser):
    """a failing test, orguser not authorized"""
    orguser.role = OrgUserRole.PIPELINE_MANAGER
    request = mock_request(orguser)
    payload = DeleteOrgUserPayload(email="useremail")
    user = User.objects.create(email=payload.email, username=payload.email)
    OrgUser.objects.create(org=orguser.org, user=user, role=OrgUserRole.ACCOUNT_MANAGER)

    with pytest.raises(HttpError) as excinfo:
        delete_organization_users(request, payload)
    assert str(excinfo.value) == "cannot delete user having higher role"


def test_delete_organization_users_cant_delete_higher_role_v1(orguser):
    """a failing test, orguser not authorized"""
    request = mock_request(orguser)
    payload = DeleteOrgUserPayload(email="useremail")
    user = User.objects.create(email=payload.email, username=payload.email)
    OrgUser.objects.create(
        org=orguser.org,
        user=user,
        new_role=Role.objects.filter(slug=SUPER_ADMIN_ROLE).first(),
    )

    with pytest.raises(HttpError) as excinfo:
        delete_organization_users_v1(request, payload)
    assert str(excinfo.value) == "cannot delete user having higher role"


def test_delete_organization_users_success(orguser):
    """a passing test"""
    request = mock_request(orguser)
    payload = DeleteOrgUserPayload(email="useremail")
    user = User.objects.create(email=payload.email, username=payload.email)
    OrgUser.objects.create(org=orguser.org, user=user)
    assert OrgUser.objects.filter(org=orguser.org, user__email=payload.email).count() == 1
    delete_organization_users(request, payload)
    assert OrgUser.objects.filter(org=orguser.org, user__email=payload.email).count() == 0
    user.delete()


def test_delete_organization_users_success_v1(orguser):
    """a passing test"""
    request = mock_request(orguser)
    payload = DeleteOrgUserPayload(email="useremail")
    user = User.objects.create(email=payload.email, username=payload.email)
    # can only delete roles below the requestor orguser
    OrgUser.objects.create(
        org=orguser.org,
        user=user,
        new_role=Role.objects.filter(slug=GUEST_ROLE).first(),
    )
    assert OrgUser.objects.filter(org=orguser.org, user__email=payload.email).count() == 1
    delete_organization_users_v1(request, payload)
    assert OrgUser.objects.filter(org=orguser.org, user__email=payload.email).count() == 0
    user.delete()


# ================================================================================
def test_put_organization_user_self(orguser):
    """a success test"""
    request = mock_request(orguser)

    new_active_status = not orguser.user.is_active
    payload = OrgUserUpdate(
        toupdate_email="unused-param",
        email="newemail",
        active=new_active_status,
    )

    response = put_organization_user_self(request, payload)

    assert response.email == "newemail"
    assert response.active is new_active_status


def test_put_organization_user_self_v1(orguser):
    """a success test"""
    request = mock_request(orguser)

    new_active_status = not orguser.user.is_active
    payload = OrgUserUpdatev1(
        toupdate_email="unused-param",
        email="newemail",
        active=new_active_status,
    )

    response = put_organization_user_self_v1(request, payload)

    assert response.email == "newemail"
    assert response.active is new_active_status


# ================================================================================
def test_put_organization_user_not_authorized(orguser, nonadminorguser):
    """a failing test, requestor cannot update another user"""
    request = mock_request(nonadminorguser)

    payload = OrgUserUpdate(
        toupdate_email=orguser.user.email,
        email="newemail",
    )

    with pytest.raises(HttpError) as excinfo:
        put_organization_user(request, payload)
    assert str(excinfo.value) == "unauthorized"


def test_put_organization_user_v1_not_authorized(orguser, nonadminorguser):
    """a failing test, requestor cannot update another user"""
    request = mock_request(nonadminorguser)

    payload = OrgUserUpdatev1(
        toupdate_email=orguser.user.email,
        email="newemail",
    )

    with pytest.raises(HttpError) as excinfo:
        put_organization_user_v1(request, payload)
    assert str(excinfo.value) == "unauthorized"


def test_put_organization_user(orguser, nonadminorguser):
    """a success test, requestor updates another user"""
    request = mock_request(orguser)

    payload = OrgUserUpdate(
        toupdate_email=nonadminorguser.user.email,
        email="newemail",
    )

    response = put_organization_user(request, payload)
    assert response.email == payload.email


def test_put_organization_user_v1(orguser, nonadminorguser):
    """a success test, requestor updates another user"""
    request = mock_request(orguser)

    payload = OrgUserUpdatev1(
        toupdate_email=nonadminorguser.user.email,
        email="newemail",
    )

    response = put_organization_user_v1(request, payload)
    assert response.email == payload.email


# ================================================================================
def test_post_transfer_ownership_only_account_owner(authuser, org_with_workspace):
    """only an account owner can transfer account ownership"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org_with_workspace,
        new_role=Role.objects.filter(slug=PIPELINE_MANAGER_ROLE).first(),
    )
    orguser.role = OrgUserRole.PIPELINE_MANAGER
    request = mock_request(orguser)
    payload = OrgUserNewOwner(new_owner_email="new-email")
    with pytest.raises(HttpError) as excinfo:
        post_transfer_ownership(request, payload)
    assert str(excinfo.value) == "unauthorized"


def test_post_transfer_ownership_no_such_user(authuser, org_with_workspace):
    """only an account owner can transfer account ownership"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org_with_workspace,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    orguser.role = OrgUserRole.ACCOUNT_MANAGER
    request = mock_request(orguser)
    payload = OrgUserNewOwner(new_owner_email="new-email")
    with pytest.raises(HttpError) as excinfo:
        post_transfer_ownership(request, payload)
    assert str(excinfo.value) == "could not find user having this email address in this org"


def test_post_transfer_ownership_not_pipeline_mgr(authuser, org_with_workspace):
    """only an account owner can transfer account ownership"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org_with_workspace,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    orguser.role = OrgUserRole.ACCOUNT_MANAGER
    request = mock_request(orguser)
    new_owner_authuser = User.objects.create(username="new-owner", email="new-owner")
    new_owner = OrgUser.objects.create(user=new_owner_authuser, org=org_with_workspace)
    new_owner.role = OrgUserRole.REPORT_VIEWER
    payload = OrgUserNewOwner(new_owner_email="new-owner")
    with pytest.raises(HttpError) as excinfo:
        post_transfer_ownership(request, payload)
    assert str(excinfo.value) == "can only promote pipeline managers"


@patch(
    "ddpui.core.orguserfunctions.transaction.atomic",
    Mock(side_effect=Exception("db error")),
)
def test_post_transfer_ownership_db_error(
    authuser,
    org_with_workspace,
):
    """only an account owner can transfer account ownership"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org_with_workspace,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    orguser.role = OrgUserRole.ACCOUNT_MANAGER
    request = mock_request(orguser)
    new_owner_authuser = User.objects.create(username="new-owner", email="new-owner")
    new_owner = OrgUser.objects.create(user=new_owner_authuser, org=org_with_workspace)
    new_owner.role = OrgUserRole.PIPELINE_MANAGER
    new_owner.save()
    payload = OrgUserNewOwner(new_owner_email="new-owner")
    with pytest.raises(HttpError) as excinfo:
        post_transfer_ownership(request, payload)
    assert str(excinfo.value) == "failed to transfer ownership"


# ================================================================================
def test_post_organization_warehouse_unknownwtype(orguser):
    """a failing test, unrecognized warehouse type"""
    request = mock_request(orguser)
    payload = OrgWarehouseSchema(
        wtype="unknown",
        name="warehousename",
        destinationDefId="destinationDefId",
        airbyteConfig={},
    )

    with pytest.raises(HttpError) as excinfo:
        post_organization_warehouse(request, payload)

    assert str(excinfo.value) == "unrecognized warehouse type unknown"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    create_destination=Mock(
        return_value={"destinationId": "destination-id", "connectionConfiguration": {}}
    ),
    get_destination_definition=Mock(
        return_value={"dockerRepository": "docker-repo", "dockerImageTag": "0.0.0"}
    ),
)
@patch.multiple(
    "ddpui.utils.secretsmanager",
    save_warehouse_credentials=Mock(return_value="credentials_lookupkey"),
)
@patch.multiple(
    "ddpui.ddpairbyte.airbytehelpers",
    create_or_update_org_cli_block=Mock(return_value=((None, None), None)),
)
def test_post_organization_warehouse_bigquery(orguser):
    """success test, warehouse creation"""
    request = mock_request(orguser)
    payload = OrgWarehouseSchema(
        wtype="bigquery",
        name="bigquery",
        destinationDefId="destinationDefId",
        airbyteConfig={
            "credentials_json": "{}",
            "dataset_location": "us-central1",
            "transformation_priority": "batch",
        },
    )

    response = post_organization_warehouse(request, payload)

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
    request = mock_request(orguser)

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
    response = get_organizations_warehouses(request)
    assert "warehouses" in response
    assert len(response["warehouses"]) == 2
    assert response["warehouses"][0]["wtype"] == "postgres"
    assert response["warehouses"][0]["airbyte_destination"]["destination_id"] == "destination_id_1"
    assert response["warehouses"][1]["wtype"] == "postgres"
    assert response["warehouses"][1]["airbyte_destination"]["destination_id"] == "destination_id_2"
    warehouse1.delete()
    warehouse2.delete()


# ================================================================================
@patch("ddpui.utils.awsses.send_invite_user_email", Mock())
def test_post_organization_user_invite_no_org(orguser):
    """failing test, no org"""
    orguser.org = None
    request = mock_request(orguser)
    payload = InvitationSchema(invited_email="some-email", invited_role_slug="report_viewer")
    with pytest.raises(HttpError) as excinfo:
        post_organization_user_invite(request, payload)
    assert str(excinfo.value) == "create an organization first"


@patch("ddpui.utils.awsses.send_invite_user_email", Mock())
def test_post_organization_user_invite_v1_no_org(orguser):
    """failing test, no org"""
    orguser.org = None
    request = mock_request(orguser)
    payload = NewInvitationSchema(
        invited_email="some-email",
        invited_role_uuid=Role.objects.filter(slug=GUEST_ROLE).first().uuid,
    )
    with pytest.raises(HttpError) as excinfo:
        post_organization_user_invite_v1(request, payload)
    assert str(excinfo.value) == "create an organization first"


@patch("ddpui.utils.awsses.send_invite_user_email", Mock())
def test_post_organization_user_invite_nosuchrole(orguser):
    """failing test, no such role"""
    request = mock_request(orguser)
    payload = InvitationSchema(invited_email="some-email", invited_role_slug="hot_stepper")
    with pytest.raises(HttpError) as excinfo:
        post_organization_user_invite(request, payload)
    assert str(excinfo.value) == "Invalid role"


@patch("ddpui.utils.awsses.send_invite_user_email", Mock())
def test_post_organization_user_invite_v1_nosuchrole(orguser):
    """failing test, no such role"""
    request = mock_request(orguser)
    payload = NewInvitationSchema(invited_email="some-email", invited_role_uuid=uuid.uuid4())
    with pytest.raises(HttpError) as excinfo:
        post_organization_user_invite_v1(request, payload)
    assert str(excinfo.value) == "Invalid role"


@patch("ddpui.utils.awsses.send_invite_user_email", Mock())
def test_post_organization_user_invite_insufficientrole(orguser):
    """failing test, no such role"""
    orguser.role = 1
    request = mock_request(orguser)
    payload = InvitationSchema(invited_email="some-email", invited_role_slug="pipeline_manager")
    with pytest.raises(HttpError) as excinfo:
        post_organization_user_invite(request, payload)
    assert str(excinfo.value) == "Insufficient permissions for this operation"


@patch("ddpui.utils.awsses.send_invite_user_email", Mock())
def test_post_organization_user_invite_v1_insufficientrole(orguser):
    """failing test, no such role"""
    request = mock_request(orguser)
    payload = NewInvitationSchema(
        invited_email="some-email",
        invited_role_uuid=Role.objects.filter(slug=SUPER_ADMIN_ROLE).first().uuid,
    )
    with pytest.raises(HttpError) as excinfo:
        post_organization_user_invite_v1(request, payload)
    assert str(excinfo.value) == "Insufficient permissions for this operation"


@patch("ddpui.utils.awsses.send_invite_user_email", mock_awsses=Mock())
def test_post_organization_user_invite(mock_awsses, orguser):
    """success test, inviting a new user"""
    payload = InvitationSchema(
        invited_email="inivted_email",
        invited_role_slug="report_viewer",
        invited_by=None,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
    )

    request = mock_request(orguser)

    assert (
        Invitation.objects.filter(invited_email=payload.invited_email, invited_by=orguser).count()
        == 0
    )
    response = post_organization_user_invite(request, payload)
    assert (
        Invitation.objects.filter(invited_email=payload.invited_email, invited_by=orguser).count()
        == 1
    )

    assert response.invited_by.email == orguser.user.email
    assert response.invited_by.role == orguser.role
    assert response.invited_role == payload.invited_role
    assert response.invited_on == payload.invited_on
    assert response.invite_code == payload.invite_code
    mock_awsses.assert_called_once()


@patch("ddpui.utils.awsses.send_invite_user_email", mock_awsses=Mock())
def test_post_organization_user_invite_v1(mock_awsses, orguser):
    """success test, inviting a new user"""
    payload = NewInvitationSchema(
        invited_email="inivted_email",
        invited_role_uuid=Role.objects.filter(slug=GUEST_ROLE).first().uuid,
    )

    request = mock_request(orguser)

    assert (
        Invitation.objects.filter(invited_email=payload.invited_email, invited_by=orguser).count()
        == 0
    )
    post_organization_user_invite_v1(request, payload)
    invitation = Invitation.objects.filter(
        invited_email=payload.invited_email, invited_by=orguser
    ).first()

    assert invitation.invited_email == payload.invited_email
    assert invitation.invited_by.user.email == orguser.user.email
    assert invitation.invited_new_role.slug == GUEST_ROLE
    assert invitation.invite_code is not None
    assert invitation.invited_on is not None

    mock_awsses.assert_called_once()


@patch("ddpui.utils.awsses.send_invite_user_email", mock_awsses=Mock())
def test_post_organization_user_invite_multiple_open_invites(mock_awsses, orguser):
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

    request = mock_request(orguser)

    assert (
        Invitation.objects.filter(invited_email=payload.invited_email, invited_by=orguser).count()
        == 0
    )
    response = post_organization_user_invite(request, payload)
    assert (
        Invitation.objects.filter(invited_email=payload.invited_email, invited_by=orguser).count()
        == 1
    )

    assert response.invited_by.email == orguser.user.email
    assert response.invited_by.role == orguser.role
    assert response.invited_role == payload.invited_role
    assert response.invited_on == payload.invited_on
    assert response.invite_code == payload.invite_code
    mock_awsses.assert_called_once()


@patch("ddpui.utils.awsses.send_invite_user_email", mock_awsses=Mock())
def test_post_organization_user_invite_v1_multiple_open_invites(mock_awsses, orguser):
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
    payload = NewInvitationSchema(
        invited_email="inivted_email",
        invited_role_uuid=Role.objects.filter(slug=GUEST_ROLE).first().uuid,
    )

    request = mock_request(orguser)

    assert (
        Invitation.objects.filter(invited_email=payload.invited_email, invited_by=orguser).count()
        == 0
    )
    post_organization_user_invite_v1(request, payload)
    invitation = Invitation.objects.filter(
        invited_email=payload.invited_email, invited_by=orguser
    ).first()

    assert invitation is not None
    assert invitation.invited_email == payload.invited_email
    assert invitation.invited_by.user.email == orguser.user.email
    assert invitation.invited_new_role.slug == GUEST_ROLE
    assert invitation.invite_code is not None
    assert invitation.invited_on is not None

    mock_awsses.assert_called_once()


@patch("ddpui.utils.awsses.send_invite_user_email", mock_awsses=Mock())
def test_post_organization_user_invite_lowercase_email(mock_awsses, orguser: OrgUser):
    """success test, inviting a new user"""
    payload = InvitationSchema(
        invited_email="INVITED_EMAIL",
        invited_role_slug="report_viewer",
        invited_by=None,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
    )

    request = mock_request(orguser)

    assert (
        Invitation.objects.filter(
            invited_email__iexact=payload.invited_email, invited_by=orguser
        ).count()
        == 0
    )
    response = post_organization_user_invite(request, payload)
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
    mock_awsses.assert_called_once()


@patch("ddpui.utils.awsses.send_invite_user_email", mock_awsses=Mock())
def test_post_organization_user_invite_v1_lowercase_email(mock_awsses, orguser: OrgUser):
    """success test, inviting a new user"""
    payload = NewInvitationSchema(
        invited_email="INVITED_EMAIL",
        invited_role_uuid=Role.objects.filter(slug=GUEST_ROLE).first().uuid,
    )

    request = mock_request(orguser)

    assert (
        Invitation.objects.filter(
            invited_email__iexact=payload.invited_email, invited_by=orguser
        ).count()
        == 0
    )
    post_organization_user_invite_v1(request, payload)
    invitation = Invitation.objects.filter(
        invited_email=payload.invited_email.lower(), invited_by=orguser
    ).first()

    assert invitation is not None
    assert invitation.invited_email == payload.invited_email.lower()
    assert invitation.invited_by.user.email == orguser.user.email
    assert invitation.invited_new_role.slug == GUEST_ROLE
    assert invitation.invite_code is not None
    assert invitation.invited_on is not None
    mock_awsses.assert_called_once()


@patch("ddpui.utils.awsses.send_youve_been_added_email", mock_awsses=Mock())
def test_post_organization_user_invite_user_exists(mock_awsses, orguser: OrgUser):
    """success test, inviting an existing user"""
    user = User.objects.create(email="existinguser", username="existinguser")
    assert OrgUser.objects.filter(user=user).count() == 0

    payload = InvitationSchema(
        invited_email="existinguser",
        invited_role_slug="report_viewer",
        invited_by=None,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
    )

    request = mock_request(orguser)

    response = post_organization_user_invite(request, payload)
    mock_awsses.assert_called_once()

    assert OrgUser.objects.filter(user=user).count() == 1
    assert OrgUser.objects.filter(user=user, org=orguser.org).count() == 1
    assert response.invited_role == payload.invited_role


@patch("ddpui.utils.awsses.send_youve_been_added_email", mock_awsses=Mock())
def test_post_organization_user_invite_v1_user_exists(mock_awsses, orguser: OrgUser):
    """success test, inviting an existing user"""
    user = User.objects.create(email="existinguser", username="existinguser")
    assert OrgUser.objects.filter(user=user).count() == 0

    guest_role = Role.objects.filter(slug=GUEST_ROLE).first()
    payload = NewInvitationSchema(
        invited_email="existinguser",
        invited_role_uuid=guest_role.uuid,
    )

    request = mock_request(orguser)

    post_organization_user_invite_v1(request, payload)
    mock_awsses.assert_called_once()

    assert OrgUser.objects.filter(user=user, new_role=guest_role).count() == 1
    assert OrgUser.objects.filter(user=user, org=orguser.org, new_role=guest_role).count() == 1


# ================================================================================


def test_post_organization_user_accept_invite_v1_fail(orguser):
    """failing test, invalid invite code"""
    request = mock_request(orguser)
    payload = AcceptInvitationSchema(invite_code="invalid-invite_code", password="password")

    with pytest.raises(HttpError) as excinfo:
        post_organization_user_accept_invite_v1(request, payload)

    assert str(excinfo.value) == "invalid invite code"


def test_post_organization_user_accept_invite_v1(orguser):
    """success test, accepting an invitation"""
    request = mock_request(orguser)
    payload = AcceptInvitationSchema(invite_code="invite_code", password="password")

    guest_role = Role.objects.filter(slug=GUEST_ROLE).first()
    Invitation.objects.create(
        invited_email="invited_email",
        invited_by=orguser,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
        invited_new_role=guest_role,
    )

    assert (
        OrgUser.objects.filter(
            user__email="invited_email",
        ).count()
        == 0
    )
    response = post_organization_user_accept_invite_v1(request, payload)
    assert response.email == "invited_email"
    assert OrgUser.objects.filter(user__email="invited_email", new_role=guest_role).count() == 1
    assert UserAttributes.objects.filter(user__email="invited_email").exists()


def test_post_organization_user_accept_invite_v1_lowercase_email(orguser):
    """success test, accepting an invitation"""
    request = mock_request()
    payload = AcceptInvitationSchema(invite_code="invite_code", password="password")

    guest_role = Role.objects.filter(slug=GUEST_ROLE).first()
    Invitation.objects.create(
        invited_email="INVITED_EMAIL",
        invited_by=orguser,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
        invited_new_role=guest_role,
    )

    assert (
        OrgUser.objects.filter(
            user__email__iexact="invited_email",
        ).count()
        == 0
    )
    response = post_organization_user_accept_invite_v1(request, payload)
    assert response.email == "invited_email"
    assert OrgUser.objects.filter(user__email="invited_email", new_role=guest_role).count() == 1


def test_post_organization_user_accept_invite_v1_firstaccount_fail(orguser):
    """failing test, invalid invite code"""
    request = mock_request(orguser)
    payload = AcceptInvitationSchema(
        invite_code="invite_code",
    )
    guest_role = Role.objects.filter(slug=GUEST_ROLE).first()
    Invitation.objects.create(
        invited_email="invited_email",
        invited_by=orguser,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
        invited_new_role=guest_role,
    )

    with pytest.raises(HttpError) as excinfo:
        post_organization_user_accept_invite_v1(request, payload)

    assert str(excinfo.value) == "password is required"


def test_post_organization_user_accept_invite_v1_secondaccount(orguser):
    """success test, accepting an invitation"""
    request = mock_request(orguser)
    payload = AcceptInvitationSchema(invite_code="invite_code")

    guest_role = Role.objects.filter(slug=GUEST_ROLE).first()
    Invitation.objects.create(
        invited_email="invited_email",
        invited_by=orguser,
        invited_on=timezone.as_ist(datetime.now()),
        invite_code="invite_code",
        invited_new_role=guest_role,
    )

    authuser = User.objects.create_user(
        username="invited_email", email="invited_email", password="oldpassword"
    )
    assert User.objects.filter(email="invited_email").count() == 1
    anotherorg = Org.objects.create(name="anotherorg", slug="anotherorg")
    OrgUser.objects.create(user=authuser, org=anotherorg, new_role=guest_role)

    assert (
        OrgUser.objects.filter(
            user__email="invited_email",
        ).count()
        == 1
    )
    response = post_organization_user_accept_invite_v1(request, payload)
    assert response.email == "invited_email"
    assert OrgUser.objects.filter(user__email="invited_email", new_role=guest_role).count() == 2
    assert (
        OrgUser.objects.filter(
            user__email="invited_email", org__slug=orguser.org.slug, new_role=guest_role
        ).count()
        == 1
    )
    assert (
        OrgUser.objects.filter(
            user__email="invited_email", org__slug=anotherorg.slug, new_role=guest_role
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
    "ddpui.utils.awsses",
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


@patch.multiple("ddpui.utils.awsses", send_password_reset_email=Mock(return_value=1))
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
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        get_invitations(request)
    assert str(excinfo.value) == "create an organization first"


def test_get_invitations_v1_no_org(orguser):
    """failure - no org"""
    orguser.org = None
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        get_invitations_v1(request)
    assert str(excinfo.value) == "create an organization first"


def test_get_invitations(orguser):
    """success - return invitations"""
    request = mock_request(orguser)
    Invitation.objects.create(
        invited_by=orguser,
        invited_email="invited-email",
        invited_role=1,
        invited_on=timezone.as_ist(datetime(2023, 1, 1, 10, 0, 0)),
    )
    response = get_invitations(request)
    assert len(response) == 1
    assert response[0]["invited_email"] == "invited-email"
    assert response[0]["invited_role_slug"] == "report_viewer"
    assert response[0]["invited_role"] == 1
    assert response[0]["invited_on"] == datetime(2023, 1, 1, 4, 30, 0, tzinfo=timezone.pytz.utc)


def test_get_invitations_v1(orguser):
    """success - return invitations"""
    request = mock_request(orguser)
    guest_role = Role.objects.filter(slug=GUEST_ROLE).first()
    Invitation.objects.create(
        invited_by=orguser,
        invited_email="invited-email",
        invited_role=1,
        invited_on=timezone.as_ist(datetime(2023, 1, 1, 10, 0, 0)),
        invited_new_role=guest_role,
    )
    response = get_invitations_v1(request)
    assert len(response) == 1
    assert response[0]["invited_email"] == "invited-email"
    assert response[0]["invited_on"] == datetime(2023, 1, 1, 4, 30, 0, tzinfo=timezone.pytz.utc)
    assert response[0]["invited_role"] == {
        "uuid": guest_role.uuid,
        "name": guest_role.name,
    }


# ================================================================================
def test_post_resend_invitation_no_org(orguser):
    """failure - no org"""
    orguser.org = None
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        post_resend_invitation(request, 1)
    assert str(excinfo.value) == "create an organization first"


@patch("ddpui.utils.awsses.send_invite_user_email", mock_awsses=Mock())
def test_post_resend_invitation(mock_awsses: Mock, orguser):
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
    request = mock_request(orguser=orguser)
    post_resend_invitation(request, invitation.id)
    mock_awsses.assert_called_once_with("email", orguser.user.email, invite_url)
    invitation.refresh_from_db()
    assert invitation.invited_on > original_invited_on


# ================================================================================
def test_delete_invitation_no_org(orguser):
    """failure - no org"""
    orguser.org = None
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        delete_invitation(request, 1)
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
    request = mock_request(orguser=orguser)
    assert Invitation.objects.filter(id=invitation.id).exists()
    delete_invitation(request, invitation.id)
    assert not Invitation.objects.filter(id=invitation.id).exists()


def test_post_organization_accept_tnc_no_org(orguser: OrgUser, org_without_workspace: Org):
    """tests post_organization_accept_tnc"""
    orguser.org = None

    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        post_organization_accept_tnc(request)
    assert str(excinfo.value) == "create an organization first"


def test_post_organization_accept_tnc_cannot(orguser: OrgUser, org_without_workspace: Org):
    """tests post_organization_accept_tnc"""
    orguser.org = org_without_workspace
    orguser.save()

    userattributes = UserAttributes.objects.filter(user=orguser.user).first()
    if userattributes is None:
        userattributes = UserAttributes.objects.create(user=orguser.user)

    userattributes.is_consultant = True
    userattributes.save()

    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        post_organization_accept_tnc(request)
    assert str(excinfo.value) == "user cannot accept tnc"


def test_post_organization_accept_tnc_already_accepted(
    orguser: OrgUser, org_without_workspace: Org
):
    """tests post_organization_accept_tnc"""
    orguser.org = org_without_workspace
    orguser.save()

    OrgTnC.objects.create(org=orguser.org, tnc_accepted_by=orguser, tnc_accepted_on=datetime.now())

    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        post_organization_accept_tnc(request)
    assert str(excinfo.value) == "tnc already accepted"


def test_post_organization_accept_tnc(orguser: OrgUser, org_without_workspace: Org):
    """tests post_organization_accept_tnc"""
    orguser.org = org_without_workspace
    orguser.save()

    assert OrgTnC.objects.filter(org=org_without_workspace).count() == 0

    request = mock_request(orguser)

    currentuserv2response = get_current_user_v2(request)
    assert currentuserv2response[0].org.tnc_accepted is None

    response = post_organization_accept_tnc(request)
    assert response["success"] == 1

    assert OrgTnC.objects.filter(org=org_without_workspace).count() == 1

    currentuserv2response = get_current_user_v2(request)
    assert currentuserv2response[0].org.tnc_accepted is True


def test_get_organization_wren_not_found(orguser):
    """failure - org_wren not found"""
    request = mock_request(orguser)

    # Mocking OrgWren.objects.filter to return an empty queryset
    mock_queryset = Mock()
    mock_queryset.first.return_value = None
    with patch("ddpui.models.OrgWren.objects.filter", return_value=mock_queryset):
        with pytest.raises(HttpError) as excinfo:
            get_organization_wren(request)
        assert str(excinfo.value) == "org_wren not found"


def test_get_organization_wren_success(orguser):
    """success - org_wren found"""
    request = mock_request(orguser)

    # Mocking OrgWren.objects.filter to return a mock OrgWren object
    mock_org_wren = Mock()
    mock_org_wren.wren_url = "http://example.com/wren"
    mock_queryset = Mock()
    mock_queryset.first.return_value = mock_org_wren
    with patch("ddpui.models.OrgWren.objects.filter", return_value=mock_queryset):
        response = get_organization_wren(request)
        assert response == {"wren_url": "http://example.com/wren"}
