from unittest.mock import Mock
from faker import Faker
import pytest
from django.contrib.auth.models import User
from ddpui.auth import (
    PlatformAdmin,
    Token,
    AdminUser,
    UNAUTHORIZED,
    HttpError,
    OrgUser,
    OrgUserRole,
    authenticate_org_user,
    AnyOrgUser,
    CanManagePipelines,
    CanManageUsers,
    FullAccess,
)
from ddpui.models.org import Org

pytestmark = pytest.mark.django_db


def create_user():
    faker = Faker()
    return User.objects.create(
        username=faker.user_name(), email=faker.email(), password=faker.password()
    )


@pytest.fixture
def user():
    temp_user = create_user()
    yield temp_user
    temp_user.delete()


# @pytest.fixture
def token(user: User):
    temp_token = Token.objects.create(key="ttt", user=user)
    return temp_token


@pytest.fixture
def admin_user(user: User):
    temp_admin_user = AdminUser.objects.create(user=user)
    yield temp_admin_user
    temp_admin_user.delete()


@pytest.fixture
def org():
    temp_org = Org.objects.create(name="temp-org")
    yield temp_org
    temp_org.delete()


@pytest.fixture
def org_user_accountmanager(user: User, org: Org):
    temp_org_user = OrgUser.objects.create(
        user=user, org=org, role=OrgUserRole.ACCOUNT_MANAGER
    )
    yield temp_org_user
    temp_org_user.delete()


@pytest.fixture
def org_user_pipelinemanager(user: User, org: Org):
    temp_org_user = OrgUser.objects.create(
        user=user, org=org, role=OrgUserRole.PIPELINE_MANAGER
    )
    yield temp_org_user
    temp_org_user.delete()


@pytest.fixture
def org_user_reportviewer(user: User, org: Org):
    temp_org_user = OrgUser.objects.create(
        user=user, org=org, role=OrgUserRole.REPORT_VIEWER
    )
    yield temp_org_user
    temp_org_user.delete()


# ====================================================================================
def test_platformadmin_authenticate_no_tokenrecord():
    obj = PlatformAdmin()
    request = Mock()
    with pytest.raises(HttpError) as excinfo:
        obj.authenticate(request, "key-dne")
    assert str(excinfo.value) == UNAUTHORIZED


def test_platformadmin_authenticate_no_adminuser(user: User):
    obj = PlatformAdmin()
    request = Mock()
    test_token = token(user)
    with pytest.raises(HttpError) as excinfo:
        obj.authenticate(request, test_token.key)
    assert str(excinfo.value) == UNAUTHORIZED


def test_platformadmin_authenticate(admin_user: AdminUser):
    obj = PlatformAdmin()
    request = Mock()
    test_token = token(admin_user.user)
    trecord = obj.authenticate(request, test_token.key)
    assert trecord == test_token
    test_token.delete()


# ====================================================================================
def test_authenticate_org_user_require_org(org_user_accountmanager: OrgUser):
    test_token = token(org_user_accountmanager.user)
    request = Mock()
    allowed_roles = [org_user_accountmanager.role]
    org_user_accountmanager.org = None
    org_user_accountmanager.save()
    require_org = True

    with pytest.raises(HttpError) as excinfo:
        authenticate_org_user(request, test_token.key, allowed_roles, require_org)
    assert str(excinfo.value) == "register an organization first"


def test_authenticate_org_user_unauthorized_no_tokenrecord(
    org_user_accountmanager: OrgUser,
):
    request = Mock()
    allowed_roles = [org_user_accountmanager.role]
    org_user_accountmanager.org = None
    org_user_accountmanager.save()
    require_org = True

    with pytest.raises(HttpError) as excinfo:
        authenticate_org_user(request, "key dne", allowed_roles, require_org)
    assert str(excinfo.value) == UNAUTHORIZED


def test_authenticate_org_user_unauthorized_no_orguser():
    another_user = create_user()
    test_token = token(another_user)
    request = Mock()
    allowed_roles = []
    require_org = True

    with pytest.raises(HttpError) as excinfo:
        authenticate_org_user(request, test_token.key, allowed_roles, require_org)
    assert str(excinfo.value) == UNAUTHORIZED


def test_authenticate_org_user_unauthorized_wrong_role(
    org_user_accountmanager: OrgUser,
):
    test_token = token(org_user_accountmanager.user)
    request = Mock()
    allowed_roles = [OrgUserRole.REPORT_VIEWER]
    require_org = True

    with pytest.raises(HttpError) as excinfo:
        authenticate_org_user(request, test_token.key, allowed_roles, require_org)
    assert str(excinfo.value) == UNAUTHORIZED


def test_authenticate_org_user_success(org_user_accountmanager: OrgUser):
    test_token = token(org_user_accountmanager.user)
    request = Mock()
    allowed_roles = [org_user_accountmanager.role]
    require_org = True

    response = authenticate_org_user(
        request, test_token.key, allowed_roles, require_org
    )
    assert response == request
    assert response.orguser == org_user_accountmanager


# ====================================================================================
def test_anyorguser_accountmanager_success(org_user_accountmanager: OrgUser):
    aou = AnyOrgUser()
    request = Mock
    temp_token = Token.objects.create(key="ttt", user=org_user_accountmanager.user)
    response = aou.authenticate(request, temp_token)
    assert response.orguser == org_user_accountmanager
    temp_token.delete()


def test_anyorguser_pipelinemanager_success(org_user_pipelinemanager: OrgUser):
    aou = AnyOrgUser()
    request = Mock
    temp_token = Token.objects.create(key="ttt", user=org_user_pipelinemanager.user)
    response = aou.authenticate(request, temp_token)
    assert response.orguser == org_user_pipelinemanager
    temp_token.delete()


def test_anyorguser_reportviewer_success(org_user_reportviewer: OrgUser):
    aou = AnyOrgUser()
    request = Mock
    temp_token = Token.objects.create(key="ttt", user=org_user_reportviewer.user)
    response = aou.authenticate(request, temp_token)
    assert response.orguser == org_user_reportviewer
    temp_token.delete()


# ====================================================================================
def test_canmanagepipelines_accountmanager_success(org_user_accountmanager: OrgUser):
    aou = CanManagePipelines()
    request = Mock
    temp_token = Token.objects.create(key="ttt", user=org_user_accountmanager.user)
    response = aou.authenticate(request, temp_token)
    assert response.orguser == org_user_accountmanager
    temp_token.delete()


def test_canmanagepipelines_pipelinemanager_success(org_user_pipelinemanager: OrgUser):
    aou = CanManagePipelines()
    request = Mock
    temp_token = Token.objects.create(key="ttt", user=org_user_pipelinemanager.user)
    response = aou.authenticate(request, temp_token)
    assert response.orguser == org_user_pipelinemanager
    temp_token.delete()


def test_canmanagepipelines_reportviewer_success(org_user_reportviewer: OrgUser):
    aou = CanManagePipelines()
    request = Mock
    temp_token = Token.objects.create(key="ttt", user=org_user_reportviewer.user)
    with pytest.raises(HttpError) as excinfo:
        aou.authenticate(request, temp_token)
    assert str(excinfo.value) == UNAUTHORIZED


# ====================================================================================
def test_canmanageusers_accountmanager_success(org_user_accountmanager: OrgUser):
    aou = CanManageUsers()
    request = Mock
    temp_token = Token.objects.create(key="ttt", user=org_user_accountmanager.user)
    response = aou.authenticate(request, temp_token)
    assert response.orguser == org_user_accountmanager
    temp_token.delete()


def test_canmanageusers_pipelinemanager_success(org_user_pipelinemanager: OrgUser):
    aou = CanManageUsers()
    request = Mock
    temp_token = Token.objects.create(key="ttt", user=org_user_pipelinemanager.user)
    response = aou.authenticate(request, temp_token)
    assert response.orguser == org_user_pipelinemanager
    temp_token.delete()


def test_canmanageusers_reportviewer_success(org_user_reportviewer: OrgUser):
    aou = CanManageUsers()
    request = Mock
    temp_token = Token.objects.create(key="ttt", user=org_user_reportviewer.user)
    with pytest.raises(HttpError) as excinfo:
        aou.authenticate(request, temp_token)
    assert str(excinfo.value) == UNAUTHORIZED


# ====================================================================================
def test_fullaccess_accountmanager_success(org_user_accountmanager: OrgUser):
    aou = FullAccess()
    request = Mock
    temp_token = Token.objects.create(key="ttt", user=org_user_accountmanager.user)
    response = aou.authenticate(request, temp_token)
    assert response.orguser == org_user_accountmanager
    temp_token.delete()


def test_fullaccess_pipelinemanager_success(org_user_pipelinemanager: OrgUser):
    aou = FullAccess()
    request = Mock
    temp_token = Token.objects.create(key="ttt", user=org_user_pipelinemanager.user)
    with pytest.raises(HttpError) as excinfo:
        aou.authenticate(request, temp_token)
    assert str(excinfo.value) == UNAUTHORIZED


def test_fullaccess_reportviewer_success(org_user_reportviewer: OrgUser):
    aou = FullAccess()
    request = Mock
    temp_token = Token.objects.create(key="ttt", user=org_user_reportviewer.user)
    with pytest.raises(HttpError) as excinfo:
        aou.authenticate(request, temp_token)
    assert str(excinfo.value) == UNAUTHORIZED
