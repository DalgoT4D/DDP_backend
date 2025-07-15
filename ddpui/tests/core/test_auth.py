import json
from unittest.mock import Mock, patch
from ninja.errors import HttpError
import pytest
from django.contrib.auth.models import User
from django.core.management import call_command
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.auth import CustomJwtAuthMiddleware, ACCOUNT_MANAGER_ROLE
from rest_framework_simplejwt.tokens import AccessToken

pytestmark = pytest.mark.django_db


@pytest.fixture(scope="session")
def seed_db(django_db_setup, django_db_blocker):
    with django_db_blocker.unblock():
        # Run the loaddata command to load the fixture
        call_command("loaddata", "001_roles.json")
        call_command("loaddata", "002_permissions.json")
        call_command("loaddata", "003_role_permissions.json")


@pytest.fixture
def mock_request():
    request = Mock()
    request.headers = {}
    return request


@pytest.fixture
def mock_user():
    user = User.objects.create_user(username="testuser", password="testpassword")
    yield user
    user.delete()


@pytest.fixture
def mock_role():
    """mocks a role"""


@pytest.fixture
def mock_org():
    org = Org.objects.create(name="Test Org", slug="test-org")
    yield org
    org.delete()


@pytest.fixture
def mock_org_user(mock_user, mock_org):
    org_user = OrgUser.objects.create(
        user=mock_user,
        org=mock_org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield org_user
    org_user.delete()


@patch("ddpui.auth.User.objects.filter")
@patch("ddpui.auth.OrgUser.objects.filter")
@patch("ddpui.auth.RedisClient.get_instance")
@patch("ddpui.auth.set_roles_and_permissions_in_redis")
def test_authenticate_success(
    mock_set_roles,
    mock_redis_client,
    mock_org_user_filter,
    mock_user_filter,
    mock_request,
    mock_user,
    mock_org_user,
    seed_db,
):
    """Test successful authentication with valid token and org header."""
    mock_user_filter.return_value.first.return_value = mock_user
    mock_org_user_filter.return_value.filter.return_value.select_related.return_value.first.return_value = (
        mock_org_user
    )
    mock_redis_client.return_value.get.return_value = json.dumps(
        {str(mock_org_user.new_role.id): ["perm1", "perm2"]}
    )
    mock_request.headers["x-dalgo-org"] = "test-org"
    token = str(AccessToken.for_user(mock_user))

    middleware = CustomJwtAuthMiddleware()
    result = middleware.authenticate(mock_request, token)

    assert result == mock_request
    assert result.user == mock_user
    assert result.orguser == mock_org_user
    assert result.permissions == ["perm1", "perm2"]


@patch("ddpui.auth.AccessToken")
def test_authenticate_invalid_token(mock_access_token, mock_request):
    """Test authentication with an invalid token."""
    mock_access_token.side_effect = Exception("Invalid token")
    middleware = CustomJwtAuthMiddleware()
    with pytest.raises(HttpError) as excinfo:
        middleware.authenticate(mock_request, "invalid-token")
    assert excinfo.value.status_code == 401
    assert str(excinfo.value) == "Invalid or expired token"


@patch("ddpui.auth.User.objects.filter")
@patch("ddpui.auth.OrgUser.objects.filter")
def test_authenticate_no_org(
    mock_org_user_filter, mock_user_filter, mock_request, mock_user, seed_db
):
    """Test authentication when user has no organization."""
    mock_user_filter.return_value.first.return_value = mock_user
    org_user_no_org = Mock(spec=OrgUser, org=None)
    mock_org_user_filter.return_value.select_related.return_value.first.return_value = (
        org_user_no_org
    )
    token = str(AccessToken.for_user(mock_user))

    middleware = CustomJwtAuthMiddleware()
    with pytest.raises(HttpError) as excinfo:
        middleware.authenticate(mock_request, token)
    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == "register an organization first"


@patch("ddpui.auth.User.objects.filter")
@patch("ddpui.auth.OrgUser.objects.filter")
@patch("ddpui.auth.RedisClient.get_instance")
@patch("ddpui.auth.set_roles_and_permissions_in_redis")
def test_authenticate_redis_cache_empty(
    mock_set_roles,
    mock_redis_client,
    mock_org_user_filter,
    mock_user_filter,
    mock_request,
    mock_user,
    mock_org_user,
    seed_db,
):
    """Test authentication when Redis cache is empty."""
    mock_user_filter.return_value.first.return_value = mock_user
    mock_org_user_filter.return_value.filter.return_value.select_related.return_value.first.return_value = (
        mock_org_user
    )
    mock_redis_client.return_value.get.return_value = None
    mock_set_roles.return_value = {str(mock_org_user.new_role.id): ["perm1"]}
    mock_request.headers["x-dalgo-org"] = "test-org"
    token = str(AccessToken.for_user(mock_user))

    middleware = CustomJwtAuthMiddleware()
    result = middleware.authenticate(mock_request, token)

    assert result.permissions == ["perm1"]
    mock_set_roles.assert_called_once()
