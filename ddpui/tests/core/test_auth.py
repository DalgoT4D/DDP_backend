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
    permissions_json = json.dumps({str(mock_org_user.new_role.id): ["perm1", "perm2"]})
    # call 1: JTI blacklist check → None (not blacklisted)
    # call 2: orguser_role_map, call 3: permissions_map
    mock_redis_client.return_value.get.side_effect = [None, permissions_json, permissions_json]
    mock_request.headers["x-dalgo-org"] = "test-org"
    token = str(AccessToken.for_user(mock_user))

    middleware = CustomJwtAuthMiddleware()
    result = middleware.authenticate(mock_request, token)

    assert result == mock_request
    assert result.user == mock_user
    assert result.orguser == mock_org_user
    assert result.permissions == ["perm1", "perm2"]


@patch("ddpui.auth.User.objects.filter")
@patch("ddpui.auth.OrgUser.objects.filter")
@patch("ddpui.auth.RedisClient.get_instance")
@patch("ddpui.auth.set_roles_and_permissions_in_redis")
def test_authenticate_blocks_deactivated_org(
    mock_set_roles,
    mock_redis_client,
    mock_org_user_filter,
    mock_user_filter,
    mock_request,
    mock_user,
    seed_db,
):
    """
    THE deactivation-enforcement test: a user whose org is deactivated is blocked with
    403 at permission-load, before any endpoint runs. If this regresses, deactivation
    silently does nothing.
    """
    deactivated_org = Org.objects.create(
        name="Deactivated Org", slug="deactivated-org", is_active=False
    )
    org_user = OrgUser.objects.create(
        user=mock_user,
        org=deactivated_org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    mock_user_filter.return_value.first.return_value = mock_user
    mock_org_user_filter.return_value.filter.return_value.select_related.return_value.first.return_value = (
        org_user
    )
    # first redis.get is the JTI blacklist check -> None (not blacklisted); the org-block
    # raises before any permission lookup, so a single None return covers the whole path
    mock_redis_client.return_value.get.return_value = None
    mock_request.headers["x-dalgo-org"] = "deactivated-org"
    token = str(AccessToken.for_user(mock_user))

    middleware = CustomJwtAuthMiddleware()
    with pytest.raises(HttpError) as excinfo:
        middleware.authenticate(mock_request, token)
    assert excinfo.value.status_code == 403
    assert str(excinfo.value) == "your organization has been deactivated"

    org_user.delete()
    deactivated_org.delete()


@patch("ddpui.auth.User.objects.filter")
@patch("ddpui.auth.OrgUser.objects.filter")
@patch("ddpui.auth.RedisClient.get_instance")
@patch("ddpui.auth.set_roles_and_permissions_in_redis")
def test_authenticate_allows_reactivated_org(
    mock_set_roles,
    mock_redis_client,
    mock_org_user_filter,
    mock_user_filter,
    mock_request,
    mock_user,
    seed_db,
):
    """
    Symmetry: reactivation restores access. An org toggled back to is_active=True
    authenticates normally, permissions loaded.
    """
    org = Org.objects.create(name="Reactivated Org", slug="reactivated-org", is_active=False)
    # ... admin reactivates it
    org.is_active = True
    org.save()
    org_user = OrgUser.objects.create(
        user=mock_user,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    mock_user_filter.return_value.first.return_value = mock_user
    mock_org_user_filter.return_value.filter.return_value.select_related.return_value.first.return_value = (
        org_user
    )
    # call 1 is the JTI blacklist check -> None (not blacklisted); later calls load perms
    permissions_json = json.dumps({str(org_user.new_role.id): ["perm1"]})
    mock_redis_client.return_value.get.side_effect = [None, permissions_json, permissions_json]
    mock_request.headers["x-dalgo-org"] = "reactivated-org"
    token = str(AccessToken.for_user(mock_user))

    middleware = CustomJwtAuthMiddleware()
    result = middleware.authenticate(mock_request, token)

    assert result == mock_request
    assert result.orguser == org_user
    assert result.permissions == ["perm1"]

    org_user.delete()
    org.delete()


@patch("ddpui.auth.User.objects.filter")
@patch("ddpui.auth.OrgUser.objects.filter")
@patch("ddpui.auth.RedisClient.get_instance")
@patch("ddpui.auth.set_roles_and_permissions_in_redis")
def test_authenticate_blocks_deactivated_orguser(
    mock_set_roles,
    mock_redis_client,
    mock_org_user_filter,
    mock_user_filter,
    mock_request,
    mock_user,
    seed_db,
):
    """
    THE per-org deactivation test (M4): a user deactivated in THIS org
    (OrgUser.is_active=False) is blocked with 403 at permission-load, even though the
    org itself is active. If this regresses, per-org "deactivate user" silently does
    nothing. Distinct message from the org-level block so the two are diagnosable.
    """
    active_org = Org.objects.create(name="Active Org", slug="active-org", is_active=True)
    deactivated_orguser = OrgUser.objects.create(
        user=mock_user,
        org=active_org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
        is_active=False,  # deactivated in THIS org only
    )
    mock_user_filter.return_value.first.return_value = mock_user
    mock_org_user_filter.return_value.filter.return_value.select_related.return_value.first.return_value = (
        deactivated_orguser
    )
    # first redis.get is the JTI blacklist check -> None (not blacklisted); the per-org
    # block raises before any permission lookup, so a single None covers the whole path
    mock_redis_client.return_value.get.return_value = None
    mock_request.headers["x-dalgo-org"] = "active-org"
    token = str(AccessToken.for_user(mock_user))

    middleware = CustomJwtAuthMiddleware()
    with pytest.raises(HttpError) as excinfo:
        middleware.authenticate(mock_request, token)
    assert excinfo.value.status_code == 403
    assert str(excinfo.value) == "your access to this organization has been deactivated"

    deactivated_orguser.delete()
    active_org.delete()


@patch("ddpui.auth.User.objects.filter")
@patch("ddpui.auth.OrgUser.objects.filter")
@patch("ddpui.auth.RedisClient.get_instance")
@patch("ddpui.auth.set_roles_and_permissions_in_redis")
def test_authenticate_allows_active_orguser(
    mock_set_roles,
    mock_redis_client,
    mock_org_user_filter,
    mock_user_filter,
    mock_request,
    mock_user,
    seed_db,
):
    """
    Risk guard: the per-org check must NEVER block an ACTIVE user. An OrgUser with
    is_active=True (the default, and what the backfill sets for every active user)
    authenticates normally. This is the test that proves the new check can't lock out
    someone who should have access.
    """
    org = Org.objects.create(name="Normal Org", slug="normal-org", is_active=True)
    active_orguser = OrgUser.objects.create(
        user=mock_user,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
        is_active=True,
    )
    mock_user_filter.return_value.first.return_value = mock_user
    mock_org_user_filter.return_value.filter.return_value.select_related.return_value.first.return_value = (
        active_orguser
    )
    # call 1 is the JTI blacklist check -> None (not blacklisted); later calls load perms
    permissions_json = json.dumps({str(active_orguser.new_role.id): ["perm1"]})
    mock_redis_client.return_value.get.side_effect = [None, permissions_json, permissions_json]
    mock_request.headers["x-dalgo-org"] = "normal-org"
    token = str(AccessToken.for_user(mock_user))

    middleware = CustomJwtAuthMiddleware()
    result = middleware.authenticate(mock_request, token)

    assert result == mock_request
    assert result.orguser == active_orguser
    assert result.permissions == ["perm1"]

    active_orguser.delete()
    org.delete()


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


@patch("ddpui.auth.RedisClient.get_instance")
def test_authenticate_blacklisted_token(mock_redis_client, mock_request, mock_user):
    """Test that a token whose JTI is blacklisted in Redis is rejected with 401."""
    mock_redis_client.return_value.get.return_value = "1"  # JTI is blacklisted
    token = str(AccessToken.for_user(mock_user))

    middleware = CustomJwtAuthMiddleware()
    with pytest.raises(HttpError) as excinfo:
        middleware.authenticate(mock_request, token)
    assert excinfo.value.status_code == 401
    assert str(excinfo.value) == "Token has been invalidated"
