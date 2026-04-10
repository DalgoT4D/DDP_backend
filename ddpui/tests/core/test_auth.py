import json
from unittest.mock import Mock, patch, MagicMock
from ninja.errors import HttpError
import pytest
from django.contrib.auth.models import User
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role, RolePermission
from ddpui.auth import (
    CustomJwtAuthMiddleware,
    CustomAuthMiddleware,
    has_permission,
    set_roles_and_permissions_in_redis,
    ACCOUNT_MANAGER_ROLE,
    UNAUTHORIZED,
)
from rest_framework.authtoken.models import Token
from rest_framework_simplejwt.tokens import AccessToken

pytestmark = pytest.mark.django_db


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


# =========================================================
# Tests for has_permission decorator
# =========================================================
class TestHasPermission:
    """Tests for the has_permission decorator."""

    def test_has_permission_success(self):
        """Request with matching permissions should succeed."""
        request = Mock()
        request.permissions = ["can_view", "can_edit"]

        @has_permission(["can_view"])
        def my_endpoint(req):
            return "ok"

        result = my_endpoint(request)
        assert result == "ok"

    def test_has_permission_missing_permission(self):
        """Request missing required permission raises 403."""
        request = Mock()
        request.permissions = ["can_view"]

        @has_permission(["can_view", "can_delete"])
        def my_endpoint(req):
            return "ok"

        with pytest.raises(HttpError) as excinfo:
            my_endpoint(request)
        # The outer try/except catches and re-raises as 404
        assert excinfo.value.status_code in [403, 404]

    def test_has_permission_empty_permissions(self):
        """Request with empty permissions raises error."""
        request = Mock()
        request.permissions = []

        @has_permission(["can_view"])
        def my_endpoint(req):
            return "ok"

        with pytest.raises(HttpError):
            my_endpoint(request)

    def test_has_permission_no_permissions_attr(self):
        """Request without permissions attribute raises error."""
        request = Mock(spec=[])  # no attributes

        @has_permission(["can_view"])
        def my_endpoint(req):
            return "ok"

        with pytest.raises(HttpError) as excinfo:
            my_endpoint(request)
        assert excinfo.value.status_code == 404


# =========================================================
# Tests for CustomAuthMiddleware (token-based)
# =========================================================
class TestCustomAuthMiddleware:
    def test_no_token_record_raises(self):
        """Invalid token raises HttpError 400."""
        middleware = CustomAuthMiddleware()
        request = Mock()
        request.headers = {}
        with patch("ddpui.auth.Token.objects.filter") as mock_filter:
            mock_filter.return_value.first.return_value = None
            with pytest.raises(HttpError) as excinfo:
                middleware.authenticate(request, "bad-token")
            assert excinfo.value.status_code == 400

    def test_valid_token_no_orguser_raises(self):
        """Valid token but no OrgUser raises HttpError 400."""
        middleware = CustomAuthMiddleware()
        request = Mock()
        request.headers = {}

        mock_token = Mock()
        mock_token.user = Mock()

        with patch("ddpui.auth.Token.objects.filter") as mock_filter:
            mock_filter.return_value.first.return_value = mock_token
            with patch("ddpui.auth.OrgUser.objects.filter") as mock_ou:
                mock_ou.return_value.select_related.return_value.first.return_value = None
                with pytest.raises(HttpError) as excinfo:
                    middleware.authenticate(request, "valid-token")
                assert excinfo.value.status_code == 400

    def test_valid_token_with_orguser_success(self):
        """Valid token with an OrgUser returns request."""
        middleware = CustomAuthMiddleware()
        request = Mock()
        request.headers = {}

        user = Mock()
        mock_token = Mock()
        mock_token.user = user

        mock_orguser = Mock()
        mock_orguser.org = Mock()  # org is not None
        mock_orguser.new_role = Mock()

        with patch("ddpui.auth.Token.objects.filter") as mock_filter:
            mock_filter.return_value.first.return_value = mock_token
            with patch("ddpui.auth.OrgUser.objects.filter") as mock_ou:
                mock_ou.return_value.select_related.return_value.first.return_value = mock_orguser
                with patch("ddpui.auth.RolePermission.objects.filter") as mock_rp:
                    mock_rp.return_value.values_list.return_value = ["perm_a", "perm_b"]
                    result = middleware.authenticate(request, "valid-token")
                    assert result.orguser == mock_orguser
                    assert result.permissions == ["perm_a", "perm_b"]

    def test_valid_token_orguser_no_org_raises(self):
        """Valid token but orguser.org is None raises 400."""
        middleware = CustomAuthMiddleware()
        request = Mock()
        request.headers = {}

        mock_token = Mock()
        mock_token.user = Mock()

        mock_orguser = Mock()
        mock_orguser.org = None

        with patch("ddpui.auth.Token.objects.filter") as mock_filter:
            mock_filter.return_value.first.return_value = mock_token
            with patch("ddpui.auth.OrgUser.objects.filter") as mock_ou:
                mock_ou.return_value.select_related.return_value.first.return_value = mock_orguser
                with pytest.raises(HttpError) as excinfo:
                    middleware.authenticate(request, "valid-token")
                assert excinfo.value.status_code == 400

    def test_valid_token_with_xdalgo_org_header(self):
        """Token with x-dalgo-org header filters by org slug."""
        middleware = CustomAuthMiddleware()
        request = Mock()
        request.headers = {"x-dalgo-org": "my-org-slug"}

        user = Mock()
        mock_token = Mock()
        mock_token.user = user

        mock_orguser = Mock()
        mock_orguser.org = Mock()
        mock_orguser.new_role = Mock()

        with patch("ddpui.auth.Token.objects.filter") as mock_filter:
            mock_filter.return_value.first.return_value = mock_token
            with patch("ddpui.auth.OrgUser.objects.filter") as mock_ou:
                mock_ou.return_value.filter.return_value.select_related.return_value.first.return_value = (
                    mock_orguser
                )
                with patch("ddpui.auth.RolePermission.objects.filter") as mock_rp:
                    mock_rp.return_value.values_list.return_value = ["perm_x"]
                    result = middleware.authenticate(request, "valid-token")
                    assert result.permissions == ["perm_x"]
                    # Verify filter was called with org slug
                    mock_ou.return_value.filter.assert_called_once_with(org__slug="my-org-slug")


# =========================================================
# Tests for set_roles_and_permissions_in_redis
# =========================================================
class TestSetRolesAndPermissionsInRedis:
    @patch("ddpui.auth.RolePermission.objects")
    def test_set_roles_and_permissions(self, mock_rp_objects):
        """Test writing role permissions to redis."""
        mock_perm1 = Mock()
        mock_perm1.role_id = 1
        mock_perm1.permission = Mock()
        mock_perm1.permission.slug = "can_view"

        mock_perm2 = Mock()
        mock_perm2.role_id = 1
        mock_perm2.permission = Mock()
        mock_perm2.permission.slug = "can_edit"

        mock_perm3 = Mock()
        mock_perm3.role_id = 2
        mock_perm3.permission = Mock()
        mock_perm3.permission.slug = "can_delete"

        mock_rp_objects.select_related.return_value.all.return_value = [
            mock_perm1,
            mock_perm2,
            mock_perm3,
        ]

        mock_redis = Mock()
        result = set_roles_and_permissions_in_redis(mock_redis, "test_key")

        assert "1" in result
        assert "2" in result
        assert result["1"] == ["can_view", "can_edit"]
        assert result["2"] == ["can_delete"]
        mock_redis.set.assert_called_once()


# =========================================================
# Tests for CustomJwtAuthMiddleware.__call__
# =========================================================
class TestCustomJwtAuthMiddlewareCall:
    def test_call_login_token_path(self):
        """For /api/login_token/ endpoint, Authorization header is prioritized."""
        middleware = CustomJwtAuthMiddleware()
        request = Mock()
        request.path = "/api/login_token/"
        request.headers = {}
        request.META = {}

        # The super().__call__ will try to parse the Authorization header
        # Since there is no valid token, it should fail, but we just test the path logic
        with patch.object(
            CustomJwtAuthMiddleware, "authenticate", side_effect=HttpError(401, "test")
        ):
            with patch("ninja.security.HttpBearer.__call__", side_effect=HttpError(401, "test")):
                with pytest.raises(HttpError):
                    middleware(request)

    def test_call_with_cookie_token(self):
        """For other endpoints, cookie token is prioritized."""
        middleware = CustomJwtAuthMiddleware()
        request = Mock()
        request.path = "/api/some-endpoint/"
        request.COOKIES = {"access_token": "cookie-jwt-token"}
        request.headers = {}

        with patch.object(middleware, "authenticate", return_value="authed") as mock_auth:
            result = middleware(request)
            mock_auth.assert_called_once_with(request, "cookie-jwt-token")
            assert result == "authed"

    def test_call_no_cookie_falls_back_to_header(self):
        """Without cookie, falls back to Authorization header."""
        middleware = CustomJwtAuthMiddleware()
        request = Mock()
        request.path = "/api/some-endpoint/"
        request.COOKIES = {}
        request.headers = {}
        request.META = {}

        with patch(
            "ninja.security.HttpBearer.__call__", return_value="header-authed"
        ) as mock_super:
            result = middleware(request)
            assert result == "header-authed"


# =========================================================
# Tests for CustomJwtAuthMiddleware.authenticate - no token
# =========================================================
class TestCustomJwtAuthMiddlewareNoToken:
    def test_authenticate_no_token(self, mock_request):
        """No token raises 401."""
        middleware = CustomJwtAuthMiddleware()
        with pytest.raises(HttpError) as excinfo:
            middleware.authenticate(mock_request, None)
        assert excinfo.value.status_code == 401

    def test_authenticate_empty_token(self, mock_request):
        """Empty string token raises 401."""
        middleware = CustomJwtAuthMiddleware()
        with pytest.raises(HttpError) as excinfo:
            middleware.authenticate(mock_request, "")
        assert excinfo.value.status_code == 401


# =========================================================
# Tests for CustomJwtAuthMiddleware - user not found
# =========================================================
class TestCustomJwtAuthMiddlewareUserNotFound:
    @patch("ddpui.auth.User.objects.filter")
    @patch("ddpui.auth.OrgUser.objects.filter")
    def test_user_exists_but_no_orguser(
        self, mock_ou_filter, mock_user_filter, mock_request, mock_user
    ):
        """Valid token and user, but no OrgUser should raise 401."""
        mock_user_filter.return_value.first.return_value = mock_user
        mock_ou_filter.return_value.select_related.return_value.first.return_value = None
        token = str(AccessToken.for_user(mock_user))

        middleware = CustomJwtAuthMiddleware()
        with pytest.raises(HttpError) as excinfo:
            middleware.authenticate(mock_request, token)
        assert excinfo.value.status_code == 401


# =========================================================
# Tests for JWT auth with corrupted redis cache
# =========================================================
class TestCustomJwtAuthMiddlewareCorruptedRedis:
    @patch("ddpui.auth.User.objects.filter")
    @patch("ddpui.auth.OrgUser.objects.filter")
    @patch("ddpui.auth.RedisClient.get_instance")
    @patch("ddpui.auth.set_roles_and_permissions_in_redis")
    def test_corrupted_orguser_role_cache(
        self,
        mock_set_roles,
        mock_redis_client,
        mock_org_user_filter,
        mock_user_filter,
        mock_request,
        mock_user,
    ):
        """When orguser role cache is corrupted (invalid JSON), it should rebuild."""
        # Create a fully mocked orguser that doesn't need DB
        fake_orguser = Mock()
        fake_orguser.id = 42
        fake_orguser.org = Mock()
        fake_orguser.new_role = Mock()
        fake_orguser.new_role.id = 7

        mock_user_filter.return_value.first.return_value = mock_user
        mock_org_user_filter.return_value.filter.return_value.select_related.return_value.first.return_value = (
            fake_orguser
        )

        redis_mock = Mock()
        # First call: orguser_role key returns invalid JSON
        # Second call: permissions key returns valid permissions
        redis_mock.get.side_effect = [
            "not-valid-json{",
            json.dumps({str(fake_orguser.new_role.id): ["perm1"]}),
        ]
        mock_redis_client.return_value = redis_mock

        mock_request.headers["x-dalgo-org"] = "test-org"
        token = str(AccessToken.for_user(mock_user))

        middleware = CustomJwtAuthMiddleware()
        result = middleware.authenticate(mock_request, token)
        assert result == mock_request

    @patch("ddpui.auth.User.objects.filter")
    @patch("ddpui.auth.OrgUser.objects.filter")
    @patch("ddpui.auth.RedisClient.get_instance")
    @patch("ddpui.auth.set_roles_and_permissions_in_redis")
    def test_empty_orguser_role_map(
        self,
        mock_set_roles,
        mock_redis_client,
        mock_org_user_filter,
        mock_user_filter,
        mock_request,
        mock_user,
    ):
        """When orguser_role_map is empty dict, it rebuilds."""
        # Create a fully mocked orguser that doesn't need DB
        fake_orguser = Mock()
        fake_orguser.id = 42
        fake_orguser.org = Mock()
        fake_orguser.new_role = Mock()
        fake_orguser.new_role.id = 7

        mock_user_filter.return_value.first.return_value = mock_user
        mock_org_user_filter.return_value.filter.return_value.select_related.return_value.first.return_value = (
            fake_orguser
        )

        redis_mock = Mock()
        redis_mock.get.side_effect = [
            json.dumps({}),  # empty orguser role map
            json.dumps({str(fake_orguser.new_role.id): ["perm1"]}),
        ]
        mock_redis_client.return_value = redis_mock

        mock_request.headers["x-dalgo-org"] = "test-org"
        token = str(AccessToken.for_user(mock_user))

        middleware = CustomJwtAuthMiddleware()
        result = middleware.authenticate(mock_request, token)
        assert result == mock_request
