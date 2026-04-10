"""Tests for superset_api.py endpoints"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from unittest.mock import Mock, patch, MagicMock
import pytest
from ninja.errors import HttpError
from ddpui.models.org import OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.superset_api import (
    post_fetch_embed_token,
    post_superset_admin_creds,
    get_dashboards,
    get_single_dashboard_embed_info,
    get_dashboard_by_id,
    post_dashboard_guest_token,
    post_dashboard_thumbnail,
    SupersetDalgoUserCreds,
    ThumbnailRequestPayload,
)
from ddpui.tests.api_tests.test_user_org_api import mock_request

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def org_with_viz(org):
    org.viz_url = "http://superset.example.com/"
    org.dalgouser_superset_creds_key = "test-secret-key"
    org.save()
    yield org


@pytest.fixture
def orguser_with_viz(authuser, org_with_viz):
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org_with_viz,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def warehouse(org):
    wh = OrgWarehouse.objects.create(
        wtype="postgres",
        credentials="test-creds",
        org=org,
    )
    yield wh
    wh.delete()


# ================================================================================
# Tests for post_fetch_embed_token
# ================================================================================


class TestPostFetchEmbedToken:
    def test_no_org(self, authuser, seed_db):
        orguser = OrgUser.objects.create(
            user=authuser,
            org=None,
            new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
        )
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            post_fetch_embed_token(request, "dashboard-uuid")
        assert exc.value.status_code == 400
        assert "create an organization first" in str(exc.value)
        orguser.delete()

    def test_no_warehouse(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            post_fetch_embed_token(request, "dashboard-uuid")
        assert exc.value.status_code == 400
        assert "create a warehouse first" in str(exc.value)

    def test_no_viz_url(self, orguser, warehouse, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            post_fetch_embed_token(request, "dashboard-uuid")
        assert exc.value.status_code == 400
        assert "superset subscription is not active" in str(exc.value)

    @patch.dict(os.environ, {"SUPERSET_USAGE_CREDS_SECRET_ID": ""}, clear=False)
    def test_no_superset_creds_env(self, orguser_with_viz, seed_db):
        # Set viz_url but remove env var
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SUPERSET_USAGE_CREDS_SECRET_ID", None)
            warehouse = OrgWarehouse.objects.create(
                wtype="postgres", credentials="creds", org=orguser_with_viz.org
            )
            request = mock_request(orguser_with_viz)
            with pytest.raises(HttpError) as exc:
                post_fetch_embed_token(request, "dashboard-uuid")
            assert exc.value.status_code == 400
            warehouse.delete()

    @patch.dict(
        os.environ,
        {
            "SUPERSET_USAGE_CREDS_SECRET_ID": "secret-id",
            "SUPERSET_USAGE_DASHBOARD_API_URL": "http://superset.test.com/api/v1",
        },
    )
    @patch("ddpui.api.superset_api.secretsmanager.retrieve_superset_usage_dashboard_credentials")
    def test_no_credentials_from_secretsmanager(self, mock_retrieve, orguser_with_viz, seed_db):
        mock_retrieve.return_value = None
        warehouse = OrgWarehouse.objects.create(
            wtype="postgres", credentials="creds", org=orguser_with_viz.org
        )
        request = mock_request(orguser_with_viz)
        with pytest.raises(HttpError) as exc:
            post_fetch_embed_token(request, "dashboard-uuid")
        assert exc.value.status_code == 400
        assert "superset usage credentials are missing" in str(exc.value)
        warehouse.delete()

    @patch.dict(
        os.environ,
        {
            "SUPERSET_USAGE_CREDS_SECRET_ID": "secret-id",
            "SUPERSET_USAGE_DASHBOARD_API_URL": "http://superset.test.com/api/v1",
        },
    )
    @patch("ddpui.api.superset_api.requests.post")
    @patch("ddpui.api.superset_api.requests.get")
    @patch("ddpui.api.superset_api.secretsmanager.retrieve_superset_usage_dashboard_credentials")
    def test_successful_embed_token(
        self, mock_retrieve, mock_get, mock_post, orguser_with_viz, seed_db
    ):
        mock_retrieve.return_value = {
            "username": "admin",
            "password": "admin",
            "first_name": "Admin",
            "last_name": "User",
        }

        # First post: login -> access_token
        login_response = Mock()
        login_response.json.return_value = {"access_token": "test-access-token"}
        login_response.raise_for_status = Mock()

        # Third post: guest_token
        guest_response = Mock()
        guest_response.json.return_value = {"token": "test-embed-token"}
        guest_response.raise_for_status = Mock()

        mock_post.side_effect = [login_response, guest_response]

        # get: csrf_token
        csrf_response = Mock()
        csrf_response.json.return_value = {"result": "test-csrf-token"}
        csrf_response.raise_for_status = Mock()
        csrf_response.headers = {"Set-Cookie": "session=test-session-cookie"}
        mock_get.return_value = csrf_response

        warehouse = OrgWarehouse.objects.create(
            wtype="postgres", credentials="creds", org=orguser_with_viz.org
        )
        request = mock_request(orguser_with_viz)
        result = post_fetch_embed_token(request, "dashboard-uuid")
        assert result["embed_token"] == "test-embed-token"
        warehouse.delete()

    @patch.dict(
        os.environ,
        {
            "SUPERSET_USAGE_CREDS_SECRET_ID": "secret-id",
            "SUPERSET_USAGE_DASHBOARD_API_URL": "http://superset.test.com/api/v1",
        },
    )
    @patch("ddpui.api.superset_api.requests.post")
    @patch("ddpui.api.superset_api.secretsmanager.retrieve_superset_usage_dashboard_credentials")
    def test_login_request_exception(self, mock_retrieve, mock_post, orguser_with_viz, seed_db):
        import requests

        mock_retrieve.return_value = {
            "username": "admin",
            "password": "admin",
            "first_name": "Admin",
            "last_name": "User",
        }
        mock_post.side_effect = requests.exceptions.RequestException("Connection refused")

        warehouse = OrgWarehouse.objects.create(
            wtype="postgres", credentials="creds", org=orguser_with_viz.org
        )
        request = mock_request(orguser_with_viz)
        with pytest.raises(HttpError) as exc:
            post_fetch_embed_token(request, "dashboard-uuid")
        assert exc.value.status_code == 500
        warehouse.delete()


# ================================================================================
# Tests for post_superset_admin_creds
# ================================================================================


class TestPostSupersetAdminCreds:
    def test_no_org(self, authuser, seed_db):
        orguser = OrgUser.objects.create(
            user=authuser,
            org=None,
            new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
        )
        request = mock_request(orguser)
        payload = SupersetDalgoUserCreds(username="admin", password="admin123")
        with pytest.raises(HttpError) as exc:
            post_superset_admin_creds(request, payload)
        assert exc.value.status_code == 400
        orguser.delete()

    @patch("ddpui.api.superset_api.secretsmanager.save_dalgo_user_superset_credentials")
    def test_success(self, mock_save, orguser, seed_db):
        mock_save.return_value = "new-secret-id"
        request = mock_request(orguser)
        payload = SupersetDalgoUserCreds(username="admin", password="admin123")
        result = post_superset_admin_creds(request, payload)
        assert result["success"] is True
        orguser.org.refresh_from_db()
        assert orguser.org.dalgouser_superset_creds_key == "new-secret-id"


# ================================================================================
# Tests for get_dashboards
# ================================================================================


class TestGetDashboards:
    def test_no_org(self, authuser, seed_db):
        orguser = OrgUser.objects.create(
            user=authuser,
            org=None,
            new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
        )
        request = mock_request(orguser)
        result = get_dashboards(request)
        assert result == {"result": [], "count": 0}
        orguser.delete()

    def test_no_viz_url(self, orguser, seed_db):
        request = mock_request(orguser)
        result = get_dashboards(request)
        assert result == {"result": [], "count": 0}

    @patch("ddpui.api.superset_api.SupersetService")
    def test_success(self, MockSupersetService, orguser_with_viz, seed_db):
        mock_service = MagicMock()
        mock_service.get_dashboards.return_value = {
            "result": [{"id": 1, "title": "Dashboard 1"}],
            "count": 1,
        }
        MockSupersetService.return_value = mock_service

        request = mock_request(orguser_with_viz)
        result = get_dashboards(request)
        assert result["count"] == 1
        assert len(result["result"]) == 1

    @patch("ddpui.api.superset_api.SupersetService")
    def test_with_search_and_pagination(self, MockSupersetService, orguser_with_viz, seed_db):
        mock_service = MagicMock()
        mock_service.get_dashboards.return_value = {"result": [], "count": 0}
        MockSupersetService.return_value = mock_service

        request = mock_request(orguser_with_viz)
        result = get_dashboards(request, page=1, page_size=10, search="test", status="published")
        mock_service.get_dashboards.assert_called_once_with(1, 10, "test", "published")


# ================================================================================
# Tests for get_single_dashboard_embed_info
# ================================================================================


class TestGetSingleDashboardEmbedInfo:
    def test_no_org(self, authuser, seed_db):
        orguser = OrgUser.objects.create(
            user=authuser,
            org=None,
            new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
        )
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            get_single_dashboard_embed_info(request, "dash-123")
        assert exc.value.status_code == 400
        orguser.delete()

    def test_no_viz_url(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            get_single_dashboard_embed_info(request, "dash-123")
        assert exc.value.status_code == 400
        assert "superset subscription is not active" in str(exc.value)

    @patch("ddpui.api.superset_api.secretsmanager.retrieve_dalgo_user_superset_credentials")
    def test_no_credentials(self, mock_retrieve, orguser_with_viz, seed_db):
        mock_retrieve.return_value = None
        request = mock_request(orguser_with_viz)
        with pytest.raises(HttpError) as exc:
            get_single_dashboard_embed_info(request, "dash-123")
        assert exc.value.status_code == 400
        assert "superset admin credentials are missing" in str(exc.value)

    @patch("ddpui.api.superset_api.requests.post")
    @patch("ddpui.api.superset_api.requests.get")
    @patch("ddpui.api.superset_api.secretsmanager.retrieve_dalgo_user_superset_credentials")
    def test_success(self, mock_retrieve, mock_get, mock_post, orguser_with_viz, seed_db):
        mock_retrieve.return_value = {"username": "admin", "password": "admin123"}

        # login
        login_resp = Mock()
        login_resp.json.return_value = {"access_token": "tok"}
        login_resp.raise_for_status = Mock()

        # embed info post
        embed_resp = Mock()
        embed_resp.json.return_value = {"result": {"uuid": "embed-uuid-123"}}
        embed_resp.raise_for_status = Mock()

        # guest token
        guest_resp = Mock()
        guest_resp.json.return_value = {"token": "guest-token-abc"}
        guest_resp.raise_for_status = Mock()

        mock_post.side_effect = [login_resp, embed_resp, guest_resp]

        csrf_resp = Mock()
        csrf_resp.json.return_value = {"result": "csrf-tok"}
        csrf_resp.raise_for_status = Mock()
        csrf_resp.headers = {"Set-Cookie": "session=sess-cookie"}
        mock_get.return_value = csrf_resp

        request = mock_request(orguser_with_viz)
        result = get_single_dashboard_embed_info(request, "dash-123")
        assert result["uuid"] == "embed-uuid-123"
        assert result["guest_token"] == "guest-token-abc"
        assert result["host"] == "http://superset.example.com"


# ================================================================================
# Tests for get_dashboard_by_id
# ================================================================================


class TestGetDashboardById:
    def test_no_org(self, authuser, seed_db):
        orguser = OrgUser.objects.create(
            user=authuser,
            org=None,
            new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
        )
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            get_dashboard_by_id(request, "1")
        assert exc.value.status_code == 400
        orguser.delete()

    def test_no_viz_url(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            get_dashboard_by_id(request, "1")
        assert exc.value.status_code == 400

    @patch("ddpui.api.superset_api.SupersetService")
    def test_success(self, MockSupersetService, orguser_with_viz, seed_db):
        mock_service = MagicMock()
        mock_service.get_dashboard_by_id.return_value = {
            "id": 1,
            "title": "My Dashboard",
        }
        MockSupersetService.return_value = mock_service

        request = mock_request(orguser_with_viz)
        result = get_dashboard_by_id(request, "1")
        assert result["id"] == 1
        assert result["title"] == "My Dashboard"


# ================================================================================
# Tests for post_dashboard_guest_token
# ================================================================================


class TestPostDashboardGuestToken:
    def test_no_org(self, authuser, seed_db):
        orguser = OrgUser.objects.create(
            user=authuser,
            org=None,
            new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
        )
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            post_dashboard_guest_token(request, "1")
        assert exc.value.status_code == 400
        orguser.delete()

    def test_no_viz_url(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            post_dashboard_guest_token(request, "1")
        assert exc.value.status_code == 400

    @patch("ddpui.api.superset_api.SupersetService")
    def test_no_embedded_uuid(self, MockSupersetService, orguser_with_viz, seed_db):
        mock_service = MagicMock()
        mock_service.get_or_create_embedded_uuid.return_value = None
        MockSupersetService.return_value = mock_service

        request = mock_request(orguser_with_viz)
        with pytest.raises(HttpError) as exc:
            post_dashboard_guest_token(request, "1")
        assert exc.value.status_code == 400

    @patch("ddpui.api.superset_api.SupersetService")
    def test_success(self, MockSupersetService, orguser_with_viz, seed_db):
        mock_service = MagicMock()
        mock_service.get_or_create_embedded_uuid.return_value = "embed-uuid"
        mock_service.get_guest_token.return_value = {"token": "guest-tok"}
        MockSupersetService.return_value = mock_service

        request = mock_request(orguser_with_viz)
        result = post_dashboard_guest_token(request, "1")
        assert result["guest_token"] == "guest-tok"
        assert result["expires_in"] == 300
        assert result["dashboard_uuid"] == "embed-uuid"
        assert result["superset_domain"] == "http://superset.example.com"


# ================================================================================
# Tests for post_dashboard_thumbnail
# ================================================================================


class TestPostDashboardThumbnail:
    def test_no_org(self, authuser, seed_db):
        orguser = OrgUser.objects.create(
            user=authuser,
            org=None,
            new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
        )
        request = mock_request(orguser)
        payload = ThumbnailRequestPayload(thumbnail_url="/thumb/1")
        with pytest.raises(HttpError) as exc:
            post_dashboard_thumbnail(request, "1", payload)
        assert exc.value.status_code == 400
        orguser.delete()

    def test_no_viz_url(self, orguser, seed_db):
        request = mock_request(orguser)
        payload = ThumbnailRequestPayload(thumbnail_url="/thumb/1")
        with pytest.raises(HttpError) as exc:
            post_dashboard_thumbnail(request, "1", payload)
        assert exc.value.status_code == 400

    @patch("ddpui.api.superset_api.SupersetService")
    def test_thumbnail_not_found_no_placeholder(
        self, MockSupersetService, orguser_with_viz, seed_db
    ):
        mock_service = MagicMock()
        mock_service.get_dashboard_thumbnail.return_value = None
        MockSupersetService.return_value = mock_service

        request = mock_request(orguser_with_viz)
        payload = ThumbnailRequestPayload(thumbnail_url="/thumb/1")

        with patch("ddpui.api.superset_api.os.path.exists", return_value=False):
            with pytest.raises(HttpError) as exc:
                post_dashboard_thumbnail(request, "1", payload)
            assert exc.value.status_code == 404

    @patch("ddpui.api.superset_api.SupersetService")
    def test_thumbnail_success(self, MockSupersetService, orguser_with_viz, seed_db):
        mock_service = MagicMock()
        mock_service.get_dashboard_thumbnail.return_value = b"\x89PNG..."
        MockSupersetService.return_value = mock_service

        request = mock_request(orguser_with_viz)
        payload = ThumbnailRequestPayload(thumbnail_url="/thumb/1")
        response = post_dashboard_thumbnail(request, "1", payload)
        assert response.content == b"\x89PNG..."


def test_seed_data(seed_db):
    """Test that seed data is loaded"""
    assert Role.objects.count() == 5
