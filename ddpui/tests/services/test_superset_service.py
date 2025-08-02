"""Unit tests for SupersetService."""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from requests.exceptions import RequestException
from ninja.errors import HttpError

from ddpui.models.org import Org
from ddpui.services.superset_service import SupersetService


class TestSupersetService:
    """Test cases for SupersetService."""

    @pytest.fixture
    def mock_org(self):
        """Create a mock organization."""
        org = Mock(spec=Org)
        org.id = 1
        org.viz_url = "https://superset.example.com/"
        org.dalgouser_superset_creds_key = "test-secret-key"
        return org

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        with patch("ddpui.services.superset_service.RedisClient") as mock_redis_class:
            mock_redis_instance = Mock()
            mock_redis_class.get_instance.return_value = mock_redis_instance
            yield mock_redis_instance

    @pytest.fixture
    def superset_service(self, mock_org, mock_redis):
        """Create a SupersetService instance."""
        return SupersetService(mock_org)

    def test_token_caching(self, superset_service, mock_redis):
        """Test that tokens are cached and retrieved correctly."""
        # Setup
        mock_redis.get.return_value = b'{"access_token": "cached-token"}'
        cache_key = f"superset:token:{superset_service.org.id}:access"

        # Mock secretsmanager
        with patch("ddpui.services.superset_service.secretsmanager") as mock_secrets:
            mock_secrets.retrieve_dalgo_user_superset_credentials.return_value = {
                "username": "admin",
                "password": "password",
            }

            # First call should use cache
            mock_redis.get.return_value = b"cached-token"
            token = superset_service.get_access_token()
            assert token == "cached-token"
            mock_redis.get.assert_called_with(cache_key)

    def test_token_refresh_on_expiry(self, superset_service, mock_redis):
        """Test automatic token refresh when cached token is expired."""
        # Setup - no cached token
        mock_redis.get.return_value = None

        with patch("ddpui.services.superset_service.secretsmanager") as mock_secrets:
            mock_secrets.retrieve_dalgo_user_superset_credentials.return_value = {
                "username": "admin",
                "password": "password",
            }

            with patch("requests.post") as mock_post:
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {"access_token": "new-token"}
                mock_post.return_value = mock_response

                # Get new token
                token = superset_service.get_access_token(force_refresh=True)

                # Verify new token is returned and cached
                assert token == "new-token"
                mock_redis.set.assert_called_once()
                args = mock_redis.set.call_args[0]
                assert args[0] == f"superset:token:{superset_service.org.id}:access"
                assert args[1] == "new-token"
                assert args[2] == 3600  # TTL

    def test_retry_logic(self, superset_service, mock_redis):
        """Test exponential backoff retry on transient failures."""
        # Mock the get_access_token to return a token
        with patch.object(superset_service, "get_access_token", return_value="test-token"):
            with patch("requests.request") as mock_request:
                # First two attempts fail, third succeeds
                mock_request.side_effect = [
                    RequestException("Connection error"),
                    RequestException("Timeout"),
                    Mock(status_code=200, json=lambda: {"result": "success"}),
                ]

                with patch("time.sleep"):  # Don't actually sleep in tests
                    response = superset_service._make_request_with_retry(
                        "GET", "https://test.com", headers={"Authorization": "Bearer test"}
                    )

                assert mock_request.call_count == 3
                assert response.json() == {"result": "success"}

    def test_401_error_handling(self, superset_service, mock_redis):
        """Test automatic token refresh on 401 response."""
        # Mock scan_iter to return list of keys to delete
        mock_redis.scan_iter.return_value = ["superset:token:1:access", "superset:token:1:csrf"]

        with patch.object(superset_service, "get_access_token") as mock_get_token:
            # First call returns old token, second call (after 401) returns new token
            mock_get_token.return_value = "new-token"

            with patch("requests.request") as mock_request:
                # First attempt returns 401, second succeeds
                response_401 = Mock(status_code=401)
                response_success = Mock(status_code=200, json=lambda: {"result": "success"})
                mock_request.side_effect = [response_401, response_success]

                with patch("time.sleep"):
                    response = superset_service._make_request_with_retry(
                        "GET", "https://test.com", headers={"Authorization": "Bearer old-token"}
                    )

                # Should clear cache and retry with new token
                mock_redis.scan_iter.assert_called_once()
                mock_redis.delete.assert_any_call("superset:token:1:access")
                mock_redis.delete.assert_any_call("superset:token:1:csrf")
                # get_access_token is called once in the retry
                assert mock_get_token.call_count == 1
                assert response.json() == {"result": "success"}

    def test_guest_token_generation(self, superset_service, mock_redis):
        """Test guest token generation and caching."""
        dashboard_uuid = "test-dashboard-uuid"
        mock_redis.get.return_value = None  # No cached token

        with patch.object(superset_service, "get_access_token", return_value="access-token"):
            with patch.object(
                superset_service, "get_csrf_token", return_value=("csrf-token", "session-cookie")
            ):
                with patch("ddpui.services.superset_service.secretsmanager") as mock_secrets:
                    mock_secrets.retrieve_dalgo_user_superset_credentials.return_value = {
                        "username": "admin"
                    }

                    with patch("requests.post") as mock_post:
                        mock_response = Mock()
                        mock_response.status_code = 200
                        mock_response.json.return_value = {"token": "guest-token-123"}
                        mock_post.return_value = mock_response

                        result = superset_service.get_guest_token(dashboard_uuid)

                        assert result["token"] == "guest-token-123"
                        assert "generated_at" in result

                        # Verify caching
                        mock_redis.set.assert_called_once()
                        cache_key = (
                            f"superset:token:{superset_service.org.id}:guest:{dashboard_uuid}"
                        )
                        args = mock_redis.set.call_args[0]
                        assert args[0] == cache_key
                        assert args[2] == 240  # Guest token TTL

    def test_http_error_raising(self, superset_service, mock_redis):
        """Test that appropriate HttpErrors are raised."""
        # Mock scan_iter to return empty list
        mock_redis.scan_iter.return_value = []

        # Test 503 error on connection failure
        with patch("requests.request") as mock_request:
            mock_request.side_effect = RequestException("Connection failed")

            with pytest.raises(HttpError) as exc_info:
                with patch("time.sleep"):
                    superset_service._make_request_with_retry("GET", "https://test.com")

            assert exc_info.value.status_code == 503
            assert "Failed to connect to Superset" in str(exc_info.value)

        # Test 401 error after all retries
        with patch.object(superset_service, "get_access_token", return_value="test-token"):
            with patch("requests.request") as mock_request:
                mock_request.return_value = Mock(status_code=401)

                with pytest.raises(HttpError) as exc_info:
                    with patch("time.sleep"):
                        superset_service._make_request_with_retry(
                            "GET", "https://test.com", headers={}
                        )

                assert exc_info.value.status_code == 401
                assert "Authentication expired" in str(exc_info.value)

    def test_get_dashboards_with_filters(self, superset_service, mock_redis):
        """Test dashboard listing with search and status filters."""
        with patch.object(superset_service, "get_access_token", return_value="test-token"):
            with patch.object(superset_service, "_make_request_with_retry") as mock_request:
                mock_response = Mock()
                mock_response.json.return_value = {
                    "result": [{"id": 1, "title": "Test Dashboard"}],
                    "count": 1,
                }
                mock_request.return_value = mock_response

                # Test with filters
                result = superset_service.get_dashboards(
                    page=1, page_size=20, search="test", status="published"
                )

                # Verify request was made with correct parameters
                call_args = mock_request.call_args
                assert call_args[0][0] == "GET"
                assert "dashboard/" in call_args[0][1]

                # Check query parameters
                params = json.loads(call_args[1]["params"]["q"])
                assert params["page"] == 1
                assert params["page_size"] == 20
                assert len(params["filters"]) == 2

                # Check filters
                search_filter = next(f for f in params["filters"] if f["col"] == "dashboard_title")
                assert search_filter["opr"] == "ct"
                assert search_filter["value"] == "test"

                status_filter = next(f for f in params["filters"] if f["col"] == "published")
                assert status_filter["opr"] == "eq"
                assert status_filter["value"] is True

    def test_dashboard_thumbnail_caching(self, superset_service, mock_redis):
        """Test dashboard thumbnail retrieval and caching."""
        dashboard_id = "123"
        thumbnail_data = b"fake-image-data"

        # Test cache hit
        mock_redis.get.return_value = thumbnail_data
        result = superset_service.get_dashboard_thumbnail(dashboard_id)
        assert result == thumbnail_data

        # Test cache miss
        mock_redis.get.return_value = None
        with patch.object(superset_service, "get_access_token", return_value="test-token"):
            with patch.object(superset_service, "_make_request_with_retry") as mock_request:
                mock_response = Mock()
                mock_response.content = thumbnail_data
                mock_request.return_value = mock_response

                result = superset_service.get_dashboard_thumbnail(dashboard_id)
                assert result == thumbnail_data

                # Verify caching
                mock_redis.set.assert_called_once()
                cache_key = f"superset:thumbnail:{superset_service.org.id}:{dashboard_id}"
                args = mock_redis.set.call_args[0]
                assert args[0] == cache_key
                assert args[1] == thumbnail_data
                assert args[2] == 3600  # Thumbnail cache TTL
