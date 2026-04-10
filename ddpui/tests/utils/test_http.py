"""Tests for ddpui/utils/http.py

Covers all four HTTP helper functions: dalgo_get, dalgo_post, dalgo_put, dalgo_delete
Each function has tests for:
  - Success path (returns JSON)
  - Connection error (raises HttpError 500)
  - HTTP error status (raises HttpError with response status)
"""

from unittest.mock import patch, MagicMock
import pytest
from ninja.errors import HttpError

from ddpui.utils.http import dalgo_get, dalgo_post, dalgo_put, dalgo_delete


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_response(status_code=200, json_data=None, text="error text"):
    """Create a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {"ok": True}
    resp.text = text
    resp.raise_for_status = MagicMock()
    return resp


def _mock_response_with_http_error(status_code=400, text="bad request"):
    """Create a mock response whose raise_for_status raises."""
    from requests.exceptions import HTTPError

    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.raise_for_status.side_effect = HTTPError(response=resp)
    return resp


# ===========================================================================
# dalgo_get
# ===========================================================================


class TestDalgoGet:
    @patch("ddpui.utils.http.requests.get")
    def test_success(self, mock_get):
        mock_get.return_value = _mock_response(json_data={"data": 1})
        result = dalgo_get("http://example.com/api", headers={"X-Key": "val"}, timeout=5)
        assert result == {"data": 1}
        mock_get.assert_called_once_with(
            "http://example.com/api",
            headers={"X-Key": "val"},
            timeout=5,
        )

    @patch("ddpui.utils.http.requests.get")
    def test_connection_error(self, mock_get):
        mock_get.side_effect = ConnectionError("refused")
        with pytest.raises(HttpError) as exc_info:
            dalgo_get("http://example.com/api")
        assert exc_info.value.status_code == 500
        assert "connection error" in str(exc_info.value)

    @patch("ddpui.utils.http.requests.get")
    def test_http_error_status(self, mock_get):
        mock_get.return_value = _mock_response_with_http_error(status_code=404, text="not found")
        with pytest.raises(HttpError) as exc_info:
            dalgo_get("http://example.com/api")
        assert exc_info.value.status_code == 404

    @patch("ddpui.utils.http.requests.get")
    def test_default_headers_and_timeout(self, mock_get):
        """When no headers/timeout kwargs, defaults are empty dict and None."""
        mock_get.return_value = _mock_response()
        dalgo_get("http://example.com/api")
        mock_get.assert_called_once_with(
            "http://example.com/api",
            headers={},
            timeout=None,
        )


# ===========================================================================
# dalgo_post
# ===========================================================================


class TestDalgoPost:
    @patch("ddpui.utils.http.requests.post")
    def test_success_with_json(self, mock_post):
        mock_post.return_value = _mock_response(json_data={"id": 42})
        result = dalgo_post("http://example.com/api", json={"name": "test"})
        assert result == {"id": 42}
        mock_post.assert_called_once_with(
            "http://example.com/api",
            headers={},
            timeout=None,
            json={"name": "test"},
            files=None,
        )

    @patch("ddpui.utils.http.requests.post")
    def test_success_with_files(self, mock_post):
        mock_post.return_value = _mock_response(json_data={"uploaded": True})
        files = {"file": ("name.csv", b"data")}
        result = dalgo_post("http://example.com/upload", files=files)
        assert result == {"uploaded": True}

    @patch("ddpui.utils.http.requests.post")
    def test_connection_error(self, mock_post):
        mock_post.side_effect = ConnectionError("refused")
        with pytest.raises(HttpError) as exc_info:
            dalgo_post("http://example.com/api", json={})
        assert exc_info.value.status_code == 500

    @patch("ddpui.utils.http.requests.post")
    def test_http_error_status(self, mock_post):
        mock_post.return_value = _mock_response_with_http_error(status_code=422, text="invalid")
        with pytest.raises(HttpError) as exc_info:
            dalgo_post("http://example.com/api", json={})
        assert exc_info.value.status_code == 422


# ===========================================================================
# dalgo_put
# ===========================================================================


class TestDalgoPut:
    @patch("ddpui.utils.http.requests.put")
    def test_success(self, mock_put):
        mock_put.return_value = _mock_response(json_data={"updated": True})
        result = dalgo_put("http://example.com/api/1", json={"name": "new"})
        assert result == {"updated": True}
        mock_put.assert_called_once_with(
            "http://example.com/api/1",
            headers={},
            timeout=None,
            json={"name": "new"},
        )

    @patch("ddpui.utils.http.requests.put")
    def test_connection_error(self, mock_put):
        mock_put.side_effect = ConnectionError("refused")
        with pytest.raises(HttpError) as exc_info:
            dalgo_put("http://example.com/api/1", json={})
        assert exc_info.value.status_code == 500

    @patch("ddpui.utils.http.requests.put")
    def test_http_error_status(self, mock_put):
        mock_put.return_value = _mock_response_with_http_error(status_code=403, text="forbidden")
        with pytest.raises(HttpError) as exc_info:
            dalgo_put("http://example.com/api/1", json={})
        assert exc_info.value.status_code == 403


# ===========================================================================
# dalgo_delete
# ===========================================================================


class TestDalgoDelete:
    @patch("ddpui.utils.http.requests.delete")
    def test_success(self, mock_delete):
        mock_delete.return_value = _mock_response(json_data={"deleted": True})
        result = dalgo_delete("http://example.com/api/1")
        assert result == {"deleted": True}
        mock_delete.assert_called_once_with(
            "http://example.com/api/1",
            headers={},
            timeout=None,
        )

    @patch("ddpui.utils.http.requests.delete")
    def test_connection_error(self, mock_delete):
        mock_delete.side_effect = ConnectionError("refused")
        with pytest.raises(HttpError) as exc_info:
            dalgo_delete("http://example.com/api/1")
        assert exc_info.value.status_code == 500

    @patch("ddpui.utils.http.requests.delete")
    def test_http_error_status(self, mock_delete):
        mock_delete.return_value = _mock_response_with_http_error(status_code=409, text="conflict")
        with pytest.raises(HttpError) as exc_info:
            dalgo_delete("http://example.com/api/1")
        assert exc_info.value.status_code == 409

    @patch("ddpui.utils.http.requests.delete")
    def test_with_custom_headers_and_timeout(self, mock_delete):
        mock_delete.return_value = _mock_response(json_data={"deleted": True})
        dalgo_delete(
            "http://example.com/api/1", headers={"Authorization": "Bearer tok"}, timeout=10
        )
        mock_delete.assert_called_once_with(
            "http://example.com/api/1",
            headers={"Authorization": "Bearer tok"},
            timeout=10,
        )
