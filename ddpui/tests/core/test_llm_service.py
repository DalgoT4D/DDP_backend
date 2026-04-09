import os
from unittest.mock import Mock, patch, MagicMock, call
import pytest

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.llm_service import (
    llm_endpoint,
    poll_llm_service_task,
    upload_text_as_file,
    upload_json_as_file,
    file_search_query_and_poll,
    close_file_search_session,
    CELERY_TERMINAL_STATES,
    CELERY_ERROR_STATES,
)

pytestmark = pytest.mark.django_db


# =========================================================
# Tests for llm_endpoint
# =========================================================
class TestLlmEndpoint:
    @patch("ddpui.core.llm_service.LLM_SERVICE_API_URL", "http://llm.example.com")
    @patch("ddpui.core.llm_service.LLM_SERVICE_API_VER", "v1")
    def test_llm_endpoint_with_version(self):
        """Test endpoint URL with API version."""
        result = llm_endpoint("file/upload")
        assert result == "http://llm.example.com/api/v1/file/upload"

    @patch("ddpui.core.llm_service.LLM_SERVICE_API_URL", "http://llm.example.com")
    @patch("ddpui.core.llm_service.LLM_SERVICE_API_VER", "")
    def test_llm_endpoint_without_version(self):
        """Test endpoint URL without API version."""
        result = llm_endpoint("file/upload")
        assert result == "http://llm.example.com/api/file/upload"

    @patch("ddpui.core.llm_service.LLM_SERVICE_API_URL", None)
    def test_llm_endpoint_no_url_raises(self):
        """Test that missing LLM_SERVICE_API_URL raises an exception."""
        with pytest.raises(Exception, match="LLM_SERVICE_API_URL is not set"):
            llm_endpoint("file/upload")


# =========================================================
# Tests for poll_llm_service_task
# =========================================================
class TestPollLlmServiceTask:
    @patch("ddpui.core.llm_service.time.sleep", return_value=None)
    @patch("ddpui.core.llm_service.dalgo_get")
    def test_poll_success_immediately(self, mock_dalgo_get, mock_sleep):
        """Task completes on first poll."""
        mock_dalgo_get.return_value = {
            "status": "SUCCESS",
            "result": {"data": "done"},
            "error": None,
        }

        result = poll_llm_service_task("task-123", poll_interval=1)
        assert result == {"data": "done"}
        mock_sleep.assert_not_called()

    @patch("ddpui.core.llm_service.time.sleep", return_value=None)
    @patch("ddpui.core.llm_service.dalgo_get")
    def test_poll_with_pending_then_success(self, mock_dalgo_get, mock_sleep):
        """Task is pending first, then completes."""
        mock_dalgo_get.side_effect = [
            {"status": "PENDING", "result": None, "error": None},
            {"status": "SUCCESS", "result": {"data": "result"}, "error": None},
        ]

        result = poll_llm_service_task("task-456", poll_interval=1)
        assert result == {"data": "result"}
        assert mock_sleep.call_count == 1

    @patch("ddpui.core.llm_service.time.sleep", return_value=None)
    @patch("ddpui.core.llm_service.dalgo_get")
    def test_poll_error_state(self, mock_dalgo_get, mock_sleep):
        """Task fails; should raise exception."""
        mock_dalgo_get.return_value = {
            "status": "FAILURE",
            "result": None,
            "error": "Something went wrong",
        }

        with pytest.raises(Exception, match="Something went wrong"):
            poll_llm_service_task("task-err", poll_interval=1)

    @patch("ddpui.core.llm_service.time.sleep", return_value=None)
    @patch("ddpui.core.llm_service.dalgo_get")
    def test_poll_error_state_no_error_message(self, mock_dalgo_get, mock_sleep):
        """Task fails with no error message; should use default."""
        mock_dalgo_get.return_value = {
            "status": "REVOKED",
            "result": None,
            "error": None,
        }

        with pytest.raises(Exception, match="error occured in llm service"):
            poll_llm_service_task("task-revoked", poll_interval=1)


# =========================================================
# Tests for upload_text_as_file
# =========================================================
class TestUploadTextAsFile:
    @patch("ddpui.core.llm_service.dalgo_post")
    @patch("ddpui.core.llm_service.LLM_SERVICE_API_URL", "http://llm.example.com")
    @patch("ddpui.core.llm_service.LLM_SERVICE_API_VER", "")
    def test_upload_text_as_file_success(self, mock_dalgo_post):
        """Test successful text file upload."""
        mock_dalgo_post.return_value = {
            "file_path": "/uploads/test.txt",
            "session_id": "sess-1",
        }

        file_path, session_id = upload_text_as_file("hello world", "test")
        assert file_path == "/uploads/test.txt"
        assert session_id == "sess-1"
        mock_dalgo_post.assert_called_once()


# =========================================================
# Tests for upload_json_as_file
# =========================================================
class TestUploadJsonAsFile:
    @patch("ddpui.core.llm_service.dalgo_post")
    @patch("ddpui.core.llm_service.LLM_SERVICE_API_URL", "http://llm.example.com")
    @patch("ddpui.core.llm_service.LLM_SERVICE_API_VER", "")
    def test_upload_json_as_file_success(self, mock_dalgo_post):
        """Test successful JSON file upload."""
        mock_dalgo_post.return_value = {
            "file_path": "/uploads/data.json",
            "session_id": "sess-2",
        }

        file_path, session_id = upload_json_as_file('{"key": "value"}', "data")
        assert file_path == "/uploads/data.json"
        assert session_id == "sess-2"
        mock_dalgo_post.assert_called_once()


# =========================================================
# Tests for file_search_query_and_poll
# =========================================================
class TestFileSearchQueryAndPoll:
    @patch("ddpui.core.llm_service.poll_llm_service_task")
    @patch("ddpui.core.llm_service.dalgo_post")
    @patch("ddpui.core.llm_service.LLM_SERVICE_API_URL", "http://llm.example.com")
    @patch("ddpui.core.llm_service.LLM_SERVICE_API_VER", "")
    def test_file_search_query_and_poll_success(self, mock_dalgo_post, mock_poll):
        """Test successful query submission and poll."""
        mock_dalgo_post.return_value = {"task_id": "task-abc"}
        mock_poll.return_value = {"result": ["answer1"]}

        result = file_search_query_and_poll(
            assistant_prompt="You are helpful.",
            queries=["What is this?"],
            session_id="sess-1",
            poll_interval=1,
        )

        assert result == {"result": ["answer1"]}
        mock_poll.assert_called_once_with("task-abc", 1)

    @patch("ddpui.core.llm_service.dalgo_post")
    @patch("ddpui.core.llm_service.LLM_SERVICE_API_URL", "http://llm.example.com")
    @patch("ddpui.core.llm_service.LLM_SERVICE_API_VER", "")
    def test_file_search_query_no_task_id(self, mock_dalgo_post):
        """Test error when no task_id is returned."""
        mock_dalgo_post.return_value = {"task_id": None}

        with pytest.raises(Exception, match="Failed to submit the task"):
            file_search_query_and_poll(
                assistant_prompt="prompt",
                queries=["q"],
                session_id="sess-1",
            )


# =========================================================
# Tests for close_file_search_session
# =========================================================
class TestCloseFileSearchSession:
    @patch("ddpui.core.llm_service.poll_llm_service_task")
    @patch("ddpui.core.llm_service.dalgo_delete")
    @patch("ddpui.core.llm_service.LLM_SERVICE_API_URL", "http://llm.example.com")
    @patch("ddpui.core.llm_service.LLM_SERVICE_API_VER", "")
    def test_close_session_success(self, mock_dalgo_delete, mock_poll):
        """Test successful session close."""
        mock_dalgo_delete.return_value = {"task_id": "task-close"}
        mock_poll.return_value = None

        close_file_search_session("sess-1", poll_interval=1)
        mock_poll.assert_called_once_with("task-close", 1)

    @patch("ddpui.core.llm_service.dalgo_delete")
    @patch("ddpui.core.llm_service.LLM_SERVICE_API_URL", "http://llm.example.com")
    @patch("ddpui.core.llm_service.LLM_SERVICE_API_VER", "")
    def test_close_session_no_task_id(self, mock_dalgo_delete):
        """Test error when no task_id is returned for close."""
        mock_dalgo_delete.return_value = {"task_id": None}

        with pytest.raises(Exception, match="Failed to submit the task"):
            close_file_search_session("sess-1")
