"""Tests for ddpui/utils/discord.py

Covers:
  - Successful notification send
  - HTTP error raised
  - Correct payload and timeout
"""

from unittest.mock import patch, MagicMock
import pytest

from ddpui.utils.discord import send_discord_notification


class TestSendDiscordNotification:
    @patch("ddpui.utils.discord.requests.post")
    def test_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        send_discord_notification("https://discord.com/webhook/123", "Hello!")

        mock_post.assert_called_once_with(
            "https://discord.com/webhook/123",
            json={"content": "Hello!"},
            timeout=10,
        )
        mock_response.raise_for_status.assert_called_once()

    @patch("ddpui.utils.discord.requests.post")
    def test_http_error_raised(self, mock_post):
        from requests.exceptions import HTTPError

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = HTTPError("429 Too Many Requests")
        mock_post.return_value = mock_response

        with pytest.raises(HTTPError):
            send_discord_notification("https://discord.com/webhook/123", "spam")

    @patch("ddpui.utils.discord.requests.post")
    def test_empty_message(self, mock_post):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        send_discord_notification("https://discord.com/webhook/123", "")

        mock_post.assert_called_once_with(
            "https://discord.com/webhook/123",
            json={"content": ""},
            timeout=10,
        )
