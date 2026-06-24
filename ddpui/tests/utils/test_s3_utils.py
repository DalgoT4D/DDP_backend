"""Tests for s3_utils — generic S3 upload and delete utilities

Tests:
1. _build_s3_url — correct URL format
2. upload_file — calls put_object with correct args, returns correct URL
3. delete_file — calls delete_object with correct args
4. download_file — calls get_object and returns response
"""

from unittest.mock import patch, MagicMock
import pytest

from ddpui.utils.s3_utils import (
    _build_s3_url,
    upload_file,
    delete_file,
    download_file,
)


# ================================================================================
# _build_s3_url
# ================================================================================


def test_build_s3_url_format():
    url = _build_s3_url("my-bucket", "ap-south-1", "orgs/test/logo/abc.png")
    assert url == "https://my-bucket.s3.ap-south-1.amazonaws.com/orgs/test/logo/abc.png"


# ================================================================================
# upload_file
# ================================================================================


def test_upload_file_returns_correct_url():
    with (
        patch("ddpui.utils.s3_utils._get_s3_region", return_value="ap-south-1"),
        patch("ddpui.utils.s3_utils.AWSClient") as mock_aws,
    ):
        mock_s3 = MagicMock()
        mock_aws.get_instance.return_value = mock_s3

        url = upload_file("test-bucket", "some/key/file.png", b"bytes", "image/png")

    assert url == "https://test-bucket.s3.ap-south-1.amazonaws.com/some/key/file.png"


def test_upload_file_calls_put_object_with_correct_args():
    with (
        patch("ddpui.utils.s3_utils._get_s3_region", return_value="ap-south-1"),
        patch("ddpui.utils.s3_utils.AWSClient") as mock_aws,
    ):
        mock_s3 = MagicMock()
        mock_aws.get_instance.return_value = mock_s3

        upload_file("test-bucket", "some/key/file.png", b"fake-bytes", "image/webp")

    mock_s3.put_object.assert_called_once_with(
        Bucket="test-bucket",
        Key="some/key/file.png",
        Body=b"fake-bytes",
        ContentType="image/webp",
    )


# ================================================================================
# delete_file
# ================================================================================


def test_delete_file_calls_delete_object_with_correct_args():
    with patch("ddpui.utils.s3_utils.AWSClient") as mock_aws:
        mock_s3 = MagicMock()
        mock_aws.get_instance.return_value = mock_s3

        delete_file("test-bucket", "some/key/file.png")

    mock_s3.delete_object.assert_called_once_with(
        Bucket="test-bucket",
        Key="some/key/file.png",
    )


# ================================================================================
# download_file
# ================================================================================


def test_download_file_calls_get_object_and_returns_response():
    mock_response = {"Body": MagicMock(), "LastModified": "2024-01-01"}

    with patch("ddpui.utils.s3_utils.AWSClient") as mock_aws:
        mock_s3 = MagicMock()
        mock_aws.get_instance.return_value = mock_s3
        mock_s3.get_object.return_value = mock_response

        result = download_file("test-bucket", "some/key/file.html")

    mock_s3.get_object.assert_called_once_with(Bucket="test-bucket", Key="some/key/file.html")
    assert result == mock_response
