"""Tests for s3_utils — validation and upload logic for org logos

Tests:
1. upload_org_logo — invalid content type, file too large, successful upload (S3 mocked)
2. _build_s3_url — correct URL format
"""

from unittest.mock import patch, MagicMock
import pytest

from ddpui.utils.s3_utils import (
    upload_org_logo,
    _build_s3_url,
    MAX_FILE_SIZE_BYTES,
    ALLOWED_CONTENT_TYPES,
)


# ================================================================================
# _build_s3_url
# ================================================================================


def test_build_s3_url_format():
    url = _build_s3_url("my-bucket", "ap-south-1", "orgs/test/logo/abc.png")
    assert url == "https://my-bucket.s3.ap-south-1.amazonaws.com/orgs/test/logo/abc.png"


# ================================================================================
# upload_org_logo — validation (no S3 needed)
# ================================================================================


def test_upload_org_logo_invalid_content_type():
    with pytest.raises(ValueError, match="Invalid file type"):
        upload_org_logo(
            file_bytes=b"fake",
            content_type="application/pdf",
            org_slug="test-org",
        )


def test_upload_org_logo_file_too_large():
    oversized = b"x" * (MAX_FILE_SIZE_BYTES + 1)
    with pytest.raises(ValueError, match="5MB"):
        upload_org_logo(
            file_bytes=oversized,
            content_type="image/png",
            org_slug="test-org",
        )


def test_upload_org_logo_exactly_at_limit_is_allowed():
    """File exactly at 5MB should not raise."""
    at_limit = b"x" * MAX_FILE_SIZE_BYTES
    with (
        patch("ddpui.utils.s3_utils._get_media_bucket", return_value="test-bucket"),
        patch("ddpui.utils.s3_utils._get_s3_region", return_value="ap-south-1"),
        patch("ddpui.utils.s3_utils.AWSClient") as mock_aws,
    ):
        mock_s3 = MagicMock()
        mock_aws.get_instance.return_value = mock_s3

        url, s3_key = upload_org_logo(at_limit, "image/png", "test-org")

    assert url.startswith("https://test-bucket.s3.ap-south-1.amazonaws.com/")
    assert s3_key.startswith("orgs/test-org/logo/")
    assert s3_key.endswith(".png")


# ================================================================================
# upload_org_logo — successful upload
# ================================================================================


@pytest.mark.parametrize(
    "content_type, expected_ext",
    [
        ("image/png", ".png"),
        ("image/jpeg", ".jpg"),
        ("image/webp", ".webp"),
        ("image/gif", ".gif"),
    ],
)
def test_upload_org_logo_success(content_type, expected_ext):
    with (
        patch("ddpui.utils.s3_utils._get_media_bucket", return_value="test-bucket"),
        patch("ddpui.utils.s3_utils._get_s3_region", return_value="ap-south-1"),
        patch("ddpui.utils.s3_utils.AWSClient") as mock_aws,
    ):
        mock_s3 = MagicMock()
        mock_aws.get_instance.return_value = mock_s3

        url, s3_key = upload_org_logo(b"fake-image-bytes", content_type, "my-org")

    assert "my-org" in s3_key
    assert s3_key.endswith(expected_ext)
    assert url == f"https://test-bucket.s3.ap-south-1.amazonaws.com/{s3_key}"
    mock_s3.put_object.assert_called_once()
    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs["Bucket"] == "test-bucket"
    assert call_kwargs["Key"] == s3_key
    assert call_kwargs["ContentType"] == content_type


def test_upload_org_logo_missing_s3_bucket_env():
    with patch("ddpui.utils.s3_utils._get_media_bucket", side_effect=ValueError("S3_IMAGES")):
        with pytest.raises(ValueError, match="S3_IMAGES"):
            upload_org_logo(b"bytes", "image/png", "test-org")


def test_all_allowed_content_types_are_covered():
    """Every content type in ALLOWED_CONTENT_TYPES must have a matching extension."""
    from ddpui.utils.s3_utils import CONTENT_TYPE_TO_EXT

    for ct in ALLOWED_CONTENT_TYPES:
        assert ct in CONTENT_TYPE_TO_EXT, f"{ct} missing from CONTENT_TYPE_TO_EXT"
