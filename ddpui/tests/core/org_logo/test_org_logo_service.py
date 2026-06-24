"""Tests for org logo functions in orgfunctions

Tests:
1. upload_logo_from_file — success, validation error (inline), S3 error
2. upload_logo_from_url — success with no prior logo, deletes old S3 key if one exists
3. delete_logo — no logo raises not found, S3 delete failure raises OrgLogoS3Error, success clears fields
"""

import os
import django
from unittest.mock import patch, MagicMock

import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from ddpui.models.org import Org
from ddpui.core import orgfunctions
from ddpui.core.org_logo.exceptions import (
    OrgLogoNotFoundError,
    OrgLogoValidationError,
    OrgLogoS3Error,
)

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def org():
    o = Org.objects.create(name="Logo Test Org", slug="logo-test-org")
    yield o
    o.delete()


@pytest.fixture
def org_with_logo(org):
    org.logo_url = "https://example.com/logo.png"
    org.logo_s3_key = "orgs/logo-test-org/logo/abc.png"
    org.logo_filename = "logo.png"
    org.save()
    return org


# ================================================================================
# upload_logo_from_file
# ================================================================================


def test_upload_logo_from_file_success(org):
    with (
        patch(
            "ddpui.core.orgfunctions._get_logo_bucket",
            return_value="test-bucket",
        ),
        patch(
            "ddpui.core.orgfunctions.upload_file",
            return_value="https://test-bucket.s3.ap-south-1.amazonaws.com/orgs/logo-test-org/logo/new.png",
        ),
    ):
        orgfunctions.upload_logo_from_file(
            file_bytes=b"fake",
            content_type="image/png",
            filename="logo.png",
            org=org,
        )

    org.refresh_from_db()
    assert (
        org.logo_url
        == "https://test-bucket.s3.ap-south-1.amazonaws.com/orgs/logo-test-org/logo/new.png"
    )
    assert org.logo_s3_key is not None and org.logo_s3_key.startswith("orgs/logo-test-org/logo/")
    assert org.logo_filename == "logo.png"


def test_upload_logo_from_file_validation_error(org):
    with pytest.raises(OrgLogoValidationError, match="Invalid file type"):
        orgfunctions.upload_logo_from_file(
            file_bytes=b"fake",
            content_type="application/pdf",
            filename="doc.pdf",
            org=org,
        )


def test_upload_logo_from_file_s3_error(org):
    with (
        patch(
            "ddpui.core.orgfunctions._get_logo_bucket",
            return_value="test-bucket",
        ),
        patch(
            "ddpui.core.orgfunctions.upload_file",
            side_effect=Exception("S3 connection refused"),
        ),
    ):
        with pytest.raises(OrgLogoS3Error):
            orgfunctions.upload_logo_from_file(
                file_bytes=b"fake",
                content_type="image/png",
                filename="logo.png",
                org=org,
            )


# ================================================================================
# upload_logo_from_url
# ================================================================================


def test_upload_logo_from_url_success(org):
    with patch(
        "ddpui.core.orgfunctions.dalgo_head",
        return_value={"content-type": "image/png"},
    ):
        orgfunctions.upload_logo_from_url("https://example.com/logo.png", org)

    org.refresh_from_db()
    assert org.logo_url == "https://example.com/logo.png"
    assert org.logo_s3_key is None
    assert org.logo_filename is None


def test_upload_logo_from_url_deletes_old_s3_key(org_with_logo):
    with (
        patch(
            "ddpui.core.orgfunctions.dalgo_head",
            return_value={"content-type": "image/png"},
        ),
        patch(
            "ddpui.core.orgfunctions._get_logo_bucket",
            return_value="test-bucket",
        ),
        patch("ddpui.core.orgfunctions.delete_file") as mock_delete,
    ):
        orgfunctions.upload_logo_from_url("https://example.com/new.png", org_with_logo)

    mock_delete.assert_called_once_with("test-bucket", "orgs/logo-test-org/logo/abc.png")
    org_with_logo.refresh_from_db()
    assert org_with_logo.logo_url == "https://example.com/new.png"
    assert org_with_logo.logo_s3_key is None


def test_upload_logo_from_url_no_s3_delete_when_no_prior_key(org):
    with (
        patch(
            "ddpui.core.orgfunctions.dalgo_head",
            return_value={"content-type": "image/png"},
        ),
        patch("ddpui.core.orgfunctions.delete_file") as mock_delete,
    ):
        orgfunctions.upload_logo_from_url("https://example.com/logo.png", org)

    mock_delete.assert_not_called()


# ================================================================================
# delete_logo
# ================================================================================


def test_delete_logo_raises_when_no_logo(org):
    with pytest.raises(OrgLogoNotFoundError):
        orgfunctions.delete_logo(org)


def test_delete_logo_success(org_with_logo):
    with (
        patch(
            "ddpui.core.orgfunctions._get_logo_bucket",
            return_value="test-bucket",
        ),
        patch("ddpui.core.orgfunctions.delete_file") as mock_delete,
    ):
        orgfunctions.delete_logo(org_with_logo)

    mock_delete.assert_called_once_with("test-bucket", "orgs/logo-test-org/logo/abc.png")
    org_with_logo.refresh_from_db()
    assert org_with_logo.logo_url is None
    assert org_with_logo.logo_s3_key is None
    assert org_with_logo.logo_filename is None


def test_delete_logo_s3_error_raises(org_with_logo):
    with (
        patch(
            "ddpui.core.orgfunctions._get_logo_bucket",
            return_value="test-bucket",
        ),
        patch(
            "ddpui.core.orgfunctions.delete_file",
            side_effect=Exception("S3 unavailable"),
        ),
    ):
        with pytest.raises(OrgLogoS3Error):
            orgfunctions.delete_logo(org_with_logo)
