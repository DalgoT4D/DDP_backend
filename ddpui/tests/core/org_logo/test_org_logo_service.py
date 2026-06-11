"""Tests for OrgLogoService

Tests:
1. get_logo — org with logo returns org, org without logo raises OrgLogoNotFoundError
2. upload_logo_from_file — success, validation error (from s3), S3 error
3. upload_logo_from_url — success with no prior logo, deletes old S3 key if one exists
4. delete_logo — no logo raises not found, S3 delete failure raises OrgLogoS3Error, success clears fields
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
from ddpui.core.org_logo.org_logo_service import OrgLogoService
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
# get_logo
# ================================================================================


def test_get_logo_returns_org_when_logo_exists(org_with_logo):
    result = OrgLogoService.get_logo(org_with_logo)
    assert result == org_with_logo


def test_get_logo_raises_when_no_logo(org):
    with pytest.raises(OrgLogoNotFoundError):
        OrgLogoService.get_logo(org)


# ================================================================================
# upload_logo_from_file
# ================================================================================


def test_upload_logo_from_file_success(org):
    with (
        patch(
            "ddpui.core.org_logo.org_logo_service.upload_org_logo",
            return_value=("https://s3.example.com/logo.png", "orgs/logo-test-org/logo/new.png"),
        ),
        patch("ddpui.core.org_logo.org_logo_service.delete_org_logo"),
    ):
        result = OrgLogoService.upload_logo_from_file(
            file_bytes=b"fake",
            content_type="image/png",
            filename="logo.png",
            org=org,
        )

    org.refresh_from_db()
    assert org.logo_url == "https://s3.example.com/logo.png"
    assert org.logo_s3_key == "orgs/logo-test-org/logo/new.png"
    assert org.logo_filename == "logo.png"


def test_upload_logo_from_file_validation_error(org):
    with patch(
        "ddpui.core.org_logo.org_logo_service.upload_org_logo",
        side_effect=ValueError("Invalid file type"),
    ):
        with pytest.raises(OrgLogoValidationError, match="Invalid file type"):
            OrgLogoService.upload_logo_from_file(
                file_bytes=b"fake",
                content_type="application/pdf",
                filename="doc.pdf",
                org=org,
            )


def test_upload_logo_from_file_s3_error(org):
    with patch(
        "ddpui.core.org_logo.org_logo_service.upload_org_logo",
        side_effect=Exception("S3 connection refused"),
    ):
        with pytest.raises(OrgLogoS3Error):
            OrgLogoService.upload_logo_from_file(
                file_bytes=b"fake",
                content_type="image/png",
                filename="logo.png",
                org=org,
            )


# ================================================================================
# upload_logo_from_url
# ================================================================================


def test_upload_logo_from_url_success(org):
    result = OrgLogoService.upload_logo_from_url("https://example.com/logo.png", org)

    org.refresh_from_db()
    assert org.logo_url == "https://example.com/logo.png"
    assert org.logo_s3_key is None
    assert org.logo_filename is None


def test_upload_logo_from_url_deletes_old_s3_key(org_with_logo):
    with patch(
        "ddpui.core.org_logo.org_logo_service.delete_org_logo"
    ) as mock_delete:
        OrgLogoService.upload_logo_from_url("https://example.com/new.png", org_with_logo)

    mock_delete.assert_called_once_with("orgs/logo-test-org/logo/abc.png")
    org_with_logo.refresh_from_db()
    assert org_with_logo.logo_url == "https://example.com/new.png"
    assert org_with_logo.logo_s3_key is None


def test_upload_logo_from_url_no_s3_delete_when_no_prior_key(org):
    with patch(
        "ddpui.core.org_logo.org_logo_service.delete_org_logo"
    ) as mock_delete:
        OrgLogoService.upload_logo_from_url("https://example.com/logo.png", org)

    mock_delete.assert_not_called()


# ================================================================================
# delete_logo
# ================================================================================


def test_delete_logo_raises_when_no_logo(org):
    with pytest.raises(OrgLogoNotFoundError):
        OrgLogoService.delete_logo(org)


def test_delete_logo_success(org_with_logo):
    with patch("ddpui.core.org_logo.org_logo_service.delete_org_logo") as mock_delete:
        OrgLogoService.delete_logo(org_with_logo)

    mock_delete.assert_called_once_with("orgs/logo-test-org/logo/abc.png")
    org_with_logo.refresh_from_db()
    assert org_with_logo.logo_url is None
    assert org_with_logo.logo_s3_key is None
    assert org_with_logo.logo_filename is None


def test_delete_logo_s3_error_raises(org_with_logo):
    with patch(
        "ddpui.core.org_logo.org_logo_service.delete_org_logo",
        side_effect=Exception("S3 unavailable"),
    ):
        with pytest.raises(OrgLogoS3Error):
            OrgLogoService.delete_logo(org_with_logo)
