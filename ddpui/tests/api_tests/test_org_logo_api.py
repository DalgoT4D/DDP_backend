"""API tests for org logo endpoints

The API layer is a thin wrapper — business logic is covered in tests/core/org_logo/test_org_logo_service.py.
These tests only verify the exception → HTTP status code mapping.

Tests:
1. upload_logo_file — OrgLogoValidationError → 400, OrgLogoS3Error → 502
2. upload_logo_from_url — OrgLogoValidationError → 400
3. delete_logo — OrgLogoNotFoundError → 404, OrgLogoS3Error → 502
"""

import os
import django
from unittest.mock import patch, MagicMock

import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.user_org_api import (
    upload_logo_file,
    upload_logo_from_url,
    delete_logo,
)
from ddpui.schemas.org_schema import OrgLogoUrlPayload
from ddpui.core.org_logo.exceptions import (
    OrgLogoNotFoundError,
    OrgLogoValidationError,
    OrgLogoS3Error,
)
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="logoapiuser", email="logoapiuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    o = Org.objects.create(name="Logo API Test Org", slug="logo-api-test-org")
    yield o
    o.delete()


@pytest.fixture
def orguser(authuser, org, seed_db):
    role = Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first()
    ou = OrgUser.objects.create(user=authuser, org=org, new_role=role)
    yield ou
    ou.delete()


# ================================================================================
# Exception → HTTP status code mapping
# ================================================================================


def test_upload_logo_file_validation_error_raises_400(orguser):
    mock_file = MagicMock()
    mock_file.read.return_value = b"fake"
    mock_file.content_type = "application/pdf"
    mock_file.name = "doc.pdf"

    with patch(
        "ddpui.api.user_org_api.orgfunctions.upload_logo_from_file",
        side_effect=OrgLogoValidationError("Invalid file type"),
    ):
        with pytest.raises(HttpError) as exc:
            upload_logo_file(mock_request(orguser), file=mock_file)
    assert exc.value.status_code == 400


def test_upload_logo_file_s3_error_raises_502(orguser):
    mock_file = MagicMock()
    mock_file.read.return_value = b"fake"
    mock_file.content_type = "image/png"
    mock_file.name = "logo.png"

    with patch(
        "ddpui.api.user_org_api.orgfunctions.upload_logo_from_file",
        side_effect=OrgLogoS3Error("S3 unavailable"),
    ):
        with pytest.raises(HttpError) as exc:
            upload_logo_file(mock_request(orguser), file=mock_file)
    assert exc.value.status_code == 502


def test_upload_logo_from_url_validation_error_raises_400(orguser):
    with patch(
        "ddpui.api.user_org_api.orgfunctions.upload_logo_from_url",
        side_effect=OrgLogoValidationError("Invalid URL"),
    ):
        with pytest.raises(HttpError) as exc:
            upload_logo_from_url(
                mock_request(orguser),
                payload=OrgLogoUrlPayload(image_url="https://example.com/logo.png"),
            )
    assert exc.value.status_code == 400


def test_delete_logo_not_found_raises_404(orguser):
    with patch(
        "ddpui.api.user_org_api.orgfunctions.delete_logo", side_effect=OrgLogoNotFoundError()
    ):
        with pytest.raises(HttpError) as exc:
            delete_logo(mock_request(orguser))
    assert exc.value.status_code == 404


def test_delete_logo_s3_error_raises_502(orguser):
    with patch(
        "ddpui.api.user_org_api.orgfunctions.delete_logo",
        side_effect=OrgLogoS3Error("S3 failed"),
    ):
        with pytest.raises(HttpError) as exc:
            delete_logo(mock_request(orguser))
    assert exc.value.status_code == 502
