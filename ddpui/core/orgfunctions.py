"""functions for working with Orgs"""

import os
import uuid
from urllib.parse import urlparse

from django.utils.text import slugify

from ddpui.ddpairbyte import airbytehelpers
from ddpui.utils.custom_logger import CustomLogger
from ddpui import settings
from ddpui.models.org import Org
from ddpui.schemas.org_schema import CreateOrgSchema
from ddpui.utils.constants import DALGO_WITH_SUPERSET, DALGO, FREE_TRIAL
from ddpui.models.org_plans import OrgPlans, OrgPlanType
from ddpui.celeryworkers.tasks import add_custom_connectors_to_workspace
from ddpui.utils.http import dalgo_get, dalgo_head
from ddpui.utils.s3_utils import upload_file, delete_file
from ddpui.core.org_logo.exceptions import (
    OrgLogoNotFoundError,
    OrgLogoValidationError,
    OrgLogoS3Error,
    OrgLogoFetchError,
)

logger = CustomLogger("ddpui")

ALLOWED_LOGO_CONTENT_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp", "image/svg+xml"}
MAX_LOGO_FILE_SIZE_BYTES = 5 * 1024 * 1024  # 5MB
LOGO_CONTENT_TYPE_TO_EXT = {
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/gif": "gif",
    "image/webp": "webp",
    "image/svg+xml": "svg",
}


def _get_logo_bucket() -> str:
    bucket = os.getenv("S3_IMAGES_BUCKET")
    if not bucket:
        raise OrgLogoS3Error("S3_IMAGES_BUCKET environment variable is not set")
    return bucket


def _replace_logo(org: Org, logo_url: str, s3_key: str, filename: str) -> None:
    """Delete the old S3 file if one exists, then persist the new logo on Org."""
    if org.logo_s3_key:
        try:
            delete_file(_get_logo_bucket(), org.logo_s3_key)
        except Exception as e:
            logger.warning(f"Failed to delete old logo from S3 for {org.slug}: {e}")

    org.logo_url = logo_url
    org.logo_s3_key = s3_key
    org.logo_filename = filename
    org.save(update_fields=["logo_url", "logo_s3_key", "logo_filename"])
    logger.info(f"Org logo saved for {org.slug}")


def get_logo_bytes(logo_url: str) -> tuple[bytes, str]:
    """Fetch raw logo bytes from the stored URL for server-side proxying."""
    try:
        resp = dalgo_get(logo_url, raw=True, timeout=10, stream=True)
    except Exception as e:
        raise OrgLogoFetchError("Failed to fetch logo from storage") from e
    if not resp.ok:
        resp.close()
        raise OrgLogoFetchError("Failed to fetch logo from storage")
    content_type = resp.headers.get("content-type", "image/png")
    chunks: list[bytes] = []
    total = 0
    try:
        for chunk in resp.iter_content(chunk_size=65536):
            total += len(chunk)
            if total > MAX_LOGO_FILE_SIZE_BYTES:
                raise OrgLogoFetchError("Logo exceeds maximum allowed size")
            chunks.append(chunk)
    except OrgLogoFetchError:
        raise
    except Exception as e:
        raise OrgLogoFetchError("Failed to read logo from storage") from e
    finally:
        resp.close()
    return b"".join(chunks), content_type


def upload_logo_from_file(file_bytes: bytes, content_type: str, filename: str, org: Org) -> Org:
    """Validate and upload an image file to S3, replacing any existing logo."""
    try:
        if content_type not in ALLOWED_LOGO_CONTENT_TYPES:
            raise ValueError(
                f"Invalid file type: {content_type}. Allowed types: {', '.join(ALLOWED_LOGO_CONTENT_TYPES)}"
            )
        if len(file_bytes) > MAX_LOGO_FILE_SIZE_BYTES:
            raise ValueError("File size exceeds the 5MB limit")

        ext = LOGO_CONTENT_TYPE_TO_EXT[content_type]
        s3_key = f"orgs/{org.slug}/logo/{uuid.uuid4()}.{ext}"
        bucket = _get_logo_bucket()
        logo_url = upload_file(bucket, s3_key, file_bytes, content_type)
        logger.info(f"Uploaded org logo for {org.slug} to s3://{bucket}/{s3_key}")
    except ValueError as e:
        raise OrgLogoValidationError(str(e)) from e
    except OrgLogoValidationError:
        raise
    except Exception as e:
        logger.error(f"S3 upload failed for {org.slug}: {e}")
        raise OrgLogoS3Error("Failed to upload logo to S3") from e

    _replace_logo(org, logo_url, s3_key, filename)
    return org


def upload_logo_from_url(image_url: str, org: Org) -> Org:
    """Validate and store an external image URL directly as the org logo — no S3 upload."""
    parsed = urlparse(image_url)
    if parsed.scheme not in ("http", "https"):
        raise OrgLogoValidationError("URL must use HTTP or HTTPS")

    try:
        response_headers = dalgo_head(image_url, timeout=10)
        content_type = response_headers.get("content-type", "").split(";")[0].strip()
    except Exception as e:
        raise OrgLogoValidationError("Could not verify the URL serves an image") from e

    if content_type not in ALLOWED_LOGO_CONTENT_TYPES:
        raise OrgLogoValidationError(
            f"URL does not point to a valid image. Allowed types: {', '.join(ALLOWED_LOGO_CONTENT_TYPES)}"
        )

    _replace_logo(org, logo_url=image_url, s3_key=None, filename=None)
    return org


def delete_logo(org: Org) -> None:
    """Delete the org logo from S3 and clear the logo fields on Org."""
    if not org.logo_url:
        raise OrgLogoNotFoundError()

    if org.logo_s3_key:
        try:
            delete_file(_get_logo_bucket(), org.logo_s3_key)
        except Exception as e:
            logger.error(f"S3 delete failed for {org.slug}: {e}")
            raise OrgLogoS3Error("Failed to delete logo from S3") from e

    org.logo_url = None
    org.logo_s3_key = None
    org.logo_filename = None
    org.save(update_fields=["logo_url", "logo_s3_key", "logo_filename"])
    logger.info(f"Org logo deleted for {org.slug}")


def create_organization(payload: CreateOrgSchema):
    """creates a new Org"""
    org = Org.objects.filter(name__iexact=payload.name).first()
    if org:
        return None, "client org with this name already exists"

    org = Org(
        name=payload.name,
        viz_url=str(payload.viz_url) if payload.viz_url else None,
        website=str(payload.website) if payload.website else None,
    )
    org.slug = slugify(org.name)[:20]
    org.save()

    try:
        workspace = airbytehelpers.setup_airbyte_workspace_v1(org.slug, org)
        # add custom sources to this workspace
        add_custom_connectors_to_workspace.delay(
            workspace.workspaceId, list(settings.AIRBYTE_CUSTOM_SOURCES.values())
        )

    except Exception as err:
        logger.error("could not create airbyte workspace: " + str(err))
        # delete the org or we won't be able to create it once airbyte comes back up
        org.delete()
        return None, "could not create airbyte workspace"

    org.refresh_from_db()

    return org, None


def create_org_plan(payload: CreateOrgSchema, org):
    """creates a new Org's plan"""
    existing_plan = OrgPlans.objects.filter(org=org).first()
    if existing_plan:
        return None, "client org's plan already exists"

    plan_payload = {
        "base_plan": payload.base_plan,
        "can_upgrade_plan": payload.can_upgrade_plan,
        "subscription_duration": payload.subscription_duration,
        "superset_included": payload.superset_included,
        "start_date": payload.start_date,
        "end_date": payload.end_date,
        "features": {},
    }

    if payload.base_plan == OrgPlanType.FREE_TRIAL:
        plan_payload["features"] = FREE_TRIAL
    elif payload.base_plan == OrgPlanType.INTERNAL:
        plan_payload["features"] = DALGO_WITH_SUPERSET
    elif payload.base_plan == OrgPlanType.DALGO and payload.superset_included:
        plan_payload["features"] = DALGO_WITH_SUPERSET
    elif payload.base_plan == OrgPlanType.DALGO and not payload.superset_included:
        plan_payload["features"] = DALGO

    org_plan = OrgPlans.objects.create(org=org, **plan_payload)

    return org_plan, None
