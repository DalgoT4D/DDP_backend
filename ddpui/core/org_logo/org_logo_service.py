"""Org logo service — business logic for logo upload and deletion"""

import os
import uuid
from urllib.parse import urlparse

from ddpui.utils.http import dalgo_get, dalgo_head

from ddpui.models.org import Org
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.s3_utils import upload_file, delete_file
from ddpui.core.org_logo.exceptions import (
    OrgLogoNotFoundError,
    OrgLogoValidationError,
    OrgLogoS3Error,
    OrgLogoFetchError,
)

logger = CustomLogger("ddpui.core.org_logo")

ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp", "image/svg+xml"}
MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024  # 5MB
CONTENT_TYPE_TO_EXT = {
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


class OrgLogoService:
    @staticmethod
    def get_logo_bytes(org: Org) -> tuple[bytes, str]:
        """Fetch raw logo bytes from the stored URL for server-side proxying."""
        if not org.logo_url:
            raise OrgLogoNotFoundError()
        try:
            resp = dalgo_get(org.logo_url, raw=True, timeout=10, stream=True)
        except Exception as e:
            raise OrgLogoFetchError("Failed to fetch logo from storage") from e
        if not resp.ok:
            resp.close()
            raise OrgLogoFetchError("Failed to fetch logo from storage")
        content_type = resp.headers.get("content-type", "image/png")
        chunks: list[bytes] = []
        total = 0
        try:
            for chunk in resp.iter_content(chunk_size=65536):  # 64 KB per read
                total += len(chunk)
                if total > MAX_FILE_SIZE_BYTES:
                    raise OrgLogoFetchError("Logo exceeds maximum allowed size")
                chunks.append(chunk)
        except OrgLogoFetchError:
            raise
        except Exception as e:
            raise OrgLogoFetchError("Failed to read logo from storage") from e
        finally:
            resp.close()
        return b"".join(chunks), content_type

    @staticmethod
    def upload_logo_from_file(file_bytes: bytes, content_type: str, filename: str, org: Org) -> Org:
        """Validate, upload file to S3, delete old logo, save URL on Org.

        Args:
            file_bytes: Raw image bytes from the uploaded file
            content_type: MIME type of the image
            filename: Original filename from the user's system
            org: The organization

        Returns:
            Updated Org instance

        Raises:
            OrgLogoValidationError: If file type or size is invalid
            OrgLogoS3Error: If S3 upload fails
        """
        try:
            if content_type not in ALLOWED_CONTENT_TYPES:
                raise ValueError(
                    f"Invalid file type: {content_type}. Allowed types: {', '.join(ALLOWED_CONTENT_TYPES)}"
                )
            if len(file_bytes) > MAX_FILE_SIZE_BYTES:
                raise ValueError("File size exceeds the 5MB limit")

            ext = CONTENT_TYPE_TO_EXT[content_type]
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

        OrgLogoService._replace_logo(org, logo_url, s3_key, filename)
        return org

    @staticmethod
    def upload_logo_from_url(image_url: str, org: Org) -> Org:
        """Store an external image URL directly in the DB — no S3 upload.

        Args:
            image_url: Public URL of the image to use as logo
            org: The organization

        Returns:
            Updated Org instance
        """
        parsed = urlparse(image_url)
        if parsed.scheme not in ("http", "https"):
            raise OrgLogoValidationError("URL must use HTTP or HTTPS")

        try:
            response_headers = dalgo_head(image_url, timeout=10)
            content_type = response_headers.get("content-type", "").split(";")[0].strip()
        except Exception as e:
            raise OrgLogoValidationError("Could not verify the URL serves an image") from e

        if content_type not in ALLOWED_CONTENT_TYPES:
            raise OrgLogoValidationError(
                f"URL does not point to a valid image. Allowed types: {', '.join(ALLOWED_CONTENT_TYPES)}"
            )

        if org.logo_s3_key:
            try:
                delete_file(_get_logo_bucket(), org.logo_s3_key)
            except Exception as e:
                logger.warning(f"Failed to delete old S3 logo for {org.slug}: {e}")

        org.logo_url = image_url
        org.logo_s3_key = None
        org.logo_filename = None
        org.save(update_fields=["logo_url", "logo_s3_key", "logo_filename"])
        logger.info(f"Org logo URL saved directly for {org.slug}")
        return org

    @staticmethod
    def delete_logo(org: Org) -> None:
        """Delete the org logo from S3 and clear the fields on Org.

        Args:
            org: The organization

        Raises:
            OrgLogoNotFoundError: If org has no logo
            OrgLogoS3Error: If S3 deletion fails
        """
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

    @staticmethod
    def _replace_logo(org: Org, logo_url: str, s3_key: str, filename: str) -> None:
        """Delete old S3 file if exists, then persist new logo on Org."""
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
