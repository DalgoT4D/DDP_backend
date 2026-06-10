"""Org logo service — business logic for logo upload and deletion"""

from ddpui.models.org import Org
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.s3_utils import upload_org_logo, delete_org_logo
from ddpui.core.org_logo.exceptions import (
    OrgLogoNotFoundError,
    OrgLogoValidationError,
    OrgLogoS3Error,
)

logger = CustomLogger("ddpui.core.org_logo")


class OrgLogoService:
    @staticmethod
    def get_logo(org: Org) -> Org:
        """Return the org if it has a logo, else raise OrgLogoNotFoundError."""
        if not org.logo_url:
            raise OrgLogoNotFoundError()
        return org

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
            logo_url, s3_key = upload_org_logo(
                file_bytes=file_bytes,
                content_type=content_type,
                org_slug=org.slug,
            )
        except ValueError as e:
            raise OrgLogoValidationError(str(e))
        except Exception as e:
            logger.error(f"S3 upload failed for {org.slug}: {e}")
            raise OrgLogoS3Error("Failed to upload logo to S3")

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
        # Delete old S3 file if the previous logo was uploaded (not a URL)
        if org.logo_s3_key:
            try:
                delete_org_logo(org.logo_s3_key)
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
                delete_org_logo(org.logo_s3_key)
            except Exception as e:
                logger.error(f"S3 delete failed for {org.slug}: {e}")
                raise OrgLogoS3Error("Failed to delete logo from S3")

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
                delete_org_logo(org.logo_s3_key)
            except Exception as e:
                logger.warning(f"Failed to delete old logo from S3 for {org.slug}: {e}")

        org.logo_url = logo_url
        org.logo_s3_key = s3_key
        org.logo_filename = filename
        org.save(update_fields=["logo_url", "logo_s3_key", "logo_filename"])
        logger.info(f"Org logo saved for {org.slug}")
