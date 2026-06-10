"""S3 utility functions for media uploads (org logos, etc.)"""

import os
import uuid
import logging

from ddpui.utils.aws_client import AWSClient

logger = logging.getLogger(__name__)

ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp"}
MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024  # 5MB

CONTENT_TYPE_TO_EXT = {
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/gif": "gif",
    "image/webp": "webp",
}


def _get_media_bucket() -> str:
    bucket = os.getenv("S3_IMAGES")
    if not bucket:
        raise ValueError("S3_IMAGES environment variable is not set")
    return bucket


def _get_s3_region() -> str:
    return os.getenv("AWS_DEFAULT_REGION", "ap-south-1")


def _build_s3_url(bucket: str, region: str, s3_key: str) -> str:
    return f"https://{bucket}.s3.{region}.amazonaws.com/{s3_key}"


def upload_org_logo(
    file_bytes: bytes,
    content_type: str,
    org_slug: str,
) -> tuple[str, str]:
    """Upload an org logo to S3.

    Args:
        file_bytes: Raw image bytes
        content_type: MIME type of the image (e.g. image/png)
        org_slug: Org slug used to namespace the S3 key

    Returns:
        Tuple of (logo_url, s3_key)

    Raises:
        ValueError: If content type is not allowed or file exceeds size limit
    """
    if content_type not in ALLOWED_CONTENT_TYPES:
        raise ValueError(
            f"Invalid file type: {content_type}. Allowed types: {', '.join(ALLOWED_CONTENT_TYPES)}"
        )

    if len(file_bytes) > MAX_FILE_SIZE_BYTES:
        raise ValueError(f"File size exceeds the 5MB limit")

    ext = CONTENT_TYPE_TO_EXT[content_type]
    s3_key = f"orgs/{org_slug}/logo/{uuid.uuid4()}.{ext}"
    bucket = _get_media_bucket()
    region = _get_s3_region()

    s3 = AWSClient.get_instance("s3")
    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=file_bytes,
        ContentType=content_type,
    )

    logo_url = _build_s3_url(bucket, region, s3_key)
    logger.info(f"Uploaded org logo for {org_slug} to s3://{bucket}/{s3_key}")
    return logo_url, s3_key


def delete_org_logo(s3_key: str) -> None:
    """Delete an org logo from S3 by its key.

    Args:
        s3_key: The S3 object key to delete
    """
    bucket = _get_media_bucket()
    s3 = AWSClient.get_instance("s3")
    s3.delete_object(Bucket=bucket, Key=s3_key)
    logger.info(f"Deleted org logo s3://{bucket}/{s3_key}")
