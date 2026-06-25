"""Generic S3 utility functions for file uploads and deletions."""

import os
import logging

from ddpui.utils.aws_client import AWSClient

logger = logging.getLogger(__name__)


def _get_s3_region() -> str:
    return os.getenv("AWS_DEFAULT_REGION", "ap-south-1")


def _build_s3_url(bucket: str, region: str, key: str) -> str:
    return f"https://{bucket}.s3.{region}.amazonaws.com/{key}"


def upload_file(bucket: str, key: str, file_bytes: bytes, content_type: str) -> str:
    """Upload any file to S3 and return its public URL.

    Args:
        bucket: S3 bucket name
        key: S3 object key (path within the bucket)
        file_bytes: Raw file bytes
        content_type: MIME type of the file

    Returns:
        Public URL of the uploaded file
    """
    region = _get_s3_region()
    s3 = AWSClient.get_instance("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=file_bytes, ContentType=content_type)
    url = _build_s3_url(bucket, region, key)
    logger.info(f"Uploaded file to s3://{bucket}/{key}")
    return url


def delete_file(bucket: str, key: str) -> None:
    """Delete any file from S3 by its key.

    Args:
        bucket: S3 bucket name
        key: S3 object key to delete
    """
    s3 = AWSClient.get_instance("s3")
    s3.delete_object(Bucket=bucket, Key=key)
    logger.info(f"Deleted file s3://{bucket}/{key}")


def download_file(bucket: str, key: str) -> dict:
    """Download a file from S3 and return the raw response.

    Args:
        bucket: S3 bucket name
        key: S3 object key to download

    Returns:
        Raw S3 response dict with Body, LastModified, ContentType, etc.
    """
    s3 = AWSClient.get_instance("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    logger.info(f"Downloaded file from s3://{bucket}/{key}")
    return response
