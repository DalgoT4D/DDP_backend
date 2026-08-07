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


def bulk_delete_files(bucket: str, keys: list[str]) -> None:
    """Delete many S3 objects in one call. S3's delete_objects accepts up to
    1000 keys per request — chunk larger inputs across multiple calls.
    No-op when keys is empty."""
    if not keys:
        return
    s3 = AWSClient.get_instance("s3")
    for i in range(0, len(keys), 1000):
        chunk = keys[i : i + 1000]
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in chunk]},
        )
    logger.info(f"Bulk-deleted {len(keys)} object(s) from s3://{bucket}")


def list_objects(
    bucket: str, prefix: str, start_after: str | None = None, max_keys: int = 100
) -> list[dict]:
    """List up to max_keys S3 objects matching prefix, in lexicographical order.

    If start_after is given, only keys strictly greater than it are returned
    (S3-native exclusive cursor).

    Returns the raw Contents list from list_objects_v2 — each entry has
    {"Key", "LastModified", "Size", "ETag", ...}. Empty list if nothing matches.
    """
    s3 = AWSClient.get_instance("s3")
    params: dict = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": max_keys}
    if start_after is not None:
        params["StartAfter"] = start_after
    resp = s3.list_objects_v2(**params)
    return resp.get("Contents", [])


def object_exists(bucket: str, key: str) -> bool:
    """Return True if an object at s3://bucket/key exists, False if it doesn't.

    Uses head_object (~1 API call, no download). Raises on any error other than
    a genuine "not found" so callers can distinguish empty state from failure.
    """
    from botocore.exceptions import ClientError

    s3 = AWSClient.get_instance("s3")
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as err:
        code = err.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


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
