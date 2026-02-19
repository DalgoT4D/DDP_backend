"""S3 storage adapter"""

import os
import logging
from typing import List, Optional
from botocore.exceptions import ClientError

from ddpui.utils.aws_client import AWSClient
from ddpui.utils.file_storage.storage_interface import StorageInterface

logger = logging.getLogger(__name__)


class S3StorageAdapter(StorageInterface):
    """S3 storage implementation"""

    def __init__(self, bucket_name: str, prefix: str = ""):
        """
        Initialize S3 storage adapter

        Args:
            bucket_name: S3 bucket name
            prefix: Optional prefix for all keys (e.g., "clients/")
        """
        self.bucket_name = bucket_name
        self.prefix = prefix.rstrip("/") + "/" if prefix else ""
        self._s3_client = None

    @property
    def s3_client(self):
        """Lazy-loaded S3 client"""
        if self._s3_client is None:
            self._s3_client = AWSClient.get_instance("s3")
        return self._s3_client

    def _get_s3_key(self, file_path: str) -> str:
        """Convert file path to S3 key"""
        # Remove leading slash and combine with prefix
        clean_path = file_path.lstrip("/")
        return f"{self.prefix}{clean_path}"

    def _get_local_path(self, s3_key: str) -> str:
        """Convert S3 key back to local path format"""
        if s3_key.startswith(self.prefix):
            return s3_key[len(self.prefix) :]
        return s3_key

    def _delete_directory(self, s3_prefix: str) -> None:
        """Delete all objects with given prefix (directory)"""
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=s3_prefix)

            objects_to_delete = []
            for page in pages:
                if "Contents" in page:
                    objects_to_delete.extend([{"Key": obj["Key"]} for obj in page["Contents"]])

            if objects_to_delete:
                self.s3_client.delete_objects(
                    Bucket=self.bucket_name, Delete={"Objects": objects_to_delete}
                )
                logger.debug(
                    f"Deleted {len(objects_to_delete)} objects from S3 prefix: {s3_prefix}"
                )
        except Exception as e:
            raise IOError(f"Failed to delete directory {s3_prefix}: {str(e)}")

    def _key_exists(self, s3_key: str) -> bool:
        """Check if S3 key exists"""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def _matches_pattern(self, file_path: str, pattern: str) -> bool:
        """Simple pattern matching (basic glob support)"""
        import fnmatch

        return fnmatch.fnmatch(os.path.basename(file_path), pattern)

    def read_file(self, file_path: str) -> str:
        """Read content from S3"""
        s3_key = self._get_s3_key(file_path)
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            return response["Body"].read().decode("utf-8")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(f"File not found: {file_path}")
            else:
                raise IOError(f"Failed to read file {file_path}: {str(e)}")
        except Exception as e:
            raise IOError(f"Failed to read file {file_path}: {str(e)}")

    def write_file(self, file_path: str, content: str) -> None:
        """Write content to S3"""
        s3_key = self._get_s3_key(file_path)
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=content.encode("utf-8"),
                ContentType="text/plain",
            )
            logger.debug(f"Wrote file to S3: s3://{self.bucket_name}/{s3_key}")
        except Exception as e:
            raise IOError(f"Failed to write file {file_path}: {str(e)}")

    def delete_file(self, file_path: str) -> None:
        """Delete file from S3"""
        s3_key = self._get_s3_key(file_path)
        try:
            # Check if it's a directory (ends with /) or single file
            if file_path.endswith("/"):
                # Delete all objects with this prefix
                self._delete_directory(s3_key)
            else:
                # Check if file exists first
                if not self._key_exists(s3_key):
                    raise FileNotFoundError(f"File not found: {file_path}")

                self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
                logger.debug(f"Deleted file from S3: s3://{self.bucket_name}/{s3_key}")
        except FileNotFoundError:
            raise
        except Exception as e:
            raise IOError(f"Failed to delete file {file_path}: {str(e)}")

    def exists(self, file_path: str) -> bool:
        """Check if file or directory exists in S3"""
        s3_key = self._get_s3_key(file_path)

        # Check if it's a single file
        if self._key_exists(s3_key):
            return True

        # Check if it's a directory (has objects with this prefix)
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=s3_key, MaxKeys=1
            )
            return "Contents" in response and len(response["Contents"]) > 0
        except Exception:
            return False

    def list_files(self, directory_path: str, pattern: Optional[str] = None) -> List[str]:
        """List files in S3 directory"""
        s3_prefix = self._get_s3_key(directory_path)
        if not s3_prefix.endswith("/"):
            s3_prefix += "/"

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=s3_prefix)

            files = []
            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        # Convert back to local path format
                        local_path = self._get_local_path(obj["Key"])

                        # Skip directories (keys ending with /)
                        if not local_path.endswith("/"):
                            # Apply pattern filter if provided
                            if pattern is None or self._matches_pattern(local_path, pattern):
                                files.append(local_path)

            return files
        except Exception as e:
            raise IOError(f"Failed to list files in {directory_path}: {str(e)}")

    def create_directory(self, directory_path: str) -> None:
        """Create directory in S3 (create empty object with trailing /)"""
        s3_key = self._get_s3_key(directory_path)
        if not s3_key.endswith("/"):
            s3_key += "/"

        try:
            self.s3_client.put_object(Bucket=self.bucket_name, Key=s3_key, Body=b"")
            logger.debug(f"Created directory in S3: s3://{self.bucket_name}/{s3_key}")
        except Exception as e:
            raise IOError(f"Failed to create directory {directory_path}: {str(e)}")

    def copy_file(self, source_path: str, destination_path: str) -> None:
        """Copy file within S3"""
        source_key = self._get_s3_key(source_path)
        dest_key = self._get_s3_key(destination_path)

        try:
            if not self._key_exists(source_key):
                raise FileNotFoundError(f"Source file not found: {source_path}")

            copy_source = {"Bucket": self.bucket_name, "Key": source_key}
            self.s3_client.copy_object(
                CopySource=copy_source, Bucket=self.bucket_name, Key=dest_key
            )
            logger.debug(f"Copied file in S3: {source_key} -> {dest_key}")
        except FileNotFoundError:
            raise
        except Exception as e:
            raise IOError(f"Failed to copy file from {source_path} to {destination_path}: {str(e)}")

    def copy_tree(self, source_directory: str, destination_directory: str) -> None:
        """Copy entire directory tree within S3"""
        source_prefix = self._get_s3_key(source_directory)
        if not source_prefix.endswith("/"):
            source_prefix += "/"

        dest_prefix = self._get_s3_key(destination_directory)
        if not dest_prefix.endswith("/"):
            dest_prefix += "/"

        try:
            # List all objects with source prefix
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=source_prefix)

            copied_count = 0
            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        source_key = obj["Key"]
                        # Replace source prefix with destination prefix
                        dest_key = dest_prefix + source_key[len(source_prefix) :]

                        copy_source = {"Bucket": self.bucket_name, "Key": source_key}
                        self.s3_client.copy_object(
                            CopySource=copy_source, Bucket=self.bucket_name, Key=dest_key
                        )
                        copied_count += 1

            if copied_count == 0:
                raise FileNotFoundError(f"Source directory not found: {source_directory}")

            logger.debug(
                f"Copied {copied_count} files from {source_directory} to {destination_directory}"
            )

        except FileNotFoundError:
            raise
        except Exception as e:
            raise IOError(
                f"Failed to copy tree from {source_directory} to {destination_directory}: {str(e)}"
            )
