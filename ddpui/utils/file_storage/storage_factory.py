"""Factory for creating storage adapters based on environment configuration"""

import os
import logging
from enum import Enum

from ddpui.utils.file_storage.storage_interface import StorageInterface
from ddpui.utils.file_storage.local_storage import LocalStorageAdapter
from ddpui.utils.file_storage.s3_storage import S3StorageAdapter

logger = logging.getLogger(__name__)


class StorageType(Enum):
    """Supported storage adapter types"""

    LOCAL = "local"
    S3 = "s3"


class StorageFactory:
    """Factory class for creating storage adapters"""

    @classmethod
    def get_storage_adapter(cls) -> StorageInterface:
        """
        Get storage adapter based on environment configuration

        Returns:
            StorageInterface: New storage adapter instance

        Environment Variables (add to .env.template):
            # Storage Configuration
            # Use "local" for development, "s3" for UAT/production
            STORAGE_BACKEND=local

            # S3 Configuration (required when STORAGE_BACKEND=s3)
            S3_BUCKET_NAME=dalgo-client-storage
            S3_PREFIX=clients/
        """
        storage_backend_str = os.getenv("STORAGE_BACKEND", StorageType.LOCAL.value).lower()

        # Validate storage backend
        try:
            storage_backend = StorageType(storage_backend_str)
        except ValueError:
            supported_types = [t.value for t in StorageType]
            raise ValueError(
                f"Unsupported storage backend: {storage_backend_str}. Supported types: {supported_types}"
            )

        if storage_backend == StorageType.S3:
            logger.info("Creating S3 storage adapter")
            bucket_name = os.getenv("S3_BUCKET_NAME")
            if not bucket_name:
                raise ValueError(
                    "S3_BUCKET_NAME environment variable is required when using S3 storage"
                )

            prefix = os.getenv("S3_PREFIX", "clients/")
            return S3StorageAdapter(bucket_name=bucket_name, prefix=prefix)

        elif storage_backend == StorageType.LOCAL:
            logger.info("Creating local storage adapter")
            return LocalStorageAdapter()

        else:
            # This shouldn't happen due to enum validation above, but keeping for safety
            supported_types = [t.value for t in StorageType]
            raise ValueError(
                f"Unsupported storage backend: {storage_backend}. Supported types: {supported_types}"
            )
