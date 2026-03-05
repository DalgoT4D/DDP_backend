"""Unified AWS client for all AWS services used in Dalgo"""

import os
import logging
import threading
import boto3

logger = logging.getLogger(__name__)


class AWSClient:
    """
    Singleton AWS client with session management.
    Use this class anywhere in the code to interact with AWS services.

    Environment Variables by Service:
    - secretsmanager: Uses SECRETSMANAGER_ACCESS_KEY_ID & SECRETSMANAGER_SECRET_ACCESS_KEY
    - s3: Uses S3_AWS_ACCESS_KEY_ID & S3_AWS_SECRET_ACCESS_KEY
    - ses: Uses SES_ACCESS_KEY_ID & SES_SECRET_ACCESS_KEY

    Each service maintains its own boto3 session and client cache.
    """

    _locks = {"secretsmanager": threading.Lock(), "s3": threading.Lock(), "ses": threading.Lock()}

    _sessions = {"secretsmanager": None, "s3": None, "ses": None}

    _clients = {}

    # Supported AWS services
    SUPPORTED_SERVICES = {"s3", "ses", "secretsmanager"}

    @classmethod
    def get_instance(cls, service_name: str):
        """
        Returns the AWS service client instance.

        Args:
            service_name: AWS service name (e.g., 's3', 'ses', 'secretsmanager')

        Returns:
            boto3 client for the specified service

        Raises:
            ValueError: If service_name is not supported
        """
        if service_name not in cls.SUPPORTED_SERVICES:
            raise ValueError(
                f"Unsupported service: {service_name}. Supported services: {cls.SUPPORTED_SERVICES}"
            )

        return cls._get_client(service_name)

    @classmethod
    def _get_client(cls, service_name: str):
        """Get client for the specified service"""
        if cls._sessions[service_name] is None:
            if cls._locks[service_name].acquire(timeout=10):
                try:
                    if cls._sessions[service_name] is None:
                        cls._initialize_session(service_name)
                finally:
                    cls._locks[service_name].release()

        if service_name not in cls._clients:
            cls._clients[service_name] = cls._sessions[service_name].client(service_name)
            logger.debug(f"Created AWS {service_name} client")

        return cls._clients[service_name]

    @classmethod
    def _initialize_session(cls, service_name: str):
        """Initialize AWS boto3 session for the specified service"""
        region = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")

        # Get credentials based on service
        if service_name == "secretsmanager":
            access_key = os.getenv("SECRETSMANAGER_ACCESS_KEY_ID")
            secret_key = os.getenv("SECRETSMANAGER_SECRET_ACCESS_KEY")
            service_display = "Secrets Manager"
        elif service_name == "s3":
            access_key = os.getenv("S3_AWS_ACCESS_KEY_ID")
            secret_key = os.getenv("S3_AWS_SECRET_ACCESS_KEY")
            service_display = "S3"
        elif service_name == "ses":
            access_key = os.getenv("SES_ACCESS_KEY_ID")
            secret_key = os.getenv("SES_SECRET_ACCESS_KEY")
            service_display = "SES"
        else:
            raise ValueError(f"Unsupported service: {service_name}")

        if not access_key or not secret_key:
            raise ValueError(
                f"Missing {service_display} AWS credentials: {service_name.upper()}_ACCESS_KEY_ID and {service_name.upper()}_SECRET_ACCESS_KEY required"
            )

        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )
        cls._sessions[service_name] = session
        logger.debug(f"Initialized {service_display} AWS session for region {region}")

    @classmethod
    def reset_instance(cls):
        """Reset all sessions and clients"""
        # Release any locks that might be held
        for service_name in cls.SUPPORTED_SERVICES:
            try:
                if cls._locks[service_name].locked():
                    cls._locks[service_name].release()
            except:
                pass

        cls._sessions = {service: None for service in cls.SUPPORTED_SERVICES}
        cls._clients = {}
