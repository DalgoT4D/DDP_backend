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

    Credential Types and Environment Variables:
    - "default": Uses AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY
    - "s3": Uses S3_AWS_ACCESS_KEY_ID & S3_AWS_SECRET_ACCESS_KEY
    - "ses": Uses SES_ACCESS_KEY_ID & SES_SECRET_ACCESS_KEY

    Each credential type maintains its own boto3 session and client cache.
    """

    _default_lock = threading.Lock()
    _s3_lock = threading.Lock()
    _ses_lock = threading.Lock()

    _default_boto_session = None
    _s3_boto_session = None
    _ses_boto_session = None

    _default_clients = {}
    _s3_clients = {}
    _ses_clients = {}

    # Supported AWS services
    SUPPORTED_SERVICES = {"s3", "ses", "secretsmanager"}

    @classmethod
    def get_instance(cls, service_name: str, credential_type: str = "default"):
        """
        Returns the AWS service client instance.

        Args:
            service_name: AWS service name (e.g., 's3', 'ses', 'secretsmanager')
            credential_type: Type of credentials to use ("default", "s3", "ses")

        Returns:
            boto3 client for the specified service

        Raises:
            ValueError: If service_name is not supported or credential_type is invalid
        """
        if service_name not in cls.SUPPORTED_SERVICES:
            raise ValueError(
                f"Unsupported service: {service_name}. Supported services: {cls.SUPPORTED_SERVICES}"
            )

        if credential_type == "default":
            return cls._get_default_client(service_name)
        elif credential_type == "s3":
            return cls._get_s3_client(service_name)
        elif credential_type == "ses":
            return cls._get_ses_client(service_name)
        else:
            raise ValueError(
                f"Invalid credential_type: {credential_type}. Use 'default', 's3', or 'ses'"
            )

    @classmethod
    def _get_default_client(cls, service_name: str):
        """Get client using default AWS credentials"""
        if cls._default_boto_session is None:
            if cls._default_lock.acquire(timeout=10):
                if cls._default_boto_session is None:
                    cls._initialize_default_boto_session()
                cls._default_lock.release()

        if service_name not in cls._default_clients:
            cls._default_clients[service_name] = cls._default_boto_session.client(service_name)
            logger.debug(f"Created default AWS {service_name} client")

        return cls._default_clients[service_name]

    @classmethod
    def _get_s3_client(cls, service_name: str):
        """Get client using S3 AWS credentials"""
        if cls._s3_boto_session is None:
            if cls._s3_lock.acquire(timeout=10):
                if cls._s3_boto_session is None:
                    cls._initialize_s3_boto_session()
                cls._s3_lock.release()

        if service_name not in cls._s3_clients:
            cls._s3_clients[service_name] = cls._s3_boto_session.client(service_name)
            logger.debug(f"Created S3 AWS {service_name} client")

        return cls._s3_clients[service_name]

    @classmethod
    def _get_ses_client(cls, service_name: str):
        """Get client using SES AWS credentials"""
        if cls._ses_boto_session is None:
            if cls._ses_lock.acquire(timeout=10):
                if cls._ses_boto_session is None:
                    cls._initialize_ses_boto_session()
                cls._ses_lock.release()

        if service_name not in cls._ses_clients:
            cls._ses_clients[service_name] = cls._ses_boto_session.client(service_name)
            logger.debug(f"Created SES AWS {service_name} client")

        return cls._ses_clients[service_name]

    @classmethod
    def _initialize_default_boto_session(cls):
        """Initialize default AWS boto3 session with credentials"""
        region = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        if not aws_access_key_id or not aws_secret_access_key:
            raise ValueError(
                "Missing default AWS credentials: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY required"
            )

        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region,
        )
        cls._default_boto_session = session
        logger.debug(f"Initialized default AWS session for region {region}")

    @classmethod
    def _initialize_s3_boto_session(cls):
        """Initialize S3 AWS boto3 session with credentials"""
        region = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
        aws_access_key_id = os.getenv("S3_AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("S3_AWS_SECRET_ACCESS_KEY")

        if not aws_access_key_id or not aws_secret_access_key:
            raise ValueError(
                "Missing S3 AWS credentials: S3_AWS_ACCESS_KEY_ID and S3_AWS_SECRET_ACCESS_KEY required"
            )

        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region,
        )
        cls._s3_boto_session = session
        logger.debug(f"Initialized S3 AWS session for region {region}")

    @classmethod
    def _initialize_ses_boto_session(cls):
        """Initialize SES AWS boto3 session with credentials"""
        region = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
        aws_access_key_id = os.getenv("SES_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("SES_SECRET_ACCESS_KEY")

        if not aws_access_key_id or not aws_secret_access_key:
            raise ValueError(
                "Missing SES AWS credentials: SES_ACCESS_KEY_ID and SES_SECRET_ACCESS_KEY required"
            )

        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region,
        )
        cls._ses_boto_session = session
        logger.debug(f"Initialized SES AWS session for region {region}")

    @classmethod
    def reset_instance(cls):
        """Reset all sessions and clients"""
        # Release any locks that might be held
        try:
            if cls._default_lock.locked():
                cls._default_lock.release()
        except:
            pass
        try:
            if cls._s3_lock.locked():
                cls._s3_lock.release()
        except:
            pass
        try:
            if cls._ses_lock.locked():
                cls._ses_lock.release()
        except:
            pass

        cls._default_boto_session = None
        cls._s3_boto_session = None
        cls._ses_boto_session = None
        cls._default_clients = {}
        cls._s3_clients = {}
        cls._ses_clients = {}
