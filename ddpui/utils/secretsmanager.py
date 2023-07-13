import os
import json
from uuid import uuid4
import boto3
from ddpui.utils.ddp_logger import logger


def get_client():
    """creates a boto3 client for AWS Secrets Manager in ap-south-1"""
    secretsmanager = boto3.client(
        "secretsmanager",
        "ap-south-1",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    return secretsmanager


def generate_github_token_name(org):
    """
    for orgs whose github repos require an access token,
    we store the token in AWS secrets manager using this name
    """
    return f"gitrepoAccessToken-{org.slug}-{uuid4()}"


def generate_warehouse_credentials_name(org):
    """store the connection credentials to their data warehouse"""
    return f"warehouseCreds-{org.slug}-{uuid4()}"


def save_github_token(org, access_token):
    """saves a github auth token for an org under a predefined secret name"""
    aws_sm = get_client()
    secret_name = generate_github_token_name(org)
    response = aws_sm.create_secret(
        Name=secret_name,
        SecretString=access_token,
    )
    logger.info(
        "saved github access token in secrets manager under name=" + response["Name"]
    )
    org.dbt.gitrepo_access_token_secret = secret_name
    org.dbt.save()


def delete_github_token(org):
    """deletes a secret corresponding to a github auth token for an org, if it exists"""
    if org.dbt and org.dbt.gitrepo_access_token_secret:
        aws_sm = get_client()
        secret_name = org.dbt.gitrepo_access_token_secret
        try:
            aws_sm.delete_secret(SecretId=secret_name)
        except Exception:
            # no secret to delete, carry on
            pass
        org.dbt.gitrepo_access_token_secret = None
        org.dbt.save()


def save_warehouse_credentials(warehouse, credentials: dict):
    """saves warehouse credentials for an org under a predefined secret name"""
    aws_sm = get_client()
    secret_name = generate_warehouse_credentials_name(warehouse.org)
    response = aws_sm.create_secret(
        Name=secret_name,
        SecretString=json.dumps(credentials),
    )
    logger.info(
        "saved warehouse credentials in secrets manager under name=" + response["Name"]
    )
    return secret_name


def update_warehouse_credentials(warehouse, credentials: dict):
    """udpates warehouse credentials for an org"""
    aws_sm = get_client()
    response = aws_sm.update_secret(
        SecretId=warehouse.credentials,
        SecretString=json.dumps(credentials),
    )
    logger.info(
        "updated warehouse credentials in secrets manager under name="
        + response["Name"]
    )


def retrieve_warehouse_credentials(warehouse) -> dict | None:
    """decodes and returns the saved warehouse credentials for an org"""
    aws_sm = get_client()
    response = aws_sm.get_secret_value(SecretId=warehouse.credentials)
    return json.loads(response["SecretString"]) if "SecretString" in response else None


def delete_warehouse_credentials(warehouse) -> None:
    """deletes the secret from SM corresponding to a warehouse's credentials"""
    aws_sm = get_client()
    try:
        aws_sm.delete_secret(SecretId=warehouse.credentials)
    except Exception:
        pass
