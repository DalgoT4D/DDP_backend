import os
import json
from uuid import uuid4
import boto3
from ddpui.utils.custom_logger import CustomLogger
from ddpui.models.org import Org, OrgWarehouse, OrgDbt

logger = CustomLogger("ddpui")


class DevSecretsManager:
    """a stub class to avoid AWS costs in development"""

    def __init__(self):
        dev_secrets_dir = os.getenv("DEV_SECRETS_DIR")
        if dev_secrets_dir is None:
            raise ValueError("DEV_SECRETS_DIR not set in environment")
        if not os.path.exists(dev_secrets_dir):
            logger.info(f"created dev-secrets-dir {dev_secrets_dir}")
            os.makedirs(dev_secrets_dir)

        self.dev_secrets_dir = dev_secrets_dir

    def secretfile(self, secretid):
        """returns the filename under which the secret is stored"""
        return f"{self.dev_secrets_dir}/{secretid}"

    def create_secret(self, **kwargs):
        """save a secret to a file on disk"""
        name = kwargs["Name"]
        secretstring = kwargs["SecretString"]
        secretid = name
        secret = {"Name": name, "SecretId": secretid, "SecretString": secretstring}
        with open(self.secretfile(secretid), "w", encoding="utf-8") as outfile:
            json.dump(secret, outfile)
            logger.info(f"created dev-secret {secret['SecretId']}")
        return secret

    def delete_secret(self, **kwargs):
        """delete the secret file on disk"""
        secretfile = self.secretfile(kwargs["SecretId"])
        if os.path.exists(secretfile):
            logger.info(f"deleted dev-secret {secretfile}")
            os.unlink(secretfile)

    def update_secret(self, **kwargs):
        """updates secret in the file on disk"""
        secretid = kwargs["SecretId"]
        secretstring = kwargs["SecretString"]
        with open(self.secretfile(secretid), "r", encoding="utf-8") as infile:
            secret = json.load(infile)
        secret["SecretString"] = secretstring
        with open(self.secretfile(secretid), "w", encoding="utf-8") as outfile:
            json.dump(secret, outfile)
        return secret

    def get_secret_value(self, **kwargs):
        """retrieves the value of the secret"""
        secretid = kwargs["SecretId"]
        if not os.path.exists(self.secretfile(secretid)):
            return None
        with open(self.secretfile(secretid), "r", encoding="utf-8") as infile:
            secret = json.load(infile)
            return secret
            # secretstring = secret["SecretString"]
            # return json.loads(secretstring)


# =============================================================================
def get_client():
    """
    in production, creates a boto3 client for AWS Secrets Manager in ap-south-1
    in development, creates a DevSecretsManager instance
    """
    if os.getenv("USE_AWS_SECRETS_MANAGER") == "True":
        logger.info("using aws secretsmanager")
        secretsmanager = boto3.client(
            "secretsmanager",
            "ap-south-1",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
    else:
        logger.info("using dev secretsmanager")
        secretsmanager = DevSecretsManager()
    return secretsmanager


def generate_github_token_name(org: Org):
    """
    for orgs whose github repos require an access token,
    we store the token in AWS secrets manager using this name
    """
    return f"gitrepoAccessToken-{org.slug}-{uuid4()}"


def generate_warehouse_credentials_name(org: Org):
    """store the connection credentials to their data warehouse"""
    return f"warehouseCreds-{org.slug}-{uuid4()}"


def save_github_token(org: Org, access_token: str):
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


def retrieve_github_token(org_dbt: OrgDbt) -> str | None:
    """retreive the github token if present otherwise return None"""
    secret_name = org_dbt.gitrepo_access_token_secret
    if secret_name is not None:
        aws_sm = get_client()
        try:
            response = aws_sm.get_secret_value(SecretId=secret_name)
            logger.info("Git token fetched from secrets manager")
            return response["SecretString"] if "SecretString" in response else None
        except Exception:  # skipcq PYL-W0703
            # no secret available by the secret_name
            logger.info("Could not find the secret")

    return None


def delete_github_token(org: Org):
    """deletes a secret corresponding to a github auth token for an org, if it exists"""
    if org.dbt and org.dbt.gitrepo_access_token_secret:
        aws_sm = get_client()
        secret_name = org.dbt.gitrepo_access_token_secret
        try:
            aws_sm.delete_secret(SecretId=secret_name)
        except Exception:  # skipcq PYL-W0703
            # no secret to delete, carry on
            pass
        org.dbt.gitrepo_access_token_secret = None
        org.dbt.save()


def save_warehouse_credentials(warehouse: OrgWarehouse, credentials: dict):
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


def update_warehouse_credentials(warehouse: OrgWarehouse, credentials: dict):
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


def retrieve_warehouse_credentials(warehouse: OrgWarehouse) -> dict | None:
    """decodes and returns the saved warehouse credentials for an org"""
    aws_sm = get_client()
    response = aws_sm.get_secret_value(SecretId=warehouse.credentials)
    return json.loads(response["SecretString"]) if "SecretString" in response else None


def delete_warehouse_credentials(warehouse: OrgWarehouse) -> None:
    """deletes the secret from SM corresponding to a warehouse's credentials"""
    aws_sm = get_client()
    try:
        aws_sm.delete_secret(SecretId=warehouse.credentials)
    except Exception:  # skipcq PYL-W0703
        pass
