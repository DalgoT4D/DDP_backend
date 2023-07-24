import os
import json

from ddpui.utils.secretsmanager import DevSecretsManager


def test_secretfile():
    """verify the filename of the secret file"""
    dsm = DevSecretsManager()
    filename = dsm.secretfile("SECRETID")
    dev_secrets_dir = os.getenv("DEV_SECRETS_DIR")
    assert filename == f"{dev_secrets_dir}/SECRETID"


def test_create_secret():
    """verifies the response as well as the existence of the secret file"""
    dsm = DevSecretsManager()
    secret = dsm.create_secret(Name="secret-name", SecretString="secret-string")
    assert secret["Name"] == "secret-name"
    assert secret["SecretString"] == "secret-string"

    secretfile = dsm.secretfile(secret["SecretId"])
    with open(secretfile, "r", encoding="utf-8") as infile:
        read_secret = json.load(infile)
        assert read_secret["Name"] == secret["Name"]
        assert read_secret["SecretString"] == secret["SecretString"]
        assert read_secret["SecretId"] == secret["SecretId"]

    os.unlink(secretfile)


def test_delete_secret():
    """deletes the file on disk holding a secret"""
    dsm = DevSecretsManager()
    secret = dsm.create_secret(Name="secret-name", SecretString="secret-string")
    assert os.path.exists(dsm.secretfile(secret["SecretId"]))
    dsm.delete_secret(SecretId=secret["SecretId"])
    assert not os.path.exists(dsm.secretfile(secret["SecretId"]))


def test_update_secret():
    """updates a secret stored on disk"""
    dsm = DevSecretsManager()
    secret = dsm.create_secret(Name="secret-name", SecretString="secret-string")
    dsm.update_secret(SecretId=secret["SecretId"], SecretString="new-secret-string")

    secretfile = dsm.secretfile(secret["SecretId"])
    with open(secretfile, "r", encoding="utf-8") as infile:
        read_secret = json.load(infile)
        assert read_secret["SecretString"] == "new-secret-string"

    os.unlink(secretfile)


def test_get_secret_value():
    """verifies the value of the secret stored on disk"""
    dsm = DevSecretsManager()
    secret = dsm.create_secret(Name="secret-name", SecretString='{"key": "value"}')
    secretvalue = dsm.get_secret_value(SecretId=secret["SecretId"])

    secretfile = dsm.secretfile(secret["SecretId"])
    with open(secretfile, "r", encoding="utf-8") as infile:
        read_secret = json.load(infile)
        assert read_secret == secretvalue

    os.unlink(secretfile)
