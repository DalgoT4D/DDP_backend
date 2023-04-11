import os
import json
import base64
import boto3
from botocore.exceptions import ClientError

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region = os.getenv("AWS_DEFAULT_REGION")
kms_key_id = os.getenv("KMS_KEY_ID")

kms_client = boto3.client(
    "kms",
    region_name=region,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)


def encrypt_dict(data):
    """Encrypt string or dictionary data using symmetric kms encryption"""

    if data is None or isinstance(data, dict) is False:
        raise Exception("Please make sure data is a json object")

    data = json.dumps(data)

    try:
        res = kms_client.encrypt(
            KeyId=kms_key_id,
            Plaintext=data,
        )
    except ClientError as cli_err:
        raise f"Couldn't encrypt the text : {cli_err.response['Error']['Message']}"
    else:
        if isinstance(res, dict) is False or "CiphertextBlob" not in res:
            raise Exception("Something went wrong with the encryption")

        return base64.b64encode(res["CiphertextBlob"])


def decrypt_dict(encoded_text):
    """Decrypt string or dictionary data using symmetric key"""

    cipher_text = base64.b64decode(encoded_text)

    try:
        res = kms_client.decrypt(KeyId=kms_key_id, CiphertextBlob=cipher_text)
    except ClientError as cli_err:
        raise f"Couldn't decrypt the text : {cli_err.response['Error']['Message']}"
    else:
        if isinstance(res, dict) is False or "Plaintext" not in res:
            raise Exception("Something went wrong with the encryption")

        return json.loads(res["Plaintext"])
