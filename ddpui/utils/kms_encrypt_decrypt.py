import os
import json
import base64
import boto3

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

    data = json.dumps(data)

    res = kms_client.encrypt(
        KeyId=kms_key_id,
        Plaintext=data,
    )

    if isinstance(res, dict) is False or "CiphertextBlob" not in res:
        raise Exception("Something went wrong with the encryption")

    return res["CiphertextBlob"]


def decrypt_dict(cipher_text):
    """Decrypt string or dictionary data using symmetric key"""

    res = kms_client.decrypt(KeyId=kms_key_id, CiphertextBlob=cipher_text)

    if isinstance(res, dict) is False or "Plaintext" not in res:
        raise Exception("Something went wrong with the encryption")

    return json.loads(res["Plaintext"])
