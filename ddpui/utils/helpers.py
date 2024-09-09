import shlex
import subprocess
import re
import string
import secrets
import hashlib
import json
from decimal import Decimal
from datetime import datetime, date
import csv
import io


def runcmd(cmd: str, cwd: str):
    """runs a shell command in a specified working directory"""
    return subprocess.run(shlex.split(cmd), cwd=str(cwd), check=True)


def runcmd_with_output(cmd, cwd):
    """
    runs a shell command in a specified working directory
    attempted to use Popen and then poll(), but poll() was blocking
    """
    return subprocess.run(shlex.split(cmd), cwd=str(cwd), capture_output=True)


def remove_nested_attribute(obj: dict, attr: str) -> dict:
    """
    this function searches for `attr` in the JSON object
    and removes any occurences it finds
    """
    if attr in obj:
        del obj[attr]

    for key in obj:
        val = obj[key]

        if isinstance(val, dict):
            obj[key] = remove_nested_attribute(val, attr)

        elif isinstance(val, list):
            for list_idx, list_val in enumerate(val):
                if isinstance(list_val, dict):
                    val[list_idx] = remove_nested_attribute(list_val, attr)

    return obj


def isvalid_email(email: str) -> bool:
    """
    this function uses a regex to check if the provided email
    address is valid
    ref: https://www.geeksforgeeks.org/check-if-email-address-valid-or-not-in-python/
    """
    regex = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b"

    return re.fullmatch(regex, email)


def generate_hash_id(l: int) -> str:
    """
    this function generates a random hash id of length `l`
    """

    alphabet = string.ascii_lowercase + string.digits + string.ascii_uppercase

    return "".join(secrets.choice(alphabet) for _ in range(l))


def cleaned_name_for_prefectblock(blockname):
    """removes characters which are not lowercase letters, digits or dashes. same helper from prefect-proxy"""
    pattern = re.compile(r"[^\-a-z0-9]")
    return re.sub(pattern, "", blockname.lower())


def map_airbyte_keys_to_postgres_keys(conn_info: dict):
    """called by `post_system_transformation_tasks` and `_get_wclient`"""
    if "tunnel_method" in conn_info:
        method = conn_info["tunnel_method"]

        if method["tunnel_method"] in ["SSH_KEY_AUTH", "SSH_PASSWORD_AUTH"]:
            conn_info["ssh_host"] = method["tunnel_host"]
            conn_info["ssh_port"] = method["tunnel_port"]
            conn_info["ssh_username"] = method["tunnel_user"]

        if method["tunnel_method"] == "SSH_KEY_AUTH":
            conn_info["ssh_pkey"] = method["ssh_key"]
            conn_info["ssh_private_key_password"] = method.get(
                "tunnel_private_key_password"
            )

        elif method["tunnel_method"] == "SSH_PASSWORD_AUTH":
            conn_info["ssh_password"] = method.get("tunnel_user_password")

    conn_info["user"] = conn_info["username"]

    return conn_info


def update_dict_but_not_stars(input_config: dict):
    """
    copies all the key-value pairs from `input_config` to `output_config`
    except for the ones where the value is "*****"
    """
    output_config: dict = {}
    for key, val in input_config.items():
        if val and isinstance(val, str):
            val = val.strip()
            if not re.match(r"^\*+$", val):
                output_config[key] = val
        elif val and isinstance(val, dict):
            output_config[key] = update_dict_but_not_stars(val)
        elif val and isinstance(val, list):
            output_config[key] = [
                update_dict_but_not_stars(item) if isinstance(item, dict) else item
                for item in val
            ]
        else:
            output_config[key] = val

    return output_config


def hash_dict(payload: dict) -> str:
    hasher = hashlib.sha256()

    hasher.update(json.dumps(payload, sort_keys=True).encode("utf-8"))

    return hasher.hexdigest()


def nice_bytes(n: int) -> str:
    """Convert bytes to string with appropriate units"""

    units = ["bytes", "KB", "MB", "GB", "TB", "PB"]

    l = 0

    while n >= 1024 and l < len(units):
        n = n / 1024
        l += 1

    return str(round(n, 2)) + " " + units[l]


def convert_to_standard_types(obj):
    """convert a sql alchemy python types to json serializable types"""
    if obj is None:
        return obj
    if isinstance(obj, Decimal):
        return float(obj)

    # add other special cases here
    if isinstance(obj, (datetime, date)):
        return str(obj)
    if isinstance(obj, dict):
        return {key: convert_to_standard_types(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [convert_to_standard_types(element) for element in obj]
    if isinstance(obj, tuple):
        return tuple(convert_to_standard_types(element) for element in obj)
    return obj


def convert_sqlalchemy_rows_to_csv_string(rows: list[dict]):
    """converts a list of sqlalchemy rows to a csv string"""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader()
    for item in rows:
        writer.writerow(item)
    csv_string = output.getvalue()
    output.close()
    return csv_string
