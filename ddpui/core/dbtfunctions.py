import os
from typing import Union

from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.utils.file_storage.storage_factory import StorageFactory


def map_airbyte_destination_spec_to_dbtcli_profile(
    conn_info: dict, dbt_project_params: Union[DbtProjectParams | None]
):
    """
    Dbt doesn't support tunnel methods
    So the translation to tunnel params is for our proxy service
    To do a hack & run dbt using ssh tunnel
    """
    if "tunnel_method" in conn_info:
        method = conn_info["tunnel_method"]

        if method["tunnel_method"] in ["SSH_KEY_AUTH", "SSH_PASSWORD_AUTH"]:
            conn_info["ssh_host"] = method["tunnel_host"]
            conn_info["ssh_port"] = method["tunnel_port"]
            conn_info["ssh_username"] = method["tunnel_user"]

        if method["tunnel_method"] == "SSH_KEY_AUTH":
            conn_info["ssh_pkey"] = method["ssh_key"]
            conn_info["ssh_private_key_password"] = method.get("tunnel_private_key_password")

        elif method["tunnel_method"] == "SSH_PASSWORD_AUTH":
            conn_info["ssh_password"] = method.get("tunnel_user_password")

    if "username" in conn_info:
        conn_info["user"] = conn_info["username"]

    # handle dbt ssl params
    if "ssl_mode" in conn_info:
        ssl_data = conn_info["ssl_mode"]
        mode = ssl_data["mode"] if "mode" in ssl_data else None
        ca_certificate = ssl_data["ca_certificate"] if "ca_certificate" in ssl_data else None
        # client_key_password = (
        #     ssl_data["client_key_password"] if "client_key_password" in ssl_data else None
        # )
        if mode:
            conn_info["sslmode"] = mode

        # TODO: if the storage backend is S3, then how would this work ?
        if ca_certificate and dbt_project_params.org_project_dir:
            storage = StorageFactory.get_storage_adapter()
            file_path = os.path.join(dbt_project_params.org_project_dir, "sslrootcert.pem")
            storage.write_file(file_path, ca_certificate)
            conn_info["sslrootcert"] = file_path

    return conn_info
