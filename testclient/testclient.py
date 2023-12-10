"""a client for testing dalgo services"""
import os
import json
import requests


class TestClient:
    """http client for api/ endpoints"""

    def __init__(self, port, **kwargs):
        self.clientheaders = None
        self.port = port
        self.verbose = kwargs.get("verbose")
        self.host = kwargs.get("host", "localhost")
        self.httpschema = "http" if self.host in ["localhost", "127.0.0.1"] else "https"

    def login(self, email, password):
        """Login"""
        req = self.clientpost("login/", json={"username": email, "password": password})
        if "token" not in req:
            raise Exception(req)
        self.clientheaders = {"Authorization": f"Bearer {req['token']}"}

    def clientget(self, endpoint, **kwargs):
        """GET"""
        print(f"GET /api/{endpoint}")
        req = requests.get(
            f"{self.httpschema}://{self.host}:{self.port}/api/{endpoint}",
            headers=self.clientheaders,
            timeout=kwargs.get("timeout", 10),
        )
        try:
            req.raise_for_status()
        except Exception as error:
            raise requests.exceptions.HTTPError(req.text) from error
        try:
            if self.verbose:
                print(req.json())
            return req.json()
        except Exception:
            print(req.text)
        return None

    def clientpost(self, endpoint, **kwargs):
        """POST"""
        print(f"POST /api/{endpoint}")
        req = requests.post(
            f"{self.httpschema}://{self.host}:{self.port}/api/{endpoint}",
            headers=self.clientheaders,
            timeout=kwargs.get("timeout", 10),
            json=kwargs.get("json"),
        )
        try:
            req.raise_for_status()
        except Exception as error:
            raise requests.exceptions.HTTPError(req.text) from error
        try:
            if self.verbose:
                print(req.json())
            return req.json()
        except Exception:
            print(req.text)
        return None

    def clientput(self, endpoint, **kwargs):
        """PUT"""
        print(f"PUT /api/{endpoint}")
        req = requests.put(
            f"{self.httpschema}://{self.host}:{self.port}/api/{endpoint}",
            headers=self.clientheaders,
            timeout=kwargs.get("timeout", 10),
            json=kwargs.get("json"),
        )
        try:
            req.raise_for_status()
        except Exception as error:
            raise requests.exceptions.HTTPError(req.text) from error
        try:
            if self.verbose:
                print(req.json())
            return req.json()
        except Exception:
            print(req.text)
        return None

    def clientdelete(self, endpoint, **kwargs):
        """DELETE"""
        print(f"DELETE /api/{endpoint}")
        req = requests.delete(
            f"{self.httpschema}://{self.host}:{self.port}/api/{endpoint}",
            headers=self.clientheaders,
            timeout=10,
        )
        try:
            req.raise_for_status()
        except Exception as error:
            raise requests.exceptions.HTTPError(req.text) from error

    def definition_name_to_id(self, definitions: list, name: str, id_key: str):
        """
        iterate through an airbyte actor definition list and return
        the id of the definition with the given name
        """
        for definition in definitions:
            if definition["name"] == name:
                return definition[id_key]
        raise ValueError(f"definition {name} not found")

    def create_warehouse(self, wtype: str, destination_definitions: dict):
        """create a warehouse"""
        if wtype == "postgres":
            dest_def_id = self.definition_name_to_id(
                destination_definitions, "Postgres", "destinationDefinitionId"
            )

            dbt_credentials = {
                "host": os.getenv("PG_WAREHOUSE_HOST"),
                "port": os.getenv("PG_WAREHOUSE_PORT"),
                "username": os.getenv("PG_WAREHOUSE_USERNAME"),
                "password": os.getenv("PG_WAREHOUSE_PASSWORD"),
                "database": os.getenv("PG_WAREHOUSE_DATABASE"),
            }
            airbyte_config = {
                "host": os.getenv("PG_WAREHOUSE_HOST"),
                "port": os.getenv("PG_WAREHOUSE_PORT"),
                "username": os.getenv("PG_WAREHOUSE_USERNAME"),
                "password": os.getenv("PG_WAREHOUSE_PASSWORD"),
                "database": os.getenv("PG_WAREHOUSE_DATABASE"),
                "schema": os.getenv("DBT_TARGETCONFIGS_SCHEMA"),
            }
        elif wtype == "bigquery":
            for destdef in destination_definitions:
                if destdef["name"] == "BigQuery":
                    dest_def_id = destdef["destinationDefinitionId"]
                    break

            with open(
                os.getenv("BQ_WAREHOUSE_SERVICE_ACCOUNT_CREDSFILE"),
                "r",
                -1,
                "utf8",
            ) as credsfile:
                dbt_credentials = json.load(credsfile)
                # print(dbtCredentials)
            airbyte_config = {
                "project_id": os.getenv("BQ_PROJECTID"),
                "dataset_id": os.getenv("BQ_DATASETID"),
                "dataset_location": os.getenv("BQ_DATASETLOCATION"),
                "credentials_json": json.dumps(dbt_credentials),
            }
        else:
            raise Exception("unknown wtype " + wtype)

        self.clientpost(
            "organizations/warehouse/",
            json={
                "name": "testwarehouse",
                "wtype": wtype,
                "destinationDefId": dest_def_id,
                "airbyteConfig": airbyte_config,
            },
            timeout=30,
        )

    def create_source(
        self,
        source_definitions: list,
        source_name: str,
        source_type: str,
        source_config: dict,
    ):
        """create a source"""
        source_def_id = self.definition_name_to_id(
            source_definitions,
            source_type,
            "sourceDefinitionId",
        )
        self.clientpost(
            "airbyte/sources/",
            json={
                "name": source_name,
                "sourceDefId": source_def_id,
                "config": source_config,
            },
            timeout=30,
        )

    def create_connection(self, connection_config: str, source_id: str):
        """create a connection"""
        payload = {
            "name": connection_config["name"],
            "normalize": False,
            "sourceId": source_id,
            "streams": [],
            "destinationSchema": connection_config["destinationSchema"],
        }

        catalog = self.clientget(
            f"airbyte/sources/{source_id}/schema_catalog", timeout=30
        )

        for streamdata in catalog["catalog"]["streams"]:
            for stream_spec in connection_config["streams"]:
                if streamdata["stream"]["name"] == stream_spec["name"]:
                    stream_config = {
                        "selected": True,
                        "name": stream_spec["name"],
                        "supportsIncremental": False,
                        "destinationSyncMode": stream_spec["syncMode"],
                        "syncMode": "full_refresh",
                    }
                    payload["streams"].append(stream_config)

        self.clientpost("airbyte/v1/connections/", json=payload, timeout=30)

    def setup_dbt_workspace(self, dbt_workspace_config: dict):
        """set up a dbt workspace"""
        self.clientdelete("dbt/workspace/")
        r = self.clientpost(
            "dbt/workspace/",
            json=dbt_workspace_config,
            timeout=30,
        )
        task_id = r["task_id"]
        return task_id
