import os
from pathlib import Path

from dbt_automation.operations.arithmetic import arithmetic
from dbt_automation.operations.castdatatypes import cast_datatypes
from dbt_automation.operations.coalescecolumns import coalesce_columns
from dbt_automation.operations.concatcolumns import concat_columns
from dbt_automation.operations.droprenamecolumns import drop_columns, rename_columns
from dbt_automation.operations.flattenairbyte import flatten_operation

# operations
from dbt_automation.operations.flattenjson import flattenjson
from dbt_automation.operations.mergetables import union_tables
from dbt_automation.operations.regexextraction import regex_extraction
from dbt_automation.operations.mergeoperations import merge_operations
from dbt_automation.operations.syncsources import sync_sources
from dbt_automation.utils.warehouseclient import get_client
from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.dbtsources import read_sources

from ddpui.schemas.dbt_workflow_schema import CompleteDbtModelPayload
from ddpui.models.org import OrgDbt, OrgWarehouse
from ddpui.models.dbt_workflow import OrgDbtModel, OrgDbtOperation
from ddpui.utils import secretsmanager

OPERATIONS_DICT = {
    "flatten": flatten_operation,
    "flattenjson": flattenjson,
    # "unionall": union_tables,
    "castdatatypes": cast_datatypes,
    "coalescecolumns": coalesce_columns,
    "arithmetic": arithmetic,
    "concat": concat_columns,
    "dropcolumns": drop_columns,
    "renamecolumns": rename_columns,
    "regexextraction": regex_extraction,
}


def _get_wclient(org_warehouse: OrgWarehouse):
    """Connect to a warehouse and return the client"""
    credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)

    return get_client(org_warehouse.wtype, credentials, org_warehouse.bq_location)


def create_dbt_model_in_project(
    org_warehouse: OrgWarehouse,
    orgdbt_model: OrgDbtModel,
    payload: CompleteDbtModelPayload,
):
    """Create a dbt model in the project for an operation"""

    wclient = _get_wclient(org_warehouse)

    merge_config = {
        "type": "mergeoperations",
        "config": {
            "output_name": payload.name,
            "dest_schema": payload.dest_schema,
            "input": "",  # TODO: need to update dbt_automation first
            "operations": [],
        },
    }

    operations = (
        OrgDbtOperation.objects.filter(dbtmodel=orgdbt_model).order_by("seq").all()
    )

    for operation in operations:
        merge_config["config"]["operations"].append(operation.config)

    model_sql_path = merge_operations(
        merge_config, wclient, orgdbt_model.orgdbt.project_dir
    )

    return str(model_sql_path), None


def sync_sources_to_dbt(
    schema_name: str, source_name: str, org: str, org_warehouse: str
):
    """
    Sync sources from a given schema to dbt.
    """
    warehouse_client = _get_wclient(org_warehouse)

    sources_file_path = sync_sources(
        config={"source_schema": schema_name, "source_name": source_name},
        warehouse=warehouse_client,
        dbtproject=dbtProject(Path(os.getenv("CLIENTDBT_ROOT")) / org.slug / "dbtrepo"),
    )

    return str(sources_file_path), None


def read_dbt_sources_in_project(orgdbt: OrgDbt):
    """Read the sources from .yml files in the dbt project"""

    return read_sources(Path(orgdbt.project_dir) / "dbtrepo")


def get_table_columns(org_warehouse: OrgWarehouse, dbtmodel: OrgDbtModel):
    """Get the columns of a table in a warehouse"""
    wclient = _get_wclient(org_warehouse)
    return wclient.get_table_columns(dbtmodel.schema, dbtmodel.name)
