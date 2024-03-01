import os
from pathlib import Path

from dbt_automation.operations.arithmetic import arithmetic, arithmetic_dbt_sql
from dbt_automation.operations.castdatatypes import cast_datatypes, cast_datatypes_sql
from dbt_automation.operations.coalescecolumns import (
    coalesce_columns,
    coalesce_columns_dbt_sql,
)
from dbt_automation.operations.concatcolumns import (
    concat_columns,
    concat_columns_dbt_sql,
)
from dbt_automation.operations.droprenamecolumns import (
    drop_columns,
    rename_columns,
    rename_columns_dbt_sql,
    drop_columns_dbt_sql,
)
from dbt_automation.operations.flattenairbyte import flatten_operation

# operations
from dbt_automation.operations.flattenjson import flattenjson, flattenjson_dbt_sql
from dbt_automation.operations.mergetables import union_tables, union_tables_sql
from dbt_automation.operations.regexextraction import (
    regex_extraction,
    regex_extraction_sql,
)
from dbt_automation.operations.mergeoperations import (
    merge_operations,
    merge_operations_sql,
)
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

OPERATIONS_DICT_SQL = {
    "flattenjson": flattenjson_dbt_sql,
    # "unionall": union_tables_sql,
    "castdatatypes": cast_datatypes_sql,
    "coalescecolumns": coalesce_columns_dbt_sql,
    "arithmetic": arithmetic_dbt_sql,
    "concat": concat_columns_dbt_sql,
    "dropcolumns": drop_columns_dbt_sql,
    "renamecolumns": rename_columns_dbt_sql,
    "regexextraction": regex_extraction_sql,
}


def _get_wclient(org_warehouse: OrgWarehouse):
    """Connect to a warehouse and return the client"""
    credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)

    return get_client(org_warehouse.wtype, credentials, org_warehouse.bq_location)


def _get_merge_operation_config(
    operations: list[dict],
    output_name: str = "",
    dest_schema: str = "",
):
    """Get the config for a merge operation"""
    return {
        "output_name": output_name,
        "dest_schema": dest_schema,
        "input": {
            "input_type": "source",
            "input_name": "dummy",
            "source_name": "dummy",
        },  # TODO: need to update dbt_automation first
        "operations": operations,
    }


def create_dbt_model_in_project(
    org_warehouse: OrgWarehouse,
    orgdbt_model: OrgDbtModel,
    payload: CompleteDbtModelPayload,
):
    """Create a dbt model in the project for an operation"""

    wclient = _get_wclient(org_warehouse)

    merge_config = _get_merge_operation_config(payload.name, payload.dest_schema, [])

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


def get_output_cols_for_operation(
    org_warehouse: OrgWarehouse, op_type: str, config: dict
):
    """
    Get the output columns from a merge operation;
    this only generates the sql and fetches the output col.
    Model is neither being run nor saved to the disk
    """
    wclient = _get_wclient(org_warehouse)
    operations = [{"type": op_type, "config": config}]
    _, output_cols = merge_operations_sql(
        _get_merge_operation_config(operations), wclient
    )
    return output_cols
