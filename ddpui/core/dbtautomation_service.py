from pathlib import Path
from ddpui.models.org import OrgWarehouse, OrgDbt
from ddpui.utils import secretsmanager
from dbt_automation.utils.warehouseclient import get_client

# operations
from dbt_automation.operations.flattenjson import flattenjson
from dbt_automation.operations.arithmetic import arithmetic
from dbt_automation.operations.castdatatypes import cast_datatypes
from dbt_automation.operations.coalescecolumns import coalesce_columns
from dbt_automation.operations.concatcolumns import concat_columns
from dbt_automation.operations.droprenamecolumns import drop_columns, rename_columns
from dbt_automation.operations.flattenairbyte import flatten_operation
from dbt_automation.operations.mergetables import union_tables
from dbt_automation.operations.regexextraction import regex_extraction


OPERATIONS_DICT = {
    "flatten": flatten_operation,
    "flattenjson": flattenjson,
    "unionall": union_tables,
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
    orgdbt: OrgDbt, org_warehouse: OrgWarehouse, op_type: str, config: dict
):
    """Create a dbt model in the project for an operation"""

    wclient = _get_wclient(org_warehouse)
    if op_type not in OPERATIONS_DICT:
        return None, "Operation not found"

    sql_file_path = OPERATIONS_DICT[op_type](
        config=config,
        warehouse=wclient,
        project_dir=Path(orgdbt.project_dir) / "dbtrepo",
    )

    return str(sql_file_path), None
