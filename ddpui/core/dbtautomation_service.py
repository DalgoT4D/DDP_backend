import os, uuid
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

from dbt_automation.operations.flattenjson import flattenjson, flattenjson_dbt_sql

# from dbt_automation.operations.mergetables import union_tables, union_tables_sql
from dbt_automation.operations.regexextraction import (
    regex_extraction,
    regex_extraction_sql,
)
from dbt_automation.operations.mergeoperations import (
    merge_operations,
    merge_operations_sql,
)
from dbt_automation.operations.syncsources import (
    sync_sources,
    generate_source_definitions_yaml,
)
from dbt_automation.operations.joins import join, joins_sql
from dbt_automation.operations.groupby import groupby, groupby_dbt_sql
from dbt_automation.operations.wherefilter import where_filter, where_filter_sql
from dbt_automation.operations.mergetables import union_tables, union_tables_sql
from dbt_automation.utils.warehouseclient import get_client
from dbt_automation.utils.dbtproject import dbtProject
from dbt_automation.utils.dbtsources import read_sources
from dbt_automation.operations.replace import replace, replace_dbt_sql
from dbt_automation.operations.casewhen import casewhen, casewhen_dbt_sql
from dbt_automation.operations.aggregate import aggregate, aggregate_dbt_sql
from dbt_automation.operations.pivot import pivot, pivot_dbt_sql
from dbt_automation.operations.unpivot import unpivot, unpivot_dbt_sql
from dbt_automation.operations.generic import generic_function, generic_function_dbt_sql
from dbt_automation.operations.rawsql import generic_sql_function, raw_generic_dbt_sql

from ddpui.schemas.dbt_workflow_schema import CompleteDbtModelPayload
from ddpui.models.org import Org, OrgDbt, OrgWarehouse
from ddpui.models.dbt_workflow import OrgDbtModel, OrgDbtOperation
from ddpui.models.tasks import TaskProgressHashPrefix
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager
from ddpui.utils.helpers import map_airbyte_keys_to_postgres_keys
from ddpui.celery import app
from ddpui.utils.taskprogress import TaskProgress

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
    "join": join,
    "groupby": groupby,
    "where": where_filter,
    "replace": replace,
    "casewhen": casewhen,
    "aggregate": aggregate,
    "pivot": pivot,
    "unpivot": unpivot,
    "generic": generic_function,
    "rawsql": generic_sql_function,
}

OPERATIONS_DICT_SQL = {
    "flattenjson": flattenjson_dbt_sql,
    "castdatatypes": cast_datatypes_sql,
    "unionall": union_tables_sql,
    "coalescecolumns": coalesce_columns_dbt_sql,
    "arithmetic": arithmetic_dbt_sql,
    "concat": concat_columns_dbt_sql,
    "dropcolumns": drop_columns_dbt_sql,
    "renamecolumns": rename_columns_dbt_sql,
    "regexextraction": regex_extraction_sql,
    "join": joins_sql,
    "groupby": groupby_dbt_sql,
    "where": where_filter_sql,
    "replace": replace_dbt_sql,
    "casewhen": casewhen_dbt_sql,
    "aggregate": aggregate_dbt_sql,
    "pivot": pivot_dbt_sql,
    "unpivot": unpivot_dbt_sql,
    "generic": generic_function_dbt_sql,
    "rawsql": raw_generic_dbt_sql,
}


logger = CustomLogger("ddpui")


def _get_wclient(org_warehouse: OrgWarehouse):
    """Connect to a warehouse and return the client"""
    credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)
    if org_warehouse.wtype == "postgres":
        credentials = map_airbyte_keys_to_postgres_keys(credentials)
    return get_client(org_warehouse.wtype, credentials, org_warehouse.bq_location)


def _get_merge_operation_config(
    operations: list[dict],
    input: dict = {
        "input_type": "source",
        "input_name": "dummy",
        "source_name": "dummy",
    },
    output_name: str = "",
    dest_schema: str = "",
):
    """Get the config for a merge operation"""
    return {
        "output_name": output_name,
        "dest_schema": dest_schema,
        "input": input,
        "operations": operations,
    }


def create_or_update_dbt_model_in_project(
    org_warehouse: OrgWarehouse,
    orgdbt_model: OrgDbtModel,
    payload: CompleteDbtModelPayload = None,
    is_create: bool = True,
):
    """
    Create or update a dbt model in the project for an operation
    Read through all the operations mapped to the target_model
    Fetch the source from the first operation
    Create the merge op config
    Call the merge operation to create sql model file on disk
    """

    wclient = _get_wclient(org_warehouse)

    operations = []
    input_models = []
    for operation in (
        OrgDbtOperation.objects.filter(dbtmodel=orgdbt_model).order_by("seq").all()
    ):
        if operation.seq == 1:
            input_models = operation.config["input_models"]
        operations.append(
            {"type": operation.config["type"], "config": operation.config["config"]}
        )

    merge_input = []
    for model in input_models:
        source_model = OrgDbtModel.objects.filter(uuid=model["uuid"]).first()
        if source_model:
            merge_input.append(
                {
                    "input_type": source_model.type,
                    "input_name": (
                        source_model.name
                        if source_model.type == "model"
                        else source_model.display_name
                    ),
                    "source_name": source_model.source_name,
                }
            )

    output_name = payload.name if is_create else orgdbt_model.name
    dest_schema = payload.dest_schema if is_create else orgdbt_model.schema

    merge_config = _get_merge_operation_config(
        operations,
        input=merge_input[
            0
        ],  # just send the first input; for multi input operations rest will be inside the operations and their config - under "other_inputs".
        output_name=output_name,
        dest_schema=dest_schema,
    )

    model_sql_path, output_cols = merge_operations(
        merge_config, wclient, Path(orgdbt_model.orgdbt.project_dir) / "dbtrepo"
    )

    return model_sql_path, output_cols


def create_dbt_model_in_project(
    org_warehouse: OrgWarehouse,
    orgdbt_model: OrgDbtModel,
    payload: CompleteDbtModelPayload,
):
    """Wrapper function to create a dbt model in the project."""
    return create_or_update_dbt_model_in_project(
        org_warehouse, orgdbt_model, payload, is_create=True
    )


def update_dbt_model_in_project(
    org_warehouse: OrgWarehouse,
    orgdbt_model: OrgDbtModel,
):
    """Wrapper function to update a dbt model in the project."""
    create_or_update_dbt_model_in_project(org_warehouse, orgdbt_model, is_create=False)


def sync_sources_in_schema(
    schema_name: str, source_name: str, org: Org, org_warehouse: OrgWarehouse
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


def delete_dbt_model_in_project(orgdbt_model: OrgDbtModel):
    """Delete a dbt model in the project"""
    dbt_project = dbtProject(Path(orgdbt_model.orgdbt.project_dir) / "dbtrepo")
    dbt_project.delete_model(orgdbt_model.sql_path)
    return True


@app.task(bind=True)
def sync_sources_for_warehouse(
    self, org_dbt_id: str, org_warehouse_id: str, orgslug: str
):
    """
    Sync all tables in all schemas in the warehouse.
    Dbt source name will be the same as the schema name.
    """
    taskprogress = TaskProgress(
        task_id=self.request.id,
        hashkey=f"{TaskProgressHashPrefix.SYNCSOURCES}-{orgslug}",
        expire_in_seconds=10 * 60,  # max 10 minutes
    )

    org_dbt: OrgDbt = OrgDbt.objects.filter(id=org_dbt_id).first()
    org_warehouse: OrgWarehouse = OrgWarehouse.objects.filter(
        id=org_warehouse_id
    ).first()

    taskprogress.add(
        {
            "message": "Started syncing sources",
            "status": "runnning",
        }
    )

    dbt_project = dbtProject(Path(org_dbt.project_dir) / "dbtrepo")

    try:
        wclient = _get_wclient(org_warehouse)

        for schema in wclient.get_schemas():
            taskprogress.add(
                {
                    "message": f"Reading sources for schema {schema} from warehouse",
                    "status": "running",
                }
            )
            logger.info(f"reading sources for schema {schema} for warehouse")
            sync_tables = []
            for table in wclient.get_tables(schema):
                if not OrgDbtModel.objects.filter(
                    orgdbt=org_dbt, schema=schema, name=table, type="model"
                ).first():
                    sync_tables.append(table)

            taskprogress.add(
                {
                    "message": f"Finished reading sources for schema {schema}",
                    "status": "running",
                }
            )

            if len(sync_tables) == 0:
                logger.info(
                    f"No new tables in schema '{schema}' to be synced as sources."
                )
                continue

            # in dbt automation, it will overwrite the sources (if name is same which it will be = "schema") and the file
            source_yml_path = generate_source_definitions_yaml(
                schema, schema, sync_tables, dbt_project
            )

            logger.info(
                f"Generated yaml for {len(sync_tables)} tables for schema '{schema}' as sources; yaml at {source_yml_path}"
            )

    except Exception as e:
        logger.error(f"Error syncing sources: {e}")
        taskprogress.add(
            {
                "message": f"Error syncing sources: {e}",
                "status": "failed",
            }
        )
        raise Exception(f"Error syncing sources: {e}")
    # sync sources to django db; create if not present
    # its okay if we have dnagling sources that they deleted from their warehouse but are still in our db;
    # we can clear them up or give them an option to delete
    # because deleting the dnagling sources might delete their workflow nodes & edges. They should see a warning for this on the UI
    logger.info("synced sources in dbt, saving to db now")
    sources = read_dbt_sources_in_project(org_dbt)
    logger.info("read fresh source from all yaml files")
    taskprogress.add(
        {
            "message": "Creating sources in dbt",
            "status": "running",
        }
    )
    for source in sources:
        orgdbt_source = OrgDbtModel.objects.filter(
            source_name=source["source_name"],
            name=source["input_name"],
            type="source",
            orgdbt=org_dbt,
        ).first()
        if not orgdbt_source:
            orgdbt_source = OrgDbtModel.objects.create(
                uuid=uuid.uuid4(),
                orgdbt=org_dbt,
                source_name=source["source_name"],
                name=source["input_name"],
                display_name=source["input_name"],
                type="source",
            )
            taskprogress.add(
                {
                    "message": "Added "
                    + source["source_name"]
                    + "."
                    + source["input_name"],
                    "status": "running",
                }
            )

        orgdbt_source.schema = source["schema"]
        orgdbt_source.sql_path = source["sql_path"]

        orgdbt_source.save()

    taskprogress.add(
        {
            "message": "Sync finished",
            "status": "completed",
        }
    )

    logger.info("saved sources to db")

    return True


def warehouse_datatypes(org_warehouse: OrgWarehouse):
    """Get the datatypes of a table in a warehouse"""
    wclient = _get_wclient(org_warehouse)
    return wclient.get_column_data_types()


def json_columnspec(warehouse: OrgWarehouse, source_schema, input_name, json_column):
    """Get json keys of a table in warehouse"""
    wclient = _get_wclient(warehouse)
    return wclient.get_json_columnspec(source_schema, input_name, json_column)
