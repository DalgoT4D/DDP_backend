import os, uuid, time
from pathlib import Path
from collections import deque

from django.db.models import Q
from ddpui.dbt_automation.operations.arithmetic import arithmetic, arithmetic_dbt_sql
from ddpui.dbt_automation.operations.castdatatypes import cast_datatypes, cast_datatypes_sql
from ddpui.dbt_automation.operations.coalescecolumns import (
    coalesce_columns,
    coalesce_columns_dbt_sql,
)
from ddpui.dbt_automation.operations.concatcolumns import (
    concat_columns,
    concat_columns_dbt_sql,
)
from ddpui.dbt_automation.operations.droprenamecolumns import (
    drop_columns,
    rename_columns,
    rename_columns_dbt_sql,
    drop_columns_dbt_sql,
)
from ddpui.dbt_automation.operations.flattenairbyte import flatten_operation

from ddpui.dbt_automation.operations.flattenjson import flattenjson, flattenjson_dbt_sql

# from ddpui.dbt_automation.operations.mergetables import union_tables, union_tables_sql
from ddpui.dbt_automation.operations.regexextraction import (
    regex_extraction,
    regex_extraction_sql,
)
from ddpui.dbt_automation.operations.mergeoperations import (
    merge_operations,
    merge_operations_sql,
)
from ddpui.dbt_automation.operations.syncsources import (
    sync_sources,
    generate_source_definitions_yaml,
)
from ddpui.dbt_automation.operations.joins import join, joins_sql
from ddpui.dbt_automation.operations.groupby import groupby, groupby_dbt_sql
from ddpui.dbt_automation.operations.wherefilter import where_filter, where_filter_sql
from ddpui.dbt_automation.operations.mergetables import union_tables, union_tables_sql
from ddpui.dbt_automation.utils.warehouseclient import get_client
from ddpui.dbt_automation.utils.dbtproject import dbtProject
from ddpui.dbt_automation.utils.dbtsources import read_sources, read_sources_from_yaml
from ddpui.dbt_automation.operations.replace import replace, replace_dbt_sql
from ddpui.dbt_automation.operations.casewhen import casewhen, casewhen_dbt_sql
from ddpui.dbt_automation.operations.aggregate import aggregate, aggregate_dbt_sql
from ddpui.dbt_automation.operations.pivot import pivot, pivot_dbt_sql
from ddpui.dbt_automation.operations.unpivot import unpivot, unpivot_dbt_sql
from ddpui.dbt_automation.operations.generic import generic_function, generic_function_dbt_sql
from ddpui.dbt_automation.operations.rawsql import generic_sql_function, raw_generic_dbt_sql

from ddpui.schemas.dbt_workflow_schema import CompleteDbtModelPayload
from ddpui.models.org import Org, OrgDbt, OrgWarehouse
from ddpui.models.dbt_workflow import OrgDbtModel, OrgDbtOperation, DbtEdge, OrgDbtModelType
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager
from ddpui.utils.helpers import map_airbyte_keys_to_postgres_keys
from ddpui.celery import app
from ddpui.utils.taskprogress import TaskProgress
from ddpui.core.orgdbt_manager import DbtProjectManager

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
    for operation in OrgDbtOperation.objects.filter(dbtmodel=orgdbt_model).order_by("seq").all():
        if operation.seq == 1:
            input_models = operation.config["input_models"]
        operations.append({"type": operation.config["type"], "config": operation.config["config"]})

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
        merge_config, wclient, Path(DbtProjectManager.get_dbt_project_dir(orgdbt_model.orgdbt))
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
        dbtproject=dbtProject(Path(DbtProjectManager.get_dbt_project_dir(org.dbt))),
    )

    return str(sources_file_path), None


def read_dbt_sources_in_project(orgdbt: OrgDbt):
    """Read the sources from .yml files in the dbt project"""

    return read_sources(DbtProjectManager.get_dbt_project_dir(orgdbt))


def get_table_columns(org_warehouse: OrgWarehouse, dbtmodel: OrgDbtModel):
    """Get the columns of a table in a warehouse"""
    wclient = _get_wclient(org_warehouse)
    return wclient.get_table_columns(dbtmodel.schema, dbtmodel.name)


def get_output_cols_for_operation(org_warehouse: OrgWarehouse, op_type: str, config: dict):
    """
    Get the output columns from a merge operation;
    this only generates the sql and fetches the output col.
    Model is neither being run nor saved to the disk
    """
    wclient = _get_wclient(org_warehouse)
    operations = [{"type": op_type, "config": config}]
    _, output_cols = merge_operations_sql(_get_merge_operation_config(operations), wclient)
    return output_cols


def delete_dbt_model_in_project(orgdbt_model: OrgDbtModel):
    """Deletes a dbt model's sql file on disk"""
    dbt_project = dbtProject(Path(DbtProjectManager.get_dbt_project_dir(orgdbt_model.orgdbt)))
    dbt_project.delete_model(orgdbt_model.sql_path)
    return True


def delete_dbt_source_in_project(orgdbt_model: OrgDbtModel):
    """Deletes a dbt model's sql file on disk"""

    # read all sources in the same yml file
    src_tables: list[dict] = read_sources_from_yaml(
        DbtProjectManager.get_dbt_project_dir(orgdbt_model.orgdbt), orgdbt_model.sql_path
    )

    filtered_src_tables: list[dict] = [
        src_table for src_table in src_tables if src_table["input_name"] != orgdbt_model.name
    ]

    # if there are sources & there is diff; update the sources.yml
    if len(src_tables) > 0 and len(src_tables) != len(filtered_src_tables):
        src_yml_path = generate_source_definitions_yaml(
            orgdbt_model.schema,
            orgdbt_model.source_name,
            [src["input_name"] for src in filtered_src_tables],
            dbtProject(Path(DbtProjectManager.get_dbt_project_dir(orgdbt_model.orgdbt))),
        )

        logger.info(f"Deleted & Updated the source tables in yml {src_yml_path}")

    return True


def delete_org_dbt_model(orgdbt_model: OrgDbtModel, cascade: bool = False):
    """
    Delete the org dbt model
    Only delete org dbt model of type "model"
    Casacde will be implemented when we re-haul the ui4t architecture
    """
    if orgdbt_model.type == OrgDbtModelType.SOURCE:
        raise ValueError("Cannot delete a source as a model")

    operations = OrgDbtOperation.objects.filter(dbtmodel=orgdbt_model).count()

    if operations > 0:
        orgdbt_model.under_construction = True
        orgdbt_model.save()
    else:
        # make sure this is not linked to any other model
        # delete if there are no edges coming or going out of this model

        cnt_edges_to_models = DbtEdge.objects.filter(
            Q(from_node=orgdbt_model) | Q(to_node=orgdbt_model)
        ).count()
        if cnt_edges_to_models == 0:
            orgdbt_model.delete()

    # delete the model file is present
    delete_dbt_model_in_project(orgdbt_model)


def delete_org_dbt_source(orgdbt_model: OrgDbtModel, cascade: bool = False):
    """
    Delete the org dbt model
    Only delete org dbt model of type "source"
    Cascade will be implemented when we re-haul the ui4t architecture
    """
    if orgdbt_model.type == OrgDbtModelType.MODEL:
        raise ValueError("Cannot delete a model as a source")

    # delete entry in sources.yml on disk; & recreate the sources.yml
    delete_dbt_source_in_project(orgdbt_model)

    orgdbt_model.delete()


def cascade_delete_org_dbt_model(orgdbt_model: OrgDbtModel):
    """
    Cascade delete the org dbt model
    Delete the model and all its children (operations & models)
    THIS iS UNUSED. Cascade will be implemented when we re-haul the ui4t architecture
    """
    # delete all children of this model (operations & models)
    q = deque()
    children: list[OrgDbtModel] = []

    q.append(orgdbt_model)
    while len(q) > 0:
        curr_node = q.popleft()
        children.append(curr_node)

        for edge in DbtEdge.objects.filter(from_node=curr_node):
            q.append(edge.to_node)

    for child_orgdbt_model in reversed(children):  # just to be clean, delete from leaf nodes first
        delete_dbt_model_in_project(child_orgdbt_model)
        child_orgdbt_model.delete()


def propagate_changes_to_downstream_operations(
    target_model: OrgDbtModel, updated_operation: OrgDbtOperation, depth: int = 1
):
    """
    - Propagate changes of an update in OrgDbtOperation downstream to all operations that build the target OrgDbtModel
    - Propagating changes mean making sure the output of the updated operation i.e. output_cols are available as source_columns to next operations
    - By default the depth is 1 i.e. it will only update the next operation
    """

    if depth == 0:
        logger.info("Terminating propagation as depth is 0")
        return

    next_op = OrgDbtOperation.objects.filter(
        dbtmodel=target_model, seq=updated_operation.seq + 1
    ).first()

    if not next_op:
        logger.info("No downstream operations left to propagate changes")
        return

    config = next_op.config  # {"type": .. , "config": {}, "input_models": [...]}
    op_config = config.get("config", {})
    if "source_columns" in op_config:
        op_config["source_columns"] = updated_operation.output_cols

    next_op.config = config
    next_op.save()

    propagate_changes_to_downstream_operations(target_model, next_op, depth - 1)


@app.task(bind=True)
def sync_sources_for_warehouse(
    self, org_dbt_id: str, org_warehouse_id: str, task_id: str, hashkey: str
):
    """
    Sync all tables in all schemas in the warehouse.
    Dbt source name will be the same as the schema name.
    """
    taskprogress = TaskProgress(
        task_id=task_id,
        hashkey=hashkey,
        expire_in_seconds=10 * 60,  # max 10 minutes
    )

    org_dbt: OrgDbt = OrgDbt.objects.filter(id=org_dbt_id).first()
    org_warehouse: OrgWarehouse = OrgWarehouse.objects.filter(id=org_warehouse_id).first()

    taskprogress.add(
        {
            "message": "Started syncing sources",
            "status": "runnning",
        }
    )

    dbt_project = dbtProject(Path(DbtProjectManager.get_dbt_project_dir(org_dbt)))

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
                    orgdbt=org_dbt, schema=schema, name=table, type=OrgDbtModelType.MODEL
                ).first():
                    sync_tables.append(table)

            taskprogress.add(
                {
                    "message": f"Finished reading sources for schema {schema}",
                    "status": "running",
                }
            )

            if len(sync_tables) == 0:
                logger.info(f"No new tables in schema '{schema}' to be synced as sources.")
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
            type=OrgDbtModelType.SOURCE,
            orgdbt=org_dbt,
        ).first()
        if not orgdbt_source:
            orgdbt_source = OrgDbtModel.objects.create(
                uuid=uuid.uuid4(),
                orgdbt=org_dbt,
                source_name=source["source_name"],
                name=source["input_name"],
                display_name=source["input_name"],
                type=OrgDbtModelType.SOURCE,
            )
            taskprogress.add(
                {
                    "message": "Added " + source["source_name"] + "." + source["input_name"],
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
