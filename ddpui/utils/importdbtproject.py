""" functions to import a dbt project into dalgo """

import uuid
from ninja import Schema
from ddpui.models.org import OrgDbt
from ddpui.models.dbt_workflow import DbtEdge, OrgDbtModel, OrgDbtOperation


class ModelMetadata(Schema):
    """metadata from dbt docs describing the model"""

    name: str | None
    dbschema: str | None
    original_file_path: str | None
    resource_type: str | None
    source_name: str | None
    depends_on: list | None
    columns: list | None


class SourceMetaData(Schema):
    """metadata from dbt docs describing the source"""

    name: str | None
    identifier: str | None
    dbschema: str | None
    database: str | None
    resource_type: str | None
    package_name: str | None
    source_name: str | None
    path: str | None


def extract_models(
    manifest: dict,
) -> dict[str, ModelMetadata]:
    """extracts model descriptions from dbt docs manifest.json"""
    models_metadata: dict[str, ModelMetadata] = {}

    for node_key, node_value in manifest["nodes"].items():
        if node_key.startswith("model."):
            model_metadata = ModelMetadata(
                name=node_value.get("name"),
                dbschema=node_value.get("schema"),
                original_file_path=node_value.get("original_file_path"),
                resource_type=node_value.get("resource_type"),
                source_name=node_value.get("package_name"),
                depends_on=node_value.get("depends_on", {}).get("nodes", []),
                columns=[
                    column_name for column_name in node_value.get("columns", {}).keys()
                ],
            )
            models_metadata[node_key] = model_metadata

    return models_metadata


def extract_sources(manifest: dict) -> dict[str, SourceMetaData]:
    """extracts source descriptions from dbt docs manifest.json"""

    sources_metadata: dict[str, SourceMetaData] = {}

    for source_key, source_value in manifest["sources"].items():
        source_metadata = SourceMetaData(
            name=source_value.get("name"),
            identifier=source_value.get("identifier"),
            dbschema=source_value.get("schema"),
            database=source_value.get("database"),
            resource_type=source_value.get("resource_type"),
            package_name=source_value.get("package_name"),
            source_name=source_value.get("source_name"),
            path=source_value.get("path"),
        )
        sources_metadata[source_key] = source_metadata

    return sources_metadata


def create_orgdbtmodel_model(orgdbt: OrgDbt, model_metadata: ModelMetadata):
    """creates OrgDbtModel from dbt project"""
    if (
        model_metadata.name
        and model_metadata.original_file_path
        and model_metadata.dbschema
        and model_metadata.resource_type
    ):
        if not OrgDbtModel.objects.filter(
            orgdbt=orgdbt, schema=model_metadata.dbschema, name=model_metadata.name
        ).exists():
            OrgDbtModel.objects.create(
                uuid=uuid.uuid4(),
                orgdbt=orgdbt,
                schema=model_metadata.dbschema,
                name=model_metadata.name,
                sql_path=model_metadata.original_file_path,
                type=model_metadata.resource_type,
                source_name=model_metadata.dbschema,
                output_cols=model_metadata.columns,
            )


def create_orgdbtmodel_source(orgdbt: OrgDbt, source_metadata: SourceMetaData):
    """creates OrgDbtModel from dbt project"""
    if (
        source_metadata.name
        and source_metadata.dbschema
        and source_metadata.database
        and source_metadata.resource_type
    ):
        if not OrgDbtModel.objects.filter(
            orgdbt=orgdbt,
            schema=source_metadata.dbschema,
            name=source_metadata.identifier,
        ).exists():
            OrgDbtModel.objects.create(
                uuid=uuid.uuid4(),
                orgdbt=orgdbt,
                schema=source_metadata.dbschema,
                name=source_metadata.identifier,
                display_name=source_metadata.name,
                type=source_metadata.resource_type,
                source_name=source_metadata.source_name,
                sql_path=source_metadata.path,
            )


def create_orgdbtedges(orgdbt: OrgDbt, model_metadata: ModelMetadata):
    """creates inbound edges to a dbt model"""
    orgdbt_model = OrgDbtModel.objects.get(
        orgdbt=orgdbt, schema=model_metadata.dbschema, name=model_metadata.name
    )
    for dependency in model_metadata.depends_on:
        dependency: str
        if dependency.startswith("source."):
            _, _, _, dep_name = dependency.split(".", 3)
            parent_model = OrgDbtModel.objects.get(display_name=dep_name, type="source")

        elif dependency.startswith("model."):
            _, _, dep_name = dependency.split(".", 2)
            parent_model = OrgDbtModel.objects.get(name=dep_name, type="model")

        else:
            continue

        DbtEdge.objects.create(from_node=parent_model, to_node=orgdbt_model)


def create_orgdbtoperation(orgdbt: OrgDbt, model_metadata: ModelMetadata):
    """creates OrgDbtOperation instances based on dbt model metadata"""
    orgdbt_model = OrgDbtModel.objects.get(
        orgdbt=orgdbt, schema=model_metadata.dbschema, name=model_metadata.name
    )

    parent_model = None
    for dependency in model_metadata.depends_on:
        dependency: str
        if dependency.startswith("source."):
            _, _, _, dep_name = dependency.split(".", 3)
            parent_model = OrgDbtModel.objects.get(display_name=dep_name, type="source")

        elif dependency.startswith("model."):
            _, _, dep_name = dependency.split(".", 2)
            parent_model = OrgDbtModel.objects.get(name=dep_name, type="model")

        else:
            continue

    if parent_model is None:
        print(
            f"did not find parent model for {model_metadata.dbschema}.{model_metadata.name} "
        )
        print(model_metadata.depends_on)

    if (
        parent_model
        and not OrgDbtOperation.objects.filter(dbtmodel=orgdbt_model).exists()
    ):
        OrgDbtOperation.objects.create(
            dbtmodel=orgdbt_model,
            uuid=uuid.uuid4(),
            seq=1,
            output_cols=model_metadata.columns,
            config={
                "config": {
                    "op_config": "op_type",
                    "input": None,
                    "other_inputs": {
                        "source_columns": model_metadata.columns,
                    },
                },
                "type": "new_op_type",
                "input_models": [
                    {
                        "uuid": str(parent_model.uuid),
                        "name": parent_model.name,
                        "source_name": parent_model.source_name,
                        "schema": parent_model.schema,
                        "type": parent_model.type,
                    }
                ],
            },
        )
