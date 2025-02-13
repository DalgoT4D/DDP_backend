"""creates the dbt models which call our flatten_json macro"""
import sys
import argparse
from logging import basicConfig, getLogger, INFO
from string import Template
from pathlib import Path
from dbt_automation.utils.sourceschemas import get_source
from dbt_automation.utils.dbtproject import dbtProject

basicConfig(level=INFO)
logger = getLogger()

# ================================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--project-dir", required=True)
parser.add_argument("--input-schema", default="staging", help="e.g. staging")
parser.add_argument("--output-schema", default="intermediate", help="e.g. intermediate")
args = parser.parse_args()


# ================================================================================
NORMALIZED_MODEL_TEMPLATE = Template(
    """
{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ]
) }}

{{
    flatten_json(
        model_name = source('$source_name', '$table_name'),
        json_column = '_airbyte_data'
    )
}}
"""
)


def mk_normalized_dbtmodel(source_name: str, table_name: str) -> str:
    """creates a .sql dbt model"""
    return NORMALIZED_MODEL_TEMPLATE.substitute(
        {"source_name": source_name, "table_name": table_name}
    )


# ================================================================================
def get_models_config(src: dict, output_schema: str):
    """iterates through the tables in a source and creates the corresponding model
    configs. only one column is specified in the model: _airbyte_ab_id"""
    models = []
    for table in src["tables"]:
        logger.info(
            "[get_models_config] adding model %s.%s",
            src["name"],
            table["name"],
        )

        models.append(
            {
                "name": table["name"],
                "description": "",
                "+schema": output_schema,
                "columns": [
                    {
                        "name": "_airbyte_ab_id",
                        "description": "",
                        "tests": ["unique", "not_null"],
                    }
                ],
            }
        )

    return models


# ================================================================================
dbtproject = dbtProject(args.project_dir)

# create the output directory
dbtproject.ensure_models_dir(args.output_schema)

# locate the sources.yml for the input-schema
sources_filename = dbtproject.sources_filename(args.input_schema)

# find the source in that file... it should be the only one
source = get_source(sources_filename, args.input_schema)
if source is None:
    logger.error("no source for schema %s in %s", args.input_schema, sources_filename)
    sys.exit(1)

# for every table in the source, generate an output model file
models_dir = dbtproject.models_dir(args.output_schema)
for srctable in source["tables"]:
    model_filename = Path(models_dir) / (srctable["name"] + ".sql")
    logger.info(
        "[write_models] %s.%s => %s", source["name"], srctable["name"], model_filename
    )

    with open(model_filename, "w", encoding="utf-8") as outfile:
        model = mk_normalized_dbtmodel(source["name"], srctable["name"])
        outfile.write(model)
        outfile.close()

# also generate a configuration yml with a `models:` key under the output-schema
dbtproject.write_model_config(
    args.output_schema, get_models_config(source, args.output_schema), logger=logger
)
