import yaml
import re
import uuid
import json
from django.db import transaction

import glob
import os
import shutil
import subprocess
from pathlib import Path
import requests
from django.utils.text import slugify
from ddpui.dbt_automation import assets
from ddpui.ddpprefect import prefect_service, SECRET, DBTCLIPROFILE
from ddpui.models.org import OrgDbt, OrgPrefectBlockv1, OrgWarehouse, TransformType
from ddpui.models.org_user import Org
from ddpui.models.tasks import Task, OrgTask, DataflowOrgTask, TaskType
from ddpui.models.dbt_workflow import OrgDbtModel, OrgDbtModelType
from ddpui.models.canvas_models import CanvasNode, CanvasNodeType, CanvasEdge
from ddpui.ddpdbt.dbthelpers import create_or_update_org_cli_block
from ddpui.utils import secretsmanager
from ddpui.utils.constants import (
    TASK_DOCSGENERATE,
    TASK_DBTTEST,
    TASK_DBTRUN,
    TASK_DBTSEED,
    TASK_DBTDEPS,
    TASK_DBTCLOUD_JOB,
)
from ddpui.core.orgdbt_manager import DbtProjectManager, DbtProjectParams
from ddpui.core.git_manager import GitManager
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


def delete_dbt_workspace(org: Org):
    """deletes the dbt workspace on disk as well as in prefect"""

    # remove transform tasks
    org_tasks_delete = []
    for org_task in OrgTask.objects.filter(
        org=org, task__type__in=[TaskType.DBT, TaskType.GIT]
    ).all():
        if (
            DataflowOrgTask.objects.filter(
                orgtask=org_task, dataflow__dataflow_type="orchestrate"
            ).count()
            > 0
        ):
            raise Exception(f"{str(org_task)} is being used in a deployment")
        org_tasks_delete.append(org_task)

    logger.info("deleting orgtasks")
    for org_task in org_tasks_delete:
        for dataflow_orgtask in DataflowOrgTask.objects.filter(orgtask=org_task).all():
            dataflow_orgtask.dataflow.delete()
        org_task.delete()

    logger.info("deleting dbt cli profile")
    if org.dbt and org.dbt.cli_profile_block:
        dbt_cli_block: OrgPrefectBlockv1 = org.dbt.cli_profile_block
        try:
            prefect_service.delete_dbt_cli_profile_block(dbt_cli_block.block_id)
        except Exception:  # pylint:disable=broad-exception-caught
            pass
        dbt_cli_block.delete()

    # we should remove this when we have sandbox envs & mutliple orgdbts per org
    for dbt_cli_profile_block in OrgPrefectBlockv1.objects.filter(
        org=org, block_type=DBTCLIPROFILE
    ).all():
        try:
            prefect_service.delete_dbt_cli_profile_block(dbt_cli_profile_block.block_id)
        except Exception:  # pylint:disable=broad-exception-caught
            pass
        dbt_cli_profile_block.delete()

    logger.info("deleting git secret block")
    # remove git token uri block
    for secret_block in OrgPrefectBlockv1.objects.filter(org=org, block_type=SECRET).all():
        try:
            prefect_service.delete_secret_block(secret_block.block_id)
        except Exception:  # pylint:disable=broad-exception-caught
            pass
        secret_block.delete()

    # delete github PAT if exists
    if org.dbt and org.dbt.gitrepo_access_token_secret:
        secretsmanager.delete_github_pat(org.dbt.gitrepo_access_token_secret)

    logger.info("deleting orgdbt, orgdbtmodel and reference to org")
    if org.dbt:
        dbt = org.dbt

        # remove org dbt models
        OrgDbtModel.objects.filter(orgdbt=dbt).delete()

        # remove dbt reference from the org
        org.dbt = None
        org.save()

        # remove dbt project dir and OrgDbt model
        if os.path.exists(dbt.project_dir):
            shutil.rmtree(dbt.project_dir)
        dbt.delete()


def task_config_params(task: Task):
    """Return the config dictionary to setup parameters on this task"""

    # dbt task config parameters
    TASK_CONIF_PARAM = {
        TASK_DBTDEPS: {"flags": ["upgrade"], "options": ["add-package"]},
        TASK_DBTRUN: {"flags": ["full-refresh"], "options": ["select", "exclude"]},
        TASK_DBTTEST: {"flags": [], "options": ["select", "exclude"]},
        TASK_DBTSEED: {"flags": ["full-refresh"], "options": ["select"]},
        TASK_DOCSGENERATE: {"flags": [], "options": []},
        TASK_DBTCLOUD_JOB: {"flags": [], "options": ["job_id"]},
    }

    return TASK_CONIF_PARAM[task.slug] if task.slug in TASK_CONIF_PARAM else None


def setup_local_dbt_workspace(org: Org, project_name: str, default_schema: str):
    """sets up an org's dbt workspace, recreating it if it already exists"""
    warehouse = OrgWarehouse.objects.filter(org=org).first()

    if not warehouse:
        raise Exception("Please set up your warehouse first")

    if org.slug is None:
        org.slug = slugify(org.name)
        org.save()

    project_dir: Path = Path(DbtProjectManager.get_org_dir(org))
    dbtrepo_dir: Path = project_dir / project_name

    orgdbt = org.dbt
    if not orgdbt:
        dbt = OrgDbt(
            project_dir=DbtProjectManager.get_dbt_repo_relative_path(dbtrepo_dir),
            dbt_venv=DbtProjectManager.DEFAULT_DBT_VENV_REL_PATH,
            target_type=warehouse.wtype,
            default_schema=default_schema,
            transform_type=TransformType.UI,
        )
        dbt.save()
        logger.info("created orgdbt for org %s", org.name)
        org.dbt = dbt
        org.save()
        logger.info("set org.dbt for org %s", org.name)
        orgdbt = dbt
    else:
        orgdbt.project_dir = DbtProjectManager.get_dbt_repo_relative_path(dbtrepo_dir)
        orgdbt.target_type = warehouse.wtype
        orgdbt.default_schema = default_schema
        orgdbt.transform_type = TransformType.UI
        orgdbt.dbt_venv = DbtProjectManager.DEFAULT_DBT_VENV_REL_PATH
        orgdbt.save()

    # this client's dbt setup happens here
    if dbtrepo_dir.exists():
        raise Exception(f"Project {project_name} already exists")

    if not project_dir.exists():
        project_dir.mkdir()
        logger.info("created project_dir %s", project_dir)

    logger.info(f"starting to setup local dbt workspace at {project_dir}")

    # dbt init
    try:
        # dbt init must run from parent directory (project_dir) because it creates the dbtrepo folder
        DbtProjectManager.run_dbt_command(
            org,
            orgdbt,
            [
                "init",
                project_name,
            ],
            cwd=str(project_dir),
            flags=["--skip-profile-setup"],
        )

        # Delete example models
        example_models_dir = dbtrepo_dir / "models" / "example"
        if example_models_dir.exists():
            shutil.rmtree(example_models_dir)

    except subprocess.CalledProcessError as e:
        logger.error(f"dbt init failed with {e.returncode}")
        raise Exception(f"dbt init failed: {e}") from e

    try:
        # copy packages.yml
        logger.info("copying packages.yml from assets")
        target_packages_yml = Path(dbtrepo_dir) / "packages.yml"
        source_packages_yml = os.path.abspath(
            os.path.join(os.path.abspath(assets.__file__), "..", "packages.yml")
        )
        shutil.copy(source_packages_yml, target_packages_yml)

        # copy all macros with .sql extension from assets
        assets_dir = assets.__path__[0]

        for sql_file_path in glob.glob(os.path.join(assets_dir, "*.sql")):
            # Get the target path in the project_dir/macros directory
            target_path = Path(dbtrepo_dir) / "macros" / Path(sql_file_path).name

            # Copy the .sql file to the target path
            shutil.copy(sql_file_path, target_path)

            # Log the creation of the file
            logger.info("created %s", target_path)
    except Exception as e:
        logger.error(f"failed to copy asset files: {e}")
        raise Exception(f"Something went wrong while copying asset files : {e}")

    saved_creds = secretsmanager.retrieve_warehouse_credentials(warehouse)
    if saved_creds is None:
        logger.error(
            "failed to retrieve warehouse credentials for org %s to create dbt profile", org.name
        )
        raise Exception(
            "failed to retrieve warehouse credentials for org %s to create dbt profile" % org.name
        )

    (cli_profile_block, dbt_project_params), error = create_or_update_org_cli_block(
        org, warehouse, saved_creds
    )

    if error:
        logger.error("failed to create dbt cli profile for org %s: %s", org.name, error)
        raise Exception(f"failed to create dbt cli profile for org {org.name}: {error}") from e

    # initializing it as git repo
    logger.info("initializing dbt workspace as git repo")
    try:
        git_manager = GitManager(repo_local_path=dbtrepo_dir)
        git_manager.init_repo()
    except Exception as err:
        logger.error(f"Failed to initialize git repo: {str(err)}")
        raise Exception(f"Failed to initialize git repo: {str(err)}") from e

    # create .gitignore file
    gitignore_content = """# dbt artifacts
target/
dbt_packages/
logs/

# Virtual environments
.venv/
venv/

# dbt profiles (contain credentials)
profiles/
profiles.yml
profiles.yaml

# dbt user config
.user.yml
package-lock.yml

# Environment files
.env*
"""
    gitignore_path = dbtrepo_dir / ".gitignore"
    gitignore_path.write_text(gitignore_content)
    logger.info("created .gitignore file at %s", gitignore_path)

    logger.info("set dbt workspace completed for org %s", org.name)


def convert_github_url(url: str) -> str:
    """convert Github repo url to api url"""
    pattern = r"https://github.com/([^/]+)/([^/]+)\.git"
    replacement = r"https://api.github.com/repos/\1/\2"
    new_url = re.sub(pattern, replacement, url)
    return new_url


def check_repo_exists(gitrepo_url: str, gitrepo_access_token: str | None) -> bool:
    """Check if a GitHub repo exists."""
    headers = {
        "Accept": "application/vnd.github.v3+json",
    }
    if gitrepo_access_token:
        headers["Authorization"] = f"token {gitrepo_access_token}"

    url = convert_github_url(gitrepo_url)

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.info(f"Error checking repo existence: {e}")
        return False

    return response.status_code == 200


def generate_manifest_json_for_dbt_project(org: Org, orgdbt: OrgDbt) -> dict:
    """Generates the manifest.json for a given OrgDbt project."""
    # we need to make sure the profiles.yml exists in the profiles dir
    dbt_project_params: DbtProjectParams = DbtProjectManager.gather_dbt_project_params(org, orgdbt)
    dbt_cli_profile_block: OrgPrefectBlockv1 = orgdbt.cli_profile_block
    if not dbt_cli_profile_block:
        raise Exception("DBT CLI profile block not found for the OrgDbt project")

    try:
        profile = prefect_service.get_dbt_cli_profile_block(dbt_cli_profile_block.block_name)[
            "profile"
        ]
        profile_dirname = Path(dbt_project_params.project_dir) / "profiles"
        os.makedirs(profile_dirname, exist_ok=True)
        profile_filename = profile_dirname / "profiles.yml"
        logger.info("writing dbt profile to " + str(profile_filename))
        with open(profile_filename, "w", encoding="utf-8") as f:
            yaml.safe_dump(profile, f)
    except Exception as err:
        logger.error(f"failed to write profiles.yml: {str(err)}")
        raise Exception(f"Something went wrong while writing profiles.yml: {str(err)}") from err

    try:
        # install dependencies
        logger.info("running dbt deps for manifest generation")
        result = DbtProjectManager.run_dbt_command(
            org, orgdbt, command=["deps"], keyword_args={"profiles-dir": "profiles/"}
        )

        logger.info(f"dbt deps output: {result.stdout}")

        # compile to generate manifest.json
        logger.info("running dbt compile for manifest generation")
        result = DbtProjectManager.run_dbt_command(
            org=org,
            orgdbt=orgdbt,
            command=["compile"],
            keyword_args={"profiles-dir": "profiles/"},
        )

        logger.info(f"dbt compile output: {result.stdout}")

    except Exception as err:
        logger.error(f"dbt compile failed with {str(err)}")
        raise Exception(f"Something went wrong while generating manifest.json: {str(err)}")

    # make sure the manifest.json file exists
    project_dir: Path = Path(DbtProjectManager.get_dbt_project_dir(orgdbt))

    if not os.path.exists(project_dir / "target" / "manifest.json"):
        logger.error("dbt compile did not generate manifest.json")
        raise Exception("dbt compile did not generate manifest.json")

    with open(project_dir / "target" / "manifest.json", "r") as manifest_file:
        manifest_json = json.load(manifest_file)

    return manifest_json


def parse_dbt_manifest_to_canvas(
    org: Org, orgdbt: OrgDbt, org_warehouse: OrgWarehouse, manifest_json: dict = None
) -> dict:
    """
    Parse dbt manifest.json and create/update CanvasNodes and CanvasEdges.

    Args:
        org: The organization
        orgdbt: The OrgDbt instance
        org_warehouse: OrgWarehouse instance to fetch column info from warehouse
        manifest_json: Optional pre-fetched manifest.json. If not provided, will generate it.

    Returns:
        dict: Summary of created/updated nodes and edges
    """
    logger.info(f"Starting manifest parsing for orgdbt {orgdbt.project_dir}")

    # Generate manifest if not provided
    if manifest_json is None:
        logger.info("Generating manifest.json...")
        manifest_json = generate_manifest_json_for_dbt_project(org, orgdbt)

    # Track created/updated objects
    stats = {
        "models_processed": 0,
        "sources_processed": 0,
        "nodes_created": 0,
        "nodes_updated": 0,
        "edges_created": 0,
        "edges_skipped": 0,
        "orgdbtmodels_created": 0,
        "orgdbtmodels_updated": 0,
    }

    # Maps to track nodes for edge creation
    node_map = {}  # unique_id -> CanvasNode

    try:
        with transaction.atomic():
            # Process sources from manifest
            sources = manifest_json.get("sources", {})
            logger.info(f"Processing {len(sources)} sources from manifest")

            for source_id, source_data in sources.items():
                # Extract source information
                source_name = source_data.get("source_name", "")
                table_name = source_data.get("name", "")
                database = source_data.get("database", "")
                schema = source_data.get("schema", "")

                # Create display name for source
                display_name = f"{source_name}.{table_name}"

                # Try to fetch columns from warehouse first
                output_cols = []
                try:
                    credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)
                    wclient = WarehouseFactory.connect(credentials, wtype=org_warehouse.wtype)
                    warehouse_cols = wclient.get_table_columns(schema, table_name)
                    output_cols = [col["name"] for col in warehouse_cols]
                    logger.info(
                        f"Fetched {len(output_cols)} columns from warehouse for source {display_name}"
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to fetch columns from warehouse for {display_name}: {str(e)}"
                    )
                    # Fallback to manifest columns
                    columns = source_data.get("columns", {})
                    output_cols = [col_name for col_name in columns.keys()]
                    logger.info(
                        f"Using {len(output_cols)} columns from manifest for source {display_name}"
                    )

                # Check if OrgDbtModel already exists
                orgdbt_model, model_created = OrgDbtModel.objects.get_or_create(
                    orgdbt=orgdbt,
                    name=table_name,
                    source_name=source_name,
                    type=OrgDbtModelType.SOURCE,
                    defaults={
                        "uuid": uuid.uuid4(),
                        "display_name": display_name,
                        "schema": schema,
                        "output_cols": output_cols,
                        "under_construction": False,
                    },
                )

                if not model_created and output_cols:
                    # Update output_cols if model exists
                    orgdbt_model.output_cols = output_cols
                    orgdbt_model.save()
                    stats["orgdbtmodels_updated"] += 1
                else:
                    stats["orgdbtmodels_created"] += 1

                # Check if CanvasNode already exists
                canvas_node, node_created = CanvasNode.objects.get_or_create(
                    orgdbt=orgdbt,
                    dbtmodel=orgdbt_model,
                    defaults={
                        "uuid": uuid.uuid4(),
                        "node_type": CanvasNodeType.SOURCE,
                        "name": display_name,
                        "output_cols": output_cols,
                    },
                )

                if node_created:
                    stats["nodes_created"] += 1
                else:
                    # Update output_cols if node exists
                    canvas_node.output_cols = output_cols
                    canvas_node.save()
                    stats["nodes_updated"] += 1

                # Store in map for edge creation
                node_map[source_id] = canvas_node
                stats["sources_processed"] += 1

                logger.info(f"Processed source: {display_name} (created: {node_created})")

            # Process models from manifest
            nodes = manifest_json.get("nodes", {})

            # Get the project name from manifest metadata
            project_name = manifest_json.get("metadata", {}).get("project_name", "")

            if not project_name:
                logger.warning("Could not determine project_name from manifest metadata")

            # Log all unique package names for debugging
            all_package_names = set(
                v.get("package_name", "")
                for v in nodes.values()
                if v.get("resource_type") == "model"
            )
            logger.info(f"All package names found in manifest: {all_package_names}")
            logger.info(f"Project name from metadata: '{project_name}'")

            # Filter to only include models from the user's project (exclude packages like elementary)
            model_nodes = {
                k: v
                for k, v in nodes.items()
                if v.get("resource_type") == "model" and v.get("package_name") == project_name
            }
            logger.info(
                f"Processing {len(model_nodes)} models from manifest (filtered to project: {project_name})"
            )

            for node_id, node_data in model_nodes.items():
                # Extract model information
                model_name = node_data.get("name", "")
                database = node_data.get("database", "")
                schema = node_data.get("schema", "")
                path = node_data.get("path", "")
                original_file_path = node_data.get("original_file_path", "")

                # Try to fetch columns from warehouse first
                output_cols = []
                try:
                    credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)
                    wclient = WarehouseFactory.connect(credentials, wtype=org_warehouse.wtype)
                    warehouse_cols = wclient.get_table_columns(schema, model_name)
                    output_cols = [col["name"] for col in warehouse_cols]
                    logger.info(
                        f"Fetched {len(output_cols)} columns from warehouse for model {model_name}"
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to fetch columns from warehouse for {model_name}: {str(e)}"
                    )
                    # Fallback to manifest columns
                    columns = node_data.get("columns", {})
                    output_cols = [col_name for col_name in columns.keys()]
                    logger.info(
                        f"Using {len(output_cols)} columns from manifest for model {model_name}"
                    )

                # Check if OrgDbtModel already exists
                orgdbt_model, model_created = OrgDbtModel.objects.get_or_create(
                    orgdbt=orgdbt,
                    name=model_name,
                    type=OrgDbtModelType.MODEL,
                    defaults={
                        "uuid": uuid.uuid4(),
                        "display_name": model_name,
                        "schema": schema,
                        "sql_path": original_file_path or path,
                        "output_cols": output_cols,
                        "under_construction": False,
                    },
                )

                if not model_created:
                    # Update fields if model exists
                    orgdbt_model.schema = schema
                    orgdbt_model.sql_path = original_file_path or path
                    if output_cols:
                        orgdbt_model.output_cols = output_cols
                    orgdbt_model.save()
                    stats["orgdbtmodels_updated"] += 1
                else:
                    stats["orgdbtmodels_created"] += 1

                # Check if CanvasNode already exists
                canvas_node, node_created = CanvasNode.objects.get_or_create(
                    orgdbt=orgdbt,
                    dbtmodel=orgdbt_model,
                    defaults={
                        "uuid": uuid.uuid4(),
                        "node_type": CanvasNodeType.MODEL,
                        "name": model_name,
                        "output_cols": output_cols,
                    },
                )

                if node_created:
                    stats["nodes_created"] += 1
                else:
                    # Update output_cols if node exists
                    canvas_node.output_cols = output_cols
                    canvas_node.save()
                    stats["nodes_updated"] += 1

                # Store in map for edge creation
                node_map[node_id] = canvas_node
                stats["models_processed"] += 1

                logger.info(f"Processed model: {model_name} (created: {node_created})")

            # Create edges based on dependencies
            logger.info("Creating edges based on dependencies...")

            for node_id, node_data in model_nodes.items():
                if node_id not in node_map:
                    continue

                to_node = node_map[node_id]
                depends_on = node_data.get("depends_on", {})
                depends_on_nodes = depends_on.get("nodes", [])

                for dependency_id in depends_on_nodes:
                    if dependency_id in node_map:
                        from_node = node_map[dependency_id]

                        # Check if edge already exists
                        edge_exists = CanvasEdge.objects.filter(
                            from_node=from_node, to_node=to_node
                        ).exists()

                        if not edge_exists:
                            CanvasEdge.objects.create(from_node=from_node, to_node=to_node, seq=1)
                            stats["edges_created"] += 1
                            logger.info(f"Created edge: {from_node.name} -> {to_node.name}")
                        else:
                            stats["edges_skipped"] += 1

    except Exception as e:
        logger.error(f"Error parsing manifest: {str(e)}")
        raise Exception(f"Failed to parse manifest: {str(e)}")

    logger.info(f"Manifest parsing complete. Stats: {stats}")
    return stats
