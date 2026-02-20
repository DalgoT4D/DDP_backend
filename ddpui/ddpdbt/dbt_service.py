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
from ddpui.core.dbt_automation import assets
from ddpui.utils.file_storage.storage_factory import StorageFactory
from ddpui.ddpprefect import prefect_service, SECRET, DBTCLIPROFILE
from ddpui.models.org import Org, OrgDbt, OrgPrefectBlockv1, OrgWarehouse, TransformType
from ddpui.models.org_user import OrgUser
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
from ddpui.core.git_manager import GitManager, GitManagerError
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory
from ddpui.utils.custom_logger import CustomLogger
from ddpui.ddpprefect.schema import PrefectSecretBlockEdit, OrgDbtConnectGitRemote

logger = CustomLogger("ddpui")

DBT_GITIGNORE_CONTENT = [
    "target/",
    "dbt_packages/",
    "logs/",
    ".venv/",
    "venv/",
    "profiles/",
    "*/profiles.yml",
    "profiles.yaml",
    ".user.yml",
    "package-lock.yml",
    ".env*",
    ".env.local",
    "elementary_profiles/",
    "*.html",  # ignore elementary reports
]


def update_github_pat_storage(
    org: Org, git_repo_url: str, access_token: str, existing_pat_secret: str = None
) -> str:
    """
    Handle PAT storage for both Prefect secret blocks and AWS Secrets Manager.

    Args:
        org: The organization instance
        git_repo_url: The GitHub repository URL
        access_token: The GitHub personal access token
        existing_pat_secret: Existing PAT secret key if updating

    Returns:
        str: The PAT secret key from secrets manager
    """
    # Create oauth URL for prefect secret block
    gitrepo_url_with_token = GitManager.generate_oauth_url_static(git_repo_url, access_token)

    # Create or update the prefect secret block
    secret_block_edit_params = PrefectSecretBlockEdit(
        block_name=f"{org.slug}-git-pull-url",
        secret=gitrepo_url_with_token,
    )

    response = prefect_service.upsert_secret_block(secret_block_edit_params)
    if not OrgPrefectBlockv1.objects.filter(
        org=org, block_type=SECRET, block_name=secret_block_edit_params.block_name
    ).exists():
        OrgPrefectBlockv1.objects.create(
            org=org,
            block_type=SECRET,
            block_name=secret_block_edit_params.block_name,
            block_id=response["block_id"],
        )

    # Update or create PAT in secrets manager
    if existing_pat_secret:
        secretsmanager.update_github_pat(existing_pat_secret, access_token)
        return existing_pat_secret
    else:
        pat_secret_key = secretsmanager.save_github_pat(access_token)
        return pat_secret_key


def clear_github_pat_storage(org: Org, pat_secret_key: str = None) -> None:
    """
    Clear all PAT storage from both Prefect secret blocks and AWS Secrets Manager.

    This function removes:
    1. Prefect secret block for git-pull-url
    2. PAT from AWS Secrets Manager (if pat_secret_key provided)

    Args:
        org: The organization instance
        pat_secret_key: Optional PAT secret key to delete from secrets manager
    """
    # 1. Delete Prefect secret block
    block_name = f"{org.slug}-git-pull-url"
    secret_block = OrgPrefectBlockv1.objects.filter(
        org=org, block_type=SECRET, block_name=block_name
    ).first()

    if secret_block:
        try:
            prefect_service.delete_secret_block(secret_block.block_id)
            secret_block.delete()
            logger.info(f"Deleted Prefect secret block: {block_name}")
        except Exception as e:
            logger.warning(f"Failed to delete Prefect secret block {block_name}: {str(e)}")

    # 2. Delete PAT from secrets manager
    if pat_secret_key:
        try:
            secretsmanager.delete_github_pat(pat_secret_key)
            logger.info(f"Deleted PAT from secrets manager: {pat_secret_key}")
        except Exception as e:
            logger.warning(f"Failed to delete PAT from secrets manager: {str(e)}")


def is_git_repository_switch(orgdbt: OrgDbt, new_repo_url: str) -> bool:
    """
    Determine if this is a Git repository switch vs new connection.

    Returns True for: Git Repo A → Git Repo B
    Returns False for: UI4T → Git Repo A or Git Repo A → Git Repo A

    Args:
        orgdbt: The OrgDbt instance
        new_repo_url: The new repository URL to connect to

    Returns:
        bool: True if this is a repository switch, False otherwise
    """
    return (
        orgdbt.gitrepo_url is not None
        and orgdbt.transform_type == TransformType.GIT
        and orgdbt.gitrepo_url != new_repo_url
    )


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
        storage = StorageFactory.get_storage_adapter()
        if storage.exists(dbt.project_dir):
            storage.delete_file(dbt.project_dir)
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


def sync_gitignore_contents(
    dbt_project_path: str, ignore_entries: list[str] = DBT_GITIGNORE_CONTENT
):
    """
    If the .gitignore file exists in the dbt project path, read and append the entries that are not already present.
    If it does not exist, create one with the provided entries.
    """

    storage = StorageFactory.get_storage_adapter()
    gitignore_path = str(Path(dbt_project_path) / ".gitignore")
    existing_entries = set()

    if storage.exists(gitignore_path):
        content = storage.read_file(gitignore_path)
        for line in content.splitlines():
            existing_entries.add(line.strip())

    # Prepare new content to append
    new_lines = []
    for entry in ignore_entries:
        if entry not in existing_entries:
            new_lines.append(entry)

    if new_lines:
        # If file exists, append to existing content, otherwise create new
        if storage.exists(gitignore_path):
            existing_content = storage.read_file(gitignore_path)
            new_content = existing_content + "\n" + "\n".join(new_lines) + "\n"
        else:
            new_content = "\n".join(new_lines) + "\n"

        storage.write_file(gitignore_path, new_content)

    logger.info(f"Synced .gitignore at {gitignore_path} with new entries.")


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
    storage = StorageFactory.get_storage_adapter()
    if storage.exists(str(dbtrepo_dir)):
        raise Exception(f"Project {project_name} already exists")

    if not storage.exists(str(project_dir)):
        storage.create_directory(str(project_dir))
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
        storage = StorageFactory.get_storage_adapter()
        example_models_dir = str(dbtrepo_dir / "models" / "example")
        if storage.exists(example_models_dir):
            storage.delete_file(example_models_dir)

    except subprocess.CalledProcessError as e:
        logger.error(f"dbt init failed with {e.returncode}")
        raise Exception(f"dbt init failed: {e}") from e

    try:
        # copy packages.yml
        logger.info("copying packages.yml from assets")
        storage = StorageFactory.get_storage_adapter()
        target_packages_yml = str(Path(dbtrepo_dir) / "packages.yml")
        source_packages_yml = os.path.abspath(
            os.path.join(os.path.abspath(assets.__file__), "..", "packages.yml")
        )
        # Read local asset file and write to storage
        with open(source_packages_yml, "r", encoding="utf-8") as f:
            content = f.read()
        storage.write_file(target_packages_yml, content)

        # copy all macros with .sql extension from assets
        assets_dir = assets.__path__[0]

        for sql_file_path in glob.glob(os.path.join(assets_dir, "*.sql")):
            # Get the target path in the project_dir/macros directory
            target_path = str(Path(dbtrepo_dir) / "macros" / Path(sql_file_path).name)

            # Read local asset file and write to storage
            with open(sql_file_path, "r", encoding="utf-8") as f:
                content = f.read()
            storage.write_file(target_path, content)

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
        raise Exception(f"failed to create dbt cli profile for org {org.name}: {error}")

    # initializing it as git repo
    logger.info("initializing dbt workspace as git repo")
    try:
        git_manager = GitManager(repo_local_path=dbtrepo_dir)
        git_manager.init_repo()
    except Exception as err:
        logger.error(f"Failed to initialize git repo: {str(err)}")
        raise Exception(f"Failed to initialize git repo: {str(err)}") from err

    # create .gitignore file
    try:
        sync_gitignore_contents(dbtrepo_dir)
    except Exception as err:
        logger.error(f"Failed to create .gitignore file: {str(err)}")
        raise Exception(f"Failed to create .gitignore file: {str(err)}") from err

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
        storage = StorageFactory.get_storage_adapter()
        profile_dirname = str(Path(dbt_project_params.project_dir) / "profiles")
        storage.create_directory(profile_dirname)
        profile_filename = str(Path(dbt_project_params.project_dir) / "profiles" / "profiles.yml")
        logger.info("writing dbt profile to " + profile_filename)
        profile_content = yaml.safe_dump(profile)
        storage.write_file(profile_filename, profile_content)
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
    storage = StorageFactory.get_storage_adapter()
    project_dir: Path = Path(DbtProjectManager.get_dbt_project_dir(orgdbt))
    manifest_path = str(project_dir / "target" / "manifest.json")

    if not storage.exists(manifest_path):
        logger.error("dbt compile did not generate manifest.json")
        raise Exception("dbt compile did not generate manifest.json")

    manifest_content = storage.read_file(manifest_path)
    manifest_json = json.loads(manifest_content)

    return manifest_json


def _has_operation_chain_between_nodes(from_node, to_node) -> bool:
    """
    Check if there's already an operation chain between two nodes.

    This prevents creating direct edges when nodes are already connected
    through a chain of operations (e.g., source -> op1 -> op2 -> model).

    Uses breadth-first search to find if there's a path from from_node to to_node
    that goes through at least one operation node.

    Args:
        from_node: Starting CanvasNode
        to_node: Target CanvasNode

    Returns:
        bool: True if operation chain exists, False otherwise
    """
    from collections import deque

    # BFS to find path from from_node to to_node through operations
    queue = deque([(from_node, False)])  # (current_node, has_passed_through_operation)
    visited = set([from_node.id])

    while queue:
        current_node, passed_through_operation = queue.popleft()

        # Check all outgoing edges from current node
        outgoing_edges = CanvasEdge.objects.filter(from_node=current_node).select_related("to_node")

        for edge in outgoing_edges:
            next_node = edge.to_node

            # If we reached the target node and passed through at least one operation
            if next_node.id == to_node.id and passed_through_operation:
                return True

            # Continue BFS if not visited
            if next_node.id not in visited:
                visited.add(next_node.id)

                # Mark if we've passed through an operation
                is_operation = next_node.node_type == CanvasNodeType.OPERATION
                has_operation_in_path = passed_through_operation or is_operation

                queue.append((next_node, has_operation_in_path))

    return False


def parse_dbt_manifest_to_canvas(
    org: Org,
    orgdbt: OrgDbt,
    org_warehouse: OrgWarehouse,
    manifest_json: dict = None,
    refresh: bool = True,
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

    # Load the manifest.json from the target folder if it exists
    storage = StorageFactory.get_storage_adapter()
    dbt_repo_dir = Path(DbtProjectManager.get_dbt_project_dir(orgdbt))
    manifest_json_path = str(dbt_repo_dir / "target" / "manifest.json")
    if storage.exists(manifest_json_path):
        manifest_content = storage.read_file(manifest_json_path)
        manifest_json = json.loads(manifest_content)

    # Overwrite/generate manifest if not provided or if we are refreshing the manifest
    if manifest_json is None or refresh:
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
        "edges_deleted": 0,
        "orgdbtmodels_created": 0,
        "orgdbtmodels_updated": 0,
    }

    # Maps to track nodes for edge creation
    node_map = {}  # unique_id -> CanvasNode

    credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)
    wclient = WarehouseFactory.connect(credentials, wtype=org_warehouse.wtype)

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
                path = source_data.get("path", "")
                original_file_path = source_data.get("original_file_path", "")

                # Create display name for source
                display_name = f"{source_name}.{table_name}"

                # Try to fetch columns from warehouse first
                output_cols = []
                try:
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
                orgdbt_model, model_created = OrgDbtModel.objects.update_or_create(
                    orgdbt=orgdbt,
                    name=table_name,
                    schema=schema,
                    defaults={
                        "uuid": uuid.uuid4(),
                        "type": OrgDbtModelType.SOURCE,
                        "display_name": table_name,
                        "output_cols": output_cols,
                        "sql_path": original_file_path or path,
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
                orgdbt_model, model_created = OrgDbtModel.objects.update_or_create(
                    orgdbt=orgdbt,
                    name=model_name,
                    schema=schema,
                    defaults={
                        "uuid": uuid.uuid4(),
                        "type": OrgDbtModelType.MODEL,
                        "display_name": model_name,
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

                        # Check if there's already an operation chain between the nodes
                        operation_chain_exists = _has_operation_chain_between_nodes(
                            from_node, to_node
                        )

                        # Check if direct edge already exists
                        direct_edge_exists = CanvasEdge.objects.filter(
                            from_node=from_node, to_node=to_node
                        ).exists()

                        if operation_chain_exists:
                            # If operation chain exists, delete any direct edge between the nodes
                            if direct_edge_exists:
                                CanvasEdge.objects.filter(
                                    from_node=from_node, to_node=to_node
                                ).delete()
                                logger.info(
                                    f"Deleted direct edge {from_node.name} -> {to_node.name}: operation chain exists"
                                )
                                stats["edges_deleted"] += 1
                            else:
                                logger.info(
                                    f"Skipped edge {from_node.name} -> {to_node.name}: operation chain exists"
                                )
                                stats["edges_skipped"] += 1
                        elif not direct_edge_exists:
                            # No operation chain and no direct edge - create direct edge
                            CanvasEdge.objects.create(from_node=from_node, to_node=to_node, seq=1)
                            stats["edges_created"] += 1
                            logger.info(f"Created edge: {from_node.name} -> {to_node.name}")
                        else:
                            # Direct edge already exists and no operation chain - keep it
                            stats["edges_skipped"] += 1
                            logger.info(
                                f"Skipped edge {from_node.name} -> {to_node.name}: direct edge exists"
                            )

    except Exception as e:
        logger.error(f"Error parsing manifest: {str(e)}")
        raise Exception(f"Failed to parse manifest: {str(e)}")

    logger.info(f"Manifest parsing complete. Stats: {stats}")
    return stats


def sync_remote_dbtproject_to_canvas(org: Org, orgdbt: OrgDbt, warehouse_obj: OrgWarehouse) -> dict:
    """
    1. Fetch the latest changes from remote git repo
    2. Compile and generate manifest.json
    3. Parse manifest.json to canvas
    """
    logger.info(f"Syncing remote dbt project to canvas for org: {org.slug}")

    storage = StorageFactory.get_storage_adapter()
    dbt_repo_dir = Path(DbtProjectManager.get_dbt_project_dir(orgdbt))
    if not storage.exists(str(dbt_repo_dir)):
        raise Exception("DBT repo directory does not exist")

    pat = secretsmanager.retrieve_github_pat(orgdbt.gitrepo_access_token_secret)

    # Fetch changes from remote git repo
    try:
        git_manager = GitManager(repo_local_path=str(dbt_repo_dir), pat=pat, validate_git=True)
    except GitManagerError as e:
        logger.error(f"GitManagerError during git init validation: {e.message}")
        raise Exception(f"Git is not initialized in the DBT project folder: {e.message}") from e

    try:
        git_manager.pull_changes()
    except GitManagerError as e:
        logger.error(f"GitManagerError during git pull: {e.message}")
        raise Exception(f"Failed to pull changes from remote git repo: {e.error}") from e

    # Parse the dbt manifest
    manifest_stats = parse_dbt_manifest_to_canvas(org, orgdbt, warehouse_obj)

    logger.info(f"Sync complete. Stats: {manifest_stats}")
    return manifest_stats


def cleanup_unused_sources(org: Org, orgdbt: OrgDbt, manifest_json=None):
    """
    Clean up sources that are not being used by any models.
    Checks manifest.json for dependencies, removes unused sources from .yml files,
    and removes corresponding CanvasNode entries if they have no edges.
    Also checks existing canvas source nodes for any without edges and cleans them up.

    Args:
        org: The organization instance
        orgdbt: The organization's dbt instance
        manifest_json: Optional manifest data, if None will be generated

    Returns:
        Dict with cleanup results
    """
    from ddpui.core.dbtautomation_service import delete_dbt_source_in_project

    results = {"sources_removed": [], "sources_with_edges_skipped": [], "errors": []}

    try:
        # Get manifest data
        if manifest_json is None:
            manifest = generate_manifest_json_for_dbt_project(org, orgdbt)
        else:
            manifest = manifest_json

        # Get all sources from manifest
        manifest_sources = manifest.get("sources", {})

        # Get all models and their dependencies from manifest
        manifest_models = manifest.get("nodes", {})

        # Build dependency map - which sources are used by models
        used_sources = set()

        # Check direct dependencies in models
        for model_key, model_data in manifest_models.items():
            if model_data.get("resource_type") == "model":
                depends_on = model_data.get("depends_on", {})
                source_nodes = depends_on.get("nodes", [])

                # Add source dependencies
                for dep in source_nodes:
                    if dep.startswith("source."):
                        used_sources.add(dep)

        # Check child_map for indirect dependencies
        child_map = manifest.get("child_map", {})
        for source_key in manifest_sources.keys():
            children = child_map.get(source_key, [])
            if any(child.startswith("model.") for child in children):
                used_sources.add(source_key)

        # Find unused sources
        all_sources = set(manifest_sources.keys())
        unused_sources = all_sources - used_sources

        # Process unused sources from manifest
        for unused_source_key in unused_sources:
            try:
                source_data = manifest_sources[unused_source_key]
                source_name = source_data.get("source_name")
                table_name = source_data.get("name")
                source_schema = source_data.get("schema")

                # Find corresponding unique OrgDbtModel using name and schema
                try:
                    orgdbt_model = OrgDbtModel.objects.get(
                        orgdbt=orgdbt,
                        type=OrgDbtModelType.SOURCE,
                        name=table_name,
                        schema=source_schema,
                    )

                    # Find corresponding unique CanvasNode using the orgdbt_model
                    try:
                        canvas_node = CanvasNode.objects.get(dbtmodel=orgdbt_model)

                        # Check if there are any edges (incoming or outgoing)
                        has_incoming_edges = CanvasEdge.objects.filter(to_node=canvas_node).exists()
                        has_outgoing_edges = CanvasEdge.objects.filter(
                            from_node=canvas_node
                        ).exists()

                        if has_incoming_edges or has_outgoing_edges:
                            # Skip deletion if the node has edges
                            results["sources_with_edges_skipped"].append(
                                f"{source_schema}.{table_name}"
                            )
                        else:
                            # No edges, safe to delete
                            # Delete the CanvasNode
                            canvas_node.delete()

                            # Remove from yml file using existing function
                            delete_dbt_source_in_project(orgdbt_model)

                            results["sources_removed"].append(f"{source_schema}.{table_name}")

                    except CanvasNode.DoesNotExist:
                        # No CanvasNode found, just remove from yml
                        delete_dbt_source_in_project(orgdbt_model)
                        results["sources_removed"].append(f"{source_schema}.{table_name}")

                    except CanvasNode.MultipleObjectsReturned:
                        results["errors"].append(
                            f"Multiple CanvasNodes found for {source_schema}.{table_name}"
                        )

                except OrgDbtModel.DoesNotExist:
                    # No OrgDbtModel found, skip
                    continue

                except OrgDbtModel.MultipleObjectsReturned:
                    results["errors"].append(
                        f"Multiple OrgDbtModels found for {source_schema}.{table_name}"
                    )

            except Exception as e:
                results["errors"].append(f"Error processing source {unused_source_key}: {str(e)}")

        # Additional cleanup: Check all existing canvas source nodes without edges
        try:
            # Get all source CanvasNodes for this orgdbt
            source_canvas_nodes = CanvasNode.objects.filter(
                orgdbt=orgdbt, node_type=CanvasNodeType.SOURCE
            )

            for canvas_node in source_canvas_nodes:
                try:
                    # Check if there are any edges (incoming or outgoing)
                    has_incoming_edges = CanvasEdge.objects.filter(to_node=canvas_node).exists()
                    has_outgoing_edges = CanvasEdge.objects.filter(from_node=canvas_node).exists()

                    if not has_incoming_edges and not has_outgoing_edges:
                        # No edges, check if this source has an OrgDbtModel
                        if canvas_node.dbtmodel:
                            orgdbt_model = canvas_node.dbtmodel
                            source_identifier = f"{orgdbt_model.schema}.{orgdbt_model.name}"

                            # Check if we haven't already processed this source
                            if source_identifier not in results["sources_removed"]:
                                # Delete the CanvasNode
                                canvas_node.delete()

                                # Remove from yml file using existing function
                                delete_dbt_source_in_project(orgdbt_model)

                                results["sources_removed"].append(source_identifier)
                        else:
                            # CanvasNode without dbtmodel, just delete the canvas node
                            canvas_node.delete()

                except Exception as e:
                    results["errors"].append(
                        f"Error processing canvas node {canvas_node.id}: {str(e)}"
                    )

        except Exception as e:
            results["errors"].append(f"Error during canvas cleanup: {str(e)}")

    except Exception as e:
        results["errors"].append(f"Error during cleanup: {str(e)}")

    return results


def connect_git_remote(orguser: OrgUser, payload: OrgDbtConnectGitRemote, actual_pat: str) -> dict:
    """
    Handle connecting to a Git remote for the first time (UI4T → Git).

    This abstracts the existing logic from put_connect_git_remote into a
    clean, testable function.
    """
    org: Org = orguser.org
    orgdbt: OrgDbt = org.dbt

    # Get dbt repo directory
    storage = StorageFactory.get_storage_adapter()
    dbt_repo_dir = Path(DbtProjectManager.get_dbt_project_dir(orgdbt))
    if not storage.exists(str(dbt_repo_dir)):
        raise Exception("DBT repo directory does not exist")

    # Use the provided actual_pat (already resolved in API layer)

    # Validate git is initialized locally
    try:
        git_manager = GitManager(
            repo_local_path=str(dbt_repo_dir), pat=actual_pat, validate_git=True
        )
    except GitManagerError as e:
        logger.error(f"GitManagerError during git init validation: {e.message}")
        raise Exception(f"Git is not initialized in the DBT project folder: {e.message}") from e

    # Verify remote URL is accessible with the PAT
    try:
        git_manager.verify_remote_url(payload.gitrepoUrl)
    except GitManagerError as e:
        logger.error(f"GitManagerError during remote URL verification: {e.message}")
        raise Exception(f"{e.message}: {e.error}") from e

    # Set or update the remote origin
    try:
        git_manager.set_remote(payload.gitrepoUrl)
    except GitManagerError as e:
        raise Exception(f"Failed to set remote: {e.message}") from e

    # sync local default to remote
    try:
        git_manager.sync_local_default_to_remote()
    except GitManagerError as e:
        raise Exception(f"Failed to sync local branch with remote: {e.error}") from e

    # Handle PAT token storage (only if not masked)
    is_token_masked = set(payload.gitrepoAccessToken.strip()) == set("*")
    if not is_token_masked:
        pat_secret_key = update_github_pat_storage(
            org, payload.gitrepoUrl, payload.gitrepoAccessToken, orgdbt.gitrepo_access_token_secret
        )
        orgdbt.gitrepo_access_token_secret = pat_secret_key

    # Update OrgDbt with the new gitrepo_url
    orgdbt.gitrepo_url = payload.gitrepoUrl
    orgdbt.transform_type = TransformType.GIT
    orgdbt.save()

    logger.info(f"Connected git remote for org {org.slug}: {payload.gitrepoUrl}")

    # sync gitignore contents
    try:
        sync_gitignore_contents(dbt_repo_dir)
    except Exception as err:
        logger.error(f"Failed to sync .gitignore contents: {err}")
        raise Exception(f"Failed to sync .gitignore contents: {err}") from err

    return {
        "success": True,
        "gitrepo_url": payload.gitrepoUrl,
        "message": "Successfully connected to remote git repository",
        "repository_switched": False,
    }


def switch_git_repository(
    orguser: OrgUser, payload: OrgDbtConnectGitRemote, actual_pat: str
) -> dict:
    """
    Handle switching from one Git repository to another (Git A → Git B).

    Complete replacement of the local repository with a fresh clone.
    """
    org = orguser.org
    orgdbt = org.dbt

    logger.info(
        f"Switching git repository for org {org.slug} from {orgdbt.gitrepo_url} to {payload.gitrepoUrl}"
    )

    # Use the provided actual_pat (already resolved in API layer)

    # Get paths
    dbt_project_dir = Path(DbtProjectManager.get_dbt_project_dir(orgdbt))
    org_dir = Path(DbtProjectManager.get_org_dir(org))

    # Clean existing references
    try:
        storage = StorageFactory.get_storage_adapter()
        dbt_project_dir_str = str(dbt_project_dir)
        if storage.exists(dbt_project_dir_str):
            storage.delete_file(dbt_project_dir_str)
            logger.info(f"Removed existing dbt directory: {dbt_project_dir_str}")

        # remove canvas nodes and edges related to the dbt project
        CanvasNode.objects.filter(orgdbt=orgdbt).delete()

    except Exception as e:
        logger.error(f"Failed to clean up existing directory: {e}")
        raise Exception(f"Failed to clean up existing repository: {e}") from e

    # Clone the new repository using GitManager
    try:
        GitManager.clone(
            cwd=str(org_dir),
            remote_repo_url=payload.gitrepoUrl,
            relative_path="dbtrepo",
            pat=actual_pat,
        )
        logger.info(f"Successfully cloned new repository to {dbt_project_dir}")
    except GitManagerError as e:
        logger.error(f"Failed to clone new repository: {e.message}")
        raise Exception(f"Failed to clone new repository: {e.message}") from e

    # Update OrgDbt with the new gitrepo_url BEFORE cloning
    orgdbt.gitrepo_url = payload.gitrepoUrl
    orgdbt.transform_type = TransformType.GIT
    orgdbt.save()

    # Update CLI profile block with new dbt_project.yml profile name
    try:
        warehouse = OrgWarehouse.objects.filter(org=org).first()
        if not warehouse:
            raise Exception("No warehouse configuration found for this organization")

        creds = secretsmanager.retrieve_warehouse_credentials(warehouse)

        # This will automatically read the new dbt_project.yml and update the profile block
        cli_profile_result, error = create_or_update_org_cli_block(
            org, warehouse, creds  # Pass the retrieved warehouse creds
        )
        if error:
            logger.error(f"Failed to update CLI profile block: {error}")
            raise Exception(f"Failed to update CLI profile block: {error}")

        logger.info("CLI profile block updated with new project configuration")
    except Exception as e:
        logger.error(f"Error updating CLI profile block: {e}")
        raise Exception(f"Repository switch failed: CLI profile block update error - {e}") from e

    # Verify remote URL is accessible with the PAT
    try:
        git_manager = GitManager(
            repo_local_path=str(dbt_project_dir), pat=actual_pat, validate_git=True
        )
        git_manager.verify_remote_url(payload.gitrepoUrl)
    except GitManagerError as e:
        logger.error(f"GitManagerError during remote URL verification: {e.message}")
        clear_github_pat_storage(org, orgdbt.gitrepo_access_token_secret)
        raise Exception(f"{e.message}: {e.error}") from e

    # Handle PAT token storage (only if not masked)
    is_token_masked = set(payload.gitrepoAccessToken.strip()) == set("*")
    if not is_token_masked:
        pat_secret_key = update_github_pat_storage(
            org, payload.gitrepoUrl, payload.gitrepoAccessToken, orgdbt.gitrepo_access_token_secret
        )
        orgdbt.gitrepo_access_token_secret = pat_secret_key
        orgdbt.save()

    # Sync .gitignore contents
    try:
        sync_gitignore_contents(dbt_project_dir)
    except Exception as err:
        logger.error(f"Failed to sync .gitignore contents: {err}")
        logger.warning("Continuing despite gitignore sync failure")

    logger.info(f"Successfully switched git repository for org {org.slug} to {payload.gitrepoUrl}")

    return {
        "success": True,
        "gitrepo_url": payload.gitrepoUrl,
        "message": "Successfully switched to new git repository",
        "repository_switched": True,
    }
