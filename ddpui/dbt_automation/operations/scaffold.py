"""setup the dbt project"""

import glob
import os, shutil, yaml
from pathlib import Path
from string import Template
from logging import basicConfig, getLogger, INFO
import subprocess, sys

from ddpui.dbt_automation.utils.warehouseclient import get_client
from ddpui.dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from ddpui.dbt_automation import assets
from ddpui.utils.file_storage.storage_factory import StorageFactory


basicConfig(level=INFO)
logger = getLogger()


def scaffold(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """scaffolds a dbt project"""
    storage = StorageFactory.get_storage_adapter()

    project_name = config["project_name"]
    default_schema = config["default_schema"]
    project_dir = str(Path(project_dir) / project_name)

    if storage.exists(project_dir):
        print("directory exists: %s", project_dir)
        return

    logger.info("mkdir %s", project_dir)
    storage.create_directory(project_dir)

    for subdir in [
        # "analyses",
        "logs",
        "macros",
        "models",
        # "seeds",
        # "snapshots",
        "target",
        "tests",
    ]:
        subdir_path = str(Path(project_dir) / subdir)
        storage.create_directory(subdir_path)
        logger.info("created %s", subdir_path)

    staging_dir = str(Path(project_dir) / "models" / "staging")
    intermediate_dir = str(Path(project_dir) / "models" / "intermediate")
    storage.create_directory(staging_dir)
    storage.create_directory(intermediate_dir)

    # copy all .sql files from assets/ to project_dir/macros
    # create if the file is not present in project_dir/macros
    assets_dir = assets.__path__[0]

    # loop over all sql macros with .sql extension
    for sql_file_path in glob.glob(os.path.join(assets_dir, "*.sql")):
        # Get the target path in the project_dir/macros directory
        target_path = str(Path(project_dir) / "macros" / Path(sql_file_path).name)

        # Read the local asset file and write to storage
        with open(sql_file_path, "r", encoding="utf-8") as f:
            content = f.read()
        storage.write_file(target_path, content)

        # Log the creation of the file
        logger.info("created %s", target_path)

    dbtproject_filename = str(Path(project_dir) / "dbt_project.yml")
    PROJECT_TEMPLATE = Template(
        """
name: '$project_name'
version: '1.0.0'
config-version: 2
profile: '$project_name'
model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]

target-path: "target"
clean-targets:
    - "target"
    - "dbt_packages"
"""
    )
    dbtproject_template = PROJECT_TEMPLATE.substitute({"project_name": project_name})
    storage.write_file(dbtproject_filename, dbtproject_template)
    logger.info("wrote %s", dbtproject_filename)

    dbtpackages_filename = str(Path(project_dir) / "packages.yml")
    packages_content = yaml.safe_dump(
        {"packages": [{"package": "dbt-labs/dbt_utils", "version": "1.1.1"}]}
    )
    storage.write_file(dbtpackages_filename, packages_content)

    # create a python virtual environment in project directory
    subprocess.call([sys.executable, "-m", "venv", Path(project_dir) / "venv"])

    # install dbt and dbt-bigquery or dbt-postgres based on the warehouse in the virtual environment
    logger.info("installing package to setup & run for a %s warehouse", warehouse.name)
    logger.info("using pip from %s", Path(project_dir) / "venv" / "bin" / "pip")
    subprocess.call(
        [
            Path(project_dir) / "venv" / "bin" / "pip",
            "install",
            "--upgrade",
            "pip",
        ]
    )
    subprocess.call(
        [Path(project_dir) / "venv" / "bin" / "pip", "install", f"dbt-{warehouse.name}"]
    )

    # create profiles.yaml that will be used to connect to the warehouse
    profiles_filename = Path(project_dir) / "profiles.yml"
    profiles_yml_obj = warehouse.generate_profiles_yaml_dbt(
        default_schema=default_schema, project_name=project_name
    )
    logger.info("successfully generated profiles.yml and now writing it to the file")
    with open(profiles_filename, "w", encoding="utf-8") as file:
        yaml.safe_dump(
            profiles_yml_obj,
            file,
        )
    logger.info("generated profiles.yml successfully")

    # run dbt debug to check warehouse connection
    logger.info("running dbt debug to check warehouse connection")
    try:
        subprocess.check_call(
            [
                Path(project_dir) / "venv/bin/dbt",
                "debug",
                "--project-dir",
                project_dir,
                "--profiles-dir",
                project_dir,
            ],
        )

    except subprocess.CalledProcessError as e:
        logger.error(f"dbt debug failed with {e.returncode}")
        raise Exception("Something went wrong while running dbt debug")

    logger.info("successfully ran dbt debug")
