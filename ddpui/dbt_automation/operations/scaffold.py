"""setup the dbt project"""

import glob
import os, shutil, yaml
from pathlib import Path
from string import Template
from logging import basicConfig, getLogger, INFO
import subprocess, sys

from dbt_automation.utils.warehouseclient import get_client
from dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface
from dbt_automation import assets


basicConfig(level=INFO)
logger = getLogger()


def scaffold(config: dict, warehouse: WarehouseInterface, project_dir: str):
    """scaffolds a dbt project"""
    project_name = config["project_name"]
    default_schema = config["default_schema"]
    project_dir = Path(project_dir) / project_name

    if os.path.exists(project_dir):
        print("directory exists: %s", project_dir)
        return

    logger.info("mkdir %s", project_dir)
    os.makedirs(project_dir)

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
        (Path(project_dir) / subdir).mkdir()
        logger.info("created %s", str(Path(project_dir) / subdir))

    (Path(project_dir) / "models" / "staging").mkdir()
    (Path(project_dir) / "models" / "intermediate").mkdir()

    # copy all .sql files from assets/ to project_dir/macros
    # create if the file is not present in project_dir/macros
    assets_dir = assets.__path__[0]

    # loop over all sql macros with .sql extension
    for sql_file_path in glob.glob(os.path.join(assets_dir, "*.sql")):
        # Get the target path in the project_dir/macros directory
        target_path = Path(project_dir) / "macros" / Path(sql_file_path).name

        # Copy the .sql file to the target path
        shutil.copy(sql_file_path, target_path)

        # Log the creation of the file
        logger.info("created %s", target_path)

    dbtproject_filename = Path(project_dir) / "dbt_project.yml"
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
    with open(dbtproject_filename, "w", encoding="utf-8") as dbtprojectfile:
        dbtprojectfile.write(dbtproject_template)
        logger.info("wrote %s", dbtproject_filename)

    dbtpackages_filename = Path(project_dir) / "packages.yml"
    with open(dbtpackages_filename, "w", encoding="utf-8") as dbtpackgesfile:
        yaml.safe_dump(
            {"packages": [{"package": "dbt-labs/dbt_utils", "version": "1.1.1"}]},
            dbtpackgesfile,
        )

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
