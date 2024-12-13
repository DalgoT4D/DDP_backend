from typing import Literal, Optional, Union
from ninja import Schema
from pathlib import Path


class DbtCliParams(Schema):
    """
    Schema to define all parameters required to run a dbt project using CLI.
    """

    dbt_env_dir: Union[str, Path]
    project_dir: Union[str, Path]
    org_project_dir: Union[str, Path]
    target: str
    venv_binary: Union[str, Path]
    dbt_binary: Union[str, Path]


class DbtCloudParams(Schema):
    """
    Schema to define all parameters required to run a dbt project using dbt Cloud.
    """

    api_key: str
    account_id: int
    job_id: int


DbtProjectParams = Union[DbtCliParams, DbtCloudParams]
