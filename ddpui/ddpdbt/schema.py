from typing import Optional, Union
from ninja import Schema
from pathlib import Path


class DbtProjectParams(Schema):
    """
    schema to define all parameters required to run a dbt project
    """

    dbt_env_dir: Union[str, Path]
    dbt_binary: Union[str, Path]
    project_dir: Union[str, Path]
    target: str
    dbt_repo_dir: Union[str, Path]
