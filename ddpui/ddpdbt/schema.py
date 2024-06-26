from typing import Union
from pathlib import Path
from ninja import Schema


class DbtProjectParams(Schema):
    """
    schema to define all parameters required to run a dbt project
    """

    dbt_env_dir: Union[str, Path]
    dbt_binary: Union[str, Path]
    project_dir: Union[str, Path]
    target: str
    dbt_repo_dir: Union[str, Path]
