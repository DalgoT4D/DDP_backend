from typing import Union
from pathlib import Path
from ninja import Schema


class DbtProjectParams(Schema):
    """
    schema to define all parameters required to run a dbt project
    """

    dbt_env_dir: Union[str, Path]
    project_dir: Union[str, Path]
    org_project_dir: Union[str, Path]
    target: str
    venv_binary: Union[str, Path]
    dbt_binary: Union[str, Path]


class DbtCloudJobParams(Schema):
    """
    Schema to define all parameters required to run a any dbt command using dbt Cloud.
    Extend this if you need to add more params while triggering a dbt cloud job
    """

    job_id: int
