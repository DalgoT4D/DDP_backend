"""the dbt project structure"""

import os
from pathlib import Path
import yaml

from ddpui.utils.file_storage.storage_factory import StorageFactory


class dbtProject:  # pylint:disable=invalid-name
    """the folder and files in a dbt project"""

    def __init__(self, project_dir: str):
        """constructor"""
        self.project_dir = project_dir
        self.storage = StorageFactory.get_storage_adapter()

    def sources_filename(self, schema: str) -> Path:
        """returns the pathname of the sources.yml in the folder for the given schema"""
        return Path(self.project_dir) / "models" / schema / "sources.yml"

    def models_dir(self, schema: str, subdir="") -> Path:
        """returns the path of the models folder for the given schema"""
        return Path(self.project_dir) / "models" / schema / subdir

    def ensure_models_dir(self, schema: str, subdir="") -> None:
        """ensures the existence of the output models folder for the given schema"""
        output_schema_dir = str(self.models_dir(schema, subdir))
        if not self.storage.exists(output_schema_dir):
            self.storage.create_directory(output_schema_dir)

    def strip_project_dir(self, child_dir: Path) -> str:
        """removes the leading project_dir from the child_dir"""
        return child_dir.relative_to(self.project_dir)

    def write_model(self, schema: str, modelname: str, model_sql: str, **kwargs) -> None:
        """writes a .sql model"""
        rel_dir_to_models = kwargs.get("rel_dir_to_models")

        if rel_dir_to_models is not None:
            self.ensure_models_dir(rel_dir_to_models)
        else:
            self.ensure_models_dir(schema, kwargs.get("subdir", ""))

        model_sql = (
            "--DBT AUTOMATION has generated this model, please DO NOT EDIT \n--Please make sure you dont change the model name \n\n"
            + model_sql
        )

        if rel_dir_to_models is not None:
            model_filename = Path(self.models_dir(rel_dir_to_models)) / (modelname + ".sql")
        else:
            model_filename = Path(self.models_dir(schema, kwargs.get("subdir", ""))) / (
                modelname + ".sql"
            )

        model_filename_str = str(model_filename)
        if kwargs.get("logger"):
            kwargs["logger"].info("[write_model] %s", model_filename_str)
        self.storage.write_file(model_filename_str, model_sql)

        return self.strip_project_dir(model_filename)

    def write_model_config(self, schema: str, models: list, **kwargs) -> None:
        """writes a .yml with a models: key"""
        self.ensure_models_dir(schema, kwargs.get("subdir", ""))
        models_filename = Path(self.models_dir(schema, kwargs.get("subdir", ""))) / "models.yml"
        models_filename_str = str(models_filename)

        if kwargs.get("logger"):
            kwargs["logger"].info("writing %s", models_filename_str)

        models_content = yaml.safe_dump(
            {
                "version": 2,
                "models": models,
            },
            sort_keys=False,
        )
        self.storage.write_file(models_filename_str, models_content)

        return self.strip_project_dir(models_filename)

    def delete_model(self, model_relative_path: Path):
        """Delete a model; relative path will look like models/intermediate/example_model.sql"""

        model_path = str(Path(self.project_dir) / model_relative_path)
        if self.storage.exists(model_path):
            self.storage.delete_file(model_path)
            return True

        return False
