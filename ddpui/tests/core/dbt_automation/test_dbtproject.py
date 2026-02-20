from ddpui.core.dbt_automation.utils.dbtproject import dbtProject
import os
import yaml


def test_init_constructor():
    """test the constructor"""
    project = dbtProject("test")
    assert project.project_dir == "test"


def test_init_constructor_failure():
    """test the constructor"""
    try:
        dbtProject()
    except TypeError:
        assert True
    else:
        assert False


def test_sources_filename(tmpdir):  # pytest tmpdir fixture
    """test the sources_filename method"""
    project = dbtProject(tmpdir)
    assert (
        project.sources_filename("test_schema") == tmpdir / "models" / "test_schema" / "sources.yml"
    )


def test_models_dir(tmpdir):  # pytest tmpdir fixture
    """test the models_dir method"""
    project = dbtProject(tmpdir)
    assert project.models_dir("test_schema") == tmpdir / "models" / "test_schema"


def test_models_dir_subdir(tmpdir):  # pytest tmpdir fixture
    """test the models_dir method"""
    project = dbtProject(tmpdir)
    assert (
        project.models_dir("test_schema", "test_subdir")
        == tmpdir / "models" / "test_schema" / "test_subdir"
    )


def test_ensure_models_dir(tmpdir):  # pytest tmpdir fixture
    """test the ensure_models_dir method"""
    assert os.path.exists(tmpdir / "models" / "test_schema") is False
    project = dbtProject(tmpdir)
    project.ensure_models_dir("test_schema")
    assert os.path.exists(tmpdir / "models" / "test_schema") is True


def test_write_model(tmpdir):  # pytest tmpdir fixture
    """test the write_model method"""
    project = dbtProject(tmpdir)
    sql_input_model = "select * from table"
    sql_filename = project.write_model("test_schema", "test_model", sql_input_model)
    assert os.path.exists(tmpdir / "models" / "test_schema" / "test_model.sql") is True
    with open(
        tmpdir / "models" / "test_schema" / "test_model.sql", "r", encoding="utf-8"
    ) as sql_file:
        model_sql = sql_file.read()

    assert model_sql.find(sql_input_model) > -1
    assert str(sql_filename) == "models/test_schema/test_model.sql"


def test_write_model_config(tmpdir):  # pytest tmpdir fixture
    """test the write_model_config method"""
    schema = "test_schema"
    project = dbtProject(tmpdir)
    models_input = [
        {
            "name": "test_table",
            "description": "",
            "+schema": schema,
            "columns": [
                {
                    "name": "_airbyte_ab_id",
                    "description": "",
                    "tests": ["unique", "not_null"],
                }
            ],
        }
    ]
    yaml_filename = project.write_model_config("test_schema", models_input)
    assert os.path.exists(tmpdir / "models" / schema / "models.yml") is True
    with open(tmpdir / "models" / schema / "models.yml", "r", encoding="utf-8") as models_file:
        models_yaml = yaml.safe_load(models_file)

    assert len(models_yaml["models"]) == 1
    assert models_yaml["models"][0] == models_input[0]
    assert str(yaml_filename) == f"models/{schema}/models.yml"
