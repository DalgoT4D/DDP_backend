from dbt_automation.utils.dbtconfigs import mk_model_config, get_columns_from_model


def test_mk_model_config():
    """test mk_model_config"""
    schemaname = "schemaname"
    modelname = "modelname"
    columnspec = ["column1", "column2"]
    model_config = mk_model_config(schemaname, modelname, columnspec)
    assert model_config["name"] == modelname
    assert model_config["columns"][0]["name"] == "_airbyte_ab_id"
    assert model_config["columns"][1]["name"] == "column1"
    assert model_config["columns"][2]["name"] == "column2"


def test_get_columns_from_model():
    """test get_columns_from_model"""
    models = {
        "models": [
            {
                "name": "modelname",
                "description": "",
                "+schema": "schemaname",
                "columns": [
                    {
                        "name": "_airbyte_ab_id",
                        "description": "",
                        "tests": ["unique", "not_null"],
                    },
                    {
                        "name": "column1",
                        "description": "",
                    },
                    {
                        "name": "column2",
                        "description": "",
                    },
                ],
            }
        ]
    }
    columns = get_columns_from_model(models, "modelname")
    assert columns == ["_airbyte_ab_id", "column1", "column2"]
    columns = get_columns_from_model(models, "modelname2")
    assert columns is None
