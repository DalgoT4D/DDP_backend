"""helpers to create .yml dbt files"""


# ================================================================================
def mk_model_config(schemaname: str, modelname_: str, columnspec: list):
    """creates a model config with the given column spec"""
    columns = [
        {
            "name": "_airbyte_ab_id",
            "description": "",
            "tests": ["unique", "not_null"],
        }
    ]
    for column in columnspec:
        columns.append(
            {
                "name": column,
                "description": "",
            }
        )
    return {
        "name": modelname_,
        "description": "",
        "+schema": schemaname,
        "columns": columns,
    }


def get_columns_from_model(models: dict, modelname: str):
    """reads a models.yml, finds the modelname and returns the columns"""
    for model in models["models"]:
        if model["name"] == modelname:
            return [x["name"] for x in model["columns"]]
    return None
