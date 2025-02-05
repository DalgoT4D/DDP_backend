import pytest
from dbt_automation.utils.tableutils import source_or_ref


@pytest.fixture
def source():
    return {
        "input_type": "source",
        "input_name": "src_table",
        "source_name": "src_name",
    }


@pytest.fixture
def model_ref():
    return {
        "input_type": "model",
        "input_name": "src_table",
        "source_name": "src_name",
    }


def test_correct_source(source):
    assert "input_type" in source
    assert "input_name" in source
    assert "source_name" in source
    assert source["input_type"] == "source"
    src_name = source["source_name"]
    inp_name = source["input_name"]
    assert source_or_ref(**source) == f"source('{src_name}', '{inp_name}')"


def test_correct_model(model_ref):
    assert "input_type" in model_ref
    assert "input_name" in model_ref
    assert "source_name" in model_ref
    assert model_ref["input_type"] == "model"
    inp_name = model_ref["input_name"]
    assert source_or_ref(**model_ref) == f"ref('{inp_name}')"
