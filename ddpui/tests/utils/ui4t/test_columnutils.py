import pytest
from dbt_automation.utils.columnutils import (
    cleaned_column_name,
    make_cleaned_column_names,
    dedup_list,
    fmt_colname,
    quote_columnname,
)


@pytest.mark.parametrize(
    "col,expected",
    [("colname", "colname"), ("col name!", "col_name_"), ("123", "c123")],
)
def test_cleaned_column_name(col: str, expected: str):
    """test cleaned_column_name"""
    assert cleaned_column_name(col) == expected


@pytest.mark.parametrize(
    "col_list,deduped_col_list",
    [(["colname", "colname", "colname"], ["colname", "colname_b", "colname_c"])],
)
def test_dedup_list(col_list, deduped_col_list):
    """test dedupe_list"""
    assert dedup_list(col_list) == deduped_col_list


def test_fmt_colname():
    """test fmt_colname"""
    assert fmt_colname("colname", "postgres") == '"colname"'
    assert fmt_colname("colname", "bigquery") == "colname"
    with pytest.raises(ValueError):
        fmt_colname("colname", "unsupported")


def test_quote_columnname():
    """test quote_columnname"""
    assert quote_columnname("colname", "postgres") == '"colname"'
    assert quote_columnname("colname", "bigquery") == "`colname`"
    with pytest.raises(ValueError):
        quote_columnname("colname", "unsupported")


def test_make_cleaned_column_names():
    """test make_cleaned_column_names"""
    assert make_cleaned_column_names(["colname", "colname", "colname"]) == [
        "colname",
        "colname_b",
        "colname_c",
    ]
    assert make_cleaned_column_names(["colname", "col name!", "123"]) == [
        "colname",
        "col_name_",
        "c123",
    ]
    assert make_cleaned_column_names(["colname", "colname", "colname"]) == [
        "colname",
        "colname_b",
        "colname_c",
    ]
