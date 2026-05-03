import pytest
from ddpui.core.charts.pivot_transform import (
    classify_row,
    get_row_labels,
    rotate_to_pivot,
    format_pivot_column_header,
    is_column_total,
)

# Simulated ROLLUP output rows with ONE column dimension (pivot_col_0)
SAMPLE_ROWS_SINGLE_COL = [
    {
        "district": "Mumbai",
        "program": "Education",
        "pivot_col_0": "2026-01-01",
        "Count": 1,
        "Spend": 450,
        "_grp_district": 0,
        "_grp_program": 0,
        "_grp_pivot_col_0": 0,
    },
    {
        "district": "Mumbai",
        "program": "Education",
        "pivot_col_0": None,
        "Count": 1,
        "Spend": 450,
        "_grp_district": 0,
        "_grp_program": 0,
        "_grp_pivot_col_0": 1,
    },
    {
        "district": "Mumbai",
        "program": "Health",
        "pivot_col_0": "2026-01-01",
        "Count": 2,
        "Spend": 600,
        "_grp_district": 0,
        "_grp_program": 0,
        "_grp_pivot_col_0": 0,
    },
    {
        "district": "Mumbai",
        "program": "Health",
        "pivot_col_0": None,
        "Count": 2,
        "Spend": 600,
        "_grp_district": 0,
        "_grp_program": 0,
        "_grp_pivot_col_0": 1,
    },
    # Subtotal for Mumbai
    {
        "district": "Mumbai",
        "program": None,
        "pivot_col_0": "2026-01-01",
        "Count": 3,
        "Spend": 1050,
        "_grp_district": 0,
        "_grp_program": 1,
        "_grp_pivot_col_0": 0,
    },
    {
        "district": "Mumbai",
        "program": None,
        "pivot_col_0": None,
        "Count": 3,
        "Spend": 1050,
        "_grp_district": 0,
        "_grp_program": 1,
        "_grp_pivot_col_0": 1,
    },
    # Grand total
    {
        "district": None,
        "program": None,
        "pivot_col_0": "2026-01-01",
        "Count": 3,
        "Spend": 1050,
        "_grp_district": 1,
        "_grp_program": 1,
        "_grp_pivot_col_0": 0,
    },
    {
        "district": None,
        "program": None,
        "pivot_col_0": None,
        "Count": 3,
        "Spend": 1050,
        "_grp_district": 1,
        "_grp_program": 1,
        "_grp_pivot_col_0": 1,
    },
]

# Simulated ROLLUP output with TWO column dimensions (pivot_col_0, pivot_col_1)
SAMPLE_ROWS_MULTI_COL = [
    # Leaf cells
    {
        "district": "Mumbai",
        "pivot_col_0": "2026-01",
        "pivot_col_1": "Education",
        "Count": 5,
        "_grp_district": 0,
        "_grp_pivot_col_0": 0,
        "_grp_pivot_col_1": 0,
    },
    {
        "district": "Mumbai",
        "pivot_col_0": "2026-01",
        "pivot_col_1": "Health",
        "Count": 3,
        "_grp_district": 0,
        "_grp_pivot_col_0": 0,
        "_grp_pivot_col_1": 0,
    },
    # Column subtotal (month total across programs) — skipped in rendering
    {
        "district": "Mumbai",
        "pivot_col_0": "2026-01",
        "pivot_col_1": None,
        "Count": 8,
        "_grp_district": 0,
        "_grp_pivot_col_0": 0,
        "_grp_pivot_col_1": 1,
    },
    # Overall column total
    {
        "district": "Mumbai",
        "pivot_col_0": None,
        "pivot_col_1": None,
        "Count": 8,
        "_grp_district": 0,
        "_grp_pivot_col_0": 1,
        "_grp_pivot_col_1": 1,
    },
    # Grand total
    {
        "district": None,
        "pivot_col_0": None,
        "pivot_col_1": None,
        "Count": 8,
        "_grp_district": 1,
        "_grp_pivot_col_0": 1,
        "_grp_pivot_col_1": 1,
    },
]


class TestClassifyRow:
    def test_data_row(self):
        row = {"_grp_district": 0, "_grp_program": 0}
        assert classify_row(row, ["district", "program"]) == "data"

    def test_subtotal_row(self):
        row = {"_grp_district": 0, "_grp_program": 1}
        assert classify_row(row, ["district", "program"]) == "subtotal"

    def test_grand_total_row(self):
        row = {"_grp_district": 1, "_grp_program": 1}
        assert classify_row(row, ["district", "program"]) == "grand_total"


class TestIsColumnTotal:
    def test_single_col_total(self):
        row = {"_grp_pivot_col_0": 1}
        assert is_column_total(row, 1) is True

    def test_single_col_not_total(self):
        row = {"_grp_pivot_col_0": 0}
        assert is_column_total(row, 1) is False

    def test_multi_col_all_total(self):
        row = {"_grp_pivot_col_0": 1, "_grp_pivot_col_1": 1}
        assert is_column_total(row, 2) is True

    def test_multi_col_partial(self):
        row = {"_grp_pivot_col_0": 0, "_grp_pivot_col_1": 1}
        assert is_column_total(row, 2) is False

    def test_no_col_dims(self):
        assert is_column_total({}, 0) is False


class TestGetRowLabels:
    def test_data_row_labels(self):
        row = {"district": "Mumbai", "program": "Education", "_grp_district": 0, "_grp_program": 0}
        assert get_row_labels(row, ["district", "program"]) == ["Mumbai", "Education"]

    def test_subtotal_row_labels(self):
        row = {"district": "Mumbai", "program": None, "_grp_district": 0, "_grp_program": 1}
        assert get_row_labels(row, ["district", "program"]) == ["Mumbai"]

    def test_null_in_real_data(self):
        row = {"district": None, "program": "Education", "_grp_district": 0, "_grp_program": 0}
        assert get_row_labels(row, ["district", "program"]) == ["(No value)", "Education"]


class TestRotateToPivotSingleColumn:
    def test_basic_rotation(self):
        result = rotate_to_pivot(
            flat_rows=SAMPLE_ROWS_SINGLE_COL,
            row_dim_cols=["district", "program"],
            num_col_dims=1,
            col_dim_names=["date"],
            metric_aliases=["Count", "Spend"],
        )
        assert "column_keys" in result
        assert "column_dimension_names" in result
        assert "metric_headers" in result
        assert "rows" in result
        assert "grand_total" in result

        # Should have 2 data rows + 1 subtotal
        assert len(result["rows"]) == 3
        assert result["grand_total"] is not None
        assert result["grand_total"]["row_total"] == [3, 1050]

    def test_subtotal_row_flagged(self):
        result = rotate_to_pivot(
            flat_rows=SAMPLE_ROWS_SINGLE_COL,
            row_dim_cols=["district", "program"],
            num_col_dims=1,
            col_dim_names=["date"],
            metric_aliases=["Count", "Spend"],
        )
        subtotal_rows = [r for r in result["rows"] if r["is_subtotal"]]
        assert len(subtotal_rows) == 1
        assert subtotal_rows[0]["row_labels"] == ["Mumbai"]


class TestRotateToPivotMultiColumn:
    def test_multi_column_rotation(self):
        result = rotate_to_pivot(
            flat_rows=SAMPLE_ROWS_MULTI_COL,
            row_dim_cols=["district"],
            num_col_dims=2,
            col_dim_names=["month", "program"],
            metric_aliases=["Count"],
        )
        # Should have composite column keys
        assert len(result["column_keys"]) == 2
        assert result["column_keys"][0] == ["2026-01", "Education"]
        assert result["column_keys"][1] == ["2026-01", "Health"]
        assert result["column_dimension_names"] == ["month", "program"]

        # Data row for Mumbai
        data_rows = [r for r in result["rows"] if not r["is_subtotal"]]
        assert len(data_rows) == 1
        assert data_rows[0]["row_labels"] == ["Mumbai"]
        assert data_rows[0]["values"][0] == [5]
        assert data_rows[0]["values"][1] == [3]
        assert data_rows[0]["row_total"] == [8]

    def test_multi_column_grand_total(self):
        result = rotate_to_pivot(
            flat_rows=SAMPLE_ROWS_MULTI_COL,
            row_dim_cols=["district"],
            num_col_dims=2,
            col_dim_names=["month", "program"],
            metric_aliases=["Count"],
        )
        assert result["grand_total"] is not None
        assert result["grand_total"]["row_total"] == [8]


class TestFormatPivotColumnHeader:
    def test_month_grain(self):
        assert format_pivot_column_header("2026-01-01 00:00:00", "month") == "Jan 2026"

    def test_year_grain(self):
        assert format_pivot_column_header("2026-01-01 00:00:00", "year") == "2026"

    def test_day_grain(self):
        assert format_pivot_column_header("2026-01-15 00:00:00", "day") == "Jan 15, 2026"

    def test_no_grain_passthrough(self):
        assert format_pivot_column_header("CategoryA", None) == "CategoryA"
