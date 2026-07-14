"""
Tests for the cells[] pivot transform.

rotate_to_pivot flattens ROLLUP output into one self-describing cell per filled
(row_key, col_key) slot. Each cell carries (row_kind, col_kind) tags instead of
being routed into positional buckets. The frontend derives the grid from cells.

Also holds the functional / contract tests (TestPivotSqlAliasContract,
TestGetPivotTableDataPipeline) that lock the SQL-builder <-> transform contract and
drive the real get_pivot_table_data entry point — the unit tests above cannot catch
alias drift on their own.
"""

from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine

from ddpui.core.charts.pivot_transform import rotate_to_pivot
from ddpui.core.charts.charts_service import build_chart_query, metric_sql_alias
from ddpui.core.charts.pivot_service import get_pivot_table_data
from ddpui.schemas.chart_schemas import ChartDataPayload, ChartMetric


def _org_warehouse(wtype="postgres"):
    ow = MagicMock()
    ow.wtype = wtype
    return ow


# Simulated ROLLUP output rows with ONE column dimension (pivot_col_0 = date).
# Each district/program pair emits a leaf cell (real date) and a row_total cell
# (date rolled up, _grp_pivot_col_0=1).
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
    # Row subtotal for Mumbai (program rolled up)
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
    # Grand total (both row dims rolled up)
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

# Simulated ROLLUP output with TWO column dimensions (pivot_col_0=month, pivot_col_1=program).
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
    # Column subtotal (month total across programs) — partial column grouping
    {
        "district": "Mumbai",
        "pivot_col_0": "2026-01",
        "pivot_col_1": None,
        "Count": 8,
        "_grp_district": 0,
        "_grp_pivot_col_0": 0,
        "_grp_pivot_col_1": 1,
    },
    # Overall column total (row_total) — all column dims grouped
    {
        "district": "Mumbai",
        "pivot_col_0": None,
        "pivot_col_1": None,
        "Count": 8,
        "_grp_district": 0,
        "_grp_pivot_col_0": 1,
        "_grp_pivot_col_1": 1,
    },
    # Grand total, row_total
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


def _cells(result):
    return result["cells"]


class TestContract:
    def test_top_level_keys(self):
        result = rotate_to_pivot(
            flat_rows=SAMPLE_ROWS_SINGLE_COL,
            row_dim_cols=["district", "program"],
            num_col_dims=1,
            col_dim_names=["date"],
            metric_aliases=["Count", "Spend"],
        )
        assert set(result.keys()) == {
            "row_dimension_names",
            "column_dimension_names",
            "metric_headers",
            "column_keys",
            "column_subtotal_keys",
            "cells",
        }
        assert result["row_dimension_names"] == ["district", "program"]
        assert result["column_dimension_names"] == ["date"]
        assert result["metric_headers"] == ["Count", "Spend"]

    def test_display_names_override_headers(self):
        result = rotate_to_pivot(
            flat_rows=[{"district": "M", "Count": 1, "_grp_district": 0}],
            row_dim_cols=["district"],
            num_col_dims=0,
            col_dim_names=[],
            metric_aliases=["Count"],
            metric_display_names=["Total Count"],
        )
        assert result["metric_headers"] == ["Total Count"]

    def test_empty_input(self):
        result = rotate_to_pivot(
            flat_rows=[],
            row_dim_cols=["district"],
            num_col_dims=1,
            col_dim_names=["date"],
            metric_aliases=["Count"],
        )
        assert result["cells"] == []


class TestSingleColumn:
    def _run(self):
        return rotate_to_pivot(
            flat_rows=SAMPLE_ROWS_SINGLE_COL,
            row_dim_cols=["district", "program"],
            num_col_dims=1,
            col_dim_names=["date"],
            metric_aliases=["Count", "Spend"],
        )

    def test_leaf_cell(self):
        cells = _cells(self._run())
        leaf = [
            c for c in cells if c["row_key"] == ["Mumbai", "Education"] and c["col_kind"] == "leaf"
        ]
        assert leaf == [
            {
                "row_key": ["Mumbai", "Education"],
                "col_key": ["2026-01-01"],
                "row_kind": "data",
                "col_kind": "leaf",
                "values": [1, 450],
            }
        ]

    def test_row_total_cell(self):
        cells = _cells(self._run())
        rt = [
            c
            for c in cells
            if c["row_key"] == ["Mumbai", "Education"] and c["col_kind"] == "row_total"
        ]
        assert rt == [
            {
                "row_key": ["Mumbai", "Education"],
                "col_key": [],
                "row_kind": "data",
                "col_kind": "row_total",
                "values": [1, 450],
            }
        ]

    def test_row_subtotal_present(self):
        cells = _cells(self._run())
        sub = [c for c in cells if c["row_kind"] == "row_subtotal"]
        # Mumbai subtotal emits a leaf cell + a row_total cell
        assert {tuple(c["col_kind"] for c in sub)} == {("leaf", "row_total")}
        assert all(c["row_key"] == ["Mumbai"] for c in sub)

    def test_grand_total_row_total(self):
        cells = _cells(self._run())
        gt = [c for c in cells if c["row_kind"] == "grand_total" and c["col_kind"] == "row_total"]
        assert gt == [
            {
                "row_key": [],
                "col_key": [],
                "row_kind": "grand_total",
                "col_kind": "row_total",
                "values": [3, 1050],
            }
        ]


class TestMultiColumn:
    def _run(self, **kw):
        return rotate_to_pivot(
            flat_rows=SAMPLE_ROWS_MULTI_COL,
            row_dim_cols=["district"],
            num_col_dims=2,
            col_dim_names=["month", "program"],
            metric_aliases=["Count"],
            **kw,
        )

    def test_leaf_cells(self):
        cells = _cells(self._run(show_column_subtotals=True))
        assert any(
            c["col_key"] == ["2026-01", "Education"]
            and c["col_kind"] == "leaf"
            and c["row_key"] == ["Mumbai"]
            and c["values"] == [5]
            for c in cells
        )
        assert any(
            c["col_key"] == ["2026-01", "Health"] and c["col_kind"] == "leaf" and c["values"] == [3]
            for c in cells
        )

    def test_column_subtotal_cell(self):
        cells = _cells(self._run(show_column_subtotals=True))
        sub = [c for c in cells if c["col_kind"] == "col_subtotal"]
        assert sub == [
            {
                "row_key": ["Mumbai"],
                "col_key": ["2026-01"],
                "row_kind": "data",
                "col_kind": "col_subtotal",
                "values": [8],
            }
        ]

    def test_column_subtotal_dropped_by_default(self):
        cells = _cells(self._run())  # show_column_subtotals defaults False
        assert all(c["col_kind"] != "col_subtotal" for c in cells)

    def test_row_total_and_grand_total(self):
        cells = _cells(self._run(show_column_subtotals=True))
        assert any(
            c["col_key"] == []
            and c["col_kind"] == "row_total"
            and c["row_key"] == ["Mumbai"]
            and c["values"] == [8]
            for c in cells
        )
        assert any(
            c["row_kind"] == "grand_total" and c["col_kind"] == "row_total" and c["values"] == [8]
            for c in cells
        )


class TestRowSubtotalToggle:
    def test_dropped_when_disabled(self):
        result = rotate_to_pivot(
            flat_rows=SAMPLE_ROWS_SINGLE_COL,
            row_dim_cols=["district", "program"],
            num_col_dims=1,
            col_dim_names=["date"],
            metric_aliases=["Count", "Spend"],
            show_row_subtotals=False,
        )
        assert all(c["row_kind"] != "row_subtotal" for c in result["cells"])
        # grand total survives
        assert any(c["row_kind"] == "grand_total" for c in result["cells"])

    def test_kept_when_enabled(self):
        result = rotate_to_pivot(
            flat_rows=SAMPLE_ROWS_SINGLE_COL,
            row_dim_cols=["district", "program"],
            num_col_dims=1,
            col_dim_names=["date"],
            metric_aliases=["Count", "Spend"],
            show_row_subtotals=True,
        )
        assert any(c["row_kind"] == "row_subtotal" for c in result["cells"])


class TestNoColumnDims:
    def test_values_go_to_row_total(self):
        result = rotate_to_pivot(
            flat_rows=[{"district": "Mumbai", "Count": 5, "_grp_district": 0}],
            row_dim_cols=["district"],
            num_col_dims=0,
            col_dim_names=[],
            metric_aliases=["Count"],
        )
        assert result["cells"] == [
            {
                "row_key": ["Mumbai"],
                "col_key": [],
                "row_kind": "data",
                "col_kind": "row_total",
                "values": [5],
            }
        ]


class TestNullHandling:
    def test_null_row_dim_value_labelled(self):
        rows = [
            {
                "district": None,
                "program": "Education",
                "pivot_col_0": "2026-01",
                "Count": 1,
                "_grp_district": 0,
                "_grp_program": 0,
                "_grp_pivot_col_0": 0,
            }
        ]
        result = rotate_to_pivot(
            flat_rows=rows,
            row_dim_cols=["district", "program"],
            num_col_dims=1,
            col_dim_names=["month"],
            metric_aliases=["Count"],
        )
        leaf = [c for c in result["cells"] if c["col_kind"] == "leaf"][0]
        assert leaf["row_key"] == ["(No value)", "Education"]

    def test_null_col_dim_value_labelled(self):
        # A genuine NULL in a real (non-rolled-up) column dim value.
        rows = [
            {
                "district": "Mumbai",
                "pivot_col_0": None,
                "pivot_col_1": "Education",
                "Count": 1,
                "_grp_district": 0,
                "_grp_pivot_col_0": 0,
                "_grp_pivot_col_1": 0,
            }
        ]
        result = rotate_to_pivot(
            flat_rows=rows,
            row_dim_cols=["district"],
            num_col_dims=2,
            col_dim_names=["month", "program"],
            metric_aliases=["Count"],
        )
        leaf = [c for c in result["cells"] if c["col_kind"] == "leaf"][0]
        assert leaf["col_key"] == ["(No value)", "Education"]


class TestCellOrderPreserved:
    def test_cells_follow_input_order(self):
        # Backend no longer re-sorts; it trusts SQL ORDER BY. Cell order mirrors
        # flat_rows arrival order (leaf cells first here).
        rows = [
            {
                "district": "Mumbai",
                "pivot_col_0": f"2026-{m:02d}",
                "Count": c,
                "_grp_district": 0,
                "_grp_pivot_col_0": 0,
            }
            for m, c in [(1, 10), (2, 20), (3, 30)]
        ]
        result = rotate_to_pivot(
            flat_rows=rows,
            row_dim_cols=["district"],
            num_col_dims=1,
            col_dim_names=["month"],
            metric_aliases=["Count"],
        )
        leaf_keys = [c["col_key"] for c in result["cells"] if c["col_kind"] == "leaf"]
        assert leaf_keys == [["2026-01"], ["2026-02"], ["2026-03"]]


class TestGrandTotalGating:
    """The two grand-total toggles are honored in the transform.

    - show_row_grand_total gates the rightmost "Total" column (col_kind row_total),
      but only when column dimensions exist (without them, row_total holds the
      primary data and must always survive).
    - show_column_grand_total gates the bottom "Total" row (row_kind grand_total).
    """

    def test_row_grand_total_dropped_with_col_dims(self):
        rows = [
            {
                "district": "Mumbai",
                "pivot_col_0": "Edu",
                "Count": 5,
                "_grp_district": 0,
                "_grp_pivot_col_0": 0,
            },
            {
                "district": "Mumbai",
                "pivot_col_0": None,
                "Count": 5,
                "_grp_district": 0,
                "_grp_pivot_col_0": 1,
            },
        ]
        result = rotate_to_pivot(
            flat_rows=rows,
            row_dim_cols=["district"],
            num_col_dims=1,
            col_dim_names=["program"],
            metric_aliases=["Count"],
            show_row_grand_total=False,
        )
        kinds = [c["col_kind"] for c in result["cells"]]
        assert "row_total" not in kinds
        assert "leaf" in kinds

    def test_row_grand_total_kept_without_col_dims(self):
        # No column dims: the single value column IS the data, never a grand total.
        rows = [{"district": "Mumbai", "Count": 5, "_grp_district": 0}]
        result = rotate_to_pivot(
            flat_rows=rows,
            row_dim_cols=["district"],
            num_col_dims=0,
            col_dim_names=[],
            metric_aliases=["Count"],
            show_row_grand_total=False,
        )
        assert result["cells"][0]["col_kind"] == "row_total"
        assert result["cells"][0]["values"] == [5]

    def test_column_grand_total_dropped(self):
        rows = [
            {
                "district": "Mumbai",
                "pivot_col_0": "Edu",
                "Count": 5,
                "_grp_district": 0,
                "_grp_pivot_col_0": 0,
            },
            {
                "district": None,
                "pivot_col_0": "Edu",
                "Count": 5,
                "_grp_district": 1,
                "_grp_pivot_col_0": 0,
            },
        ]
        result = rotate_to_pivot(
            flat_rows=rows,
            row_dim_cols=["district"],
            num_col_dims=1,
            col_dim_names=["program"],
            metric_aliases=["Count"],
            show_column_grand_total=False,
        )
        assert all(c["row_kind"] != "grand_total" for c in result["cells"])

    def test_column_grand_total_kept_when_enabled(self):
        rows = [
            {
                "district": "Mumbai",
                "pivot_col_0": "Edu",
                "Count": 5,
                "_grp_district": 0,
                "_grp_pivot_col_0": 0,
            },
            {
                "district": None,
                "pivot_col_0": "Edu",
                "Count": 5,
                "_grp_district": 1,
                "_grp_pivot_col_0": 0,
            },
        ]
        result = rotate_to_pivot(
            flat_rows=rows,
            row_dim_cols=["district"],
            num_col_dims=1,
            col_dim_names=["program"],
            metric_aliases=["Count"],
            show_column_grand_total=True,
        )
        assert any(c["row_kind"] == "grand_total" for c in result["cells"])


class TestColumnOrdering:
    """The response carries a canonical, globally-sorted column axis.

    ROLLUP output is row-major and can be sparse (a row may skip a column that a
    later row has). The column order therefore cannot be derived from cell arrival
    order — the backend sorts it here by raw value so headers are always correct.
    """

    def test_sparse_leaf_columns_sorted_globally(self):
        # District A has no Feb; district B has all three months. In arrival order
        # the columns first appear as Jan, Mar (from A), then Feb (from B) — so a
        # first-appearance axis would wrongly yield [Jan, Mar, Feb].
        rows = [
            {
                "district": "A",
                "pivot_col_0": "2026-01",
                "Count": 1,
                "_grp_district": 0,
                "_grp_pivot_col_0": 0,
            },
            {
                "district": "A",
                "pivot_col_0": "2026-03",
                "Count": 2,
                "_grp_district": 0,
                "_grp_pivot_col_0": 0,
            },
            {
                "district": "B",
                "pivot_col_0": "2026-01",
                "Count": 3,
                "_grp_district": 0,
                "_grp_pivot_col_0": 0,
            },
            {
                "district": "B",
                "pivot_col_0": "2026-02",
                "Count": 4,
                "_grp_district": 0,
                "_grp_pivot_col_0": 0,
            },
            {
                "district": "B",
                "pivot_col_0": "2026-03",
                "Count": 5,
                "_grp_district": 0,
                "_grp_pivot_col_0": 0,
            },
        ]
        result = rotate_to_pivot(
            flat_rows=rows,
            row_dim_cols=["district"],
            num_col_dims=1,
            col_dim_names=["month"],
            metric_aliases=["Count"],
        )
        assert result["column_keys"] == [["2026-01"], ["2026-02"], ["2026-03"]]

    def test_column_subtotal_keys_ordered_and_gated(self):
        result = rotate_to_pivot(
            flat_rows=SAMPLE_ROWS_MULTI_COL,
            row_dim_cols=["district"],
            num_col_dims=2,
            col_dim_names=["month", "program"],
            metric_aliases=["Count"],
            show_column_subtotals=True,
        )
        assert result["column_keys"] == [["2026-01", "Education"], ["2026-01", "Health"]]
        assert result["column_subtotal_keys"] == [["2026-01"]]

    def test_column_subtotal_keys_empty_when_disabled(self):
        result = rotate_to_pivot(
            flat_rows=SAMPLE_ROWS_MULTI_COL,
            row_dim_cols=["district"],
            num_col_dims=2,
            col_dim_names=["month", "program"],
            metric_aliases=["Count"],
        )
        assert result["column_subtotal_keys"] == []

    def test_no_column_dims_empty_axis(self):
        result = rotate_to_pivot(
            flat_rows=[{"district": "M", "Count": 1, "_grp_district": 0}],
            row_dim_cols=["district"],
            num_col_dims=0,
            col_dim_names=[],
            metric_aliases=["Count"],
        )
        assert result["column_keys"] == []
        assert result["column_subtotal_keys"] == []


class TestPivotSqlAliasContract:
    """The SQL the builder emits must carry exactly the columns rotate_to_pivot reads.

    rotate_to_pivot reads, per input row:
      - _grp_<row_dim>            (row-dimension GROUPING flags)
      - pivot_col_<i>             (column-dimension values)
      - _grp_pivot_col_<i>        (column-dimension GROUPING flags)
      - metric_sql_alias(metric)  (metric values)

    If the builder renames any of these, the transform silently reads None and the
    pivot renders empty. This test fails the moment that contract drifts.
    """

    def test_all_transform_keys_present_in_sql(self):
        metrics = [
            ChartMetric(column="id", aggregation="count", alias="Beneficiaries"),
            ChartMetric(column="amount", aggregation="sum", alias="Spend"),
        ]
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="beneficiaries",
            row_dimensions=["district", "program"],
            column_dimensions=["month", "program_type"],
            metrics=metrics,
        )
        qb = build_chart_query(payload, _org_warehouse())
        compiled = str(qb.build().compile(compile_kwargs={"literal_binds": True}))

        # Row-dimension grouping flags (the keys _classify_row_kind / _row_key read).
        for row_dim in payload.row_dimensions:
            assert f"_grp_{row_dim}" in compiled

        # Column-dimension value + grouping-flag aliases (0-indexed per col dim).
        for i in range(len(payload.column_dimensions)):
            assert f"pivot_col_{i}" in compiled
            assert f"_grp_pivot_col_{i}" in compiled

        # Metric aliases the transform reads back with row.get(alias).
        for metric in metrics:
            assert metric_sql_alias(metric) in compiled


class TestGetPivotTableDataPipeline:
    """Drive the real service entry point, mocking only the warehouse round-trip."""

    def _run(self, payload, flat_rows):
        mock_client = MagicMock()
        # A real (lazy, never-connected) Postgres engine so .compile(bind=...) uses
        # the actual dialect; the DB round-trip itself is what we stub.
        mock_client.engine = create_engine("postgresql+psycopg2://u:p@localhost/db")
        mock_client.execute.return_value = flat_rows
        with patch(
            "ddpui.core.charts.pivot_service.get_warehouse_client",
            return_value=mock_client,
        ):
            return get_pivot_table_data(_org_warehouse(), payload), mock_client

    def test_single_col_pipeline_produces_expected_cells(self):
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="beneficiaries",
            row_dimensions=["district"],
            column_dimensions=["program"],
            metrics=[ChartMetric(column="id", aggregation="count", alias="Count")],
            # Enable both grand totals so the pipeline emits the row_total + grand_total
            # cells asserted below (the flags now gate these end-to-end).
            show_row_grand_total=True,
            show_column_grand_total=True,
        )
        alias = metric_sql_alias(payload.metrics[0])  # "Count"

        # Simulated ROLLUP output: Mumbai leaves (Edu/Health), Mumbai row total,
        # and the grand-total rows.
        flat_rows = [
            {
                "district": "Mumbai",
                "pivot_col_0": "Edu",
                alias: 5,
                "_grp_district": 0,
                "_grp_pivot_col_0": 0,
            },
            {
                "district": "Mumbai",
                "pivot_col_0": "Health",
                alias: 3,
                "_grp_district": 0,
                "_grp_pivot_col_0": 0,
            },
            {
                "district": "Mumbai",
                "pivot_col_0": None,
                alias: 8,
                "_grp_district": 0,
                "_grp_pivot_col_0": 1,
            },
            {
                "district": None,
                "pivot_col_0": "Edu",
                alias: 5,
                "_grp_district": 1,
                "_grp_pivot_col_0": 0,
            },
            {
                "district": None,
                "pivot_col_0": "Health",
                alias: 3,
                "_grp_district": 1,
                "_grp_pivot_col_0": 0,
            },
            {
                "district": None,
                "pivot_col_0": None,
                alias: 8,
                "_grp_district": 1,
                "_grp_pivot_col_0": 1,
            },
        ]

        result, mock_client = self._run(payload, flat_rows)

        # The warehouse was actually queried.
        assert mock_client.execute.called
        # Metadata wired through from the payload.
        assert result["row_dimension_names"] == ["district"]
        assert result["column_dimension_names"] == ["program"]
        assert result["metric_headers"] == ["Count"]

        cells = result["cells"]
        assert len(cells) == 6
        # Leaf cell for Mumbai/Edu carries the real metric value (not None -> alias ok).
        assert {
            "row_key": ["Mumbai"],
            "col_key": ["Edu"],
            "row_kind": "data",
            "col_kind": "leaf",
            "values": [5],
        } in cells
        # Row total for Mumbai across programs.
        assert {
            "row_key": ["Mumbai"],
            "col_key": [],
            "row_kind": "data",
            "col_kind": "row_total",
            "values": [8],
        } in cells
        # Overall grand total.
        assert {
            "row_key": [],
            "col_key": [],
            "row_kind": "grand_total",
            "col_kind": "row_total",
            "values": [8],
        } in cells

    def test_wrong_metric_alias_would_null_all_values(self):
        """Guard proving the alias contract matters: if the warehouse rows are keyed
        by a different alias than metric_sql_alias, every value reads None. This is
        what alias drift looks like end-to-end."""
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="beneficiaries",
            row_dimensions=["district"],
            column_dimensions=["program"],
            metrics=[ChartMetric(column="id", aggregation="count", alias="Count")],
        )
        flat_rows = [
            {
                "district": "Mumbai",
                "pivot_col_0": "Edu",
                "WRONG_ALIAS": 5,
                "_grp_district": 0,
                "_grp_pivot_col_0": 0,
            },
        ]
        result, _ = self._run(payload, flat_rows)
        leaf = [c for c in result["cells"] if c["col_kind"] == "leaf"][0]
        assert leaf["values"] == [None]  # value lost because the alias didn't match

    def test_grand_total_flags_gate_cells_end_to_end(self):
        # show_row_grand_total / show_column_grand_total default False on the payload;
        # the service must then drop the rightmost Total column and bottom Total row.
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="beneficiaries",
            row_dimensions=["district"],
            column_dimensions=["program"],
            metrics=[ChartMetric(column="id", aggregation="count", alias="Count")],
        )
        alias = metric_sql_alias(payload.metrics[0])
        flat_rows = [
            {
                "district": "Mumbai",
                "pivot_col_0": "Edu",
                alias: 5,
                "_grp_district": 0,
                "_grp_pivot_col_0": 0,
            },
            {
                "district": "Mumbai",
                "pivot_col_0": None,
                alias: 5,
                "_grp_district": 0,
                "_grp_pivot_col_0": 1,
            },
            {
                "district": None,
                "pivot_col_0": None,
                alias: 5,
                "_grp_district": 1,
                "_grp_pivot_col_0": 1,
            },
        ]
        result, _ = self._run(payload, flat_rows)
        # Only the leaf cell survives; row_total (rightmost) and grand_total (bottom) gone.
        assert [c["col_kind"] for c in result["cells"]] == ["leaf"]
        assert all(c["row_kind"] != "grand_total" for c in result["cells"])
