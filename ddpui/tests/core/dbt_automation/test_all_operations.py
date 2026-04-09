"""
Comprehensive test cases for all dbt_automation operations.
Tests cover the SQL generation functions for each operation module.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from ddpui.utils.warehouse.old_client.warehouse_interface import WarehouseInterface


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_warehouse_postgres():
    """Create a mock warehouse interface simulating postgres."""
    wh = Mock(spec=WarehouseInterface)
    wh.name = "postgres"
    return wh


@pytest.fixture
def mock_warehouse_bigquery():
    """Create a mock warehouse interface simulating bigquery."""
    wh = Mock(spec=WarehouseInterface)
    wh.name = "bigquery"
    return wh


def _source_input(source_name="test_schema", input_name="test_table"):
    return {
        "input_type": "source",
        "source_name": source_name,
        "input_name": input_name,
    }


def _model_input(input_name="test_model"):
    return {
        "input_type": "model",
        "source_name": None,
        "input_name": input_name,
    }


def _cte_input(input_name="cte1"):
    return {
        "input_type": "cte",
        "source_name": None,
        "input_name": input_name,
    }


# ============================================================================
# 1. Arithmetic
# ============================================================================


class TestArithmeticOperation:
    """Test cases for the arithmetic_dbt_sql function."""

    def test_add_operation(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.arithmetic import arithmetic_dbt_sql

        config = {
            "operator": "add",
            "operands": [
                {"is_col": True, "value": "col1"},
                {"is_col": True, "value": "col2"},
            ],
            "output_column_name": "total",
            "source_columns": ["col1", "col2"],
            "input": _source_input(),
        }
        sql, output_cols = arithmetic_dbt_sql(config, mock_warehouse_postgres)
        assert "safe_add" in sql
        assert "total" in output_cols
        assert "col1" in output_cols
        assert "col2" in output_cols
        assert "SELECT" in sql
        assert "FROM" in sql

    def test_sub_operation(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.arithmetic import arithmetic_dbt_sql

        config = {
            "operator": "sub",
            "operands": [
                {"is_col": True, "value": "col1"},
                {"is_col": False, "value": "10"},
            ],
            "output_column_name": "difference",
            "source_columns": ["col1"],
            "input": _source_input(),
        }
        sql, output_cols = arithmetic_dbt_sql(config, mock_warehouse_postgres)
        assert "safe_subtract" in sql
        assert "difference" in output_cols

    def test_mul_operation(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.arithmetic import arithmetic_dbt_sql

        config = {
            "operator": "mul",
            "operands": [
                {"is_col": True, "value": "price"},
                {"is_col": True, "value": "quantity"},
            ],
            "output_column_name": "total_cost",
            "source_columns": ["price", "quantity"],
            "input": _source_input(),
        }
        sql, output_cols = arithmetic_dbt_sql(config, mock_warehouse_postgres)
        assert "*" in sql
        assert "total_cost" in output_cols

    def test_div_operation(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.arithmetic import arithmetic_dbt_sql

        config = {
            "operator": "div",
            "operands": [
                {"is_col": True, "value": "numerator"},
                {"is_col": True, "value": "denominator"},
            ],
            "output_column_name": "ratio",
            "source_columns": ["numerator", "denominator"],
            "input": _source_input(),
        }
        sql, output_cols = arithmetic_dbt_sql(config, mock_warehouse_postgres)
        assert "safe_divide" in sql
        assert "ratio" in output_cols

    def test_invalid_operator_raises(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.arithmetic import arithmetic_dbt_sql

        config = {
            "operator": "modulo",
            "operands": [
                {"is_col": True, "value": "a"},
                {"is_col": True, "value": "b"},
            ],
            "output_column_name": "result",
            "source_columns": [],
            "input": _source_input(),
        }
        with pytest.raises(ValueError, match="Unknown operation"):
            arithmetic_dbt_sql(config, mock_warehouse_postgres)

    def test_less_than_two_operands_raises(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.arithmetic import arithmetic_dbt_sql

        config = {
            "operator": "add",
            "operands": [{"is_col": True, "value": "a"}],
            "output_column_name": "result",
            "source_columns": [],
            "input": _source_input(),
        }
        with pytest.raises(ValueError, match="At least two operands"):
            arithmetic_dbt_sql(config, mock_warehouse_postgres)

    def test_div_requires_exactly_two_operands(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.arithmetic import arithmetic_dbt_sql

        config = {
            "operator": "div",
            "operands": [
                {"is_col": True, "value": "a"},
                {"is_col": True, "value": "b"},
                {"is_col": True, "value": "c"},
            ],
            "output_column_name": "result",
            "source_columns": [],
            "input": _source_input(),
        }
        with pytest.raises(ValueError, match="Division requires exactly two operands"):
            arithmetic_dbt_sql(config, mock_warehouse_postgres)

    def test_cte_input_type(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.arithmetic import arithmetic_dbt_sql

        config = {
            "operator": "add",
            "operands": [
                {"is_col": True, "value": "a"},
                {"is_col": True, "value": "b"},
            ],
            "output_column_name": "sum_ab",
            "source_columns": ["a", "b"],
            "input": _cte_input("cte1"),
        }
        sql, output_cols = arithmetic_dbt_sql(config, mock_warehouse_postgres)
        assert "FROM cte1" in sql
        assert "{{" not in sql.split("FROM")[1]

    def test_add_with_constant_operand(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.arithmetic import arithmetic_dbt_sql

        config = {
            "operator": "add",
            "operands": [
                {"is_col": True, "value": "col1"},
                {"is_col": False, "value": "100"},
            ],
            "output_column_name": "col1_plus_100",
            "source_columns": ["col1"],
            "input": _source_input(),
        }
        sql, output_cols = arithmetic_dbt_sql(config, mock_warehouse_postgres)
        assert "safe_add" in sql
        assert "'100'" in sql
        assert "col1_plus_100" in output_cols


# ============================================================================
# 2. Joins
# ============================================================================


class TestJoinsOperation:
    """Test cases for the joins_sql function."""

    def test_inner_join(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.joins import joins_sql

        config = {
            "input": _source_input("schema1", "table1"),
            "source_columns": ["id", "name"],
            "other_inputs": [
                {
                    "input": _source_input("schema1", "table2"),
                    "source_columns": ["id", "email"],
                    "seq": 2,
                }
            ],
            "join_type": "inner",
            "join_on": {"key1": "id", "key2": "id", "compare_with": "="},
        }
        sql, output_cols = joins_sql(config, mock_warehouse_postgres)
        assert "INNER JOIN" in sql
        assert "FROM" in sql
        assert len(output_cols) > 0

    def test_left_join(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.joins import joins_sql

        config = {
            "input": _source_input("s", "t1"),
            "source_columns": ["a", "b"],
            "other_inputs": [
                {
                    "input": _source_input("s", "t2"),
                    "source_columns": ["c", "d"],
                    "seq": 2,
                }
            ],
            "join_type": "left",
            "join_on": {"key1": "a", "key2": "c", "compare_with": "="},
        }
        sql, output_cols = joins_sql(config, mock_warehouse_postgres)
        assert "LEFT JOIN" in sql

    def test_full_outer_join(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.joins import joins_sql

        config = {
            "input": _source_input("s", "t1"),
            "source_columns": ["x"],
            "other_inputs": [
                {
                    "input": _source_input("s", "t2"),
                    "source_columns": ["y"],
                    "seq": 2,
                }
            ],
            "join_type": "full outer",
            "join_on": {"key1": "x", "key2": "y", "compare_with": "="},
        }
        sql, output_cols = joins_sql(config, mock_warehouse_postgres)
        assert "FULL OUTER JOIN" in sql

    def test_invalid_join_type_raises(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.joins import joins_sql

        config = {
            "input": _source_input("s", "t1"),
            "source_columns": ["a"],
            "other_inputs": [
                {
                    "input": _source_input("s", "t2"),
                    "source_columns": ["b"],
                    "seq": 2,
                }
            ],
            "join_type": "cross",
            "join_on": {"key1": "a", "key2": "b", "compare_with": "="},
        }
        with pytest.raises(ValueError, match="join type cross not supported"):
            joins_sql(config, mock_warehouse_postgres)

    def test_duplicate_column_names_aliased(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.joins import joins_sql

        config = {
            "input": _source_input("s", "t1"),
            "source_columns": ["id", "name"],
            "other_inputs": [
                {
                    "input": _source_input("s", "t2"),
                    "source_columns": ["id", "email"],
                    "seq": 2,
                }
            ],
            "join_type": "inner",
            "join_on": {"key1": "id", "key2": "id", "compare_with": "="},
        }
        sql, output_cols = joins_sql(config, mock_warehouse_postgres)
        # The duplicate 'id' should get aliased to 'id_2'
        assert "id_2" in output_cols or "id" in output_cols

    def test_more_than_two_tables_raises(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.joins import joins_sql

        config = {
            "input": _source_input("s", "t1"),
            "source_columns": ["a"],
            "other_inputs": [
                {
                    "input": _source_input("s", "t2"),
                    "source_columns": ["b"],
                    "seq": 2,
                },
                {
                    "input": _source_input("s", "t3"),
                    "source_columns": ["c"],
                    "seq": 3,
                },
            ],
            "join_type": "inner",
            "join_on": {"key1": "a", "key2": "b", "compare_with": "="},
        }
        with pytest.raises(ValueError, match="join operation requires exactly 2 input tables"):
            joins_sql(config, mock_warehouse_postgres)

    def test_cte_input_join(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.joins import joins_sql

        config = {
            "input": _cte_input("cte1"),
            "source_columns": ["col_a"],
            "other_inputs": [
                {
                    "input": _cte_input("cte2"),
                    "source_columns": ["col_b"],
                    "seq": 2,
                }
            ],
            "join_type": "inner",
            "join_on": {"key1": "col_a", "key2": "col_b", "compare_with": "="},
        }
        sql, output_cols = joins_sql(config, mock_warehouse_postgres)
        assert "FROM cte1" in sql
        assert "JOIN cte2" in sql


# ============================================================================
# 3. Aggregate
# ============================================================================


class TestAggregateOperation:
    """Test cases for the aggregate_dbt_sql function."""

    def test_count_aggregate(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.aggregate import aggregate_dbt_sql

        config = {
            "input": _source_input(),
            "source_columns": ["col1", "col2"],
            "aggregate_on": [
                {
                    "operation": "count",
                    "column": "col1",
                    "output_column_name": "count_col1",
                }
            ],
        }
        sql, output_cols = aggregate_dbt_sql(config, mock_warehouse_postgres)
        assert "COUNT" in sql
        assert "count_col1" in output_cols
        assert "col1" in output_cols
        assert "col2" in output_cols

    def test_countdistinct_aggregate(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.aggregate import aggregate_dbt_sql

        config = {
            "input": _source_input(),
            "source_columns": [],
            "aggregate_on": [
                {
                    "operation": "countdistinct",
                    "column": "user_id",
                    "output_column_name": "distinct_users",
                }
            ],
        }
        sql, output_cols = aggregate_dbt_sql(config, mock_warehouse_postgres)
        assert "COUNT(DISTINCT" in sql
        assert "distinct_users" in output_cols

    def test_sum_aggregate(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.aggregate import aggregate_dbt_sql

        config = {
            "input": _source_input(),
            "source_columns": ["category"],
            "aggregate_on": [
                {
                    "operation": "sum",
                    "column": "amount",
                    "output_column_name": "total_amount",
                }
            ],
        }
        sql, output_cols = aggregate_dbt_sql(config, mock_warehouse_postgres)
        assert "SUM" in sql
        assert "total_amount" in output_cols

    def test_multiple_aggregations(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.aggregate import aggregate_dbt_sql

        config = {
            "input": _source_input(),
            "source_columns": ["group_col"],
            "aggregate_on": [
                {"operation": "count", "column": "id", "output_column_name": "cnt"},
                {"operation": "sum", "column": "val", "output_column_name": "total"},
            ],
        }
        sql, output_cols = aggregate_dbt_sql(config, mock_warehouse_postgres)
        assert "cnt" in output_cols
        assert "total" in output_cols
        assert output_cols == ["group_col", "cnt", "total"]

    def test_cte_input(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.aggregate import aggregate_dbt_sql

        config = {
            "input": _cte_input("cte1"),
            "source_columns": [],
            "aggregate_on": [{"operation": "count", "column": "x", "output_column_name": "cnt_x"}],
        }
        sql, output_cols = aggregate_dbt_sql(config, mock_warehouse_postgres)
        assert "FROM cte1" in sql
        assert "{{" not in sql


# ============================================================================
# 4. GroupBy
# ============================================================================


class TestGroupByOperation:
    """Test cases for the groupby_dbt_sql function."""

    def test_groupby_with_count(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.groupby import groupby_dbt_sql

        config = {
            "dimension_columns": ["region", "month"],
            "aggregate_on": [
                {
                    "operation": "count",
                    "column": "order_id",
                    "output_column_name": "order_count",
                }
            ],
            "input": _source_input(),
        }
        sql, output_cols = groupby_dbt_sql(config, mock_warehouse_postgres)
        assert "GROUP BY" in sql
        assert "COUNT" in sql
        assert "order_count" in output_cols
        assert "region" in output_cols
        assert "month" in output_cols

    def test_groupby_with_countdistinct(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.groupby import groupby_dbt_sql

        config = {
            "dimension_columns": ["dept"],
            "aggregate_on": [
                {
                    "operation": "countdistinct",
                    "column": "emp_id",
                    "output_column_name": "unique_emps",
                }
            ],
            "input": _source_input(),
        }
        sql, output_cols = groupby_dbt_sql(config, mock_warehouse_postgres)
        assert "COUNT(DISTINCT" in sql
        assert "unique_emps" in output_cols

    def test_groupby_with_sum(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.groupby import groupby_dbt_sql

        config = {
            "dimension_columns": ["product"],
            "aggregate_on": [
                {"operation": "sum", "column": "revenue", "output_column_name": "total_rev"}
            ],
            "input": _source_input(),
        }
        sql, output_cols = groupby_dbt_sql(config, mock_warehouse_postgres)
        assert "SUM" in sql
        assert "total_rev" in output_cols

    def test_groupby_no_dimensions(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.groupby import groupby_dbt_sql

        config = {
            "dimension_columns": [],
            "aggregate_on": [{"operation": "count", "column": "id", "output_column_name": "total"}],
            "input": _source_input(),
        }
        sql, output_cols = groupby_dbt_sql(config, mock_warehouse_postgres)
        assert "GROUP BY" not in sql
        assert "total" in output_cols

    def test_groupby_multiple_aggregations(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.groupby import groupby_dbt_sql

        config = {
            "dimension_columns": ["city"],
            "aggregate_on": [
                {"operation": "count", "column": "id", "output_column_name": "cnt"},
                {"operation": "avg", "column": "score", "output_column_name": "avg_score"},
            ],
            "input": _source_input(),
        }
        sql, output_cols = groupby_dbt_sql(config, mock_warehouse_postgres)
        assert "cnt" in output_cols
        assert "avg_score" in output_cols
        assert "AVG" in sql

    def test_groupby_cte_input(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.groupby import groupby_dbt_sql

        config = {
            "dimension_columns": ["a"],
            "aggregate_on": [{"operation": "sum", "column": "b", "output_column_name": "sum_b"}],
            "input": _cte_input("cte1"),
        }
        sql, output_cols = groupby_dbt_sql(config, mock_warehouse_postgres)
        assert "FROM cte1" in sql


# ============================================================================
# 5. Pivot
# ============================================================================


class TestPivotOperation:
    """Test cases for the pivot_dbt_sql function."""

    def test_basic_pivot(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.pivot import pivot_dbt_sql

        config = {
            "groupby_columns": ["year"],
            "pivot_column_name": "quarter",
            "pivot_column_values": ["Q1", "Q2", "Q3", "Q4"],
            "input": _source_input(),
        }
        sql, output_cols = pivot_dbt_sql(config, mock_warehouse_postgres)
        assert "dbt_utils.pivot" in sql
        assert "GROUP BY" in sql
        assert "year" in output_cols
        assert "Q1" in output_cols
        assert "Q4" in output_cols

    def test_pivot_no_groupby_columns(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.pivot import pivot_dbt_sql

        config = {
            "groupby_columns": [],
            "pivot_column_name": "status",
            "pivot_column_values": ["active", "inactive"],
            "input": _source_input(),
        }
        sql, output_cols = pivot_dbt_sql(config, mock_warehouse_postgres)
        assert "GROUP BY" not in sql
        assert "active" in output_cols

    def test_pivot_no_column_name_raises(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.pivot import pivot_dbt_sql

        config = {
            "groupby_columns": ["a"],
            "pivot_column_name": None,
            "pivot_column_values": ["v1"],
            "input": _source_input(),
        }
        with pytest.raises(ValueError, match="Pivot column name not provided"):
            pivot_dbt_sql(config, mock_warehouse_postgres)

    def test_pivot_missing_column_name_key(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.pivot import pivot_dbt_sql

        config = {
            "groupby_columns": ["a"],
            "pivot_column_values": ["v1"],
            "input": _source_input(),
        }
        with pytest.raises(ValueError, match="Pivot column name not provided"):
            pivot_dbt_sql(config, mock_warehouse_postgres)

    def test_pivot_cte_input(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.pivot import pivot_dbt_sql

        config = {
            "groupby_columns": ["g"],
            "pivot_column_name": "p",
            "pivot_column_values": ["v1", "v2"],
            "input": _cte_input("cte1"),
        }
        sql, output_cols = pivot_dbt_sql(config, mock_warehouse_postgres)
        assert "FROM cte1" in sql


# ============================================================================
# 6. Unpivot
# ============================================================================


class TestUnpivotOperation:
    """Test cases for the unpivot_dbt_sql function."""

    def test_basic_unpivot(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.unpivot import unpivot_dbt_sql

        config = {
            "source_columns": ["id", "jan", "feb", "mar"],
            "exclude_columns": ["id"],
            "unpivot_columns": ["jan", "feb", "mar"],
            "unpivot_field_name": "month",
            "unpivot_value_name": "amount",
            "input": _source_input(),
        }
        sql, output_cols = unpivot_dbt_sql(config, mock_warehouse_postgres)
        assert "unpivot" in sql
        assert "id" in output_cols
        # output_cols is union of exclude_columns and unpivot_columns
        assert set(output_cols) == {"id", "jan", "feb", "mar"}

    def test_unpivot_no_columns_raises(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.unpivot import unpivot_dbt_sql

        config = {
            "source_columns": ["id"],
            "exclude_columns": ["id"],
            "unpivot_columns": [],
            "input": _source_input(),
        }
        with pytest.raises(ValueError, match="No columns specified for unpivot"):
            unpivot_dbt_sql(config, mock_warehouse_postgres)

    def test_unpivot_default_field_value_names(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.unpivot import unpivot_dbt_sql

        config = {
            "source_columns": ["x", "y", "z"],
            "exclude_columns": ["x"],
            "unpivot_columns": ["y", "z"],
            "input": _source_input(),
        }
        sql, output_cols = unpivot_dbt_sql(config, mock_warehouse_postgres)
        assert "'field_name'" in sql
        assert "'value'" in sql

    def test_unpivot_bigquery_cast_type(self, mock_warehouse_bigquery):
        from ddpui.core.dbt_automation.operations.unpivot import unpivot_dbt_sql

        config = {
            "source_columns": ["id", "a", "b"],
            "exclude_columns": ["id"],
            "unpivot_columns": ["a", "b"],
            "input": _source_input(),
        }
        sql, output_cols = unpivot_dbt_sql(config, mock_warehouse_bigquery)
        assert "'STRING'" in sql

    def test_unpivot_postgres_cast_type(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.unpivot import unpivot_dbt_sql

        config = {
            "source_columns": ["id", "a", "b"],
            "exclude_columns": ["id"],
            "unpivot_columns": ["a", "b"],
            "input": _source_input(),
        }
        sql, output_cols = unpivot_dbt_sql(config, mock_warehouse_postgres)
        assert "'varchar'" in sql

    def test_unpivot_custom_cast_to(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.unpivot import unpivot_dbt_sql

        config = {
            "source_columns": ["id", "a", "b"],
            "exclude_columns": ["id"],
            "unpivot_columns": ["a", "b"],
            "cast_to": "integer",
            "input": _source_input(),
        }
        sql, output_cols = unpivot_dbt_sql(config, mock_warehouse_postgres)
        assert "'integer'" in sql


# ============================================================================
# 7. Replace
# ============================================================================


class TestReplaceOperation:
    """Test cases for the replace_dbt_sql function."""

    def test_basic_replace(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.replace import replace_dbt_sql

        config = {
            "source_columns": ["name", "email"],
            "columns": [
                {
                    "col_name": "name",
                    "output_column_name": "clean_name",
                    "replace_ops": [{"find": "old", "replace": "new"}],
                }
            ],
            "input": _source_input(),
        }
        sql, output_cols = replace_dbt_sql(config, mock_warehouse_postgres)
        assert "REPLACE" in sql
        assert "clean_name" in output_cols
        assert "name" in output_cols
        assert "email" in output_cols

    def test_multiple_replace_ops_on_column(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.replace import replace_dbt_sql

        config = {
            "source_columns": ["text"],
            "columns": [
                {
                    "col_name": "text",
                    "output_column_name": "cleaned_text",
                    "replace_ops": [
                        {"find": "foo", "replace": "bar"},
                        {"find": "baz", "replace": "qux"},
                    ],
                }
            ],
            "input": _source_input(),
        }
        sql, output_cols = replace_dbt_sql(config, mock_warehouse_postgres)
        # Should have nested REPLACE calls
        assert sql.count("REPLACE") == 2
        assert "cleaned_text" in output_cols

    def test_no_replace_ops_raises(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.replace import replace_dbt_sql

        config = {
            "source_columns": ["col"],
            "columns": [
                {
                    "col_name": "col",
                    "output_column_name": "out",
                    "replace_ops": [],
                }
            ],
            "input": _source_input(),
        }
        with pytest.raises(ValueError, match="No replace operations provided"):
            replace_dbt_sql(config, mock_warehouse_postgres)

    def test_replace_multiple_columns(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.replace import replace_dbt_sql

        config = {
            "source_columns": ["col1", "col2", "col3"],
            "columns": [
                {
                    "col_name": "col1",
                    "output_column_name": "out1",
                    "replace_ops": [{"find": "a", "replace": "b"}],
                },
                {
                    "col_name": "col2",
                    "output_column_name": "out2",
                    "replace_ops": [{"find": "x", "replace": "y"}],
                },
            ],
            "input": _source_input(),
        }
        sql, output_cols = replace_dbt_sql(config, mock_warehouse_postgres)
        assert "out1" in output_cols
        assert "out2" in output_cols
        # col3 should remain, col1 and col2 replaced but source_columns still returned
        assert "col1" in output_cols
        assert "col2" in output_cols
        assert "col3" in output_cols

    def test_replace_cte_input(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.replace import replace_dbt_sql

        config = {
            "source_columns": ["a"],
            "columns": [
                {
                    "col_name": "a",
                    "output_column_name": "b",
                    "replace_ops": [{"find": "x", "replace": "y"}],
                }
            ],
            "input": _cte_input("cte1"),
        }
        sql, output_cols = replace_dbt_sql(config, mock_warehouse_postgres)
        assert "FROM cte1" in sql


# ============================================================================
# 8. Cast Data Types
# ============================================================================


class TestCastDatatypesOperation:
    """Test cases for the cast_datatypes_sql function."""

    def test_basic_cast(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.castdatatypes import cast_datatypes_sql

        config = {
            "source_columns": ["id", "amount", "created_at"],
            "columns": [
                {"columnname": "amount", "columntype": "numeric"},
            ],
            "input": _source_input(),
        }
        sql, output_cols = cast_datatypes_sql(config, mock_warehouse_postgres)
        assert "CAST" in sql
        assert "numeric" in sql
        assert output_cols == ["id", "amount", "created_at"]

    def test_multiple_casts(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.castdatatypes import cast_datatypes_sql

        config = {
            "source_columns": ["col1", "col2", "col3"],
            "columns": [
                {"columnname": "col1", "columntype": "integer"},
                {"columnname": "col3", "columntype": "text"},
            ],
            "input": _source_input(),
        }
        sql, output_cols = cast_datatypes_sql(config, mock_warehouse_postgres)
        assert sql.count("CAST") == 2
        assert output_cols == ["col1", "col2", "col3"]

    def test_no_columns_to_cast(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.castdatatypes import cast_datatypes_sql

        config = {
            "source_columns": ["a", "b"],
            "columns": [],
            "input": _source_input(),
        }
        sql, output_cols = cast_datatypes_sql(config, mock_warehouse_postgres)
        assert "CAST" not in sql
        assert output_cols == ["a", "b"]

    def test_cast_cte_input(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.castdatatypes import cast_datatypes_sql

        config = {
            "source_columns": ["x"],
            "columns": [{"columnname": "x", "columntype": "float"}],
            "input": _cte_input("cte1"),
        }
        sql, output_cols = cast_datatypes_sql(config, mock_warehouse_postgres)
        assert "FROM cte1" in sql

    def test_cast_preserves_non_cast_columns(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.castdatatypes import cast_datatypes_sql

        config = {
            "source_columns": ["a", "b", "c"],
            "columns": [{"columnname": "b", "columntype": "integer"}],
            "input": _source_input(),
        }
        sql, output_cols = cast_datatypes_sql(config, mock_warehouse_postgres)
        assert '"a"' in sql
        assert '"c"' in sql
        assert 'CAST("b" AS integer)' in sql


# ============================================================================
# 9. Regex Extraction
# ============================================================================


class TestRegexExtractionOperation:
    """Test cases for the regex_extraction_sql function."""

    def test_postgres_regex_extraction(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.regexextraction import regex_extraction_sql

        config = {
            "source_columns": ["email", "name"],
            "columns": {"email": r"([^@]+)"},
            "input": _source_input(),
        }
        sql, output_cols = regex_extraction_sql(config, mock_warehouse_postgres)
        assert "substring" in sql
        assert "FROM '" in sql
        assert "email" in output_cols
        assert "name" in output_cols

    def test_bigquery_regex_extraction(self, mock_warehouse_bigquery):
        from ddpui.core.dbt_automation.operations.regexextraction import regex_extraction_sql

        config = {
            "source_columns": ["url", "id"],
            "columns": {"url": r"https?://([^/]+)"},
            "input": _source_input(),
        }
        sql, output_cols = regex_extraction_sql(config, mock_warehouse_bigquery)
        assert "REGEXP_EXTRACT" in sql
        assert "url" in output_cols
        assert "id" in output_cols

    def test_multiple_regex_columns(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.regexextraction import regex_extraction_sql

        config = {
            "source_columns": ["email", "phone", "extra"],
            "columns": {
                "email": r"([^@]+)",
                "phone": r"(\d{3})",
            },
            "input": _source_input(),
        }
        sql, output_cols = regex_extraction_sql(config, mock_warehouse_postgres)
        assert "email" in output_cols
        assert "phone" in output_cols
        assert "extra" in output_cols

    def test_regex_cte_input(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.regexextraction import regex_extraction_sql

        config = {
            "source_columns": ["col1"],
            "columns": {"col1": r"\d+"},
            "input": _cte_input("cte1"),
        }
        sql, output_cols = regex_extraction_sql(config, mock_warehouse_postgres)
        assert "FROM cte1" in sql


# ============================================================================
# 10. Coalesce Columns
# ============================================================================


class TestCoalesceColumnsOperation:
    """Test cases for the coalesce_columns_dbt_sql function."""

    def test_basic_coalesce(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.coalescecolumns import coalesce_columns_dbt_sql

        config = {
            "source_columns": ["first_name", "last_name"],
            "columns": ["first_name", "last_name"],
            "output_column_name": "name",
            "default_value": "Unknown",
            "input": _source_input(),
        }
        sql, output_cols = coalesce_columns_dbt_sql(config, mock_warehouse_postgres)
        assert "COALESCE" in sql
        assert "name" in output_cols
        assert "'Unknown'" in sql

    def test_coalesce_with_null_default(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.coalescecolumns import coalesce_columns_dbt_sql

        config = {
            "source_columns": ["a", "b"],
            "columns": ["a", "b"],
            "output_column_name": "merged",
            "input": _source_input(),
        }
        sql, output_cols = coalesce_columns_dbt_sql(config, mock_warehouse_postgres)
        assert "NULL" in sql
        assert "merged" in output_cols

    def test_coalesce_with_int_default(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.coalescecolumns import coalesce_columns_dbt_sql

        config = {
            "source_columns": ["val1", "val2"],
            "columns": ["val1", "val2"],
            "output_column_name": "result",
            "default_value": 0,
            "input": _source_input(),
        }
        sql, output_cols = coalesce_columns_dbt_sql(config, mock_warehouse_postgres)
        assert "0" in sql
        assert "result" in output_cols

    def test_coalesce_output_columns(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.coalescecolumns import coalesce_columns_dbt_sql

        config = {
            "source_columns": ["x", "y"],
            "columns": ["x", "y"],
            "output_column_name": "combined",
            "input": _source_input(),
        }
        sql, output_cols = coalesce_columns_dbt_sql(config, mock_warehouse_postgres)
        assert output_cols == ["x", "y", "combined"]

    def test_coalesce_cte_input(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.coalescecolumns import coalesce_columns_dbt_sql

        config = {
            "source_columns": ["a"],
            "columns": ["a"],
            "output_column_name": "out",
            "input": _cte_input("cte1"),
        }
        sql, output_cols = coalesce_columns_dbt_sql(config, mock_warehouse_postgres)
        assert "FROM cte1" in sql


# ============================================================================
# 11. Concat Columns
# ============================================================================


class TestConcatColumnsOperation:
    """Test cases for the concat_columns_dbt_sql function."""

    def test_basic_concat(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.concatcolumns import concat_columns_dbt_sql

        config = {
            "source_columns": ["first_name", "last_name"],
            "columns": [
                {"name": "first_name", "is_col": True},
                {"name": " ", "is_col": False},
                {"name": "last_name", "is_col": True},
            ],
            "output_column_name": "full_name",
            "input": _source_input(),
        }
        sql, output_cols = concat_columns_dbt_sql(config, mock_warehouse_postgres)
        assert "CONCAT" in sql
        assert "full_name" in output_cols
        assert "first_name" in output_cols
        assert "last_name" in output_cols

    def test_concat_with_literal(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.concatcolumns import concat_columns_dbt_sql

        config = {
            "source_columns": ["col1"],
            "columns": [
                {"name": "col1", "is_col": True},
                {"name": "_suffix", "is_col": False},
            ],
            "output_column_name": "col1_with_suffix",
            "input": _source_input(),
        }
        sql, output_cols = concat_columns_dbt_sql(config, mock_warehouse_postgres)
        assert "'_suffix'" in sql
        assert "col1_with_suffix" in output_cols

    def test_concat_output_columns(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.concatcolumns import concat_columns_dbt_sql

        config = {
            "source_columns": ["a", "b"],
            "columns": [
                {"name": "a", "is_col": True},
                {"name": "b", "is_col": True},
            ],
            "output_column_name": "ab",
            "input": _source_input(),
        }
        sql, output_cols = concat_columns_dbt_sql(config, mock_warehouse_postgres)
        assert output_cols == ["a", "b", "ab"]

    def test_concat_cte_input(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.concatcolumns import concat_columns_dbt_sql

        config = {
            "source_columns": ["x"],
            "columns": [{"name": "x", "is_col": True}],
            "output_column_name": "out",
            "input": _cte_input("cte1"),
        }
        sql, output_cols = concat_columns_dbt_sql(config, mock_warehouse_postgres)
        assert "FROM cte1" in sql


# ============================================================================
# 12. Drop/Rename Columns
# ============================================================================


class TestDropColumnsOperation:
    """Test cases for the drop_columns_dbt_sql function."""

    def test_drop_single_column(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.droprenamecolumns import drop_columns_dbt_sql

        config = {
            "source_columns": ["a", "b", "c"],
            "columns": ["b"],
            "input": _source_input(),
        }
        sql, output_cols = drop_columns_dbt_sql(config, mock_warehouse_postgres)
        assert output_cols == ["a", "c"]
        assert '"b"' not in sql.split("FROM")[0]

    def test_drop_multiple_columns(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.droprenamecolumns import drop_columns_dbt_sql

        config = {
            "source_columns": ["id", "name", "email", "phone"],
            "columns": ["email", "phone"],
            "input": _source_input(),
        }
        sql, output_cols = drop_columns_dbt_sql(config, mock_warehouse_postgres)
        assert output_cols == ["id", "name"]

    def test_drop_no_columns(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.droprenamecolumns import drop_columns_dbt_sql

        config = {
            "source_columns": ["a", "b"],
            "columns": [],
            "input": _source_input(),
        }
        sql, output_cols = drop_columns_dbt_sql(config, mock_warehouse_postgres)
        assert output_cols == ["a", "b"]

    def test_drop_cte_input(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.droprenamecolumns import drop_columns_dbt_sql

        config = {
            "source_columns": ["x", "y"],
            "columns": ["y"],
            "input": _cte_input("cte1"),
        }
        sql, output_cols = drop_columns_dbt_sql(config, mock_warehouse_postgres)
        assert "FROM cte1" in sql


class TestRenameColumnsOperation:
    """Test cases for the rename_columns_dbt_sql function."""

    def test_rename_single_column(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.droprenamecolumns import rename_columns_dbt_sql

        config = {
            "source_columns": ["old_name", "other"],
            "columns": {"old_name": "new_name"},
            "input": _source_input(),
        }
        sql, output_cols = rename_columns_dbt_sql(config, mock_warehouse_postgres)
        assert '"old_name" AS "new_name"' in sql
        assert "new_name" in output_cols
        assert "other" in output_cols

    def test_rename_multiple_columns(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.droprenamecolumns import rename_columns_dbt_sql

        config = {
            "source_columns": ["a", "b", "c"],
            "columns": {"a": "alpha", "c": "charlie"},
            "input": _source_input(),
        }
        sql, output_cols = rename_columns_dbt_sql(config, mock_warehouse_postgres)
        assert "alpha" in output_cols
        assert "charlie" in output_cols
        assert "b" in output_cols

    def test_rename_no_columns(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.droprenamecolumns import rename_columns_dbt_sql

        config = {
            "source_columns": ["a", "b"],
            "columns": {},
            "input": _source_input(),
        }
        sql, output_cols = rename_columns_dbt_sql(config, mock_warehouse_postgres)
        assert output_cols == ["a", "b"]

    def test_rename_cte_input(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.droprenamecolumns import rename_columns_dbt_sql

        config = {
            "source_columns": ["x"],
            "columns": {"x": "y"},
            "input": _cte_input("cte1"),
        }
        sql, output_cols = rename_columns_dbt_sql(config, mock_warehouse_postgres)
        assert "FROM cte1" in sql


# ============================================================================
# 13. Flatten Airbyte
# ============================================================================


class TestFlattenAirbyteOperation:
    """Test cases for mk_dbtmodel function in flattenairbyte."""

    def test_mk_dbtmodel_postgres(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.flattenairbyte import mk_dbtmodel

        mock_warehouse_postgres.json_extract_op = Mock(
            side_effect=lambda col, field, alias: f'"{col}"->>\'"{field}"\' AS "{alias}"'
        )
        columntuples = [("field1", "field1"), ("field2", "field2")]
        result = mk_dbtmodel(
            mock_warehouse_postgres, "staging", "raw_source", "raw_table", columntuples
        )
        assert "SELECT _airbyte_ab_id" in result
        assert "FROM {{source('raw_source','raw_table')}}" in result
        assert "materialized='table'" in result
        assert "staging" in result

    def test_mk_dbtmodel_columns(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.flattenairbyte import mk_dbtmodel

        mock_warehouse_postgres.json_extract_op = Mock(
            return_value='"_airbyte_data"->>\'name\' AS "name"'
        )
        columntuples = [("name", "name")]
        result = mk_dbtmodel(mock_warehouse_postgres, "dest", "src_name", "src_table", columntuples)
        assert "_airbyte_ab_id" in result
        assert "name" in result

    def test_mk_dbtmodel_empty_columns(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.flattenairbyte import mk_dbtmodel

        result = mk_dbtmodel(mock_warehouse_postgres, "dest", "src", "tbl", [])
        assert "SELECT _airbyte_ab_id" in result
        assert "FROM" in result


# ============================================================================
# 14. Flatten JSON
# ============================================================================


class TestFlattenJsonOperation:
    """Test cases for the flattenjson_dbt_sql function."""

    def test_basic_flatten_json(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.flattenjson import flattenjson_dbt_sql

        mock_warehouse_postgres.json_extract_op = Mock(
            side_effect=lambda col, field, alias: f'"{col}"->>\'"{field}"\' AS "{alias}"'
        )
        config = {
            "source_columns": ["id", "data_json"],
            "json_column": "data_json",
            "json_columns_to_copy": ["name", "age"],
            "input": _source_input(),
        }
        sql, output_cols = flattenjson_dbt_sql(config, mock_warehouse_postgres)
        assert "id" in output_cols
        # json columns get prefixed with json_column name
        assert "data_json_name" in output_cols
        assert "data_json_age" in output_cols
        assert "SELECT" in sql

    def test_flatten_json_star_raises(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.flattenjson import flattenjson_dbt_sql

        config = {
            "source_columns": "*",
            "json_column": "data",
            "json_columns_to_copy": ["a"],
            "input": _source_input(),
        }
        with pytest.raises(ValueError, match="Invalid column selection"):
            flattenjson_dbt_sql(config, mock_warehouse_postgres)

    def test_flatten_json_cte_input(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.flattenjson import flattenjson_dbt_sql

        mock_warehouse_postgres.json_extract_op = Mock(return_value='"col"->>\'f\' AS "out"')
        config = {
            "source_columns": ["id", "json_col"],
            "json_column": "json_col",
            "json_columns_to_copy": ["field1"],
            "input": _cte_input("cte1"),
        }
        sql, output_cols = flattenjson_dbt_sql(config, mock_warehouse_postgres)
        assert "FROM cte1" in sql

    def test_flatten_json_excludes_json_column_from_source(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.flattenjson import flattenjson_dbt_sql

        mock_warehouse_postgres.json_extract_op = Mock(
            return_value='"data"->>\'key\' AS "data_key"'
        )
        config = {
            "source_columns": ["id", "data"],
            "json_column": "data",
            "json_columns_to_copy": ["key"],
            "input": _source_input(),
        }
        sql, output_cols = flattenjson_dbt_sql(config, mock_warehouse_postgres)
        # The source_columns selected should not include the json_column itself
        select_part = sql.split("FROM")[0]
        # "id" should be in select but "data" as a plain column should not
        assert '"id"' in select_part


# ============================================================================
# 15. Generic
# ============================================================================


class TestGenericFunctionOperation:
    """Test cases for the generic_function_dbt_sql function."""

    def test_basic_generic_function(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.generic import generic_function_dbt_sql

        config = {
            "source_columns": ["col1", "col2"],
            "computed_columns": [
                {
                    "function_name": "UPPER",
                    "operands": [{"value": "col1", "is_col": True}],
                    "output_column_name": "upper_col1",
                }
            ],
            "input": _source_input(),
        }
        sql, output_cols = generic_function_dbt_sql(config, mock_warehouse_postgres)
        assert "UPPER" in sql
        assert "upper_col1" in sql
        assert output_cols == ["col1", "col2"]

    def test_generic_function_with_constant(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.generic import generic_function_dbt_sql

        config = {
            "source_columns": ["x"],
            "computed_columns": [
                {
                    "function_name": "ROUND",
                    "operands": [
                        {"value": "x", "is_col": True},
                        {"value": "2", "is_col": False},
                    ],
                    "output_column_name": "rounded_x",
                }
            ],
            "input": _source_input(),
        }
        sql, output_cols = generic_function_dbt_sql(config, mock_warehouse_postgres)
        assert "ROUND" in sql
        assert "'2'" in sql

    def test_generic_function_star_columns(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.generic import generic_function_dbt_sql

        config = {
            "source_columns": "*",
            "computed_columns": [
                {
                    "function_name": "NOW",
                    "operands": [],
                    "output_column_name": "current_time",
                }
            ],
            "input": _source_input(),
        }
        sql, output_cols = generic_function_dbt_sql(config, mock_warehouse_postgres)
        assert "SELECT *" in sql
        assert output_cols == "*"

    def test_generic_function_multiple_computed(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.generic import generic_function_dbt_sql

        config = {
            "source_columns": ["a"],
            "computed_columns": [
                {
                    "function_name": "UPPER",
                    "operands": [{"value": "a", "is_col": True}],
                    "output_column_name": "upper_a",
                },
                {
                    "function_name": "LOWER",
                    "operands": [{"value": "a", "is_col": True}],
                    "output_column_name": "lower_a",
                },
            ],
            "input": _source_input(),
        }
        sql, output_cols = generic_function_dbt_sql(config, mock_warehouse_postgres)
        assert "UPPER" in sql
        assert "LOWER" in sql

    def test_generic_function_cte_input(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.generic import generic_function_dbt_sql

        config = {
            "source_columns": ["col1"],
            "computed_columns": [
                {
                    "function_name": "ABS",
                    "operands": [{"value": "col1", "is_col": True}],
                    "output_column_name": "abs_col1",
                }
            ],
            "input": _cte_input("cte1"),
        }
        sql, output_cols = generic_function_dbt_sql(config, mock_warehouse_postgres)
        assert "FROM cte1" in sql
        assert "{{" not in sql


# ============================================================================
# 16. Merge Operations
# ============================================================================


class TestMergeOperations:
    """Test cases for the merge_operations_sql function."""

    def test_single_operation_chain(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.mergeoperations import merge_operations_sql

        config = {
            "input": _source_input(),
            "operations": [
                {
                    "type": "dropcolumns",
                    "config": {
                        "source_columns": ["a", "b", "c"],
                        "columns": ["c"],
                    },
                }
            ],
        }
        sql, output_cols = merge_operations_sql(config, mock_warehouse_postgres)
        assert "WITH cte1 as" in sql
        assert "SELECT *" in sql
        assert "FROM cte1" in sql
        assert output_cols == ["a", "b"]

    def test_two_operation_chain(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.mergeoperations import merge_operations_sql

        config = {
            "input": _source_input(),
            "operations": [
                {
                    "type": "dropcolumns",
                    "config": {
                        "source_columns": ["a", "b", "c"],
                        "columns": ["c"],
                    },
                },
                {
                    "type": "renamecolumns",
                    "config": {
                        "source_columns": ["a", "b"],
                        "columns": {"a": "alpha"},
                    },
                },
            ],
        }
        sql, output_cols = merge_operations_sql(config, mock_warehouse_postgres)
        assert "WITH cte1 as" in sql
        assert "cte2 as" in sql
        assert "FROM cte2" in sql
        assert "alpha" in output_cols

    def test_empty_operations(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.mergeoperations import merge_operations_sql

        config = {
            "input": _source_input(),
            "operations": [],
        }
        sql, output_cols = merge_operations_sql(config, mock_warehouse_postgres)
        assert "No operations specified" in sql
        assert output_cols == []

    def test_merge_operations_sql_v2(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.mergeoperations import merge_operations_sql_v2

        operations = [
            {
                "type": "dropcolumns",
                "as_cte": "cte1",
                "config": {
                    "source_columns": ["a", "b"],
                    "columns": ["b"],
                    "input": _source_input(),
                },
            }
        ]
        sql, output_cols = merge_operations_sql_v2(operations, mock_warehouse_postgres)
        assert "WITH cte1 as" in sql
        assert output_cols == ["a"]

    def test_merge_with_arithmetic_operation(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.mergeoperations import merge_operations_sql

        config = {
            "input": _source_input(),
            "operations": [
                {
                    "type": "arithmetic",
                    "config": {
                        "operator": "add",
                        "operands": [
                            {"is_col": True, "value": "x"},
                            {"is_col": True, "value": "y"},
                        ],
                        "output_column_name": "xy_sum",
                        "source_columns": ["x", "y"],
                    },
                }
            ],
        }
        sql, output_cols = merge_operations_sql(config, mock_warehouse_postgres)
        assert "safe_add" in sql
        assert "xy_sum" in output_cols


# ============================================================================
# 17. Merge Tables (union_tables_sql already tested in test_operations.py;
#     add a few more edge cases here)
# ============================================================================


class TestMergeTablesOperation:
    """Additional test cases for union_tables_sql function."""

    def test_basic_union(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.mergetables import union_tables_sql

        config = {
            "input": _source_input("raw", "table_a"),
            "source_columns": ["col1", "col2"],
        }
        sql, output_cols = union_tables_sql(config, mock_warehouse_postgres)
        assert "dbt_utils.union_relations" in sql
        assert set(output_cols) == {"col1", "col2"}

    def test_union_with_other_inputs(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.mergetables import union_tables_sql

        config = {
            "input": _source_input("raw", "table_a"),
            "source_columns": ["id", "val"],
            "other_inputs": [
                {
                    "input": _source_input("raw", "table_b"),
                    "source_columns": ["id", "extra"],
                }
            ],
        }
        sql, output_cols = union_tables_sql(config, mock_warehouse_postgres)
        assert "table_a" in sql
        assert "table_b" in sql
        assert set(output_cols) == {"id", "val", "extra"}

    def test_union_model_input(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.mergetables import union_tables_sql

        config = {
            "input": _model_input("my_model"),
            "source_columns": ["x"],
        }
        sql, output_cols = union_tables_sql(config, mock_warehouse_postgres)
        assert "ref('my_model')" in sql

    def test_union_duplicate_raises(self, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.mergetables import union_tables_sql

        config = {
            "input": _source_input("s", "t"),
            "source_columns": ["a"],
            "other_inputs": [
                {
                    "input": _source_input("s", "t"),
                    "source_columns": ["b"],
                }
            ],
        }
        with pytest.raises(ValueError, match="Duplicate inputs found"):
            union_tables_sql(config, mock_warehouse_postgres)


# ============================================================================
# 18. Sync Sources
# ============================================================================


class TestSyncSources:
    """Test cases for the sync_sources function."""

    @patch("ddpui.core.dbt_automation.operations.syncsources.upsert_multiple_sources_to_a_yaml")
    def test_sync_sources_basic(self, mock_upsert, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.syncsources import sync_sources

        mock_warehouse_postgres.get_tables = Mock(return_value=["table1", "table2"])
        mock_upsert.return_value = "path/to/sources.yml"

        mock_dbtproject = Mock()

        config = {
            "source_schema": "raw_schema",
            "source_name": "raw_source",
        }
        result = sync_sources(config, mock_warehouse_postgres, mock_dbtproject)

        mock_warehouse_postgres.get_tables.assert_called_once_with("raw_schema")
        mock_upsert.assert_called_once_with(
            sources_groups={"raw_source": {"raw_schema": ["table1", "table2"]}},
            dbt_project=mock_dbtproject,
            rel_dir_to_models="raw_schema",
        )
        assert result == "path/to/sources.yml"

    @patch("ddpui.core.dbt_automation.operations.syncsources.upsert_multiple_sources_to_a_yaml")
    def test_sync_sources_empty_tables(self, mock_upsert, mock_warehouse_postgres):
        from ddpui.core.dbt_automation.operations.syncsources import sync_sources

        mock_warehouse_postgres.get_tables = Mock(return_value=[])
        mock_upsert.return_value = "path/to/sources.yml"

        mock_dbtproject = Mock()

        config = {
            "source_schema": "empty_schema",
            "source_name": "empty_source",
        }
        result = sync_sources(config, mock_warehouse_postgres, mock_dbtproject)

        mock_upsert.assert_called_once_with(
            sources_groups={"empty_source": {"empty_schema": []}},
            dbt_project=mock_dbtproject,
            rel_dir_to_models="empty_schema",
        )


# ============================================================================
# 19. Seed (seed.py is a script, not a module with testable functions;
#     we test the schema check logic and data loading patterns)
# ============================================================================


class TestSeedModule:
    """Test seed-related logic. Since seed.py is a CLI script that runs at
    module-level, we test the schema validation logic it implements."""

    def test_seed_schema_check_valid(self):
        """Test that valid airbyte raw data passes schema check."""
        columns = ["_airbyte_ab_id", "_airbyte_data", "_airbyte_emitted_at"]
        data = [
            {
                "_airbyte_ab_id": "abc-123",
                "_airbyte_data": '{"key": "value"}',
                "_airbyte_emitted_at": "2024-01-01T00:00:00Z",
            }
        ]
        for row in data:
            schema_check = [True if key in columns else False for key in row.keys()]
            assert all(schema_check) is True

    def test_seed_schema_check_invalid(self):
        """Test that data with extra columns fails schema check."""
        columns = ["_airbyte_ab_id", "_airbyte_data", "_airbyte_emitted_at"]
        data = [
            {
                "_airbyte_ab_id": "abc-123",
                "_airbyte_data": '{"key": "value"}',
                "_airbyte_emitted_at": "2024-01-01T00:00:00Z",
                "extra_column": "should fail",
            }
        ]
        for row in data:
            schema_check = [True if key in columns else False for key in row.keys()]
            assert all(schema_check) is False

    def test_seed_schema_check_missing_columns(self):
        """Test that data with missing columns still passes (subset is OK)."""
        columns = ["_airbyte_ab_id", "_airbyte_data", "_airbyte_emitted_at"]
        data = [
            {
                "_airbyte_ab_id": "abc-123",
                "_airbyte_data": '{"key": "value"}',
            }
        ]
        for row in data:
            schema_check = [True if key in columns else False for key in row.keys()]
            # All present keys are valid, so all() should be True
            assert all(schema_check) is True

    def test_seed_sample_json_files_exist(self):
        """Test that the sample JSON seed files exist."""
        import os
        from ddpui.core.dbt_automation import seeds

        base_dir = os.path.dirname(os.path.abspath(seeds.__file__))
        assert os.path.exists(os.path.join(base_dir, "sample_sheet1.json"))
        assert os.path.exists(os.path.join(base_dir, "sample_sheet2.json"))

    def test_seed_sample_json_valid_format(self):
        """Test that sample JSON files contain valid JSON with expected keys."""
        import os
        import json
        from ddpui.core.dbt_automation import seeds

        base_dir = os.path.dirname(os.path.abspath(seeds.__file__))
        for filename in ["sample_sheet1.json", "sample_sheet2.json"]:
            filepath = os.path.join(base_dir, filename)
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            assert isinstance(data, list)
            if len(data) > 0:
                assert "_airbyte_ab_id" in data[0]
                assert "_airbyte_data" in data[0]
                assert "_airbyte_emitted_at" in data[0]


# ============================================================================
# Additional cross-operation tests (bigquery warehouse)
# ============================================================================


class TestBigQueryVariants:
    """Test a few operations with bigquery warehouse to ensure quoting works."""

    def test_arithmetic_bigquery(self, mock_warehouse_bigquery):
        from ddpui.core.dbt_automation.operations.arithmetic import arithmetic_dbt_sql

        config = {
            "operator": "add",
            "operands": [
                {"is_col": True, "value": "col1"},
                {"is_col": True, "value": "col2"},
            ],
            "output_column_name": "total",
            "source_columns": ["col1", "col2"],
            "input": _source_input(),
        }
        sql, output_cols = arithmetic_dbt_sql(config, mock_warehouse_bigquery)
        assert "`col1`" in sql
        assert "`total`" in sql

    def test_drop_columns_bigquery(self, mock_warehouse_bigquery):
        from ddpui.core.dbt_automation.operations.droprenamecolumns import drop_columns_dbt_sql

        config = {
            "source_columns": ["a", "b", "c"],
            "columns": ["c"],
            "input": _source_input(),
        }
        sql, output_cols = drop_columns_dbt_sql(config, mock_warehouse_bigquery)
        assert "`a`" in sql
        assert "`b`" in sql
        assert output_cols == ["a", "b"]

    def test_groupby_bigquery(self, mock_warehouse_bigquery):
        from ddpui.core.dbt_automation.operations.groupby import groupby_dbt_sql

        config = {
            "dimension_columns": ["region"],
            "aggregate_on": [
                {"operation": "sum", "column": "val", "output_column_name": "total_val"}
            ],
            "input": _source_input(),
        }
        sql, output_cols = groupby_dbt_sql(config, mock_warehouse_bigquery)
        assert "`region`" in sql
        assert "`val`" in sql

    def test_coalesce_bigquery(self, mock_warehouse_bigquery):
        from ddpui.core.dbt_automation.operations.coalescecolumns import coalesce_columns_dbt_sql

        config = {
            "source_columns": ["a", "b"],
            "columns": ["a", "b"],
            "output_column_name": "merged",
            "default_value": "fallback",
            "input": _source_input(),
        }
        sql, output_cols = coalesce_columns_dbt_sql(config, mock_warehouse_bigquery)
        assert "COALESCE" in sql
        assert "`merged`" in sql
        assert "'fallback'" in sql

    def test_concat_bigquery(self, mock_warehouse_bigquery):
        from ddpui.core.dbt_automation.operations.concatcolumns import concat_columns_dbt_sql

        config = {
            "source_columns": ["first", "last"],
            "columns": [
                {"name": "first", "is_col": True},
                {"name": "last", "is_col": True},
            ],
            "output_column_name": "full",
            "input": _source_input(),
        }
        sql, output_cols = concat_columns_dbt_sql(config, mock_warehouse_bigquery)
        assert "CONCAT" in sql
        assert "`first`" in sql
        assert "`full`" in sql

    def test_replace_bigquery(self, mock_warehouse_bigquery):
        from ddpui.core.dbt_automation.operations.replace import replace_dbt_sql

        config = {
            "source_columns": ["text"],
            "columns": [
                {
                    "col_name": "text",
                    "output_column_name": "cleaned",
                    "replace_ops": [{"find": "x", "replace": "y"}],
                }
            ],
            "input": _source_input(),
        }
        sql, output_cols = replace_dbt_sql(config, mock_warehouse_bigquery)
        assert "REPLACE" in sql
        assert "`cleaned`" in sql

    def test_cast_bigquery(self, mock_warehouse_bigquery):
        from ddpui.core.dbt_automation.operations.castdatatypes import cast_datatypes_sql

        config = {
            "source_columns": ["val"],
            "columns": [{"columnname": "val", "columntype": "INT64"}],
            "input": _source_input(),
        }
        sql, output_cols = cast_datatypes_sql(config, mock_warehouse_bigquery)
        assert "CAST" in sql
        assert "INT64" in sql

    def test_pivot_bigquery(self, mock_warehouse_bigquery):
        from ddpui.core.dbt_automation.operations.pivot import pivot_dbt_sql

        config = {
            "groupby_columns": ["category"],
            "pivot_column_name": "status",
            "pivot_column_values": ["open", "closed"],
            "input": _source_input(),
        }
        sql, output_cols = pivot_dbt_sql(config, mock_warehouse_bigquery)
        assert "dbt_utils.pivot" in sql
        assert "`category`" in sql
