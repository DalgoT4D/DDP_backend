"""
Test cases for dbt_automation operations, specifically the union_tables_sql function.
"""

import pytest
from unittest.mock import Mock

from ddpui.dbt_automation.operations.mergetables import union_tables_sql
from ddpui.dbt_automation.utils.interfaces.warehouse_interface import WarehouseInterface


class TestUnionTablesSql:
    """Test cases for union_tables_sql function"""

    @pytest.fixture
    def mock_warehouse(self):
        """Create a mock warehouse interface"""
        return Mock(spec=WarehouseInterface)

    def test_single_table_source_input(self, mock_warehouse):
        """Test union_tables_sql with a single source table"""
        config = {
            "input": {
                "input_type": "source",
                "source_name": "test_source",
                "input_name": "table1",
            },
            "source_columns": ["id", "name", "email"],
        }

        sql, output_cols = union_tables_sql(config, mock_warehouse)

        # Verify SQL structure components
        assert sql.startswith("{{ dbt_utils.union_relations(")
        assert sql.endswith(")}}")
        assert "relations=[source('test_source', 'table1')]" in sql
        assert "source_column_name=None" in sql

        # Verify all columns are included (order may vary)
        for col in ["id", "name", "email"]:
            assert f"'{col}'" in sql

        # Verify output columns (order may vary since it's from a set)
        assert set(output_cols) == {"id", "name", "email"}
        assert len(output_cols) == 3

    def test_single_table_model_input(self, mock_warehouse):
        """Test union_tables_sql with a single model reference"""
        config = {
            "input": {
                "input_type": "model",
                "source_name": "test_schema",
                "input_name": "my_model",
            },
            "source_columns": ["user_id", "created_at"],
        }

        sql, output_cols = union_tables_sql(config, mock_warehouse)

        # Verify SQL structure components
        assert sql.startswith("{{ dbt_utils.union_relations(")
        assert sql.endswith(")}}")
        assert "relations=[ref('my_model')]" in sql
        assert "source_column_name=None" in sql

        # Verify all columns are included (order may vary)
        for col in ["user_id", "created_at"]:
            assert f"'{col}'" in sql

        assert set(output_cols) == {"user_id", "created_at"}

    def test_multiple_tables_same_columns(self, mock_warehouse):
        """Test union_tables_sql with multiple tables having same columns"""
        config = {
            "input": {
                "input_type": "source",
                "source_name": "raw_data",
                "input_name": "users_2023",
            },
            "source_columns": ["id", "name", "email", "created_at"],
            "other_inputs": [
                {
                    "input": {
                        "input_type": "source",
                        "source_name": "raw_data",
                        "input_name": "users_2024",
                    },
                    "source_columns": ["id", "name", "email", "created_at"],
                }
            ],
        }

        sql, output_cols = union_tables_sql(config, mock_warehouse)

        # Verify SQL contains both tables
        assert "source('raw_data', 'users_2023')" in sql
        assert "source('raw_data', 'users_2024')" in sql

        # Verify the relations array has both tables
        assert (
            "relations=[source('raw_data', 'users_2023'),source('raw_data', 'users_2024')]" in sql
        )

        # Verify output columns
        assert set(output_cols) == {"id", "name", "email", "created_at"}

    def test_multiple_tables_different_columns(self, mock_warehouse):
        """Test union_tables_sql with multiple tables having different columns"""
        config = {
            "input": {
                "input_type": "source",
                "source_name": "analytics",
                "input_name": "events_table",
            },
            "source_columns": ["event_id", "user_id", "event_type"],
            "other_inputs": [
                {
                    "input": {
                        "input_type": "source",
                        "source_name": "analytics",
                        "input_name": "purchases_table",
                    },
                    "source_columns": ["purchase_id", "user_id", "amount"],
                },
                {
                    "input": {
                        "input_type": "model",
                        "source_name": "intermediate",
                        "input_name": "processed_data",
                    },
                    "source_columns": ["user_id", "session_id", "device_type"],
                },
            ],
        }

        sql, output_cols = union_tables_sql(config, mock_warehouse)

        # Verify SQL contains all three tables
        assert "source('analytics', 'events_table')" in sql
        assert "source('analytics', 'purchases_table')" in sql
        assert "ref('processed_data')" in sql

        # Verify all unique columns are included (union of all column sets)
        expected_columns = {
            "event_id",
            "user_id",
            "event_type",
            "purchase_id",
            "amount",
            "session_id",
            "device_type",
        }
        assert set(output_cols) == expected_columns

    def test_mixed_input_types(self, mock_warehouse):
        """Test union_tables_sql with mixed source and model inputs"""
        config = {
            "input": {
                "input_type": "model",
                "source_name": "staging",
                "input_name": "stg_users",
            },
            "source_columns": ["id", "username", "status"],
            "other_inputs": [
                {
                    "input": {
                        "input_type": "source",
                        "source_name": "raw",
                        "input_name": "legacy_users",
                    },
                    "source_columns": ["id", "username", "active_flag"],
                }
            ],
        }

        sql, output_cols = union_tables_sql(config, mock_warehouse)

        # Verify mixed references
        assert "ref('stg_users')" in sql
        assert "source('raw', 'legacy_users')" in sql

        # Verify union of columns
        expected_columns = {"id", "username", "status", "active_flag"}
        assert set(output_cols) == expected_columns

    def test_duplicate_table_references_error(self, mock_warehouse):
        """Test that duplicate table references raise ValueError"""
        config = {
            "input": {
                "input_type": "source",
                "source_name": "test",
                "input_name": "table1",
            },
            "source_columns": ["id", "name"],
            "other_inputs": [
                {
                    "input": {
                        "input_type": "source",
                        "source_name": "test",
                        "input_name": "table1",  # Same table as main input
                    },
                    "source_columns": ["id", "email"],
                }
            ],
        }

        with pytest.raises(ValueError, match="Duplicate inputs found"):
            union_tables_sql(config, mock_warehouse)

    def test_empty_columns_list(self, mock_warehouse):
        """Test union_tables_sql with empty column lists"""
        config = {
            "input": {
                "input_type": "source",
                "source_name": "test",
                "input_name": "empty_table",
            },
            "source_columns": [],
        }

        sql, output_cols = union_tables_sql(config, mock_warehouse)

        # Should still generate valid SQL with empty include list
        assert "include=[]" in sql
        assert output_cols == []

    def test_no_other_inputs(self, mock_warehouse):
        """Test union_tables_sql without other_inputs key in config"""
        config = {
            "input": {
                "input_type": "source",
                "source_name": "test",
                "input_name": "single_table",
            },
            "source_columns": ["col1", "col2"]
            # No "other_inputs" key
        }

        sql, output_cols = union_tables_sql(config, mock_warehouse)

        # Should work with just the main input
        assert "source('test', 'single_table')" in sql
        assert set(output_cols) == {"col1", "col2"}

        # Should only have one table in relations
        assert sql.count("source(") == 1

    def test_column_quoting_in_sql(self, mock_warehouse):
        """Test that column names are properly quoted in the SQL output"""
        config = {
            "input": {
                "input_type": "source",
                "source_name": "test",
                "input_name": "table1",
            },
            "source_columns": ["user_id", "first_name", "last_name"],
        }

        sql, output_cols = union_tables_sql(config, mock_warehouse)

        # Verify columns are quoted (using quote_constvalue with 'postgres')
        assert "'user_id'" in sql
        assert "'first_name'" in sql
        assert "'last_name'" in sql

    def test_special_characters_in_column_names(self, mock_warehouse):
        """Test union_tables_sql with column names containing special characters"""
        config = {
            "input": {
                "input_type": "source",
                "source_name": "test",
                "input_name": "special_table",
            },
            "source_columns": ["user-id", "email@domain", "created at", "data_field"],
        }

        sql, output_cols = union_tables_sql(config, mock_warehouse)

        # All columns should be present in output
        expected_columns = {"user-id", "email@domain", "created at", "data_field"}
        assert set(output_cols) == expected_columns

        # All columns should be quoted in SQL
        for col in expected_columns:
            assert f"'{col}'" in sql

    def test_large_number_of_tables(self, mock_warehouse):
        """Test union_tables_sql with many tables"""
        other_inputs = []
        for i in range(10):
            other_inputs.append(
                {
                    "input": {
                        "input_type": "source",
                        "source_name": "batch_data",
                        "input_name": f"batch_{i}",
                    },
                    "source_columns": ["id", "timestamp", f"metric_{i}"],
                }
            )

        config = {
            "input": {
                "input_type": "source",
                "source_name": "batch_data",
                "input_name": "batch_main",
            },
            "source_columns": ["id", "timestamp", "primary_metric"],
            "other_inputs": other_inputs,
        }

        sql, output_cols = union_tables_sql(config, mock_warehouse)

        # Should contain all tables
        assert "source('batch_data', 'batch_main')" in sql
        for i in range(10):
            assert f"source('batch_data', 'batch_{i}')" in sql

        # Should have union of all columns
        expected_cols = {"id", "timestamp", "primary_metric"}
        for i in range(10):
            expected_cols.add(f"metric_{i}")
        assert set(output_cols) == expected_cols

    def test_cte_input_type(self, mock_warehouse):
        """Test union_tables_sql with CTE input type"""
        config = {
            "input": {
                "input_type": "cte",
                "source_name": "temp_schema",  # This should be ignored for CTE
                "input_name": "my_cte",
            },
            "source_columns": ["cte_col1", "cte_col2"],
        }

        sql, output_cols = union_tables_sql(config, mock_warehouse)

        # Should use CTE name directly without source() or ref()
        assert "relations=[my_cte]" in sql
        assert set(output_cols) == {"cte_col1", "cte_col2"}

    def test_sql_structure_format(self, mock_warehouse):
        """Test that the generated SQL has the correct dbt_utils.union_relations structure"""
        config = {
            "input": {
                "input_type": "source",
                "source_name": "test",
                "input_name": "table1",
            },
            "source_columns": ["id", "name"],
        }

        sql, output_cols = union_tables_sql(config, mock_warehouse)

        # Verify exact SQL structure
        assert sql.startswith("{{ dbt_utils.union_relations(")
        assert sql.endswith(")}}")
        assert "relations=" in sql
        assert "include=" in sql
        assert "source_column_name=None" in sql

        # Verify no extra spaces or formatting issues
        assert " , " in sql  # Space before and after comma
        assert "] , " in sql  # Proper spacing around parameters

    def test_column_order_consistency(self, mock_warehouse):
        """Test that column order is deterministic for the same input"""
        config = {
            "input": {
                "input_type": "source",
                "source_name": "test",
                "input_name": "table1",
            },
            "source_columns": ["z_col", "a_col", "m_col"],
            "other_inputs": [
                {
                    "input": {
                        "input_type": "source",
                        "source_name": "test",
                        "input_name": "table2",
                    },
                    "source_columns": ["b_col", "z_col", "a_col"],
                }
            ],
        }

        # Run multiple times to check consistency
        results = []
        for _ in range(3):
            sql, output_cols = union_tables_sql(config, mock_warehouse)
            results.append((sql, tuple(sorted(output_cols))))

        # All results should be identical when sorted
        assert all(result == results[0] for result in results)

        # Verify we have all expected columns
        expected_columns = {"z_col", "a_col", "m_col", "b_col"}
        assert set(results[0][1]) == expected_columns

    def test_duplicate_columns_across_tables(self, mock_warehouse):
        """Test that duplicate columns across tables are deduplicated"""
        config = {
            "input": {
                "input_type": "source",
                "source_name": "raw",
                "input_name": "table1",
            },
            "source_columns": ["id", "name", "email"],
            "other_inputs": [
                {
                    "input": {
                        "input_type": "source",
                        "source_name": "raw",
                        "input_name": "table2",
                    },
                    "source_columns": ["id", "name", "phone"],  # id, name are common
                }
            ],
        }

        sql, output_cols = union_tables_sql(config, mock_warehouse)

        # Should contain all unique columns (duplicates removed by set)
        expected_columns = {"id", "name", "email", "phone"}
        assert set(output_cols) == expected_columns

        # Each column should appear only once in the SQL
        for col in expected_columns:
            assert sql.count(f"'{col}'") == 1

        # Verify all tables are included
        assert "source('raw', 'table1')" in sql
        assert "source('raw', 'table2')" in sql
